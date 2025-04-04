# Add GKE provider to the required providers block
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "1.51.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "0.9.1"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
}

# Configure the Confluent Provider
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# Configure the Google Provider
# Configure the Kubernetes Provider after GKE cluster is created
provider "kubernetes" {
  host                   = "https://${google_container_cluster.kafka_consumer_cluster.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.kafka_consumer_cluster.master_auth[0].cluster_ca_certificate)
}

# Get access token for Kubernetes provider
data "google_client_config" "default" {}

resource "google_dataproc_cluster" "dp_cluster" {
  name     = var.cluster_name
  region   = var.region
  labels   = var.labels

  cluster_config {
    staging_bucket = var.staging_bucket

    software_config {
      image_version       = var.cluster_version
      optional_components = ["JUPYTER", "DOCKER"] # Include Jupyter and Docker components
    }

    gce_cluster_config {
      network         = var.network
      service_account = var.service_account
      tags            = [var.cluster_name]
      zone            = var.zone
    }

    master_config {
      num_instances = var.master_ha ? 3 : 1
      machine_type  = var.master_instance_type

      disk_config {
        boot_disk_type   = var.master_disk_type
        boot_disk_size_gb = var.master_disk_size
        num_local_ssds   = var.master_local_ssd
      }
    }

    worker_config {
      machine_type = var.worker_instance_type

      disk_config {
        boot_disk_type   = var.worker_disk_type
        boot_disk_size_gb = var.worker_disk_size
        num_local_ssds   = var.worker_local_ssd
      }
    }

    preemptible_worker_config {
      num_instances = var.preemptible_worker_min_instances
    }

    # autoscaling_config {
    #   policy_uri = google_dataproc_autoscaling_policy.asp.name
    # }
  }
}

output "master_node_ip" {
  value = google_dataproc_cluster.dp_cluster.cluster_config[0].master_config[0].instance_names[0]
}

# Create a Confluent Environment
resource "confluent_environment" "environment" {
  display_name = var.environment_name
}

# Create a Kafka cluster
resource "confluent_kafka_cluster" "cluster" {
  display_name = var.kafka_cluster_name
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.kafka_region
  basic {}

  environment {
    id = confluent_environment.environment.id
  }
}

# Create a service account (Kafka client)
resource "confluent_service_account" "client" {
  display_name = var.service_account_name
  description  = "Service account for Kafka client applications"
}

# Create a Role binding for the client
resource "confluent_role_binding" "client_kafka_cluster_admin" {
  principal   = "User:${confluent_service_account.client.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.cluster.rbac_crn
}

# Create an API key for the service account
resource "confluent_api_key" "client_kafka_api_key" {
  display_name = "kafka-client-api-key"
  description  = "Kafka API Key for the Kafka client service account"
  owner {
    id          = confluent_service_account.client.id
    api_version = confluent_service_account.client.api_version
    kind        = confluent_service_account.client.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.cluster.id
    api_version = confluent_kafka_cluster.cluster.api_version
    kind        = confluent_kafka_cluster.cluster.kind
    environment {
      id = confluent_environment.environment.id
    }
  }
}

# Wait for API key and role binding to propagate
resource "time_sleep" "wait_for_permissions" {
  depends_on = [
    confluent_api_key.client_kafka_api_key,
    confluent_role_binding.client_kafka_cluster_admin
  ]
  create_duration = "15s"
}

# Create ACLs to allow the service account to manage topics
resource "confluent_kafka_acl" "client_acls" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.client.id}"
  host          = "*"
  operation     = "ALL"
  permission    = "ALLOW"
  
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.client_kafka_api_key.id
    secret = confluent_api_key.client_kafka_api_key.secret
  }
  
  depends_on = [time_sleep.wait_for_permissions]
}

# Create Kafka topics
resource "confluent_kafka_topic" "topics" {
  count = length(var.topic_names)
  
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  
  topic_name         = var.topic_names[count.index]
  partitions_count   = 6
  rest_endpoint      = confluent_kafka_cluster.cluster.rest_endpoint
  
  config = {
    "cleanup.policy"    = "delete"
    "retention.ms"      = "604800000"  # 7 days
    "min.insync.replicas" = "1"
  }
  
  credentials {
    key    = confluent_api_key.client_kafka_api_key.id
    secret = confluent_api_key.client_kafka_api_key.secret
  }
  
  depends_on = [confluent_kafka_acl.client_acls]
}

# Create a GKE cluster instead of GCE VM
resource "google_container_cluster" "kafka_consumer_cluster" {
  name     = "kafka-consumer-cluster"
  location = var.vm_zone
  
  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1

  networking_mode = "VPC_NATIVE"
  network         = var.network
  
  # IP allocation policy for VPC-native cluster
  ip_allocation_policy {
    # Let GKE choose the IP ranges
  }

  # Enable Workload Identity if you want to use it
  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }
}

# Create a node pool for the GKE cluster
resource "google_container_node_pool" "kafka_consumer_nodes" {
  name       = "kafka-consumer-node-pool"
  location   = var.vm_zone
  cluster    = google_container_cluster.kafka_consumer_cluster.name
  node_count = 2  # Adjust based on your needs
  
  node_config {
    preemptible  = false
    machine_type = "e2-standard-2"  # Adjust based on your needs
    
    # Google recommends custom service accounts with minimal permissions
    service_account = var.service_account
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    # Enable workload identity on the node pool
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# Create Kubernetes Secret for Kafka credentials
resource "kubernetes_secret" "kafka_credentials" {
  metadata {
    name = "kafka-credentials"
  }

  data = {
    "kafka-api-key"    = confluent_api_key.client_kafka_api_key.id
    "kafka-api-secret" = confluent_api_key.client_kafka_api_key.secret
  }

  depends_on = [
    google_container_node_pool.kafka_consumer_nodes
  ]
}

# Create Kubernetes Secret for GCP service account key
resource "kubernetes_secret" "gcp_key" {
  metadata {
    name = "gcp-key"
  }

  data = {
    "key.json" = file("./key.json")
  }

  depends_on = [
    google_container_node_pool.kafka_consumer_nodes
  ]
}

# Create Kubernetes Deployment for Kafka consumer
resource "kubernetes_deployment" "kafka-consumer-i" {
  metadata {
    name = "kafka-consumer-i"
    labels = {
      app = "kafka-consumer-i"
    }
  }

  spec {
    replicas = 1  # Adjust based on your needs

    selector {
      match_labels = {
        app = "kafka-consumer-i"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka-consumer-i"
        }
      }

      spec {
        container {
          image = "docker.io/kaushikdkrikhanu/backendserver:010"  # Replace with your Docker Hub image
          name  = "kafka-consumer-i"

          env {
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = confluent_kafka_cluster.cluster.bootstrap_endpoint
          }
          
          env {
            name  = "KAFKA_SECURITY_PROTOCOL"
            value = "SASL_SSL"
          }
          
          env {
            name  = "KAFKA_SASL_MECHANISM"
            value = "PLAIN"
          }
          
          env {
            name  = "KAFKA_SASL_USERNAME"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.kafka_credentials.metadata[0].name
                key  = "kafka-api-key"
              }
            }
          }
          
          env {
            name  = "KAFKA_SASL_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.kafka_credentials.metadata[0].name
                key  = "kafka-api-secret"
              }
            }
          }
          
          env {
            name  = "GCS_BUCKET_NAME"
            value = var.staging_bucket
          }
          
          env {
            name  = "PROJECT_ID"
            value = var.project
          }
          
          env {
            name  = "REGION"
            value = var.region
          }
          
          env {
            name  = "CLUSTER_NAME"
            value = var.cluster_name
          }
          
          env {
            name  = "FILE_PROCESSING_TOPIC"
            value = "file-processing"
          }
          
          env {
            name  = "SEARCH_REQUEST_TOPIC"
            value = "search-request"
          }
          
          env {
            name  = "TOPN_REQUEST_TOPIC"
            value = "topn-request"
          }
          
          env {
            name  = "SEARCH_RESPOND_TOPIC"
            value = "search-response"
          }
          
          env {
            name  = "TOPN_RESPOND_TOPIC"
            value = "topn-response"
          }
          
          env {
            name  = "FILE_PROCESSING_DONE_TOPIC"
            value = "file-processing-done"
          }
          
          volume_mount {
            name       = "gcp-key"
            mount_path = "/app/key.json"
            sub_path   = "key.json"
            read_only  = true
          }
          
          resources {
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
            requests = {
              cpu    = "250m"
              memory = "256Mi"
            }
          }
        }
        
        volume {
          name = "gcp-key"
          secret {
            secret_name = kubernetes_secret.gcp_key.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [
    confluent_kafka_topic.topics,
    google_dataproc_cluster.dp_cluster,
    kubernetes_secret.kafka_credentials,
    kubernetes_secret.gcp_key
  ]
}

# Output important information
output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = confluent_kafka_cluster.cluster.bootstrap_endpoint
}

output "service_account_id" {
  description = "Service Account ID"
  value       = confluent_service_account.client.id
}

output "kafka_api_key" {
  description = "Kafka API Key"
  value       = confluent_api_key.client_kafka_api_key.id
  sensitive   = true
}

output "kafka_api_secret" {
  description = "Kafka API Secret"
  value       = confluent_api_key.client_kafka_api_key.secret
  sensitive   = true
}

output "created_topics" {
  description = "List of created Kafka topics"
  value       = var.topic_names
}

output "gke_cluster_name" {
  description = "Name of the GKE cluster"
  value       = google_container_cluster.kafka_consumer_cluster.name
}

output "gke_cluster_endpoint" {
  description = "Endpoint of the GKE cluster"
  value       = google_container_cluster.kafka_consumer_cluster.endpoint
}

output "kubectl_connection_command" {
  description = "Command to configure kubectl"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.kafka_consumer_cluster.name} --zone ${var.vm_zone} --project ${var.project}"
}