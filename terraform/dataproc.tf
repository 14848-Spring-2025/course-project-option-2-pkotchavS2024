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
  }
}

# Configure the Confluent Provider
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

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

# Google Compute Engine VM to run the Kafka consumer container
resource "google_compute_instance" "kafka_consumer_vm" {
  name         = "kafka-consumer-vm"
  machine_type = "e2-medium"
  zone         = var.vm_zone
  tags         = ["kafka-consumer"]

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"  # Container-Optimized OS
      size  = 30
    }
  }

  network_interface {
    network = var.network
    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    gce-container-declaration = <<EOT
spec:
  containers:
    - name: kafka-consumer
      image: 'docker.io/kaushikdkrikhanu/backendserver:001'  # Replace with your Docker Hub image
      env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: '${confluent_kafka_cluster.cluster.bootstrap_endpoint}'
        - name: KAFKA_SECURITY_PROTOCOL
          value: 'SASL_SSL'
        - name: KAFKA_SASL_MECHANISM
          value: 'PLAIN'
        - name: KAFKA_SASL_USERNAME
          value: '${confluent_api_key.client_kafka_api_key.id}'
        - name: KAFKA_SASL_PASSWORD
          value: '${confluent_api_key.client_kafka_api_key.secret}'
        - name: GCS_BUCKET_NAME
          value: '${var.staging_bucket}'
        - name: PROJECT_ID
          value: '${var.project}'
        - name: REGION
          value: '${var.region}'
        - name: CLUSTER_NAME
          value: '${var.cluster_name}'
        - name: FILE_PROCESSING_TOPIC
          value: 'file-processing' 
        - name: SEARCH_REQUEST_TOPIC
          value: 'search-request'
        - name: TOPN_REQUEST_TOPIC
          value: 'topn-request'
        - name: SEARCH_RESPOND_TOPIC
          value: 'search-response'
        - name: TOPN_RESPOND_TOPIC
          value: 'topn-response'
        - name: FILE_PROCESSING_DONE_TOPIC
          value: 'file-processing-done'
      volumeMounts:
        - name: gcp-key
          mountPath: /app/key.json
          readOnly: true
  volumes:
    - name: gcp-key
      hostPath:
        path: /etc/gcp/key.json
EOT

    google-logging-enabled = "true"
    google-monitoring-enabled = "true"
  }

  # Copy the service account key to the VM
  metadata_startup_script = <<SCRIPT
mkdir -p /etc/gcp
echo '${file("./key.json")}' > /etc/gcp/key.json
chmod 400 /etc/gcp/key.json
SCRIPT

  service_account {
    email  = var.service_account
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  # Only create VM after Kafka is fully configured
  depends_on = [
    confluent_kafka_topic.topics,
    google_dataproc_cluster.dp_cluster
  ]
}

# Output the VM's external IP
output "kafka_consumer_vm_ip" {
  description = "External IP of the Kafka consumer VM"
  value       = google_compute_instance.kafka_consumer_vm.network_interface[0].access_config[0].nat_ip
}