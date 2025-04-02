
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