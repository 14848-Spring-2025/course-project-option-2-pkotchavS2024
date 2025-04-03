from google.cloud import dataproc_v1
from google.cloud import storage
import os
import time
def submit_job(project_id, region, cluster_name, bucket_name):
    """Submit a Hadoop streaming job for inverted indexing to a Dataproc cluster."""
    
    # Create the job client
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # Configure the job with the correct path to the streaming jar
    job = {
        "placement": {
            "cluster_name": cluster_name
        },
        "hadoop_job": {
            "main_jar_file_uri": "file:///usr/lib/hadoop/hadoop-streaming-3.2.4.jar",
            "args": [
                "-files", f"gs://{bucket_name}/mapper.py,gs://{bucket_name}/reducer.py",
                "-mapper", "python3 mapper.py",
                "-reducer", "python3 reducer.py",
                "-input", f"gs://{bucket_name}/processed_files/*/*.txt",
                "-output", f"gs://{bucket_name}/search_index_output"  # Adds timestamp to make it unique
            ]
        }
    }
    
    # Submit the job
    operation = job_client.submit_job_as_operation(
        request={
            "project_id": project_id,
            "region": region,
            "job": job
        }
    )
    
    print(f"Submitted job operation")
    
    try:
        # Wait for the job to complete
        response = operation.result()
        
        # Extract job ID from response
        job_id = response.reference.job_id
        
        # Get the job status
        job_status = job_client.get_job(
            request={
                "project_id": project_id,
                "region": region,
                "job_id": job_id
            }
        )
        
        print(f"Job completed with status: {job_status.status.state.name}")
        
        return job_id
    except Exception as e:
        print(f"Error waiting for job completion: {e}")
        # Try to get the job ID from the operation if possible
        if hasattr(operation, 'metadata') and hasattr(operation.metadata, 'job'):
            return operation.metadata.job.reference.job_id
        return None

