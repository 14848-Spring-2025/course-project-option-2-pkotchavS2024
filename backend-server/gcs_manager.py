import os
from google.cloud import storage
import logging

class GCSManager:
    def __init__(self, bucket_name, credentials_path=None):
        """
        Initialize Google Cloud Storage manager.
        
        Args:
            bucket_name: Name of the GCS bucket to use
            credentials_path: Path to the service account credentials JSON file
        """
        self.bucket_name = bucket_name
        self.logger = logging.getLogger("gcs-manager")
        
        # Set up credentials if provided
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            self.logger.info(f"Using credentials from {credentials_path}")
        
        try:
            # Initialize storage client
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
            
            if not self.bucket.exists():
                self.logger.warning(f"Bucket {bucket_name} does not exist. Creating it.")
                self.bucket = self.client.create_bucket(bucket_name)
                
            self.logger.info(f"Successfully connected to GCS bucket: {bucket_name}")
        except Exception as e:
            self.logger.error(f"Error initializing GCS client: {e}")
            raise
    
    def upload_folder(self, local_folder, gcs_folder_prefix="", delete_after_upload=False):
        """
        Upload an entire folder to GCS.
        
        Args:
            local_folder: Path to local folder to upload
            gcs_folder_prefix: Prefix to use in GCS (like a folder path)
            delete_after_upload: Whether to delete local files after successful upload
            
        Returns:
            List of uploaded file paths
        """
        if not os.path.exists(local_folder):
            self.logger.error(f"Local folder does not exist: {local_folder}")
            return []
            
        uploaded_files = []
        
        try:
            for root, _, files in os.walk(local_folder):
                for file in files:
                    local_path = os.path.join(root, file)
                    
                    # Create relative path from the base directory
                    rel_path = os.path.relpath(local_path, local_folder)
                    
                    # Construct GCS path
                    if gcs_folder_prefix:
                        gcs_path = f"{gcs_folder_prefix}/{rel_path}"
                    else:
                        gcs_path = rel_path
                    
                    # Upload the file
                    success = self.upload_file(local_path, gcs_path)
                    
                    if success:
                        uploaded_files.append(local_path)
                        
                        # Delete if requested and upload was successful
                        if delete_after_upload:
                            try:
                                os.remove(local_path)
                                self.logger.info(f"Deleted local file: {local_path}")
                            except Exception as e:
                                self.logger.error(f"Error deleting file {local_path}: {e}")
            
            # If requested to delete and all files were uploaded, remove the directory structure
            if delete_after_upload and uploaded_files:
                try:
                    # Remove empty directories
                    for root, dirs, _ in os.walk(local_folder, topdown=False):
                        for dir in dirs:
                            dir_path = os.path.join(root, dir)
                            if not os.listdir(dir_path):  # Check if directory is empty
                                os.rmdir(dir_path)
                                self.logger.info(f"Removed empty directory: {dir_path}")
                    
                    # Try to remove base directory if empty
                    if not os.listdir(local_folder):
                        os.rmdir(local_folder)
                        self.logger.info(f"Removed empty base directory: {local_folder}")
                except Exception as e:
                    self.logger.error(f"Error cleaning up directories: {e}")
                    
            return uploaded_files
            
        except Exception as e:
            self.logger.error(f"Error uploading folder {local_folder}: {e}")
            return uploaded_files
    
    def upload_file(self, local_path, gcs_path):
        """
        Upload a single file to GCS.
        
        Args:
            local_path: Path to local file
            gcs_path: Destination path in GCS
            
        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            self.logger.info(f"Uploaded {local_path} to gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading file {local_path}: {e}")
            return False
    
    def download_folder(self, gcs_folder_prefix, local_folder, delete_existing=False):
        """
        Download a folder from GCS.
        
        Args:
            gcs_folder_prefix: Prefix (folder) in GCS to download
            local_folder: Local folder to save files to
            delete_existing: Whether to delete existing local folder before download
            
        Returns:
            List of downloaded file paths
        """
        downloaded_files = []
        
        # Delete existing folder if requested
        if delete_existing and os.path.exists(local_folder):
            try:
                import shutil
                shutil.rmtree(local_folder)
                self.logger.info(f"Deleted existing folder: {local_folder}")
            except Exception as e:
                self.logger.error(f"Error deleting folder {local_folder}: {e}")
                return []
        
        # Create the directory if it doesn't exist
        os.makedirs(local_folder, exist_ok=True)
        
        # Normalize the GCS path to use forward slashes
        gcs_folder_prefix = gcs_folder_prefix.replace('\\', '/')
        
        try:
            # List all blobs with the given prefix
            blobs = self.client.list_blobs(self.bucket_name, prefix=gcs_folder_prefix)
            for blob in blobs:
                # Skip directory objects (they end with a slash)
                if blob.name.endswith('/'):
                    continue
                    
                # Get relative path from the prefix
                rel_path = blob.name
                if gcs_folder_prefix and blob.name.startswith(gcs_folder_prefix):
                    rel_path = blob.name[len(gcs_folder_prefix):].lstrip('/')
                
                # Skip if rel_path is empty (this is the directory itself)
                if not rel_path:
                    continue
                    
                # Construct local file path with OS-appropriate separators
                local_path = os.path.join(local_folder, rel_path.replace('/', os.path.sep))
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Download the file
                blob.download_to_filename(local_path)
                downloaded_files.append(local_path)
                self.logger.info(f"Downloaded gs://{self.bucket_name}/{blob.name} to {local_path}")
            
            self.logger.info(f"Downloaded {len(downloaded_files)} files to {local_folder}")
            return downloaded_files
            
        except Exception as e:
            self.logger.error(f"Error downloading folder {gcs_folder_prefix}: {e}")
            return downloaded_files
    def download_file(self, gcs_path, local_path):
        """
        Download a single file from GCS.
        
        Args:
            gcs_path: Path to file in GCS
            local_path: Local path to save file to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            blob = self.bucket.blob(gcs_path)
            blob.download_to_filename(local_path)
            self.logger.info(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading file {gcs_path}: {e}")
            return False
    
    def delete_folder(self, gcs_folder_prefix):
        """
        Delete all objects in a GCS folder.
        
        Args:
            gcs_folder_prefix: Prefix (folder) in GCS to delete
            
        Returns:
            Number of deleted objects
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=gcs_folder_prefix)
            count = 0
            
            for blob in blobs:
                blob.delete()
                count += 1
                self.logger.info(f"Deleted gs://{self.bucket_name}/{blob.name}")
            
            self.logger.info(f"Deleted {count} objects with prefix {gcs_folder_prefix}")
            return count
        except Exception as e:
            self.logger.error(f"Error deleting folder {gcs_folder_prefix}: {e}")
            return 0