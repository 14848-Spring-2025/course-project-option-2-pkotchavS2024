import os
import json
import base64
import tarfile
import zipfile
import time
from threading import Thread
from confluent_kafka import Consumer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kafka_consumer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("kafka-processor")

# Function to read Kafka configuration from client.properties
def read_config(file_path):
    config = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        return config
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise

# Read Kafka configuration from client.properties
kafka_config = read_config("../client.properties")

# Add consumer-specific configurations
kafka_config['group.id'] = 'file-processor'
kafka_config['auto.offset.reset'] = 'earliest'

# Directory to store processed files
OUTPUT_DIR = "processed_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Directory for temporary files
TEMP_DIR = "temp_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Dictionary to store file chunks
file_chunks = {}

def extract_archive(file_path, extract_path):
    """Extract tar.gz, tar, or zip file"""
    logger.info(f"Extracting archive: {file_path}")
    
    try:
        if file_path.endswith('.tar.gz') or file_path.endswith('.tgz'):
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(path=extract_path)
                logger.info(f"Successfully extracted tar.gz archive: {file_path}")
        elif file_path.endswith('.tar'):
            with tarfile.open(file_path, 'r') as tar:
                tar.extractall(extract_path)
                logger.info(f"Successfully extracted tar archive: {file_path}")
        elif file_path.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
                logger.info(f"Successfully extracted zip archive: {file_path}")
        else:
            logger.warning(f"Unsupported archive format: {file_path}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"Error extracting archive {file_path}: {e}")
        return False

def process_complete_file(job_id, filename, file_path):
    """Process a complete file after all chunks are received"""
    logger.info(f"Processing complete file: {filename} (Job ID: {job_id})")
    
    # Create a directory for this job
    job_dir = os.path.join(OUTPUT_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    # Create extraction directory - use the filename as a subdirectory to keep files separate
    extract_dir = os.path.join(job_dir, os.path.splitext(filename)[0])
    os.makedirs(extract_dir, exist_ok=True)
    
    # Extract archive
    if extract_archive(file_path, extract_dir):
        logger.info(f"Archive extracted to: {extract_dir}")
        
        # Count the number of text files
        txt_count = 0
        for root, _, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.txt'):
                    txt_count += 1
        
        logger.info(f"Found {txt_count} text files in extracted directory")
    else:
        logger.error(f"Failed to extract archive: {filename}")

def process_chunks(job_id, filename):
    """Process all chunks for a file"""
    chunks = file_chunks[job_id][filename]["chunks"]
    total_chunks = file_chunks[job_id][filename]["total_chunks"]
    
    # Check if we have all chunks
    if len(chunks) != total_chunks:
        logger.warning(f"Not all chunks received for {filename}. Got {len(chunks)}/{total_chunks}")
        return False
    
    # Sort chunks by index
    sorted_chunks = sorted(chunks.items(), key=lambda x: x[0])
    
    # Combine all chunks
    combined_data = ""
    for _, chunk_data in sorted_chunks:
        combined_data += chunk_data
    
    # Decode base64 data
    file_data = base64.b64decode(combined_data)
    
    # Save to temporary file
    temp_file_path = os.path.join(TEMP_DIR, f"{job_id}_{filename}")
    with open(temp_file_path, "wb") as f:
        f.write(file_data)
    
    # Process the file
    process_complete_file(job_id, filename, temp_file_path)
    
    # Clean up
    file_chunks[job_id][filename]["processed"] = True
    logger.info(f"Completed processing {filename} for job {job_id}")
    
    return True

def kafka_consumer_loop():
    """Main Kafka consumer loop"""
    consumer = Consumer(kafka_config)
    consumer.subscribe(['file-processing'])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                # Parse message
                data = json.loads(msg.value().decode('utf-8'))
                
                job_id = data.get('jobId')
                filename = data.get('filename')
                chunk_index = data.get('chunkIndex')
                total_chunks = data.get('totalChunks')
                chunk = data.get('chunk')
                
                if not all([job_id, filename, chunk_index is not None, total_chunks, chunk]):
                    logger.warning(f"Invalid message format: {data}")
                    continue
                
                logger.info(f"Received chunk {chunk_index+1}/{total_chunks} for {filename} (Job ID: {job_id})")
                
                # Initialize data structures if needed
                if job_id not in file_chunks:
                    file_chunks[job_id] = {}
                
                if filename not in file_chunks[job_id]:
                    file_chunks[job_id][filename] = {
                        "total_chunks": total_chunks,
                        "chunks": {},
                        "processed": False,
                        "last_update": time.time()
                    }
                
                # Store this chunk
                file_chunks[job_id][filename]["chunks"][chunk_index] = chunk
                file_chunks[job_id][filename]["last_update"] = time.time()
                
                # If we have all chunks, process the file
                if len(file_chunks[job_id][filename]["chunks"]) == total_chunks:
                    logger.info(f"All chunks received for {filename}. Processing...")
                    process_chunks(job_id, filename)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("Interrupted. Closing consumer.")
    
    finally:
        consumer.close()

def cleanup_thread():
    """Periodically clean up processed files and check for incomplete transfers"""
    while True:
        try:
            current_time = time.time()
            jobs_to_remove = []
            
            # Check each job
            for job_id, files in file_chunks.items():
                all_processed = True
                for filename, file_info in files.items():
                    # If the file hasn't been processed and it's been more than 10 minutes
                    if not file_info["processed"] and current_time - file_info.get("last_update", current_time) > 600:
                        logger.warning(f"File transfer incomplete for {filename} (Job ID: {job_id}). Attempting to process anyway.")
                        if process_chunks(job_id, filename):
                            file_info["processed"] = True
                    
                    all_processed = all_processed and file_info["processed"]
                
                # If all files in this job are processed, mark for removal
                if all_processed:
                    jobs_to_remove.append(job_id)
            
            # Remove completed jobs
            for job_id in jobs_to_remove:
                logger.info(f"Cleaning up job {job_id}")
                file_chunks.pop(job_id, None)
                
                # Delete temporary files
                for file in os.listdir(TEMP_DIR):
                    if file.startswith(f"{job_id}_"):
                        try:
                            os.remove(os.path.join(TEMP_DIR, file))
                        except Exception as e:
                            logger.error(f"Error removing file {file}: {e}")
            
        except Exception as e:
            logger.error(f"Error in cleanup thread: {e}", exc_info=True)
        
        # Sleep for 5 minutes
        time.sleep(300)

if __name__ == "__main__":
    # Log the Kafka configuration (without sensitive info)
    safe_config = kafka_config.copy()
    if 'sasl.password' in safe_config:
        safe_config['sasl.password'] = '******'
    logger.info(f"Starting with Kafka config: {safe_config}")
    
    # Start cleanup thread
    cleanup_thread = Thread(target=cleanup_thread, daemon=True)
    cleanup_thread.start()
    
    # Start Kafka consumer
    logger.info("Starting Kafka consumer loop")
    kafka_consumer_loop()
