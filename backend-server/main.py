import os
import json
import base64
import tarfile
import zipfile
import time
import shutil
import heapq
import re
from threading import Thread
from confluent_kafka import Consumer, Producer
from google.cloud import dataproc_v1
from collections import defaultdict
import logging
from gcs_manager import GCSManager  # Import the GCS Manager
from submit_job import submit_job

# Configuration variables from environment
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_SECURITY_PROTOCOL = os.environ.get('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME', '')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD', '')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'file-processor')
KAFKA_AUTO_OFFSET_RESET = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# GCS configuration
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', "emerald-skill-436000-t2_cloudbuild")
GCS_CREDENTIALS_PATH = os.environ.get('GCS_CREDENTIALS_PATH', "./key.json")
GCS_PROCESSED_FILES_PREFIX = os.environ.get('GCS_PROCESSED_FILES_PREFIX', "processed_files")
GCS_INDEX_PREFIX = os.environ.get('GCS_INDEX_PREFIX', "search_index_output")
PROJECT_ID = os.environ.get('PROJECT_ID', "emerald-skill-436000-t2")
REGION = os.environ.get('REGION', "us-central1")
CLUSTER_NAME = os.environ.get('CLUSTER_NAME', "my-dataproc-cluster")

# Kafka topics
FILE_PROCESSING_TOPIC = os.environ.get('FILE_PROCESSING_TOPIC', 'file-processing')
SEARCH_REQUEST_TOPIC = os.environ.get('SEARCH_REQUEST_TOPIC', 'search-request')
TOPN_REQUEST_TOPIC = os.environ.get('TOPN_REQUEST_TOPIC', 'topn-request')
SEARCH_RESPOND_TOPIC = os.environ.get('SEARCH_RESPOND_TOPIC', 'search-response')
TOPN_RESPOND_TOPIC = os.environ.get('TOPN_RESPOND_TOPIC', 'topn-response')
FILE_PROCESSING_DONE_TOPIC = os.environ.get('FILE_PROCESSING_DONE_TOPIC', 'file-processing-done')

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
search_engine = None

# Create Kafka config from environment variables instead of reading from file
def get_kafka_config():
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET
    }
    
    # Add security configuration if using SASL
    if KAFKA_SECURITY_PROTOCOL != 'PLAINTEXT':
        config.update({
            'security.protocol': KAFKA_SECURITY_PROTOCOL,
            'sasl.mechanism': KAFKA_SASL_MECHANISM,
            'sasl.username': KAFKA_SASL_USERNAME,
            'sasl.password': KAFKA_SASL_PASSWORD
        })
    
    return config

# Directory to store processed files
OUTPUT_DIR = "processed_files"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Directory for temporary files
TEMP_DIR = "temp_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Directory for search index
INDEX_DIR = "search_index_output"
os.makedirs(INDEX_DIR, exist_ok=True)

# Dictionary to store file chunks
file_chunks = {}

# Initialize GCS Manager
try:
    gcs_manager = GCSManager(GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH)
    logger.info("GCS Manager initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize GCS Manager: {e}")
    gcs_manager = None

class SearchEngine:
    def __init__(self, index_dir, default_top_n=10):
        """
        Initialize the search engine with the index directory.
        
        Args:
            index_dir: Directory containing the inverted index files (part-*.txt)
            default_top_n: Default number of top results to return
        """
        self.index = {}
        self.default_top_n = default_top_n
        self.load_index(index_dir)
        
    def load_index(self, index_dir):
        """Load the inverted index from part-*.txt files."""
        logger.info(f"Loading index from {index_dir}...")
        
        # Find all part files in the index directory
        try:
            for filename in os.listdir(index_dir):
                if filename.startswith('part-'):
                    file_path = os.path.join(index_dir, filename)
                    self._load_index_file(file_path)
                    
            logger.info(f"Loaded {len(self.index)} terms into the index.")
        except Exception as e:
            logger.error(f"Error loading index: {e}")
        
    def _load_index_file(self, file_path):
        """Load a single index file into the index dictionary."""
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Parse the line: term\tdoc1:freq1,doc2:freq2,...
                parts = line.split('\t')
                if len(parts) != 2:
                    continue
                    
                term, posting_list_str = parts
                
                # Parse the posting list
                posting_list = {}
                for posting in posting_list_str.split(','):
                    doc_parts = posting.split(':')
                    if len(doc_parts) == 2:
                        doc_id, freq = doc_parts
                        posting_list[doc_id] = int(freq)
                
                # Add to the index
                self.index[term] = posting_list
    
    def search(self, query):
        """
        Search for documents matching the query terms.
        
        Args:
            query: The search query string
            
        Returns:
            A dictionary where keys are query terms and values are lists of (doc_id, score) pairs
        """
        query_terms = self._tokenize_query(query)
        results = {}

        for term in query_terms:
            if term in self.index:
                # Sort documents by frequency
                docs = [(doc_id, freq) for doc_id, freq in self.index[term].items()]
                docs.sort(key=lambda x: x[1], reverse=True)
                results[term] = docs

        return results
    
    def top_n(self, n=None):
        """
        Find the top N most frequent words in the entire corpus.
        
        Args:
            n: Number of top words to return (default: self.default_top_n)
            
        Returns:
            A list of (word, total_frequency) pairs for the top N words
        """
        if n is None:
            n = self.default_top_n
        
        # Calculate total frequency for each word across all documents
        word_frequencies = {}
        for word, posting_list in self.index.items():
            total_freq = sum(posting_list.values())
            word_frequencies[word] = total_freq
        
        # Use a min heap to find the top N words
        top_words = []
        for word, freq in word_frequencies.items():
            if len(top_words) < n:
                heapq.heappush(top_words, (freq, word))
            elif freq > top_words[0][0]:
                heapq.heappushpop(top_words, (freq, word))
        
        # Convert to a list of (word, freq) pairs, sorted by frequency (highest first)
        results = [(word, freq) for freq, word in sorted(top_words, reverse=True)]
        
        return results
    
    def _tokenize_query(self, query):
        """Tokenize and normalize the query string."""
        query = query.lower()
        words = re.findall(r'\w+', query)
        stop_words = set(['a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'in', 'to', 'for', 'with'])
        return [word for word in words if word not in stop_words and len(word) > 1]

def download_search_index():
    """Download search index from GCS bucket"""
    if not gcs_manager:
        logger.error("GCS Manager not initialized. Cannot download search index.")
        return False
    
    try:
        # Delete existing index if it exists
        if os.path.exists(INDEX_DIR):
            logger.info(f"Deleting existing index directory: {INDEX_DIR}")
            shutil.rmtree(INDEX_DIR)
        
        # Create fresh directory
        os.makedirs(INDEX_DIR, exist_ok=True)
        
        # Make sure to use forward slashes for GCS path
        gcs_prefix = GCS_INDEX_PREFIX.replace('\\', '/')
        
        # Download from GCS
        print(gcs_prefix, INDEX_DIR)
        downloaded_files = gcs_manager.download_folder(
            gcs_folder_prefix=gcs_prefix,
            local_folder=INDEX_DIR, 
            delete_existing=True,
        )
        
        if downloaded_files:
            logger.info(f"Successfully downloaded search index with {len(downloaded_files)} files")
            return True
        else:
            logger.warning("No index files found in GCS bucket")
            return False
    
    except Exception as e:
        logger.error(f"Error downloading search index: {e}")
        return False
    
def extract_archive(file_path, extract_path):
    """Extract tar.gz, tar, or zip file"""
    logger.info(f"Extracting archive: {file_path}")
    
    try:
        if file_path.endswith('.tar.gz') or file_path.endswith('.tgz'):
            with tarfile.open(file_path, 'r:gz') as tar:
                # Make sure to normalize paths during extraction
                for member in tar.getmembers():
                    member.name = os.path.normpath(member.name)
                tar.extractall(path=extract_path)
                logger.info(f"Successfully extracted tar.gz archive: {file_path}")
        elif file_path.endswith('.tar'):
            with tarfile.open(file_path, 'r') as tar:
                # Make sure to normalize paths during extraction
                for member in tar.getmembers():
                    member.name = os.path.normpath(member.name)
                tar.extractall(extract_path)
                logger.info(f"Successfully extracted tar archive: {file_path}")
        elif file_path.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                # For zip files, extract with proper path handling
                for zip_info in zip_ref.infolist():
                    zip_info.filename = os.path.normpath(zip_info.filename)
                    zip_ref.extract(zip_info, extract_path)
                logger.info(f"Successfully extracted zip archive: {file_path}")
        else:
            logger.warning(f"Unsupported archive format: {file_path}")
            return False
        
        # Additionally normalize all paths within the extracted directory
        normalize_directory_paths(extract_path)
        
        return True
    
    except Exception as e:
        logger.error(f"Error extracting archive {file_path}: {e}")
        return False

# New helper function to normalize paths recursively
def normalize_directory_paths(directory):
    """Normalize all paths in the directory to use consistent separators"""
    for root, dirs, files in os.walk(directory, topdown=True):
        for name in dirs:
            old_path = os.path.join(root, name)
            new_path = os.path.normpath(old_path)
            if old_path != new_path:
                os.rename(old_path, new_path)
                
        for name in files:
            old_path = os.path.join(root, name)
            new_path = os.path.normpath(old_path)
            if old_path != new_path:
                os.rename(old_path, new_path)

def convert_to_txt_extensions(directory):
    """Convert all file extensions to .txt"""
    logger.info(f"Converting file extensions to .txt in: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            # Skip if already a .txt file
            if file.endswith('.txt'):
                continue
                
            # Get the file's path
            file_path = os.path.join(root, file)
            
            # New path with .txt extension
            new_path = os.path.splitext(file_path)[0] + '.txt'
            
            try:
                # Rename the file
                os.rename(file_path, new_path)
                logger.info(f"Renamed: {file_path} -> {new_path}")
            except Exception as e:
                logger.error(f"Error renaming file {file_path}: {e}")

def simplify_directory_name(path):
    """Remove random IDs from directory names, keep only the last part"""
    # Get the base directory name
    base_name = os.path.basename(path)
    
    # If the name contains a dash (typical for random IDs), take the last part
    if '-' in base_name:
        # Extract what's after the last dash
        simple_name = base_name.split('-')[-1]
        
        # If we have a parent directory
        parent_dir = os.path.dirname(path)
        if parent_dir:
            new_path = os.path.join(parent_dir, simple_name)
        else:
            new_path = simple_name
            
        try:
            # Rename the directory
            os.rename(path, new_path)
            logger.info(f"Renamed directory: {path} -> {new_path}")
            return new_path
        except Exception as e:
            logger.error(f"Error renaming directory {path}: {e}")
            return path
    
    return path

def upload_processed_files(archive_dir):
    """Upload processed files to GCS"""
    if not gcs_manager:
        logger.error("GCS Manager not initialized. Cannot upload processed files.")
        return False
    
    try:
        # Get archive name from directory
        archive_name = os.path.basename(archive_dir)
        gcs_prefix = f"{GCS_PROCESSED_FILES_PREFIX}/{archive_name}"
        
        # Convert backslash paths to forward slash paths for GCS
        for root, dirs, files in os.walk(archive_dir):
            for file in files:
                # Check if the filename contains backslashes
                if '\\' in file:
                    old_path = os.path.join(root, file)
                    
                    # Create directory structure for nested path
                    dirname, basename = os.path.split(file.replace('\\', '/'))
                    new_dir = os.path.join(root, dirname)
                    
                    # Create the directory structure if it doesn't exist
                    os.makedirs(new_dir, exist_ok=True)
                    
                    # New path with corrected structure
                    new_path = os.path.join(root, dirname, basename)
                    
                    try:
                        # Move the file to the proper directory structure
                        os.rename(old_path, new_path)
                        logger.info(f"Fixed path structure: {old_path} -> {new_path}")
                    except Exception as e:
                        logger.error(f"Error fixing path structure for {old_path}: {e}")
        
        # Now upload to GCS with corrected structure
        uploaded_files = gcs_manager.upload_folder(
            local_folder=archive_dir,
            gcs_folder_prefix=gcs_prefix,
            delete_after_upload=True  # Delete local files after upload
        )
        
        if uploaded_files:
            logger.info(f"Successfully uploaded {len(uploaded_files)} files to GCS: {gcs_prefix}")
            return True
        else:
            logger.warning(f"No files uploaded for {archive_dir}")
            return False
    
    except Exception as e:
        logger.error(f"Error uploading processed files: {e}")
        return False
    
def process_complete_file(job_id, filename, file_path):
    """Process a complete file after all chunks are received"""
    logger.info(f"Processing complete file: {filename} (Job ID: {job_id})")
    
    # Create a directory for the archive without the random ID
    archive_name = os.path.splitext(filename)[0]
    archive_dir = os.path.join(OUTPUT_DIR, archive_name)
    
    # Delete the directory if it already exists
    if os.path.exists(archive_dir):
        logger.info(f"Deleting existing directory: {archive_dir}")
        shutil.rmtree(archive_dir)
    
    # Create fresh directory
    os.makedirs(archive_dir, exist_ok=True)
    
    # Extract archive
    if extract_archive(file_path, archive_dir):
        logger.info(f"Archive extracted to: {archive_dir}")
        
        # Convert all file extensions to .txt
        convert_to_txt_extensions(archive_dir)
        
        # Count the number of text files
        txt_count = 0
        for root, _, files in os.walk(archive_dir):
            for file in files:
                if file.endswith('.txt'):
                    txt_count += 1
        
        logger.info(f"Found {txt_count} text files in extracted directory")
        
        # Upload processed files to GCS
        upload_processed_files(archive_dir)
        
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
    temp_file_path = os.path.join(TEMP_DIR, filename)
    with open(temp_file_path, "wb") as f:
        f.write(file_data)
    
    # Process the file
    process_complete_file(job_id, filename, temp_file_path)
    
    # Clean up
    file_chunks[job_id][filename]["processed"] = True
    logger.info(f"Completed processing {filename} for job {job_id}")
    
    return True

def handle_search_request(producer, search_engine, msg):
    """Handle search request messages"""
    try:
        data = json.loads(msg.value().decode('utf-8'))
        request_id = data.get('requestId')
        keyword = data.get('keyword')
        
        if not request_id or not keyword:
            logger.warning(f"Invalid search request format: {data}")
            return
        
        logger.info(f"Processing search request: {request_id} - '{keyword}'")
        
        # Perform search
        results = search_engine.search(keyword)
        
        # Prepare response
        response = {
            'requestId': request_id,
            'keyword': keyword,
            'results': {
                term: [{'doc_id': doc_id, 'score': score} for doc_id, score in docs] 
                for term, docs in results.items()
            },
            'timestamp': time.time()
        }
        
        # Send response
        producer.produce(
            SEARCH_RESPOND_TOPIC,
            key=request_id,
            value=json.dumps(response).encode('utf-8'),
            callback=lambda err, msg: logger.error(f"Failed to send search response: {err}") if err else None
        )
        producer.flush()
        
        logger.info(f"Sent search response for request: {request_id}")
        
    except Exception as e:
        logger.error(f"Error processing search request: {e}", exc_info=True)

def handle_topn_request(producer, search_engine, msg):
    """Handle top-n request messages"""
    try:
        data = json.loads(msg.value().decode('utf-8'))
        request_id = data.get('requestId')
        n = data.get('n', search_engine.default_top_n)
        
        if not request_id:
            logger.warning(f"Invalid top-n request format: {data}")
            return
        
        logger.info(f"Processing top-n request: {request_id} - n={n}")
        
        # Get top N words
        results = search_engine.top_n(n)
        
        # Prepare response
        response = {
            'requestId': request_id,
            'n': n,
            'results': [{'word': word, 'frequency': freq} for word, freq in results],
            'timestamp': time.time()
        }
        
        # Send response
        producer.produce(
            TOPN_RESPOND_TOPIC,
            key=request_id,
            value=json.dumps(response).encode('utf-8'),
            callback=lambda err, msg: logger.error(f"Failed to send top-n response: {err}") if err else None
        )
        producer.flush()
        
        logger.info(f"Sent top-n response for request: {request_id}")
        
    except Exception as e:
        logger.error(f"Error processing top-n request: {e}", exc_info=True)

def handle_file_processing(producer, msg):
    """Handle file processing messages"""
    try:
        # Parse message
        data = json.loads(msg.value().decode('utf-8'))
        
        # Check message type
        message_type = data.get('messageType', 'fileChunk')  # Default to fileChunk for backward compatibility
        job_id = data.get('jobId')
        
        if not job_id:
            logger.warning(f"Missing jobId in message: {data}")
            return
            
        # Initialize data structures if needed
        if job_id not in file_chunks:
            file_chunks[job_id] = {
                '__reindexing_started__': False,
                '__files_received__': set(),  # Track which files we've seen
                '__expected_files__': None    # Will be populated from metadata
            }
        
        # Handle batch metadata message
        if message_type == 'batchMetadata':
            batch_size = data.get('batchSize')
            total_chunks = data.get('totalChunks')
            files = data.get('files', [])
            
            logger.info(f"Received batch metadata for job {job_id}: {batch_size} files, {total_chunks} total chunks")
            
            # Store batch information
            file_chunks[job_id]['__batch_info__'] = {
                'total_files': batch_size,
                'total_chunks': total_chunks,
                'files': files,
                'received_chunks': 0,
                'last_update': time.time(),
                'processed_files': 0
            }
            
            # Create a set of expected filenames from metadata
            expected_files = set()
            for file_info in files:
                if 'filename' in file_info:
                    expected_files.add(file_info['filename'])
            
            if expected_files:
                file_chunks[job_id]['__expected_files__'] = expected_files
                logger.info(f"Expecting {len(expected_files)} files for job {job_id}: {', '.join(expected_files)}")
            
            return
            
        # Handle regular file chunks
        filename = data.get('filename')
        chunk_index = data.get('chunkIndex')
        total_file_chunks = data.get('totalChunks')
        chunk = data.get('chunk')
        
        if not all([filename, chunk_index is not None, total_file_chunks, chunk]):
            logger.warning(f"Invalid chunk message format: {data}")
            return
        
        # Add this filename to the set of files we've seen
        file_chunks[job_id]['__files_received__'].add(filename)
        
        # Log receipt of chunk (but not for every chunk to avoid flooding logs)
        if chunk_index % 10 == 0 or chunk_index == 0 or chunk_index == total_file_chunks - 1:
            logger.info(f"Received chunk {chunk_index+1}/{total_file_chunks} for {filename} (Job ID: {job_id})")
        
        # Initialize file entry if needed
        if filename not in file_chunks[job_id]:
            file_chunks[job_id][filename] = {
                "total_chunks": total_file_chunks,
                "chunks": {},
                "processed": False,
                "last_update": time.time()
            }
        
        # Store this chunk
        file_chunks[job_id][filename]["chunks"][chunk_index] = chunk
        file_chunks[job_id][filename]["last_update"] = time.time()
        
        # Update batch information if available
        if '__batch_info__' in file_chunks[job_id]:
            file_chunks[job_id]['__batch_info__']['received_chunks'] += 1
            received = file_chunks[job_id]['__batch_info__']['received_chunks']
            total = file_chunks[job_id]['__batch_info__']['total_chunks']
            
            # Log progress less frequently to avoid flooding logs
            if received % 50 == 0 or received == total:
                logger.info(f"Batch progress for job {job_id}: {received}/{total} chunks received ({(received/total)*100:.1f}%)")
        
        # If we have all chunks for this file, process it
        if len(file_chunks[job_id][filename]["chunks"]) == total_file_chunks:
            logger.info(f"All chunks received for {filename}. Processing...")
            process_chunks(job_id, filename)
            
            # Update batch processed files count if available
            if '__batch_info__' in file_chunks[job_id]:
                file_chunks[job_id]['__batch_info__']['processed_files'] += 1
                processed = file_chunks[job_id]['__batch_info__']['processed_files']
                total_files = file_chunks[job_id]['__batch_info__']['total_files']
                logger.info(f"Batch file progress for job {job_id}: {processed}/{total_files} files processed")
        
        # Only check if all files are processed if:
        # 1. Reindexing hasn't already started, and
        # 2. We have metadata about expected files
        if (not file_chunks[job_id]['__reindexing_started__'] and 
                file_chunks[job_id].get('__expected_files__') is not None):
            
            expected_files = file_chunks[job_id]['__expected_files__']
            files_received = file_chunks[job_id]['__files_received__']
            
            # First, check if we've received all the expected files
            if not expected_files.issubset(files_received):
                # We haven't received all expected files yet, so don't start reindexing
                return
            
            # Now check if all received files are fully processed
            all_processed = True
            for fname in expected_files:
                if fname not in file_chunks[job_id] or not file_chunks[job_id][fname].get("processed", False):
                    all_processed = False
                    break
            
            # If all files are processed, start reindexing
            if all_processed:
                # Set the flag to prevent multiple reindexing jobs
                file_chunks[job_id]['__reindexing_started__'] = True
                
                logger.info(f"All {len(expected_files)} files for job {job_id} processed. Starting reindexing (once)...")
                
                # Get batch info if available
                batch_info = file_chunks[job_id].get('__batch_info__', {})
                total_files = batch_info.get('total_files', len(expected_files))
                
                logger.info(f"Completed processing all {total_files} files for job {job_id}")
                
                # Clean up now-processed files to free memory
                for key in file_chunks[job_id]:
                    if key not in ['__batch_info__', '__reindexing_started__', '__files_received__', '__expected_files__']:
                        if 'chunks' in file_chunks[job_id][key]:
                            file_chunks[job_id][key]['chunks'] = {}
                
                # Delete existing index folder
                if gcs_manager:
                    try:
                        gcs_manager.delete_folder(GCS_INDEX_PREFIX)
                        logger.info(f"Deleted existing index in GCS: {GCS_INDEX_PREFIX}")
                    except Exception as e:
                        logger.error(f"Error deleting existing index: {e}")
                
                # Submit Dataproc job
                try:
                    dataproc_job_id = submit_job(PROJECT_ID, REGION, CLUSTER_NAME, GCS_BUCKET_NAME)
                    logger.info(f"Submitted Dataproc job: {dataproc_job_id}")
                    
                    if dataproc_job_id:
                        # Wait for job completion
                        # job_successful = wait_for_job_completion(dataproc_job_id, PROJECT_ID, REGION)
                        
                        # if job_successful:
                        #     # Download new search index
                        index_downloaded = download_search_index()
                            
                        #     # Send completion message to Kafka
                        logger.info(f"Kaushik Done")
                        if index_downloaded:
                            logger.info(f"Index Downloaded")
                            search_engine = SearchEngine(INDEX_DIR)
                            send_processing_complete(producer, job_id, True, 
                                                    f"Processing and reindexing completed successfully for {total_files} files")
                            
                            logger.info(f"Reindexing completed for job {job_id}")
                        else:
                            send_processing_complete(producer, job_id, False, "Dataproc job failed")
                            logger.error(f"Dataproc job {dataproc_job_id} failed or timed out")
                    else:
                        send_processing_complete(producer, job_id, False, "Failed to submit Dataproc job")
                        logger.error(f"Failed to submit Dataproc job for job {job_id}")
                except Exception as e:
                    send_processing_complete(producer, job_id, False, f"Error in reindexing: {str(e)}")
                    logger.error(f"Error in reindexing process: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Error processing file message: {e}", exc_info=True)

def send_processing_complete(producer, job_id, success, message):
    """Send a processing completion message back to Kafka"""
    try:
        response = {
            'requestId': job_id,  # Using job_id as the requestId for consistency
            'success': success,
            'message': message,
            'timestamp': time.time(),
            'results': {
                'jobId': job_id,
                'status': 'completed' if success else 'failed',
                'details': message
            }
        }
        
        # Send to FILE_PROCESSING_DONE topic
        producer.produce(
            FILE_PROCESSING_DONE_TOPIC,  # Use the env var instead of hardcoded string
            key=job_id,
            value=json.dumps(response).encode('utf-8'),
            callback=lambda err, msg: logger.error(f"Failed to send completion response: {err}") if err else None
        )
        producer.flush()
        
        logger.info(f"Sent processing completion notification for job: {job_id}")
    except Exception as e:
        logger.error(f"Error sending processing completion: {e}", exc_info=True)

# Update the kafka_consumer_loop function to use the environment-based configuration
def kafka_consumer_loop():
    """Main Kafka consumer loop"""
    # Get Kafka configuration from environment variables
    kafka_config = get_kafka_config()
    
    # Create Kafka consumer for all topics
    consumer = Consumer(kafka_config)
    
    # Define topics with constants to match your API endpoints
    consumer.subscribe([FILE_PROCESSING_TOPIC, SEARCH_REQUEST_TOPIC, TOPN_REQUEST_TOPIC])
    
    # Create Kafka producer for response topics
    producer_config = kafka_config.copy()
    producer = Producer(producer_config)
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Route message to the appropriate handler based on topic
            topic = msg.topic()
            
            if topic == FILE_PROCESSING_TOPIC:
                handle_file_processing(producer, msg)
            elif topic == SEARCH_REQUEST_TOPIC:
                handle_search_request(producer, search_engine, msg)
            elif topic == TOPN_REQUEST_TOPIC:
                handle_topn_request(producer, search_engine, msg)
            else:
                logger.warning(f"Received message on unexpected topic: {topic}")
    
    except KeyboardInterrupt:
        logger.info("Interrupted. Closing consumer.")
    
    finally:
        consumer.close()
        producer.flush()

def wait_for_job_completion(job_id, project_id, region, timeout_seconds=1800, check_interval_seconds=30):
    """
    Wait for a Dataproc job to complete with monitoring and timeout.
    
    Args:
        job_id: The ID of the job to monitor
        project_id: Google Cloud project ID
        region: Google Cloud region
        timeout_seconds: Maximum time to wait (default: 30 minutes)
        check_interval_seconds: How often to check status (default: 30 seconds)
        
    Returns:
        True if job completed successfully, False if failed or timed out
    """
    logger = logging.getLogger("dataproc-job-monitor")
    logger.info(f"Waiting for job {job_id} to complete...")
    
    # Create the Dataproc job client
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # Format the job resource name
    job_name = f"projects/{project_id}/regions/{region}/jobs/{job_id}"
    
    # Track start time for timeout
    start_time = time.time()
    previous_state = None
    
    while True:
        # Check if we've exceeded the timeout
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            logger.error(f"Timed out waiting for job {job_id} after {elapsed_time:.1f} seconds")
            return False
        
        try:
            # Get the current job state
            job = job_client.get_job(name=job_name)
            current_state = job.status.state.name
            
            # Log when state changes
            if current_state != previous_state:
                logger.info(f"Job {job_id} state: {current_state} (running for {elapsed_time:.1f} seconds)")
                previous_state = current_state
            
            # Check if job completed
            if current_state == "DONE":
                logger.info(f"Job {job_id} completed successfully after {elapsed_time:.1f} seconds")
                return True
                
            # Check if job failed or was cancelled
            if current_state in ["ERROR", "CANCELLED"]:
                error_detail = job.status.details if hasattr(job.status, 'details') else "No details available"
                logger.error(f"Job {job_id} failed with state {current_state}: {error_detail}")
                return False
                
            # Wait before checking again
            time.sleep(check_interval_seconds)
            
        except Exception as e:
            logger.error(f"Error checking status of job {job_id}: {e}")
            # Continue monitoring despite error in getting status
            time.sleep(check_interval_seconds)

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
                    # Skip metadata fields
                    if filename.startswith('__') and filename.endswith('__'):
                        continue
                        
                    # If the file hasn't been processed and it's been more than 10 minutes
                    if not file_info.get("processed", False) and current_time - file_info.get("last_update", current_time) > 600:
                        logger.warning(f"File transfer incomplete for {filename} (Job ID: {job_id}). Attempting to process anyway.")
                        if process_chunks(job_id, filename):
                            file_info["processed"] = True
                    
                    all_processed = all_processed and file_info.get("processed", False)
                
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
    # Log environment variables (sanitized)
    logger.info(f"Starting with configuration:")
    logger.info(f"- KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"- KAFKA_SECURITY_PROTOCOL: {KAFKA_SECURITY_PROTOCOL}")
    logger.info(f"- KAFKA_SASL_MECHANISM: {KAFKA_SASL_MECHANISM}")
    logger.info(f"- KAFKA_SASL_USERNAME: {'[REDACTED]' if KAFKA_SASL_USERNAME else 'Not set'}")
    logger.info(f"- KAFKA_SASL_PASSWORD: {'[REDACTED]' if KAFKA_SASL_PASSWORD else 'Not set'}")
    logger.info(f"- GCS_BUCKET_NAME: {GCS_BUCKET_NAME}")
    logger.info(f"- PROJECT_ID: {PROJECT_ID}")
    logger.info(f"- REGION: {REGION}")
    
    # Initialize search engine with index directory
    index_dir = INDEX_DIR  # Use the constant defined at the top
    search_engine = SearchEngine(index_dir)
    logger.info(f"Search engine initialized with {len(search_engine.index)} terms")
    
    # Start cleanup thread
    cleanup_thread = Thread(target=cleanup_thread, daemon=True)
    cleanup_thread.start()
    
    # Start Kafka consumer
    logger.info("Starting Kafka consumer loop")
    try:
        kafka_consumer_loop()
    except Exception as e:
        logger.error(f"Error in consumer loop: {e}", exc_info=True)

    download_search_index()