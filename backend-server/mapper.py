#!/usr/bin/env python3
import sys
import re
import os

# List of stop words to exclude
STOP_WORDS = set([
    'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'am', 'was', 'were', 
    'be', 'been', 'being', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 
    'about', 'against', 'between', 'into', 'through', 'during', 'before', 
    'after', 'above', 'below', 'from', 'up', 'down', 'of', 'off', 'over', 
    'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 
    'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 
    'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 
    'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 
    'don', 'should', 'now'
])

# Get input file path from environment
file_path = os.environ.get('map_input_file', '')
if not file_path:
    # If environment variable not available, try to get from STDIN filename
    file_path = os.environ.get('mapreduce_map_input_file', '')

# Extract doc folder and name from the path
parts = file_path.split('/')
doc_folder = parts[-2] if len(parts) >= 2 else 'unknown'
doc_name = parts[-1] if parts else 'unknown'

doc_id = f"{doc_folder}/{doc_name}"

# Read input lines
for line in sys.stdin:
    # Remove leading/trailing whitespace and convert to lowercase
    line = line.strip().lower()
    
    # Skip empty lines
    if not line:
        continue
    
    # Tokenize the text (remove punctuation and split into words)
    words = re.findall(r'\w+', line)
    
    # Process each word
    for word in words:
        # Skip stop words and very short words
        if word not in STOP_WORDS and len(word) > 1:
            # Emit: word\tdoc_id\t1
            print(f"{word}\t{doc_id}\t1")
