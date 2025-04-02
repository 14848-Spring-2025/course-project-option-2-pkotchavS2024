#!/usr/bin/env python3
import sys

current_word = None
current_doc = None
current_count = 0
word = None
doc_counts = {}

# Process each line from the mapper
for line in sys.stdin:
    # Remove leading/trailing whitespace
    line = line.strip()
    
    # Parse the input
    parts = line.split('\t')
    if len(parts) != 3:
        continue
        
    word, doc_id, count = parts
    
    try:
        count = int(count)
    except ValueError:
        continue
    
    # If this is a new word, output the previous word's results
    if current_word != word:
        if current_word:
            # Format the posting list as word\tdoc_id:count,doc_id:count,...
            posting_list = ','.join([f"{doc}:{count}" for doc, count in doc_counts.items()])
            print(f"{current_word}\t{posting_list}")
        
        # Reset for the new word
        current_word = word
        doc_counts = {}
    
    # Aggregate counts for each document
    if doc_id in doc_counts:
        doc_counts[doc_id] = doc_counts[doc_id] + count
    else:
        doc_counts[doc_id] = count

# Output the last word
if current_word:
    posting_list = ','.join([f"{doc}:{count}" for doc, count in doc_counts.items()])
    print(f"{current_word}\t{posting_list}")
