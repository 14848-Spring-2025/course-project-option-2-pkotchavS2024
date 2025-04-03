import os
import heapq
import re
import json
import time
from collections import defaultdict
import threading

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
        print(f"Loading index from {index_dir}...")
        
        # Find all part files in the index directory
        for filename in os.listdir(index_dir):
            if filename.startswith('part-'):
                file_path = os.path.join(index_dir, filename)
                self._load_index_file(file_path)
                
        print(f"Loaded {len(self.index)} terms into the index.")
        
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


class TopicListener:
    """
    A class to listen for search-request and topn-request topics and respond
    with search-respond and topn-respond topics.
    """
    def __init__(self, search_engine, search_request_topic="search-request", 
                 topn_request_topic="topn-request", search_respond_topic="search-respond",
                 topn_respond_topic="topn-respond"):
        """
        Initialize the topic listener with the search engine and topic names.
        
        Args:
            search_engine: SearchEngine instance to use for queries
            search_request_topic: Topic to listen for search requests
            topn_request_topic: Topic to listen for top-n requests
            search_respond_topic: Topic to publish search results
            topn_respond_topic: Topic to publish top-n results
        """
        self.search_engine = search_engine
        self.search_request_topic = search_request_topic
        self.topn_request_topic = topn_request_topic
        self.search_respond_topic = search_respond_topic
        self.topn_respond_topic = topn_respond_topic
        
        self.running = False
        self.search_thread = None
        self.topn_thread = None
        
    def start(self):
        """Start listening for topics."""
        if self.running:
            print("Topic listener is already running.")
            return
            
        self.running = True
        self.search_thread = threading.Thread(target=self._listen_for_search_requests)
        self.topn_thread = threading.Thread(target=self._listen_for_topn_requests)
        
        self.search_thread.daemon = True
        self.topn_thread.daemon = True
        
        self.search_thread.start()
        self.topn_thread.start()
        
        print(f"Started listening for topics: {self.search_request_topic} and {self.topn_request_topic}")
        
    def stop(self):
        """Stop listening for topics."""
        self.running = False
        print("Stopping topic listeners...")
        
    def _listen_for_search_requests(self):
        """Listen for search requests and respond with search results."""
        try:
            # In a real implementation, this would connect to a message broker
            # and use a proper subscriber pattern. For this example, we'll simulate it.
            print(f"Listening for {self.search_request_topic} topic...")
            
            while self.running:
                # Simulate checking for messages
                # In a real implementation, this would be a blocking call to receive a message
                
                # This is where you would get the message payload
                # For example: message = message_broker.receive(self.search_request_topic)
                
                # For demonstration, we'll just simulate a message every 10 seconds
                time.sleep(10)
                
                # Simulate a request
                simulated_request = {"query": "example query", "request_id": "12345"}
                print(f"Received search request: {simulated_request}")
                
                # Process the request
                query = simulated_request.get("query", "")
                request_id = simulated_request.get("request_id", "unknown")
                
                # Get search results
                results = self.search_engine.search(query)
                
                # Prepare response
                response = {
                    "request_id": request_id,
                    "query": query,
                    "results": results,
                    "timestamp": time.time()
                }
                
                # Publish the response
                # In a real implementation: message_broker.publish(self.search_respond_topic, json.dumps(response))
                print(f"Published to {self.search_respond_topic}: {json.dumps(response, indent=2)}")
                
        except Exception as e:
            print(f"Error in search request listener: {e}")
            
    def _listen_for_topn_requests(self):
        """Listen for top-n requests and respond with top-n results."""
        try:
            # In a real implementation, this would connect to a message broker
            # and use a proper subscriber pattern. For this example, we'll simulate it.
            print(f"Listening for {self.topn_request_topic} topic...")
            
            while self.running:
                # Simulate checking for messages
                # In a real implementation, this would be a blocking call to receive a message
                
                # This is where you would get the message payload
                # For example: message = message_broker.receive(self.topn_request_topic)
                
                # For demonstration, we'll just simulate a message every 15 seconds
                time.sleep(15)
                
                # Simulate a request
                simulated_request = {"n": 5, "request_id": "67890"}
                print(f"Received top-n request: {simulated_request}")
                
                # Process the request
                n = simulated_request.get("n", 10)  # Default to 10 if not specified
                request_id = simulated_request.get("request_id", "unknown")
                
                # Get top-n results
                results = self.search_engine.top_n(n)
                
                # Prepare response
                response = {
                    "request_id": request_id,
                    "n": n,
                    "results": results,
                    "timestamp": time.time()
                }
                
                # Publish the response
                # In a real implementation: message_broker.publish(self.topn_respond_topic, json.dumps(response))
                print(f"Published to {self.topn_respond_topic}: {json.dumps(response, indent=2)}")
                
        except Exception as e:
            print(f"Error in top-n request listener: {e}")


# Updated usage example
def main():
    index_dir = "search_index_output"  # Directory containing part-*.txt files
    search_engine = SearchEngine(index_dir)
    
    # Create and start the topic listener
    topic_listener = TopicListener(search_engine)
    topic_listener.start()
    
    try:
        # Keep the main thread running
        print("Press Ctrl+C to stop the listeners...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Stop the topic listener when Ctrl+C is pressed
        topic_listener.stop()
        print("Listeners stopped.")

if __name__ == "__main__":
    main()