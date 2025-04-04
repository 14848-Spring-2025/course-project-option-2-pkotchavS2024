// Import required modules
const fs = require("fs");
const express = require("express");
const multer = require("multer");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;

// Initialize Express app
const app = express();

// Basic CORS setup
app.use(cors());
app.use(express.json());

// Configure storage for file uploads
const storage = multer.memoryStorage();
const upload = multer({ storage });

// Port for the server
const PORT = 3000;

// In-memory storage for job status
const jobStatus = new Map();

// Kafka topic names
const SEARCH_REQUEST_TOPIC = "search-request";
const SEARCH_RESPONSE_TOPIC = "search-response";
const TOPN_REQUEST_TOPIC = "topn-request";
const TOPN_RESPONSE_TOPIC = "topn-response";
const FILE_PROCESSING_TOPIC = "file-processing";
const FILE_PROCESSING_DONE = "file-processing-done";

// Initialize Kafka producer and consumer
let producer;
let consumer;

// Map to store pending requests awaiting responses
const pendingRequests = new Map();

// Read Kafka configuration
function readConfig(fileName) {
  try {
    const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
    return data.reduce((config, line) => {
      const [key, value] = line.split("=");
      if (key && value) {
        config[key] = value;
      }
      return config;
    }, {});
  } catch (error) {
    console.error(`Error reading Kafka config: ${error.message}`);
    return {};
  }
}

// Initialize Kafka connections
async function initKafka() {
  try {
    // Load Kafka configuration
    const config = readConfig("./client.properties");
    
    // Initialize producer
    producer = new Kafka().producer(config);
    await producer.connect();
    console.log("Connected to Confluent Kafka as producer");

    // Initialize consumer
    consumer = new Kafka().consumer({
      ...config,
      kafkaJS: {
        groupId: 'search-engine-server-group'
      }
    });
    await consumer.connect();
    console.log("Connected to Confluent Kafka as consumer");

    // Subscribe to response topics
    await consumer.subscribe({ topics: [SEARCH_RESPONSE_TOPIC, TOPN_RESPONSE_TOPIC, FILE_PROCESSING_DONE] });

    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const messageValue = JSON.parse(message.value.toString());
          const requestId = messageValue.requestId;
          
          console.log(`Received response on topic ${topic} for requestId: ${requestId}`);
          
          // Check if we have a pending request with this ID
          if (pendingRequests.has(requestId)) {
            const { res, timer, type, jobId } = pendingRequests.get(requestId);
            
            // Clear the timeout timer
            clearTimeout(timer);
            
            // If this is a search or topn response, send directly to the client
            if (type !== 'fileProcessing' && res) {
              // Send the response back to the client
              res.json(messageValue.results);
              console.log(`Response for requestId ${requestId} sent to client`);
            } 
            // If this is a file processing completion update, update the job status
            else if (type === 'fileProcessing') {
              // Update job status
              const currentStatus = jobStatus.get(jobId) || {};
              jobStatus.set(jobId, {
                ...currentStatus,
                status: messageValue.success ? 'completed' : 'failed',
                message: messageValue.message,
                completedAt: new Date().toISOString(),
                details: messageValue.results
              });
              
              console.log(`Updated job status for ${jobId}: ${messageValue.success ? 'completed' : 'failed'}`);
            }
            
            // Remove this request from pending requests
            pendingRequests.delete(requestId);
          } else {
            console.log(`No pending request found for requestId: ${requestId}`);
          }
        } catch (error) {
          console.error(`Error processing message from topic ${topic}:`, error);
        }
      }
    });
  } catch (error) {
    console.error("Failed to connect to Kafka:", error);
    // Don't exit process, allow server to start in degraded mode
    console.log("Server starting in degraded mode (no Kafka connectivity)");
  }
}

// Routes

// Test endpoint
app.get('/test', (req, res) => {
  res.json({ 
    message: 'Server is working',
    stats: {
      pendingRequests: pendingRequests.size,
      kafkaConnected: !!producer
    }
  });
});

// Process and send a single file to Kafka
async function processAndSendFile(file) {
  // Generate a unique job ID for this file
  const jobId = uuidv4();
  
  // Sanitize the filename
  const sanitizedFileName = file.originalname.replace(/\s+/g, "_");
  
  // Check if Kafka producer is available
  if (!producer) {
    throw new Error("Kafka producer is not connected");
  }
  
  // Set up job tracking
  const requestId = jobId; // Using jobId as the requestId
  
  // Set timeout for request (5 minutes)
  const timer = setTimeout(() => {
    if (pendingRequests.has(requestId)) {
      // Update job status to timeout
      jobStatus.set(jobId, {
        status: 'timeout',
        fileName: sanitizedFileName,
        progress: 100,
        message: 'Processing timed out after 5 minutes',
      });
      
      // Remove this request from pending requests
      pendingRequests.delete(requestId);
    }
  }, 5 * 60 * 1000); // 5 minutes
  
  // Store tracking info in pending requests
  pendingRequests.set(requestId, { timer, type: 'fileProcessing', jobId });
  
  // Convert file buffer to base64
  const fileBase64 = file.buffer.toString('base64');
  
  // Calculate chunks - max 800KB per chunk (safely under 1MB limit after JSON overhead)
  const chunkSize = 800 * 1024; // 800KB in base64
  const totalChunks = Math.ceil(fileBase64.length / chunkSize);
  
  console.log(`Splitting file ${sanitizedFileName} into ${totalChunks} chunks for Kafka`);
  
  // Set job status to "processing"
  jobStatus.set(jobId, {
    status: 'processing',
    fileName: sanitizedFileName,
    progress: 0,
    message: 'Sending file chunks to Kafka',
    totalChunks: totalChunks,
    processedChunks: 0
  });
  
  // Send chunks to Kafka
  for (let i = 0; i < totalChunks; i++) {
    const startIndex = i * chunkSize;
    const endIndex = Math.min(startIndex + chunkSize, fileBase64.length);
    const chunk = fileBase64.substring(startIndex, endIndex);
    
    const messageValue = JSON.stringify({
      jobId,
      filename: sanitizedFileName,
      fileType: file.mimetype,
      fileSize: file.size,
      chunkIndex: i,
      totalChunks: totalChunks,
      chunk: chunk,
      timestamp: Date.now()
    });
    
    await producer.send({
      topic: FILE_PROCESSING_TOPIC,
      messages: [{ 
        key: `${sanitizedFileName}_${i}`, 
        value: messageValue 
      }],
    });
    
    // Update job status progress
    const currentStatus = jobStatus.get(jobId);
    jobStatus.set(jobId, {
      ...currentStatus,
      progress: Math.round(((i + 1) / totalChunks) * 100),
      processedChunks: i + 1
    });
    
    console.log(`Sent chunk ${i+1}/${totalChunks} of file ${sanitizedFileName} to Kafka`);
  }
  
  // Update job status to "Kafka processing"
  jobStatus.set(jobId, {
    status: 'processing',
    fileName: sanitizedFileName,
    progress: 100,
    message: 'All chunks sent to Kafka, awaiting processing',
    totalChunks: totalChunks,
    processedChunks: totalChunks
  });
  
  console.log(`All chunks of file sent to Kafka topic ${FILE_PROCESSING_TOPIC}: ${sanitizedFileName}`);
  
  return {
    filename: sanitizedFileName,
    jobId: jobId,
    statusUrl: `/job/${jobId}/status`
  };
}

// Job status endpoint
app.get('/job/:jobId/status', (req, res) => {
  const jobId = req.params.jobId;
  
  if (!jobId) {
    return res.status(400).json({ error: 'Missing job ID' });
  }
  
  // Check if we have this job in our status map
  if (!jobStatus.has(jobId)) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  // Return the current status
  const status = jobStatus.get(jobId);
  return res.json(status);
});

// File upload endpoint
// File upload endpoint
app.post('/upload', upload.array('files'), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }
  
  try {
    console.log(`Processing ${req.files.length} files as a batch`);
    
    // Generate a single job ID for all files
    const batchJobId = uuidv4();
    
    // Check if Kafka producer is available
    if (!producer) {
      throw new Error("Kafka producer is not connected");
    }
    
    // Set up job tracking
    const requestId = batchJobId;
    
    // Set timeout for request (15 minutes for batch processing)
    const timer = setTimeout(() => {
      if (pendingRequests.has(requestId)) {
        // Update job status to timeout
        jobStatus.set(batchJobId, {
          status: 'timeout',
          filesCount: req.files.length,
          progress: 100,
          message: 'Batch processing timed out after 15 minutes',
        });
        
        // Remove this request from pending requests
        pendingRequests.delete(requestId);
      }
    }, 15 * 60 * 1000); // 15 minutes
    
    // Store tracking info in pending requests
    pendingRequests.set(requestId, { timer, type: 'fileProcessing', jobId: batchJobId });
    
    // Initialize job status
    jobStatus.set(batchJobId, {
      status: 'processing',
      filesCount: req.files.length,
      progress: 0,
      message: 'Preparing files for processing',
      processedFiles: 0,
      totalChunks: 0,
      sentChunks: 0
    });
    
    // Define maximum chunk size (500KB in base64, safely under Kafka message size limits)
    const MAX_CHUNK_SIZE = 500 * 1024; 
    
    // First pass: calculate total chunks across all files
    let totalChunks = 0;
    const fileMetadata = [];
    
    for (let fileIndex = 0; fileIndex < req.files.length; fileIndex++) {
      const file = req.files[fileIndex];
      const fileBase64 = file.buffer.toString('base64');
      const fileChunks = Math.ceil(fileBase64.length / MAX_CHUNK_SIZE);
      totalChunks += fileChunks;
      
      fileMetadata.push({
        index: fileIndex,
        filename: file.originalname.replace(/\s+/g, "_"),
        fileType: file.mimetype,
        fileSize: file.size,
        chunks: fileChunks
      });
    }
    
    // Update job status with total chunks information
    jobStatus.set(batchJobId, {
      ...jobStatus.get(batchJobId),
      totalChunks: totalChunks,
      message: `Chunking ${req.files.length} files into ${totalChunks} pieces`
    });
    
    console.log(`Batch ${batchJobId}: Will send ${totalChunks} chunks for ${req.files.length} files`);
    
    // Send batch metadata message to inform the processor about the incoming batch
    const batchMetadata = {
      jobId: batchJobId,
      batchSize: req.files.length,
      totalChunks: totalChunks,
      files: fileMetadata,
      messageType: 'batchMetadata',
      timestamp: Date.now()
    };
    
    // Send the metadata message (small enough to fit in a single message)
    await producer.send({
      topic: FILE_PROCESSING_TOPIC,
      messages: [{ 
        key: `${batchJobId}_metadata`, 
        value: JSON.stringify(batchMetadata) 
      }],
    });
    
    console.log(`Sent batch metadata for job ${batchJobId}`);
    
    // Second pass: process and send each file in chunks
    let sentChunks = 0;
    
    for (let fileIndex = 0; fileIndex < req.files.length; fileIndex++) {
      const file = req.files[fileIndex];
      const sanitizedFileName = file.originalname.replace(/\s+/g, "_");
      const fileBase64 = file.buffer.toString('base64');
      
      // Calculate chunks needed for this file
      const fileChunks = Math.ceil(fileBase64.length / MAX_CHUNK_SIZE);
      
      console.log(`File ${fileIndex+1}/${req.files.length} (${sanitizedFileName}): Splitting into ${fileChunks} chunks`);
      
      // Send each chunk for this file
      for (let chunkIndex = 0; chunkIndex < fileChunks; chunkIndex++) {
        const startIndex = chunkIndex * MAX_CHUNK_SIZE;
        const endIndex = Math.min(startIndex + MAX_CHUNK_SIZE, fileBase64.length);
        const chunkData = fileBase64.substring(startIndex, endIndex);
        
        const chunkMessage = {
          jobId: batchJobId,
          filename: sanitizedFileName,
          fileType: file.mimetype,
          fileSize: file.size,
          chunkIndex: chunkIndex,
          totalChunks: fileChunks,
          chunk: chunkData,
          fileIndex: fileIndex,
          totalFiles: req.files.length,
          timestamp: Date.now()
        };
        
        await producer.send({
          topic: FILE_PROCESSING_TOPIC,
          messages: [{ 
            key: `${batchJobId}_${fileIndex}_${chunkIndex}`, 
            value: JSON.stringify(chunkMessage) 
          }],
        });
        
        sentChunks++;
        
        // Update job status progress (0-100%)
        const progressPct = Math.round((sentChunks / totalChunks) * 100);
        
        jobStatus.set(batchJobId, {
          status: 'processing',
          filesCount: req.files.length,
          progress: progressPct,
          message: `Sending file chunks to Kafka (${sentChunks}/${totalChunks})`,
          processedFiles: Math.floor((fileIndex * fileChunks + chunkIndex + 1) / fileChunks),
          totalChunks: totalChunks,
          sentChunks: sentChunks
        });
        
        // Log less frequently for many chunks
        if (chunkIndex % 10 === 0 || chunkIndex === fileChunks - 1) {
          console.log(`Sent chunk ${chunkIndex+1}/${fileChunks} of file ${fileIndex+1}/${req.files.length} (${sanitizedFileName})`);
        }
      }
      
      console.log(`Completed sending file ${fileIndex+1}/${req.files.length} (${sanitizedFileName})`);
    }
    
    // Final status update
    jobStatus.set(batchJobId, {
      status: 'processing',
      filesCount: req.files.length,
      progress: 100,
      message: 'All chunks sent to Kafka, awaiting processing',
      totalChunks: totalChunks,
      sentChunks: sentChunks
    });
    
    console.log(`Batch ${batchJobId}: Successfully sent all ${sentChunks} chunks for ${req.files.length} files`);
    
    // Return success response with the batchJobId
    res.status(200).json({ 
      message: `Successfully sent ${req.files.length} files (in ${sentChunks} chunks) for processing as a batch`,
      batchJobId: batchJobId,
      statusUrl: `/job/${batchJobId}/status`,
      filesCount: req.files.length,
      totalChunks: totalChunks
    });
  } catch (error) {
    console.error(`Error processing files: ${error.message}`);
    return res.status(500).json({ error: 'Failed to process files', details: error.message });
  }
});
// Search endpoint
app.get('/search', async (req, res) => {
  const keyword = req.query.keyword;
  
  if (!keyword) {
    return res.status(400).json({
      error: 'Missing required parameter: keyword'
    });
  }
  
  try {
    // Check if Kafka producer is available
    if (!producer) {
      return res.status(503).json({
        error: 'Service unavailable',
        message: 'Kafka connection is not established'
      });
    }
    
    const requestId = uuidv4();
    
    // Prepare message for Kafka
    const message = {
      requestId,
      keyword,
      timestamp: Date.now()
    };
    
    console.log(`Sending search request for keyword: ${keyword}, requestId: ${requestId}`);
    
    // Send message to search-request topic
    await producer.send({
      topic: SEARCH_REQUEST_TOPIC,
      messages: [{ 
        key: requestId, 
        value: JSON.stringify(message) 
      }],
    });
    
    // Set timeout for request (30 seconds)
    const timer = setTimeout(() => {
      if (pendingRequests.has(requestId)) {
        pendingRequests.delete(requestId);
        res.status(504).json({
          error: 'Request timed out',
          message: 'The search service did not respond within the timeout period'
        });
      }
    }, 30000);
    
    // Store the response object and timer to use when we get a response
    pendingRequests.set(requestId, { res, timer, type: 'search' });
    
    console.log(`Search request sent to Kafka, awaiting response for requestId: ${requestId}`);
    
    // The response will be sent when we receive a message from the search-response topic
    
  } catch (error) {
    console.error('Error in search process:', error);
    res.status(500).json({ 
      error: 'Failed to process search request',
      details: error.message 
    });
  }
});

// Top-N endpoint
app.get('/topn', async (req, res) => {
  const n = req.query.n;
  
  if (!n || isNaN(parseInt(n)) || parseInt(n) < 1) {
    return res.status(400).json({
      error: 'Missing or invalid required parameter: n (must be a positive integer)'
    });
  }
  
  try {
    // Check if Kafka producer is available
    if (!producer) {
      return res.status(503).json({
        error: 'Service unavailable',
        message: 'Kafka connection is not established'
      });
    }
    
    const requestId = uuidv4();
    
    // Prepare message for Kafka
    const message = {
      requestId,
      n: parseInt(n),
      timestamp: Date.now()
    };
    
    console.log(`Sending top-n request for n=${n}, requestId: ${requestId}`);
    
    // Send message to topn-request topic
    await producer.send({
      topic: TOPN_REQUEST_TOPIC,
      messages: [{ 
        key: requestId, 
        value: JSON.stringify(message) 
      }],
    });
    
    // Set timeout for request (30 seconds)
    const timer = setTimeout(() => {
      if (pendingRequests.has(requestId)) {
        pendingRequests.delete(requestId);
        res.status(504).json({
          error: 'Request timed out',
          message: 'The top-n service did not respond within the timeout period'
        });
      }
    }, 30000);
    
    // Store the response object and timer to use when we get a response
    pendingRequests.set(requestId, { res, timer, type: 'topn' });
    
    console.log(`Top-N request sent to Kafka, awaiting response for requestId: ${requestId}`);
    
  } catch (error) {
    console.error('Error in top-n process:', error);
    res.status(500).json({ 
      error: 'Failed to process top-n request',
      details: error.message 
    });
  }
});

// Serve static files (for the frontend)
app.use(express.static('public'));

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server');
  if (producer) {
    await producer.disconnect();
  }
  if (consumer) {
    await consumer.disconnect();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT signal received: closing HTTP server');
  if (producer) {
    await producer.disconnect();
  }
  if (consumer) {
    await consumer.disconnect();
  }
  process.exit(0);
});

// Initialize Kafka and start server
(async () => {
  try {
    // Initialize Kafka
    await initKafka();
    
    // Start server
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`Visit http://localhost:${PORT} to access the application`);
    });
  } catch (err) {
    console.error("Failed to connect to Kafka:", err);
    console.error("Server startup aborted due to Kafka connection failure");
    process.exit(1);
  }
})();