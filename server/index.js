const fs = require("fs");
const express = require("express");
const multer = require("multer");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;

// Read Kafka configuration
function readConfig(fileName) {
  const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
  return data.reduce((config, line) => {
    const [key, value] = line.split("=");
    if (key && value) {
      config[key] = value;
    }
    return config;
  }, {});
}

// Initialize Express app
const app = express();
app.use(cors());
app.use(express.json());
const PORT = 3000;

// Load Kafka configuration
const config = readConfig("client.properties");
const TOPIC = "file-processing"; // Change to your desired topic name

// Initialize Kafka producer
let producer;
async function initProducer() {
  producer = new Kafka().producer(config);
  await producer.connect();
  console.log("Connected to Confluent Kafka");
}

// Initialize producer on startup
initProducer().catch(err => {
  console.error("Failed to connect to Kafka:", err);
  process.exit(1);
});

// Multer storage in memory
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage });

// Process and send a single file to Kafka
async function processAndSendFile(file) {
  // Generate a unique job ID for this file
  const jobId = uuidv4();
  
  // Sanitize the filename
  const sanitizedFileName = file.originalname.replace(/\s+/g, "_");
  
  // Convert file buffer to base64
  const fileBase64 = file.buffer.toString('base64');
  
  // Calculate chunks - max 800KB per chunk (safely under 1MB limit after JSON overhead)
  const chunkSize = 800 * 1024; // 800KB in base64
  const totalChunks = Math.ceil(fileBase64.length / chunkSize);
  
  console.log(`Splitting file ${sanitizedFileName} into ${totalChunks} chunks`);
  
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
      topic: TOPIC,
      messages: [{ 
        key: `${sanitizedFileName}_${i}`, 
        value: messageValue 
      }],
    });
        
    console.log(`Sent chunk ${i+1}/${totalChunks} of file ${sanitizedFileName} to Kafka`);
  }
  
  console.log(`All chunks of file sent to Kafka topic ${TOPIC}: ${sanitizedFileName}`);
  
  return {
    filename: sanitizedFileName,
    jobId: jobId
  };
}

// Multiple file upload endpoint
app.post('/upload', upload.array('files'), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).send('No files uploaded.');
  }

  try {
    console.log(`Processing ${req.files.length} files`);
    
    // Process each file in sequence
    const results = [];
    for (const file of req.files) {
      console.log(file)
      const result = await processAndSendFile(file);
      results.push(result);
    }
    
    // Return success response with jobIds
    res.status(200).json({ 
      message: `Successfully sent ${req.files.length} files for processing`,
      files: results
    });

  } catch (error) {
    console.error('Error in file upload process:', error);
    res.status(500).json({ 
      error: 'Failed to process files',
      details: error.message 
    });
  }
});

// Single file upload endpoint (keep for backward compatibility)
app.post('/upload-single', upload.single('myFile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded.');
  }

  try {
    const result = await processAndSendFile(req.file);
    
    // Return success response with jobId
    res.status(200).json({ 
      message: 'File sent for processing successfully',
      jobId: result.jobId,
      filename: result.filename
    });

  } catch (error) {
    console.error('Error in file upload process:', error);
    res.status(500).json({ 
      error: 'Failed to process file',
      details: error.message 
    });
  }
});

// Job status endpoint (mock implementation)
app.get('/job-status/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  
  // In a real implementation, you would check a database or cache
  // for the status of the job
  res.json({
    jobId,
    status: 'processing',
    message: 'File is being processed'
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server');
  if (producer) {
    await producer.disconnect();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT signal received: closing HTTP server');
  if (producer) {
    await producer.disconnect();
  }
  process.exit(0);
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
