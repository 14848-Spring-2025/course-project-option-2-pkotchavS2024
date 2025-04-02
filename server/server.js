import express from 'express';
import multer from 'multer';
import cors from 'cors';
import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
const PORT = 3000;

// Configure Confluent Kafka
const kafka = new Kafka({
  clientId: 'search-engine-uploader',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVER || 'your-bootstrap-server'],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_API_KEY || 'your-api-key',
    password: process.env.KAFKA_API_SECRET || 'your-api-secret'
  }
});

const producer = kafka.producer();

// Connect to Kafka on server startup
(async () => {
  try {
    await producer.connect();
    console.log('Connected to Confluent Kafka');
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
  }
})();

// Multer storage in memory (no local disk writes)
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage });

app.post('/upload', upload.single('myFile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded.');
  }

  try {
    // Generate a unique job ID
    const jobId = uuidv4();
    
    // Sanitize the filename
    const sanitizedFileName = req.file.originalname.replace(/\s+/g, "_");
    
    // Convert file buffer to base64 to send through Kafka
    const fileBase64 = req.file.buffer.toString('base64');
    
    // Send message to Kafka
    await producer.send({
      topic: 'file-processing',
      messages: [
        { 
          key: sanitizedFileName,
          value: JSON.stringify({
            jobId,
            filename: sanitizedFileName,
            fileType: req.file.mimetype,
            fileSize: req.file.size,
            fileContent: fileBase64,  // Include the file content in base64
            timestamp: Date.now()
          })
        }
      ]
    });

    console.log(`File sent to Kafka: ${sanitizedFileName}`);
    
    // Return success response with jobId
    res.status(200).json({ 
      message: 'File sent for processing successfully',
      jobId: jobId
    });

  } catch (error) {
    console.error('Error in file upload process:', error);
    res.status(500).json({ 
      error: 'Failed to process file',
      details: error.message 
    });
  }
});

app.get('/job-status/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  
  // In a real implementation, you would check a database or cache
  // for the status of the job, or set up a consumer to listen for status updates
  
  // For demonstration, we're returning a mock response
  res.json({
    jobId,
    status: 'processing',
    message: 'File is being processed'
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server');
  await producer.disconnect();
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
