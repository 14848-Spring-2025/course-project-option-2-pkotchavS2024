import express from 'express';
import multer from 'multer';
import cors from 'cors';
import { Storage } from '@google-cloud/storage';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
const PORT = 3000;

// Configure Google Cloud Storage
const storage = new Storage({

    projectId: "aerial-jigsaw-449121-f0",
    keyFilename: "my-service.json",
  
  });
const bucketName = "bucket-14848";
const bucket = storage.bucket(bucketName);

// Multer storage in memory (no local disk writes)
const multerStorage = multer.memoryStorage();
const upload = multer({ storage: multerStorage });

app.post('/upload', upload.single('myFile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded.');
  }

  const sanitizedFileName = req.file.originalname.replace(/\s+/g, "_"); 
  const blob = bucket.file(`${Date.now()}_${sanitizedFileName}`);  
  const blobStream = blob.createWriteStream({
    resumable: false,
    metadata: {
      contentType: req.file.mimetype,
    },
  });

  blobStream.on('finish', async () => {
    const publicUrl = `https://storage.googleapis.com/${bucketName}/${blob.name}`;
    res.status(200).json({ message: 'File uploaded successfully', url: publicUrl });
  });

  blobStream.on('error', (err) => {
    console.error(err);
    res.status(500).send('Error uploading file');
  });

  blobStream.end(req.file.buffer);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
