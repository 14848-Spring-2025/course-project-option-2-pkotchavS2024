// src/components/UploadSection.js
import React, { useState, useEffect } from 'react';
import DebugOutput from './DebugOutput';

const UploadSection = ({ onUploadComplete }) => {
  const [files, setFiles] = useState([]);
  const [message, setMessage] = useState({ text: '', type: '' });
  const [processingStatus, setProcessingStatus] = useState([]);
  const [isUploading, setIsUploading] = useState(false);
  const [debugMessages, setDebugMessages] = useState([]);
  const [jobId, setJobId] = useState(null);
  const [pollInterval, setPollInterval] = useState(null);

  // Debug helper function
  const debug = (text) => {
    const timestamp = new Date().toLocaleTimeString();
    setDebugMessages(prev => [...prev, { timestamp, text }]);
    console.log(`[${timestamp}] ${text}`);
  };

  // Poll job status
  const checkJobStatus = async (id) => {
    try {
      debug(`Checking job status for: ${id}`);
      const response = await fetch(`http://localhost:3000/job/${id}/status`);
      
      if (!response.ok) {
        debug(`Status check failed: ${response.status} ${response.statusText}`);
        return;
      }
      
      const statusData = await response.json();
      debug(`Job status: ${statusData.status}, Progress: ${statusData.progress}%`);
      
      // Update the message with current status
      setMessage({ 
        text: `${statusData.message || 'Processing'} (${statusData.progress || 0}%)`, 
        type: 'info' 
      });
      
      // If the job is completed or failed, stop polling
      if (statusData.status === 'completed') {
        debug('Job completed successfully');
        clearInterval(pollInterval);
        setPollInterval(null);
        setIsUploading(false);
        setMessage({ text: 'Files processed successfully!', type: 'success' });
        
        // Wait a moment, then show alert and transition
        setTimeout(() => {
          debug("Showing completion alert");
          alert("Engine was loaded & Inverted Indices were created successfully!");
          debug("Transitioning to action section");
          onUploadComplete();
        }, 1000);
      } 
      else if (statusData.status === 'failed' || statusData.status === 'timeout') {
        debug(`Job failed: ${statusData.message}`);
        clearInterval(pollInterval);
        setPollInterval(null);
        setIsUploading(false);
        setMessage({ text: `Processing failed: ${statusData.message}`, type: 'error' });
      }
      
      // Update processing status with more details
      if (statusData.filesCount) {
        setProcessingStatus([
          { 
            filename: `Batch of ${statusData.filesCount} files`, 
            jobId: id,
            progress: statusData.progress || 0,
            status: statusData.status
          }
        ]);
      }
      
    } catch (error) {
      debug(`Error checking job status: ${error.message}`);
      console.error('Status check error:', error);
    }
  };

  // Clean up interval on component unmount
  useEffect(() => {
    return () => {
      if (pollInterval) {
        clearInterval(pollInterval);
      }
    };
  }, [pollInterval]);

  // Handle file selection
  const handleFileChange = (e) => {
    e.preventDefault();
    debug("File selection changed");
    
    const selectedFiles = Array.from(e.target.files);
    setFiles(selectedFiles);
    
    if (selectedFiles.length === 0) {
      debug("No files selected");
    } else {
      debug(`Selected ${selectedFiles.length} files`);
    }
  };

  // Handle upload
  const handleUpload = async (e) => {
    e.preventDefault();
    
    // If already uploading, ignore the click
    if (isUploading) {
      debug("Upload already in progress, ignoring click");
      return;
    }
    
    debug("Upload button clicked");
    
    setMessage({ text: '', type: '' });
    setProcessingStatus([]);
    
    if (!files || files.length === 0) {
      setMessage({ text: 'Please select files to upload.', type: 'error' });
      debug("No files selected");
      return;
    }
    
    // Set uploading flag
    setIsUploading(true);
    
    // Show uploading status
    setMessage({ text: 'Uploading files...', type: 'info' });
    debug("Starting upload process");
    
    // Create FormData with all selected files
    const formData = new FormData();
    for (const file of files) {
      formData.append("files", file);
      debug(`Added file to form: ${file.name}`);
    }
    
    try {
      debug("Sending fetch request");
      const response = await fetch("http://localhost:3000/upload", {
        method: "POST",
        body: formData,
      });
      
      if (!response.ok) {
        setIsUploading(false);
        throw new Error(`Server error: ${response.status} ${response.statusText}`);
      }
      
      const result = await response.json();
      debug(`Response received: ${JSON.stringify(result)}`);
      
      // Store the batch job ID
      const batchId = result.batchJobId;
      setJobId(batchId);
      debug(`Batch job ID: ${batchId}`);
      
      setMessage({ 
        text: `Files uploaded. Processing started (0%)`, 
        type: 'info' 
      });
      
      // Set initial processing status
      setProcessingStatus([
        { 
          filename: `Batch of ${result.filesCount} files`, 
          jobId: batchId,
          progress: 0,
          status: 'processing'
        }
      ]);
      
      // Start polling for job status (every 2 seconds)
      debug("Starting status polling");
      const interval = setInterval(() => {
        checkJobStatus(batchId);
      }, 2000);
      
      setPollInterval(interval);
      
    } catch (error) {
      // Reset upload state on error
      setIsUploading(false);
      
      debug(`Error occurred: ${error.message}`);
      console.error(error);
      setMessage({ text: `Upload failed: ${error.message}`, type: 'error' });
    }
  };

  return (
    <div className="container">
      <h1>Load My Engine</h1>
      <div>
        <div className="upload-section">
          <input 
            type="file" 
            name="files" 
            id="file-upload" 
            multiple 
            onChange={handleFileChange}
            disabled={isUploading} 
          />
        </div>
        <button 
          type="button" 
          className="big-button" 
          onClick={handleUpload} 
          disabled={isUploading}
        >
          {isUploading ? 'Uploading...' : 'Upload'}
        </button>
      </div>
      
      <div>
        {files.length > 0 && (
          <ul>
            {files.map((file, index) => (
              <li key={index}>{file.name}</li>
            ))}
          </ul>
        )}
      </div>
      
      {message.text && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}
      
      {processingStatus.length > 0 && (
        <div className="status-container">
          <p>Processing status:</p>
          <ul>
            {processingStatus.map((status, index) => (
              <li key={index}>
                <div>
                  <strong>{status.filename}</strong> (Job ID: {status.jobId})
                  {status.progress !== undefined && (
                    <div className="progress-bar">
                      <div 
                        className="progress" 
                        style={{width: `${status.progress}%`}}
                      ></div>
                      <span className="progress-text">{status.progress}%</span>
                    </div>
                  )}
                  <div className="status-badge">{status.status || 'processing'}</div>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
      
      <DebugOutput messages={debugMessages} />
    </div>
  );
};

export default UploadSection;