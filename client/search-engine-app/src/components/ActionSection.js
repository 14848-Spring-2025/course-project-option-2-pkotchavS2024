import React, { useState } from 'react';
import DebugOutput from './DebugOutput';

const ActionSection = ({ onSearch, onTopN, onBack }) => {
  const [keyword, setKeyword] = useState('');
  const [topN, setTopN] = useState(10);
  const [debugMessages, setDebugMessages] = useState([]);

  // Debug helper function
  const debug = (text) => {
    const timestamp = new Date().toLocaleTimeString();
    setDebugMessages(prev => [...prev, { timestamp, text }]);
    console.log(`[${timestamp}] ${text}`);
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    
    const trimmedKeyword = keyword.trim();
    
    if (!trimmedKeyword) {
      alert("Please enter a keyword to search");
      return;
    }
    
    debug(`Search button clicked with keyword: ${trimmedKeyword}`);
    
    try {
      debug("Sending search request");
      const response = await fetch(`http://localhost:3000/search?keyword=${encodeURIComponent(trimmedKeyword)}`);
      
      if (!response.ok) {
        throw new Error(`Server error: ${response.status} ${response.statusText}`);
      }
      
      const result = await response.json();
      debug(`Search results received: ${JSON.stringify(result)}`);
      
      // Call the parent's callback with the results
      onSearch(result, trimmedKeyword);
      
    } catch (error) {
      debug(`Search error: ${error.message}`);
      alert(`Search failed: ${error.message}`);
    }
  };
  
  const handleTopN = async (e) => {
    e.preventDefault();
    
    const n = topN;
    
    if (!n || isNaN(parseInt(n)) || parseInt(n) < 1) {
      alert("Please enter a valid number");
      return;
    }
    
    debug(`Top-N button clicked with N: ${n}`);
    
    try {
      debug("Sending top-n request");
      const response = await fetch(`http://localhost:3000/topn?n=${encodeURIComponent(n)}`);
      
      if (!response.ok) {
        throw new Error(`Server error: ${response.status} ${response.statusText}`);
      }
      
      const result = await response.json();
      debug(`Top-N results received: ${JSON.stringify(result)}`);
      
      // Call the parent's callback with the results
      onTopN(result, n);
      
    } catch (error) {
      debug(`Top-N error: ${error.message}`);
      alert(`Top-N failed: ${error.message}`);
    }
  };

  const handleBack = (e) => {
    e.preventDefault();
    debug("Back button clicked");
    if (onBack) onBack();
  };

  return (
    <div className="container">
      <h1>Please Select Action</h1>
      
      <div className="input-group">
        <label htmlFor="search-input">Search Keyword:</label>
        <input 
          type="text" 
          id="search-input" 
          placeholder="Enter keyword to search..." 
          value={keyword}
          onChange={(e) => setKeyword(e.target.value)}
        />
      </div>
      <button className="big-button" onClick={handleSearch}>Search</button>

      <div className="input-group">
        <label htmlFor="topn-input">Top N Value:</label>
        <input 
          type="number" 
          id="topn-input" 
          placeholder="Enter a number..." 
          min="1" 
          value={topN}
          onChange={(e) => setTopN(e.target.value)}
        />
      </div>
      <button className="big-button" onClick={handleTopN}>Top-N</button>
      
      {onBack && (
        <button className="big-button" onClick={handleBack}>Back</button>
      )}
      
      <DebugOutput messages={debugMessages} />
    </div>
  );
};

export default ActionSection;