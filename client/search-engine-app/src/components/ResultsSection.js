import React, { useState, useEffect } from 'react';
import DebugOutput from './DebugOutput';

const ResultsSection = ({ title, data, keyword, onBack }) => {
  const [debugMessages, setDebugMessages] = useState([]);

  // Debug helper function
  const debug = (text) => {
    const timestamp = new Date().toLocaleTimeString();
    setDebugMessages(prev => [...prev, { timestamp, text }]);
    console.log(`[${timestamp}] ${text}`);
  };

  // Move the debug call inside useEffect to prevent it from running on every render
  useEffect(() => {
    debug(`Displaying results: ${JSON.stringify(data)}`);
  }, [data]); // Only run when data changes

  const handleBack = (e) => {
    e.preventDefault();
    debug("Back button clicked");
    onBack();
  };

  const renderSearchResults = () => {
    // Check if we have results for this keyword
    const results = data;
    
    if (!results || Object.keys(results).length === 0) {
      return <p>No results found for "{keyword}".</p>;
    }
    
    // Process each term in the results
    return Object.keys(results).map(term => (
      <div key={term}>
        <h3>Results for "{term}"</h3>
        <ul>
          {results[term].map((doc, index) => (
            <li key={index}>
              <div><strong>Document:</strong> {doc.doc_id}</div>
              <div><strong>Score:</strong> {doc.score}</div>
            </li>
          ))}
        </ul>
      </div>
    ));
  };

  const renderTopNResults = () => {
    return (
      <ul>
        {data.map((item, index) => (
          <li key={index}>
            <div><strong>Word:</strong> {item.word}</div>
            <div><strong>Frequency:</strong> {item.frequency}</div>
          </li>
        ))}
      </ul>
    );
  };

  return (
    <div className="container">
      <h1>Results</h1>
      <div>
        <h2>{title}</h2>
        
        {!data || Object.keys(data).length === 0 ? (
          <p>No results found.</p>
        ) : (
          keyword ? renderSearchResults() : renderTopNResults()
        )}
      </div>
      <button className="big-button" onClick={handleBack}>Back</button>
      
      <DebugOutput messages={debugMessages} />
    </div>
  );
};

export default ResultsSection;