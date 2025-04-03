// src/App.js
import React, { useState } from 'react';
import './App.css';
import UploadSection from './components/UploadSection';
import ActionSection from './components/ActionSection';
import ResultsSection from './components/ResultsSection';

function App() {
  const [currentView, setCurrentView] = useState('action'); // 'upload', 'action', 'results'
  const [results, setResults] = useState(null);
  const [resultsTitle, setResultsTitle] = useState('');
  const [searchKeyword, setSearchKeyword] = useState('');

  // Handle upload completion
  const handleUploadComplete = () => {
    setCurrentView('action');
  };

  // Handle search action
  const handleSearch = (data, keyword) => {
    setResults(data);
    setResultsTitle(`Search Results for "${keyword}"`);
    setSearchKeyword(keyword);
    setCurrentView('results');
  };

  // Handle top-n action
  const handleTopN = (data, n) => {
    setResults(data);
    setResultsTitle(`Top ${n} Words`);
    setSearchKeyword(''); // Clear search keyword as this is not a search result
    setCurrentView('results');
  };

  // Handle back button from results
  const handleBackToAction = () => {
    setCurrentView('action');
  };

  return (
    <div className="App">
      <div className="watermark">Search Engine</div>
      
      {currentView === 'upload' && (
        <UploadSection onUploadComplete={handleUploadComplete} />
      )}
      
      {currentView === 'action' && (
        <ActionSection onSearch={handleSearch} onTopN={handleTopN} />
      )}
      
      {currentView === 'results' && (
        <ResultsSection 
          title={resultsTitle}
          data={results}
          keyword={searchKeyword}
          onBack={handleBackToAction}
        />
      )}
    </div>
  );
}

export default App; 