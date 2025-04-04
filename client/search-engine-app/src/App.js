// App.js
import React, { useState } from 'react';
import './App.css';
import UploadSection from './components/UploadSection';
import ActionSection from './components/ActionSection';
import ResultsSection from './components/ResultsSection';

function App() {
  const [currentView, setCurrentView] = useState('upload'); // 'upload', 'action', 'results'
  const [results, setResults] = useState(null);
  const [resultsTitle, setResultsTitle] = useState('');
  const [searchKeyword, setSearchKeyword] = useState('');
  const [timeTaken, setTimeTaken] = useState(0);

  // Handle upload completion
  const handleUploadComplete = () => {
    setCurrentView('action');
  };

  // Handle search action
  const handleSearch = (data, keyword, time) => {
    setResults(data);
    setResultsTitle(`Search Results for "${keyword}"`);
    setSearchKeyword(keyword);
    setTimeTaken(time);
    setCurrentView('results');
  };

  // Handle top-n action
  const handleTopN = (data, n, time) => {
    setResults(data);
    setResultsTitle(`Top ${n} Words`);
    setSearchKeyword(''); // Clear search keyword as this is not a search result
    setTimeTaken(time);
    setCurrentView('results');
  };

  // Handle back button from results
  const handleBackToAction = () => {
    setCurrentView('action');
  };

  // Handle back button from action
  const handleBackToUpload = () => {
    setCurrentView('upload');
  };

  return (
    <div className="App">
      <div className="watermark">Search Engine</div>

      {currentView === 'upload' && (
        <UploadSection onUploadComplete={handleUploadComplete} />
      )}

      {currentView === 'action' && (
        <ActionSection 
          onSearch={handleSearch} 
          onTopN={handleTopN} 
          onBack={handleBackToUpload} 
        />
      )}

      {currentView === 'results' && (
        <ResultsSection 
          title={resultsTitle}
          data={results}
          keyword={searchKeyword}
          timeTaken={timeTaken}
          onBack={handleBackToAction}
        />
      )}
    </div>
  );
}

export default App;
