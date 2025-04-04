// src/components/DebugOutput.js
import React, { useRef, useEffect } from 'react';

const DebugOutput = ({ messages }) => {
  const debugRef = useRef(null);

  useEffect(() => {
    // Auto-scroll to bottom when new messages arrive
    if (debugRef.current) {
      debugRef.current.scrollTop = debugRef.current.scrollHeight;
    }
  }, [messages]);

  return (
    <div className="debug-output" ref={debugRef}>
      {messages.map((msg, index) => (
        <div key={index}>[{msg.timestamp}] {msg.text}</div>
      ))}
    </div>
  );
};

export default DebugOutput;