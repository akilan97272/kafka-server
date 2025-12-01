import React from 'react'
export default function Header({ onStartWs }) {
  return (
    <div className="bg-white shadow">
      <div className="max-w-6xl mx-auto p-4 flex items-center justify-between">
        <h1 className="text-xl font-bold">Kafka React Dashboard</h1>
        <div className="flex gap-2">
          <button onClick={onStartWs} className="px-3 py-1 bg-blue-600 text-white rounded">Connect Stream</button>
        </div>
      </div>
    </div>
  )
}