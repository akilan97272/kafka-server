import React, { useState } from 'react'
import axios from 'axios'

export default function TopicList({ topics, onRefresh, onSelect }) {
  const [name, setName] = useState('')

  async function createTopic() {
    if (!name.trim()) return alert("Enter a topic name")
    try {
      await axios.post('/api/topics', { name })
      setName('')
      onRefresh()
    } catch (e) {
      alert('Failed to create topic')
    }
  }

  async function deleteTopic(topic) {
    if (!confirm(`Delete topic "${topic}"?`)) return
    try {
      await axios.delete(`/api/topics/${topic}`)
      onRefresh()
    } catch (e) {
      alert('Failed to delete topic')
    }
  }

  return (
    <div>
      <div className="mb-4 space-y-2">
        <input 
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder="Topic name"
          className="border p-2 rounded w-full"
        />
        <button 
          onClick={createTopic}
          className="w-full bg-green-600 text-white p-2 rounded"
        >
          Create Topic
        </button>
      </div>

      <h3 className="font-semibold mb-2">Topics</h3>
      <ul className="space-y-2 max-h-[60vh] overflow-auto">
        {topics.map(t => (
          <li
            key={t.name}
            className="p-2 rounded border flex justify-between items-center hover:bg-gray-50"
          >
            <span className="cursor-pointer" onClick={() => onSelect(t.name)}>
              {t.name}
            </span>

            <button
              onClick={() => deleteTopic(t.name)}
              className="text-red-600 hover:text-red-900"
            >
              Delete
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
