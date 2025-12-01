import React, { useEffect, useState, useRef } from 'react'
import axios from 'axios'
import Header from './components/Header'
import TopicList from './components/TopicList'
import TopicViewer from './components/TopicViewer'
import KsqlConsole from './components/KsqlConsole'

export default function App() {
  const [topics, setTopics] = useState([])
  const [selected, setSelected] = useState(null)
  const [messages, setMessages] = useState([])
  const wsRef = useRef(null)

  useEffect(() => {
    fetchTopics()
  }, [])

  async function fetchTopics() {
    try {
      const r = await axios.get('/api/topics')
      setTopics(r.data)
    } catch (e) {
      console.error(e)
      alert('Failed to fetch topics')
    }
  }

  function startWs() {
    if (wsRef.current) wsRef.current.close()
    const protocol = window.location.hostname === 'localhost' ? 'ws' : 'ws'
    const url = `${protocol}://${window.location.hostname}:8000/ws`
    const ws = new WebSocket(url)
    ws.onopen = () => console.log('ws open')
    ws.onmessage = e => {
      try {
        const payload = JSON.parse(e.data)
        if (payload.type === 'row') setMessages(m => [payload.row.columns, ...m].slice(0,200))
        if (payload.type === 'error') console.error('ksqldb error', payload.error)
      } catch (err) { console.error(err) }
    }
    ws.onclose = () => console.log('ws closed')
    wsRef.current = ws
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header onStartWs={startWs} />
      <div className="max-w-6xl mx-auto p-4 grid grid-cols-4 gap-4">
        <div className="col-span-1 bg-white p-4 rounded shadow">
          <TopicList topics={topics} onRefresh={fetchTopics} onSelect={setSelected} />
        </div>
        <div className="col-span-3 bg-white p-4 rounded shadow">
          <TopicViewer selected={selected} messages={messages} />
          <div className="mt-4">
            <KsqlConsole onRun={() => { /* optional run */ }} />
          </div>
        </div>
      </div>
    </div>
  )
}