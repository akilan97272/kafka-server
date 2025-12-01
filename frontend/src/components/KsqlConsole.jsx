import React, { useState } from 'react'
import axios from 'axios'

export default function KsqlConsole({ onRun }) {
  const [sql, setSql] = useState('SELECT * FROM new_template_stream EMIT CHANGES;')
  const [output, setOutput] = useState('')

  async function run() {
    try {
      const r = await axios.post('/api/ksql', { ksql: sql })
      setOutput(JSON.stringify(r.data, null, 2))
    } catch (e) { setOutput(String(e)) }
  }

  return (
    <div>
      <textarea className="w-full border p-2" rows={4} value={sql} onChange={e=>setSql(e.target.value)} />
      <div className="flex gap-2 mt-2">
        <button onClick={run} className="px-3 py-1 bg-indigo-600 text-white rounded">Run</button>
      </div>
      <pre className="mt-2 bg-gray-900 text-white p-2 rounded max-h-52 overflow-auto">{output}</pre>
    </div>
  )
}