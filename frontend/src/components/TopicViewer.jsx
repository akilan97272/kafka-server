import React from 'react'

export default function TopicViewer({ selected, messages }) {
  return (
    <div>
      <h2 className="text-lg font-bold">Topic: {selected || '—'}</h2>
      <div className="mt-3">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="bg-gray-100">
              <th className="p-2 text-left">#</th>
              <th className="p-2 text-left">Message</th>
            </tr>
          </thead>
          <tbody>
            {messages.map((m,i) => (
              <tr key={i} className="border-t">
                <td className="p-2 align-top">{i+1}</td>
                <td className="p-2"><pre className="whitespace-pre-wrap">{JSON.stringify(m)}</pre></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}