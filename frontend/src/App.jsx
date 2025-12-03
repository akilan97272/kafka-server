import React, { useEffect, useState, useRef } from "react";
import axios from "axios";

export default function App() {
  const [topics, setTopics] = useState([]);
  const [count, setCount] = useState(0);
  const [selected, setSelected] = useState(null);
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    fetchTopics();
  }, []);

  async function fetchTopics() {
    try {
      const r = await axios.get("/api/topics");
      setTopics(r.data.topics);
      setCount(r.data.count);
    } catch (e) {
      console.error(e);
    }
  }

  async function loadLogs(topic) {
    setLoading(true);
    try {
      const r = await axios.get("/api/logs", { params: { topic, limit: 200 } });
      setLogs(r.data);
      setSelected(topic);
    } catch (e) {
      console.error(e);
      setLogs([]);
    } finally {
      setLoading(false);
    }
  }

  function connectWS(topic) {
    // build WebSocket URL using same origin so nginx proxies /ws to backend
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${window.location.host}/ws`;
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    const ws = new WebSocket(url);
    ws.onopen = () => {
      ws.send(JSON.stringify({ type: "subscribe", topic }));
      console.log("ws connected");
    };
    ws.onmessage = (e) => {
      try {
        const payload = JSON.parse(e.data);
        if (payload.type === "row") {
          // prepend if matches selected topic
          if (payload.row.topic === selected) {
            setLogs((s) => [payload.row, ...s].slice(0, 500));
          }
        } else if (payload.type === "info") {
          console.log("info:", payload.message);
        } else if (payload.type === "error") {
          console.error("ws error:", payload.error);
        }
      } catch (err) {
        console.error(err);
      }
    };
    ws.onclose = () => console.log("ws closed");
    ws.onerror = (e) => console.error("ws err", e);
    wsRef.current = ws;
  }

  function subscribeToTopic(topic) {
    loadLogs(topic);
    connectWS(topic);
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-6xl mx-auto">
        <header className="flex justify-between items-center mb-6">
          <h1 className="text-3xl font-bold">Kafka → Timescale Dashboard</h1>
          <div className="text-sm text-gray-600">Topics: {count}</div>
        </header>

        <div className="grid grid-cols-4 gap-4">
          <div className="col-span-1 bg-white p-4 rounded shadow">
            <h3 className="font-semibold mb-2">Topics</h3>
            <button onClick={fetchTopics} className="mb-2 text-sm text-blue-600">Refresh</button>
            <ul className="space-y-2 max-h-[60vh] overflow-auto">
              {topics.map((t) => (
                <li key={t} className="p-2 rounded border flex justify-between items-center hover:bg-gray-50">
                  <span className="cursor-pointer" onClick={() => subscribeToTopic(t)}>{t}</span>
                </li>
              ))}
            </ul>
          </div>

          <div className="col-span-3 bg-white p-4 rounded shadow">
            <h2 className="text-lg font-bold mb-2">Topic: {selected || "—"}</h2>
            <div className="mb-2">
              <button onClick={() => selected && loadLogs(selected)} className="px-3 py-1 bg-gray-200 rounded mr-2">Reload</button>
            </div>
            {loading ? <div>Loading...</div> : (
              <div className="mt-3">
                <table className="min-w-full text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="p-2 text-left">#</th>
                      <th className="p-2 text-left">Timestamp</th>
                      <th className="p-2 text-left">Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {logs.map((m, i) => (
                      <tr key={i} className="border-t">
                        <td className="p-2 align-top">{i + 1}</td>
                        <td className="p-2">{new Date(m.ts).toLocaleString()}</td>
                        <td className="p-2"><pre className="whitespace-pre-wrap">{JSON.stringify(m.value, null, 2)}</pre></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
