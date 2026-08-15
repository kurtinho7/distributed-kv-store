import React from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, Database, ListOrdered, Play, RefreshCcw, Trash2 } from 'lucide-react';
import './styles.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080';

type Entry = {
  key: string;
  value: string;
};

type Member = {
  id: string;
  address: string;
  role: 'leader' | 'follower';
  healthy: boolean;
  logIndex: number;
  updatedAt: string;
};

type LogEntry = {
  index: number;
  operation: 'put' | 'delete';
  key: string;
  value?: string;
  createdAt: string;
};

function App() {
  const [keyName, setKeyName] = React.useState('project');
  const [value, setValue] = React.useState('distributed-kv');
  const [result, setResult] = React.useState('Ready.');
  const [entries, setEntries] = React.useState<Entry[]>([]);
  const [members, setMembers] = React.useState<Member[]>([]);
  const [logEntries, setLogEntries] = React.useState<LogEntry[]>([]);

  const refresh = React.useCallback(async () => {
    const [kvResponse, clusterResponse, logResponse] = await Promise.all([
      fetch(`${API_BASE_URL}/kv`),
      fetch(`${API_BASE_URL}/cluster`),
      fetch(`${API_BASE_URL}/log`),
    ]);
    const kvBody = await kvResponse.json();
    const clusterBody = await clusterResponse.json();
    const logBody = await logResponse.json();
    setEntries(kvBody.entries ?? []);
    setMembers(clusterBody.members ?? []);
    setLogEntries(logBody.entries ?? []);
  }, []);

  React.useEffect(() => {
    refresh().catch(() => setResult('API is not reachable yet.'));
  }, [refresh]);

  async function putValue() {
    const response = await fetch(`${API_BASE_URL}/kv/${encodeURIComponent(keyName)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value }),
    });
    const body = await response.json();
    setResult(response.ok ? `Stored ${body.key}.` : body.error);
    await refresh();
  }

  async function getValue() {
    const response = await fetch(`${API_BASE_URL}/kv/${encodeURIComponent(keyName)}`);
    const body = await response.json();
    setResult(response.ok ? `${body.key} = ${body.value}` : body.error);
  }

  async function deleteValue() {
    const response = await fetch(`${API_BASE_URL}/kv/${encodeURIComponent(keyName)}`, {
      method: 'DELETE',
    });
    setResult(response.ok ? `Deleted ${keyName}.` : (await response.json()).error);
    await refresh();
  }

  return (
    <main>
      <header className="topbar">
        <div>
          <h1>Distributed KV Store</h1>
          <p>Single-node skeleton today, cluster-ready architecture tomorrow.</p>
        </div>
        <button className="iconButton" onClick={refresh} title="Refresh cluster state">
          <RefreshCcw size={18} />
        </button>
      </header>

      <section className="layout">
        <div className="panel operations">
          <div className="panelTitle">
            <Database size={18} />
            <h2>KV Operations</h2>
          </div>
          <label>
            Key
            <input value={keyName} onChange={(event) => setKeyName(event.target.value)} />
          </label>
          <label>
            Value
            <input value={value} onChange={(event) => setValue(event.target.value)} />
          </label>
          <div className="actions">
            <button onClick={putValue}>
              <Play size={16} />
              Put
            </button>
            <button onClick={getValue}>Get</button>
            <button className="danger" onClick={deleteValue}>
              <Trash2 size={16} />
              Delete
            </button>
          </div>
          <output>{result}</output>
        </div>

        <div className="panel">
          <div className="panelTitle">
            <Activity size={18} />
            <h2>Cluster</h2>
          </div>
          <div className="nodeGrid">
            {members.map((member) => (
              <article className="node" key={member.id}>
                <strong>{member.id}</strong>
                <span>{member.role}</span>
                <small>{member.address}</small>
                <small>log index {member.logIndex}</small>
              </article>
            ))}
          </div>
        </div>

        <div className="panel entries">
          <div className="panelTitle">
            <Database size={18} />
            <h2>Stored Keys</h2>
          </div>
          {entries.length === 0 ? (
            <p className="empty">No keys stored yet.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((entry) => (
                  <tr key={entry.key}>
                    <td>{entry.key}</td>
                    <td>{entry.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="panel entries">
          <div className="panelTitle">
            <ListOrdered size={18} />
            <h2>Operation Log</h2>
          </div>
          {logEntries.length === 0 ? (
            <p className="empty">No mutations logged yet.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Index</th>
                  <th>Operation</th>
                  <th>Key</th>
                  <th>Value</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {logEntries.map((entry) => (
                  <tr key={entry.index}>
                    <td>{entry.index}</td>
                    <td>
                      <span className={`operationBadge ${entry.operation}`}>{entry.operation}</span>
                    </td>
                    <td>{entry.key}</td>
                    <td>{entry.value ?? ''}</td>
                    <td>{new Date(entry.createdAt).toLocaleTimeString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
