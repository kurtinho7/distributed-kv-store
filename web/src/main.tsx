import React from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, Database, ListOrdered, Play, Power, RefreshCcw, RotateCcw, ShieldAlert, ShieldCheck, Terminal, Trash2 } from 'lucide-react';
import './styles.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080';
const DEMOCTL_URL = import.meta.env.VITE_DEMOCTL_URL ?? 'http://localhost:9090';

const NODES = [
  { id: 'node-1', url: 'http://localhost:8080' },
  { id: 'node-2', url: 'http://localhost:8081' },
  { id: 'node-3', url: 'http://localhost:8082' },
  { id: 'node-4', url: 'http://localhost:8083' },
  { id: 'node-5', url: 'http://localhost:8084' },
];

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

type RaftState = {
  nodeId: string;
  currentTerm: number;
  votedFor: string;
  role: 'follower' | 'candidate' | 'leader';
  leaderId: string;
  lastHeartbeat: string;
};

type NodeSnapshot = {
  id: string;
  url: string;
  reachable: boolean;
  raft?: RaftState;
  entries: Entry[];
  logEntries: LogEntry[];
};

type FaultState = {
  droppedReplicationTo: string[];
};

type DemoJobState = 'idle' | 'running' | 'passed' | 'failed';

type DemoJobStatus = {
  state: DemoJobState;
  exitCode?: number;
  output: string;
  startedAt?: string;
  endedAt?: string;
};

function App() {
  const [keyName, setKeyName] = React.useState('project');
  const [value, setValue] = React.useState('distributed-kv');
  const [result, setResult] = React.useState('Ready.');
  const [entries, setEntries] = React.useState<Entry[]>([]);
  const [members, setMembers] = React.useState<Member[]>([]);
  const [logEntries, setLogEntries] = React.useState<LogEntry[]>([]);
  const [nodeSnapshots, setNodeSnapshots] = React.useState<NodeSnapshot[]>([]);
  const [faultState, setFaultState] = React.useState<FaultState>({ droppedReplicationTo: [] });
  const [activeApiUrl, setActiveApiUrl] = React.useState(API_BASE_URL);
  const [chaosStatus, setChaosStatus] = React.useState<DemoJobStatus>({
    state: 'idle',
    output: '',
  });
  const [hammerStatus, setHammerStatus] = React.useState<DemoJobStatus>({
    state: 'idle',
    output: '',
  });
  const [hammerDuration, setHammerDuration] = React.useState(30);
  const [hammerWriters, setHammerWriters] = React.useState(10);
  const [hammerKeyspace, setHammerKeyspace] = React.useState(100);
  const [hammerReadAfterWrite, setHammerReadAfterWrite] = React.useState(false);

  const replicatedLogRows = React.useMemo(() => {
    const entriesByIndex = new Map<number, LogEntry>();

    for (const node of nodeSnapshots) {
      for (const entry of node.logEntries) {
        if (!entriesByIndex.has(entry.index)) {
          entriesByIndex.set(entry.index, entry);
        }
      }
    }

    return Array.from(entriesByIndex.values()).sort((a, b) => a.index - b.index);
  }, [nodeSnapshots]);

  const currentLeaderID = React.useMemo(() => {
    return nodeSnapshots.find((node) => node.raft?.role === 'leader')?.id ?? '';
  }, [nodeSnapshots]);

  const refresh = React.useCallback(async () => {
    const snapshots = await Promise.all(
      NODES.map(async (node): Promise<NodeSnapshot> => {
        try {
          const [raftResponse, kvResponse, logResponse] = await Promise.all([
            fetch(`${node.url}/raft`),
            fetch(`${node.url}/kv`),
            fetch(`${node.url}/log`),
          ]);

          const raftBody = await raftResponse.json();
          const kvBody = await kvResponse.json();
          const logBody = await logResponse.json();

          return {
            ...node,
            reachable: true,
            raft: raftBody,
            entries: kvBody.entries ?? [],
            logEntries: logBody.entries ?? [],
          };
        } catch {
          return {
            ...node,
            reachable: false,
            raft: undefined,
            entries: [],
            logEntries: [],
          };
        }
      }),
    );

    setNodeSnapshots(snapshots);

    const activeNode =
      snapshots.find((node) => node.reachable && node.raft?.role === 'leader') ??
      snapshots.find((node) => node.reachable);

    if (!activeNode) {
      throw new Error('no reachable nodes');
    }

    setActiveApiUrl(activeNode.url);

    const [kvResponse, clusterResponse, logResponse, faultsResponse] = await Promise.all([
      fetch(`${activeNode.url}/kv`),
      fetch(`${activeNode.url}/cluster`),
      fetch(`${activeNode.url}/log`),
      fetch(`${activeNode.url}/faults`),
    ]);
    const kvBody = await kvResponse.json();
    const clusterBody = await clusterResponse.json();
    const logBody = await logResponse.json();
    const faultsBody = await faultsResponse.json();
    setEntries(kvBody.entries ?? []);
    setMembers(clusterBody.members ?? []);
    setLogEntries(logBody.entries ?? []);
    setFaultState(faultsBody);
  }, []);

  React.useEffect(() => {
    refresh().catch(() => setResult('API is not reachable yet.'));
  }, [refresh]);

  const refreshChaosStatus = React.useCallback(async () => {
    const response = await fetch(`${DEMOCTL_URL}/demo/chaos/status`);
    const body = await response.json();
    setChaosStatus(body);
  }, []);

  const refreshHammerStatus = React.useCallback(async () => {
    const response = await fetch(`${DEMOCTL_URL}/demo/hammer/status`);
    const body = await response.json();
    setHammerStatus(body);
  }, []);

  React.useEffect(() => {
    refreshChaosStatus().catch(() => {
      setChaosStatus({
        state: 'idle',
        output: 'Demo controller is not reachable. Start it with: go run ./cmd/democtl',
      });
    });
    refreshHammerStatus().catch(() => {
      setHammerStatus({
        state: 'idle',
        output: 'Demo controller is not reachable. Start it with: go run ./cmd/democtl',
      });
    });
  }, [refreshChaosStatus, refreshHammerStatus]);

  React.useEffect(() => {
    if (chaosStatus.state !== 'running') {
      return;
    }

    const interval = window.setInterval(() => {
      refreshChaosStatus()
        .then(() => refresh())
        .catch(() => {
          setChaosStatus((current) => ({
            ...current,
            output: `${current.output}\nDemo controller became unreachable.`,
          }));
        });
    }, 1500);

    return () => window.clearInterval(interval);
  }, [chaosStatus.state, refresh, refreshChaosStatus]);

  React.useEffect(() => {
    if (hammerStatus.state !== 'running') {
      return;
    }

    const interval = window.setInterval(() => {
      refreshHammerStatus()
        .then(() => refresh())
        .catch(() => {
          setHammerStatus((current) => ({
            ...current,
            output: `${current.output}\nDemo controller became unreachable.`,
          }));
        });
    }, 1500);

    return () => window.clearInterval(interval);
  }, [hammerStatus.state, refresh, refreshHammerStatus]);

  async function putValue() {
    const response = await fetch(`${activeApiUrl}/kv/${encodeURIComponent(keyName)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value }),
    });
    const body = await response.json();
    setResult(response.ok ? `Stored ${body.key}.` : body.error);
    await refresh();
  }

  async function getValue() {
    const response = await fetch(`${activeApiUrl}/kv/${encodeURIComponent(keyName)}`);
    const body = await response.json();
    setResult(response.ok ? `${body.key} = ${body.value}` : body.error);
  }

  async function deleteValue() {
    const response = await fetch(`${activeApiUrl}/kv/${encodeURIComponent(keyName)}`, {
      method: 'DELETE',
    });
    setResult(response.ok ? `Deleted ${keyName}.` : (await response.json()).error);
    await refresh();
  }

  async function partitionNode(nodeID: string) {
    const response = await fetch(`${activeApiUrl}/faults/replication/${nodeID}`, {
      method: 'POST',
    });

    const body = await response.json();
    setResult(response.ok ? `Partitioned ${nodeID}.` : body.error);
    await refresh();
  }

  async function healNode(nodeID: string) {
    const response = await fetch(`${activeApiUrl}/faults/replication/${nodeID}`, {
      method: 'DELETE',
    });

    const body = await response.json();
    setResult(response.ok ? `Healed ${nodeID}.` : body.error);
    await refresh();

    if (response.ok) {
      window.setTimeout(() => {
        refresh().catch(() => setResult('API is not reachable yet.'));
      }, 1200);
    }
  }

  async function startChaosDemo() {
    const response = await fetch(`${DEMOCTL_URL}/demo/chaos/start`, {
      method: 'POST',
    });
    const body = await response.json();
    setChaosStatus(body);
    setResult(response.ok ? 'Chaos demo started.' : 'Chaos demo is already running.');
  }

  async function startHammer() {
    const response = await fetch(`${DEMOCTL_URL}/demo/hammer/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        durationSeconds: hammerDuration,
        writers: hammerWriters,
        keyspace: hammerKeyspace,
        readAfterWrite: hammerReadAfterWrite,
      }),
    });
    const body = await response.json();
    setHammerStatus(body);
    setResult(response.ok ? 'Traffic hammer started.' : 'Traffic hammer is already running.');
  }

  async function runNodeAction(nodeID: string, action: 'start' | 'stop' | 'restart') {
    const response = await fetch(`${DEMOCTL_URL}/demo/nodes/${nodeID}/${action}`, {
      method: 'POST',
    });
    const body = await response.json();
    setResult(response.ok ? `${action} sent to ${nodeID}.` : body.error ?? body.output ?? 'Node action failed.');

    window.setTimeout(() => {
      refresh().catch(() => setResult('API is not reachable yet.'));
    }, 1200);
  }

  return (
    <main>
      <header className="topbar">
        <div>
          <h1>Distributed KV Store</h1>
          <p>Active API: {activeApiUrl}</p>
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

        <div className="panel entries">
          <div className="panelTitle">
            <Activity size={18} />
            <h2>Cluster</h2>
          </div>
          <div className="nodeGrid">
            {nodeSnapshots.map((node) => (
              <article
                className={`node ${node.reachable ? '' : 'unhealthy'} ${node.raft?.role ?? ''}`}
                key={node.id}
              >
                <div className="nodeHeader">
                  <strong>{node.id}</strong>
                  <span className={`statusDot ${node.reachable ? 'online' : 'offline'}`} />
                </div>
                <span>{node.reachable ? node.raft?.role ?? 'unknown' : 'offline'}</span>
                <small>{node.url}</small>
                <small>term {node.raft?.currentTerm ?? '-'}</small>
                <small>leader {node.raft?.leaderId || '-'}</small>
                <small>log index {node.logEntries[node.logEntries.length - 1]?.index ?? 0}</small>
                <div className="nodeActions">
                  <button
                    className="danger iconOnly"
                    onClick={() => runNodeAction(node.id, 'stop')}
                    title={`Stop ${node.id}`}
                  >
                    <Power size={15} />
                  </button>
                  <button
                    className="iconOnly"
                    onClick={() => runNodeAction(node.id, 'start')}
                    title={`Start ${node.id}`}
                  >
                    <Play size={15} />
                  </button>
                  <button
                    className="secondary iconOnly"
                    onClick={() => runNodeAction(node.id, 'restart')}
                    title={`Restart ${node.id}`}
                  >
                    <RotateCcw size={15} />
                  </button>
                </div>
              </article>
            ))}
          </div>
        </div>

        <div className="panel operations">
          <div className="panelTitle">
            <ShieldAlert size={18} />
            <h2>Fault Controls</h2>
          </div>
          <div className="actions verticalActions">
            <div className="faultGrid">
              {NODES.map((node) => {
                const isPartitioned = faultState.droppedReplicationTo.includes(node.id);
                const isLeader = node.id === currentLeaderID;

                return (
                  <div className="faultNode" key={node.id}>
                    <strong>{node.id}</strong>
                    <div className="actions faultActions">
                      <button
                        className="danger"
                        disabled={isPartitioned || isLeader}
                        title={isLeader ? 'Current leader cannot be partitioned from itself' : undefined}
                        onClick={() => partitionNode(node.id)}
                      >
                        <ShieldAlert size={16} />
                        {isLeader ? 'Leader' : 'Partition'}
                      </button>
                      <button disabled={!isPartitioned} onClick={() => healNode(node.id)}>
                        <ShieldCheck size={16} />
                        Heal
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
            <button
              onClick={async () => {
                await Promise.all(NODES.map((node) => healNode(node.id)));
                await refresh();
              }}
            >
              <ShieldCheck size={16} />
              Heal All
            </button>
            <div className="faultSummary">
              {faultState.droppedReplicationTo.length === 0 ? (
                <span>No active partitions</span>
              ) : (
                faultState.droppedReplicationTo.map((nodeID) => (
                  <span className="faultPill" key={nodeID}>
                    {nodeID} partitioned
                  </span>
                ))
              )}
            </div>
          </div>
        </div>

        <div className="panel entries">
          <div className="panelTitle">
            <Terminal size={18} />
            <h2>Chaos Demo</h2>
          </div>
          <div className="chaosHeader">
            <span className={`jobBadge ${chaosStatus.state}`}>{chaosStatus.state}</span>
            <button disabled={chaosStatus.state === 'running'} onClick={startChaosDemo}>
              <Play size={16} />
              Run Chaos Demo
            </button>
            <button className="secondary" onClick={refreshChaosStatus}>
              <RefreshCcw size={16} />
              Status
            </button>
          </div>
          <pre className="jobOutput">
            {chaosStatus.output || 'Start the demo controller, then run the chaos demo from here.'}
          </pre>
        </div>

        <div className="panel entries">
          <div className="panelTitle">
            <Activity size={18} />
            <h2>Traffic Hammer</h2>
          </div>
          <div className="hammerControls">
            <label>
              Duration
              <input
                min={1}
                type="number"
                value={hammerDuration}
                onChange={(event) => setHammerDuration(Number(event.target.value))}
              />
            </label>
            <label>
              Writers
              <input
                min={1}
                type="number"
                value={hammerWriters}
                onChange={(event) => setHammerWriters(Number(event.target.value))}
              />
            </label>
            <label>
              Keyspace
              <input
                min={1}
                type="number"
                value={hammerKeyspace}
                onChange={(event) => setHammerKeyspace(Number(event.target.value))}
              />
            </label>
            <label className="checkboxLabel">
              <input
                checked={hammerReadAfterWrite}
                type="checkbox"
                onChange={(event) => setHammerReadAfterWrite(event.target.checked)}
              />
              Read after write
            </label>
          </div>
          <div className="chaosHeader">
            <span className={`jobBadge ${hammerStatus.state}`}>{hammerStatus.state}</span>
            <button disabled={hammerStatus.state === 'running'} onClick={startHammer}>
              <Play size={16} />
              Start Hammer
            </button>
            <button className="secondary" onClick={refreshHammerStatus}>
              <RefreshCcw size={16} />
              Status
            </button>
          </div>
          <pre className="jobOutput">
            {hammerStatus.output || 'Start the demo controller, then hammer the cluster from here.'}
          </pre>
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
            <h2>Replication Matrix</h2>
          </div>
          {replicatedLogRows.length === 0 ? (
            <p className="empty">No replicated operations yet.</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Index</th>
                  <th>Operation</th>
                  <th>Key</th>
                  {NODES.map((node) => (
                    <th key={node.id}>{node.id}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {replicatedLogRows.map((entry) => (
                  <tr key={entry.index}>
                    <td>{entry.index}</td>
                    <td>
                      <span className={`operationBadge ${entry.operation}`}>{entry.operation}</span>
                    </td>
                    <td>{entry.key}</td>
                    {NODES.map((node) => {
                      const snapshot = nodeSnapshots.find((item) => item.id === node.id);
                      const appliedEntry = snapshot?.logEntries.find((item) => item.index === entry.index);
                      const matches =
                        appliedEntry?.operation === entry.operation &&
                        appliedEntry?.key === entry.key &&
                        appliedEntry?.value === entry.value;

                      return (
                        <td key={node.id}>
                          <span className={`replicationCell ${matches ? 'applied' : 'missing'}`}>
                            {matches ? 'applied' : 'missing'}
                          </span>
                        </td>
                      );
                    })}
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
