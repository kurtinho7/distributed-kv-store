import React from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, CheckCircle2, Database, ListOrdered, Play, Power, RefreshCcw, RotateCcw, ShieldAlert, ShieldCheck, Terminal, Trash2 } from 'lucide-react';
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

type VerifyLogIndex = {
  nodeId: string;
  index: number;
};

type VerifySummary = {
  title: string;
  reachableNodes?: number;
  leaders?: number;
  logIndexes: VerifyLogIndex[];
  logsMatch?: boolean;
};

type ScenarioSummary = {
  title: string;
  initialLeader?: string;
  newLeader?: string;
  partitionedFollower?: string;
  lagEntries?: number;
  writeSuccesses?: number;
  writeFailures?: number;
  availability?: number;
  correctness: string;
  finalLogIndex?: number;
  logIndexes: VerifyLogIndex[];
};

function parseVerifySummary(status: DemoJobStatus): VerifySummary {
  const output = status.output;
  const reachableMatch = output.match(/Reachable nodes:\s+(\d+)/);
  const leadersMatch = output.match(/(?:Leaders|Leader count):\s+(\d+)/);
  const logIndexes = Array.from(output.matchAll(/(node-\d+)=(\d+)/g)).map((match) => ({
    nodeId: match[1],
    index: Number(match[2]),
  }));

  const uniqueIndexes = new Set(logIndexes.map((entry) => entry.index));
  const logsMatch = logIndexes.length > 0 ? uniqueIndexes.size === 1 : undefined;

  let title = 'Not run';
  if (status.state === 'running') {
    title = 'Checking';
  } else if (status.state === 'passed' && output.includes('Cluster verified')) {
    title = 'Verified';
  } else if (status.state === 'failed') {
    title = 'Needs attention';
  }

  return {
    title,
    reachableNodes: reachableMatch ? Number(reachableMatch[1]) : undefined,
    leaders: leadersMatch ? Number(leadersMatch[1]) : undefined,
    logIndexes,
    logsMatch,
  };
}

function parseScenarioSummary(status: DemoJobStatus): ScenarioSummary {
  const output = status.output;
  const initialLeaderMatch = output.match(/current leader:\s+(node-\d+)/);
  const newLeaderMatch = output.match(/new leader:\s+(node-\d+)/);
  const partitionedFollowerMatch = output.match(/partitioned follower:\s+(node-\d+)/);
  const lagMatch = output.match(/lag while partitioned:\s+(\d+) entries/);
  const writeSuccessesMatch = output.match(/Write successes:\s+(\d+)/);
  const writeFailuresMatch = output.match(/Write failures:\s+(\d+)/);
  const logIndexes = Array.from(output.matchAll(/(node-\d+)=(\d+)/g)).map((match) => ({
    nodeId: match[1],
    index: Number(match[2]),
  }));
  const uniqueIndexes = new Set(logIndexes.map((entry) => entry.index));
  const finalLogIndex = uniqueIndexes.size === 1 && logIndexes.length > 0 ? logIndexes[0].index : undefined;
  const writeSuccesses = writeSuccessesMatch ? Number(writeSuccessesMatch[1]) : undefined;
  const writeFailures = writeFailuresMatch ? Number(writeFailuresMatch[1]) : undefined;
  const totalWrites =
    writeSuccesses !== undefined && writeFailures !== undefined ? writeSuccesses + writeFailures : undefined;
  const availability =
    totalWrites !== undefined && totalWrites > 0 && writeSuccesses !== undefined
      ? Math.round((writeSuccesses / totalWrites) * 100)
      : undefined;

  let correctness = '-';
  if (status.state === 'passed' && output.includes('Cluster verified')) {
    correctness = 'verified';
  } else if (status.state === 'failed') {
    correctness = 'failed';
  } else if (status.state === 'running') {
    correctness = 'checking';
  }

  let title = 'Not run';
  if (status.state === 'running') {
    title = 'Running';
  } else if (status.state === 'passed') {
    title = 'Passed';
  } else if (status.state === 'failed') {
    title = 'Failed';
  }

  return {
    title,
    initialLeader: initialLeaderMatch?.[1],
    newLeader: newLeaderMatch?.[1],
    partitionedFollower: partitionedFollowerMatch?.[1],
    lagEntries: lagMatch ? Number(lagMatch[1]) : undefined,
    writeSuccesses,
    writeFailures,
    availability,
    correctness,
    finalLogIndex,
    logIndexes,
  };
}

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
  const [verifyStatus, setVerifyStatus] = React.useState<DemoJobStatus>({
    state: 'idle',
    output: '',
  });
  const [scenarioStatus, setScenarioStatus] = React.useState<DemoJobStatus>({
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

  const verifySummary = React.useMemo(() => parseVerifySummary(verifyStatus), [verifyStatus]);
  const scenarioSummary = React.useMemo(() => parseScenarioSummary(scenarioStatus), [scenarioStatus]);

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

  const refreshVerifyStatus = React.useCallback(async () => {
    const response = await fetch(`${DEMOCTL_URL}/demo/verify/status`);
    const body = await response.json();
    setVerifyStatus(body);
  }, []);

  const refreshScenarioStatus = React.useCallback(async () => {
    const response = await fetch(`${DEMOCTL_URL}/demo/scenarios/leader-failover/status`);
    const body = await response.json();
    setScenarioStatus(body);
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
    refreshVerifyStatus().catch(() => {
      setVerifyStatus({
        state: 'idle',
        output: 'Demo controller is not reachable. Start it with: go run ./cmd/democtl',
      });
    });
    refreshScenarioStatus().catch(() => {
      setScenarioStatus({
        state: 'idle',
        output: 'Demo controller is not reachable. Start it with: go run ./cmd/democtl',
      });
    });
  }, [refreshChaosStatus, refreshHammerStatus, refreshScenarioStatus, refreshVerifyStatus]);

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

  React.useEffect(() => {
    if (verifyStatus.state !== 'running') {
      return;
    }

    const interval = window.setInterval(() => {
      refreshVerifyStatus()
        .then(() => refresh())
        .catch(() => {
          setVerifyStatus((current) => ({
            ...current,
            output: `${current.output}\nDemo controller became unreachable.`,
          }));
        });
    }, 1500);

    return () => window.clearInterval(interval);
  }, [verifyStatus.state, refresh, refreshVerifyStatus]);

  React.useEffect(() => {
    if (scenarioStatus.state !== 'running') {
      return;
    }

    const interval = window.setInterval(() => {
      refreshScenarioStatus()
        .then(() => refresh())
        .catch(() => {
          setScenarioStatus((current) => ({
            ...current,
            output: `${current.output}\nDemo controller became unreachable.`,
          }));
        });
    }, 1500);

    return () => window.clearInterval(interval);
  }, [scenarioStatus.state, refresh, refreshScenarioStatus]);

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

  async function startVerify() {
    const response = await fetch(`${DEMOCTL_URL}/demo/verify/start`, {
      method: 'POST',
    });
    const body = await response.json();
    setVerifyStatus(body);
    setResult(response.ok ? 'Cluster verification started.' : 'Cluster verification is already running.');
  }

  async function startLeaderFailover() {
    const response = await fetch(`${DEMOCTL_URL}/demo/scenarios/leader-failover/start`, {
      method: 'POST',
    });
    const body = await response.json();
    setScenarioStatus(body);
    setResult(response.ok ? 'Leader failover scenario started.' : 'Leader failover scenario is already running.');
  }

  async function startFollowerCatchUp() {
    const response = await fetch(`${DEMOCTL_URL}/demo/scenarios/follower-catchup/start`, {
      method: 'POST',
    });
    const body = await response.json();
    setScenarioStatus(body);
    setResult(response.ok ? 'Follower catch-up scenario started.' : 'A scenario is already running.');
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
            <CheckCircle2 size={18} />
            <h2>Cluster Verify</h2>
          </div>
          <div className="chaosHeader">
            <span className={`jobBadge ${verifyStatus.state}`}>{verifyStatus.state}</span>
            <button disabled={verifyStatus.state === 'running'} onClick={startVerify}>
              <CheckCircle2 size={16} />
              Verify Cluster
            </button>
            <button className="secondary" onClick={refreshVerifyStatus}>
              <RefreshCcw size={16} />
              Status
            </button>
          </div>
          <div className={`verifySummary ${verifyStatus.state}`}>
            <div className="verifyMetric">
              <span>Result</span>
              <strong>{verifySummary.title}</strong>
            </div>
            <div className="verifyMetric">
              <span>Reachable</span>
              <strong>{verifySummary.reachableNodes ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Leaders</span>
              <strong>{verifySummary.leaders ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Logs</span>
              <strong>
                {verifySummary.logsMatch === undefined ? '-' : verifySummary.logsMatch ? 'match' : 'diverge'}
              </strong>
            </div>
          </div>
          {verifySummary.logIndexes.length > 0 && (
            <div className="verifyIndexes">
              {verifySummary.logIndexes.map((entry) => (
                <span key={entry.nodeId}>
                  {entry.nodeId}={entry.index}
                </span>
              ))}
            </div>
          )}
          <pre className="jobOutput">
            {verifyStatus.output || 'Run verification after hammering or healing partitions.'}
          </pre>
        </div>

        <div className="panel entries">
          <div className="panelTitle">
            <RotateCcw size={18} />
            <h2>Scenario Runner</h2>
          </div>
          <div className="chaosHeader">
            <span className={`jobBadge ${scenarioStatus.state}`}>{scenarioStatus.state}</span>
            <button disabled={scenarioStatus.state === 'running'} onClick={startLeaderFailover}>
              <Play size={16} />
              Leader Failover Under Load
            </button>
            <button disabled={scenarioStatus.state === 'running'} onClick={startFollowerCatchUp}>
              <Play size={16} />
              Follower Partition And Catch-Up
            </button>
            <button className="secondary" onClick={refreshScenarioStatus}>
              <RefreshCcw size={16} />
              Status
            </button>
          </div>
          <div className={`scenarioSummary ${scenarioStatus.state}`}>
            <div className="verifyMetric">
              <span>Result</span>
              <strong>{scenarioSummary.title}</strong>
            </div>
            <div className="verifyMetric">
              <span>Old Leader</span>
              <strong>{scenarioSummary.initialLeader ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>New Leader</span>
              <strong>{scenarioSummary.newLeader ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Partitioned</span>
              <strong>{scenarioSummary.partitionedFollower ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Lag</span>
              <strong>{scenarioSummary.lagEntries === undefined ? '-' : `${scenarioSummary.lagEntries}`}</strong>
            </div>
            <div className="verifyMetric">
              <span>Availability</span>
              <strong>{scenarioSummary.availability === undefined ? '-' : `${scenarioSummary.availability}%`}</strong>
            </div>
            <div className="verifyMetric">
              <span>Correctness</span>
              <strong>{scenarioSummary.correctness}</strong>
            </div>
            <div className="verifyMetric">
              <span>Accepted</span>
              <strong>{scenarioSummary.writeSuccesses ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Rejected</span>
              <strong>{scenarioSummary.writeFailures ?? '-'}</strong>
            </div>
            <div className="verifyMetric">
              <span>Final Index</span>
              <strong>{scenarioSummary.finalLogIndex ?? '-'}</strong>
            </div>
          </div>
          {scenarioSummary.logIndexes.length > 0 && (
            <div className="verifyIndexes">
              {scenarioSummary.logIndexes.map((entry) => (
                <span key={entry.nodeId}>
                  {entry.nodeId}={entry.index}
                </span>
              ))}
            </div>
          )}
          <pre className="jobOutput">
            {scenarioStatus.output ||
              'Run a scripted failover: hammer traffic, stop the leader, elect a new leader, restart the old leader, then verify convergence.'}
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
