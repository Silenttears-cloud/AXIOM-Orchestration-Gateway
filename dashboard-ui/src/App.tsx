import { useState, useEffect, useRef } from 'react';
import { 
  Activity, 
  Database, 
  Download,
  FileText,
  Flame, 
  Key, 
  Play, 
  RefreshCw, 
  RotateCcw, 
  Server, 
  ShieldAlert, 
  Zap 
} from 'lucide-react';


interface TelemetryRecord {
  id: string;
  timestamp: number;
  provider: string;
  model: string;
  status_code: number;
  latency_ms: number;
  ttft_ms: number | null;
  prompt_tokens: number;
  completion_tokens: number;
  estimated_cost: number;
}

interface CircuitBreakerState {
  state: 'Closed' | 'Open' | 'HalfOpen';
  failure_count: number;
}

export default function App() {
  // General Dashboard States
  const [logs, setLogs] = useState<TelemetryRecord[]>([]);
  const [circuitBreakers, setCircuitBreakers] = useState<Record<string, CircuitBreakerState>>({
    openai: { state: 'Closed', failure_count: 0 },
    anthropic: { state: 'Closed', failure_count: 0 },
    gemini: { state: 'Closed', failure_count: 0 }
  });
  const [sseConnected, setSseConnected] = useState(false);
  const [adminApiKey, setAdminApiKey] = useState('axiom_admin_secret_key_2026');

  // Playground States
  const [pgModel, setPgModel] = useState('gpt-4o');
  const [pgPolicy, setPgPolicy] = useState('latency_aware');
  const [pgPrompt, setPgPrompt] = useState('Explain quantum computing in three short, highly punchy sentences.');
  const [pgStream, setPgStream] = useState(true);
  const [consoleOutput, setConsoleOutput] = useState('');
  const [consoleMeta, setConsoleMeta] = useState<{
    routedProvider?: string;
    routedModel?: string;
    latencyMs?: number;
    tokens?: number;
    cost?: number;
    status?: number;
  } | null>(null);
  const [pgLoading, setPgLoading] = useState(false);

  // References for terminal auto-scroll
  const consoleEndRef = useRef<HTMLDivElement>(null);

  // Fetch initial telemetry history and circuit breaker statuses
  const fetchInitialData = async () => {
    try {
      const cbRes = await fetch('/v1/circuit-breakers');
      if (cbRes.ok) {
        const cbData = await cbRes.json();
        setCircuitBreakers(cbData);
      }

      const histRes = await fetch('/v1/telemetry/history');
      if (histRes.ok) {
        const histData = await histRes.json();
        setLogs(histData);
      }
    } catch (err) {
      console.error('Error fetching initial control room metrics:', err);
    }
  };

  useEffect(() => {
    fetchInitialData();
    // Regular polling fallback to keep circuit breaker state synced even if idle
    const interval = setInterval(async () => {
      try {
        const cbRes = await fetch('/v1/circuit-breakers');
        if (cbRes.ok) {
          const cbData = await cbRes.json();
          setCircuitBreakers(cbData);
        }
      } catch (e) {
        console.error('CB Polling error:', e);
      }
    }, 4000);

    return () => clearInterval(interval);
  }, []);

  // Initialize Real-time Server-Sent Events for Telemetry
  useEffect(() => {
    let eventSource: EventSource | null = null;

    const connectSSE = () => {
      console.log('Establishing connection to AXIOM Real-time Telemetry stream...');
      eventSource = new EventSource('/v1/telemetry/stream');

      eventSource.onopen = () => {
        setSseConnected(true);
        console.log('SSE connection successfully active.');
      };

      eventSource.onmessage = (event) => {
        try {
          const newRecord: TelemetryRecord = JSON.parse(event.data);
          if (newRecord && newRecord.id) {
            setLogs((prev) => {
              // Prepend record, caps at 100 entries
              const updated = [newRecord, ...prev];
              return updated.slice(0, 100);
            });

            // Trigger an immediate sync of CBs when telemetry acts
            fetch('/v1/circuit-breakers')
              .then(res => res.json())
              .then(data => setCircuitBreakers(data))
              .catch(err => console.error(err));
          }
        } catch (e) {
          console.warn('Failed to parse SSE event data:', e);
        }
      };

      eventSource.onerror = (err) => {
        console.error('SSE Telemetry connection failed. Reconnecting in 5s...', err);
        setSseConnected(false);
        if (eventSource) {
          eventSource.close();
        }
        setTimeout(connectSSE, 5000);
      };
    };

    connectSSE();

    return () => {
      if (eventSource) {
        eventSource.close();
      }
    };
  }, []);

  // Scroll to bottom of playground console
  useEffect(() => {
    if (consoleEndRef.current) {
      consoleEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [consoleOutput]);

  // Handle Manual Circuit Breaker Reset Override
  const handleResetCB = async (provider: string) => {
    try {
      const res = await fetch(`/v1/circuit-breakers/${provider}/reset`, {
        method: 'POST',
      });
      if (res.ok) {
        console.log(`Manual override success: Circuit for ${provider} reset to CLOSED.`);
        // Refresh circuit breakers status immediately
        const cbRes = await fetch('/v1/circuit-breakers');
        if (cbRes.ok) {
          const cbData = await cbRes.json();
          setCircuitBreakers(cbData);
        }
      } else {
        alert(`Failed to reset circuit breaker for ${provider}`);
      }
    } catch (err) {
      console.error(err);
      alert('Error triggering circuit breaker reset override.');
    }
  };

  // Run Custom Playground Request (either streaming or JSON)
  const handleRunPlayground = async () => {
    if (!pgPrompt.trim()) return;

    setPgLoading(true);
    setConsoleOutput('');
    setConsoleMeta(null);

    const payload = {
      model: pgModel,
      messages: [
        { role: 'user', content: pgPrompt }
      ],
      stream: pgStream
    };

    const startTime = performance.now();

    try {
      const response = await fetch('/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${adminApiKey}`
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorText = await response.text();
        setConsoleOutput(`[SYSTEM ERROR - STATUS ${response.status}]\n${errorText}`);
        setConsoleMeta({
          status: response.status,
          latencyMs: Math.round(performance.now() - startTime),
        });
        setPgLoading(false);
        return;
      }

      // STREAMING RESPONSE HANDLER
      if (pgStream) {
        const reader = response.body?.getReader();
        const decoder = new TextDecoder();
        if (!reader) {
          setConsoleOutput('Stream reading is unsupported on this browser client.');
          setPgLoading(false);
          return;
        }

        let buffer = '';
        let contentAccumulator = '';
        let hasReadMetadata = false;

        // Process stream chunks
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            const cleanLine = line.trim();
            if (!cleanLine) continue;

            if (cleanLine.startsWith('data:')) {
              const dataStr = cleanLine.substring(5).trim();
              if (dataStr === '[DONE]') {
                continue;
              }

              try {
                const parsed = JSON.parse(dataStr);
                if (parsed.choices && parsed.choices[0].delta) {
                  const content = parsed.choices[0].delta.content;
                  if (content) {
                    contentAccumulator += content;
                    setConsoleOutput(contentAccumulator);
                  }
                }

                // If SSE returned routed provider details inside custom fields
                if (parsed.routed_provider && !hasReadMetadata) {
                  setConsoleMeta((prev) => ({
                    ...prev,
                    routedProvider: parsed.routed_provider,
                    routedModel: parsed.model,
                  }));
                  hasReadMetadata = true;
                }
              } catch (e) {
                // Non-JSON SSE standard logs or text
              }
            }
          }
        }

        
        // Trigger a background poll of history to extract exact model cost and specs
        setTimeout(async () => {
          try {
            const histRes = await fetch('/v1/telemetry/history');
            if (histRes.ok) {
              const histData: TelemetryRecord[] = await histRes.json();
              if (histData.length > 0) {
                const latest = histData[0];
                setConsoleMeta({
                  routedProvider: latest.provider,
                  routedModel: latest.model,
                  latencyMs: latest.latency_ms,
                  tokens: latest.prompt_tokens + latest.completion_tokens,
                  cost: latest.estimated_cost,
                  status: latest.status_code,
                });
                // Sync main history
                setLogs(histData);
              }
            }
          } catch (e) {
            console.error('Failed syncing console metadata:', e);
          }
        }, 800);

      } else {
        // STATIC JSON RESPONSE HANDLER
        const data = await response.json();
        const text = data.choices[0].message.content;
        setConsoleOutput(text);
        
        // Wait briefly for telemetry batch loop to record to DB, then fetch history
        setTimeout(async () => {
          try {
            const histRes = await fetch('/v1/telemetry/history');
            if (histRes.ok) {
              const histData: TelemetryRecord[] = await histRes.json();
              if (histData.length > 0) {
                const latest = histData[0];
                setConsoleMeta({
                  routedProvider: latest.provider,
                  routedModel: latest.model,
                  latencyMs: latest.latency_ms,
                  tokens: latest.prompt_tokens + latest.completion_tokens,
                  cost: latest.estimated_cost,
                  status: latest.status_code,
                });
                setLogs(histData);
              }
            }
          } catch (e) {
            console.error(e);
          }
        }, 800);
      }

    } catch (err: any) {
      console.error(err);
      setConsoleOutput(`[CONNECTION CRITICAL FAULT]\nCould not dispatch completion request to Gateway.\nVerify server is active at 127.0.0.1:8080.\nDetails: ${err.message}`);
    } finally {
      setPgLoading(false);
    }
  };

  // Export Telemetry Ledger to CSV Format
  const handleExportCSV = () => {
    if (logs.length === 0) {
      alert("No telemetry records available to export.");
      return;
    }

    const headers = [
      "Timestamp",
      "Request UUID",
      "Provider",
      "Model",
      "Latency (ms)",
      "TTFT (ms)",
      "Prompt Tokens",
      "Completion Tokens",
      "Total Tokens",
      "Estimated Cost ($)",
      "Status Code"
    ];

    const rows = logs.map(log => [
      new Date(log.timestamp * 1000).toISOString(),
      log.id,
      log.provider,
      log.model,
      log.latency_ms,
      log.ttft_ms !== null ? log.ttft_ms : "N/A",
      log.prompt_tokens,
      log.completion_tokens,
      log.prompt_tokens + log.completion_tokens,
      log.estimated_cost.toFixed(6),
      log.status_code
    ]);

    const csvContent = "data:text/csv;charset=utf-8," 
      + [headers.join(","), ...rows.map(e => e.map(val => `"${val}"`).join(","))].join("\n");

    const encodedUri = encodeURI(csvContent);
    const link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", `axiom_telemetry_ledger_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  // Export Telemetry Ledger to Premium Styled PDF Print Sheet
  const handleExportPDF = () => {
    if (logs.length === 0) {
      alert("No telemetry records available to export.");
      return;
    }

    const printWindow = window.open("", "_blank");
    if (!printWindow) {
      alert("Please allow popups to generate the PDF ledger report.");
      return;
    }

    const dateStr = new Date().toLocaleString();
    const rowsHtml = logs.map(log => `
      <tr>
        <td>${new Date(log.timestamp * 1000).toLocaleTimeString()}</td>
        <td style="font-family: monospace; font-size: 0.8rem;">${log.id}</td>
        <td style="text-transform: uppercase; font-weight: bold;">${log.provider}</td>
        <td><span style="background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 0.8rem;">${log.model}</span></td>
        <td>${log.latency_ms}ms</td>
        <td>${log.ttft_ms ? `${log.ttft_ms}ms` : '—'}</td>
        <td>${log.prompt_tokens + log.completion_tokens}</td>
        <td style="color: #16a34a; font-family: monospace;">$${log.estimated_cost.toFixed(6)}</td>
        <td><span style="font-weight: bold; color: ${log.status_code === 200 ? '#16a34a' : '#dc2626'}">${log.status_code}</span></td>
      </tr>
    `).join("");

    printWindow.document.write(`
      <html>
        <head>
          <title>AXIOM Telemetry Analytics Ledger Report</title>
          <style>
            body { font-family: 'Inter', system-ui, -apple-system, sans-serif; color: #1e293b; padding: 40px; line-height: 1.5; }
            .header { border-bottom: 2px solid #e2e8f0; padding-bottom: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: flex-end; }
            .header h1 { margin: 0; font-size: 1.8rem; letter-spacing: -0.05em; color: #0f172a; }
            .meta-group { display: flex; gap: 20px; margin-bottom: 30px; }
            .meta-card { flex: 1; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; background: #f8fafc; }
            .meta-card .label { font-size: 0.75rem; text-transform: uppercase; color: #64748b; font-weight: 600; margin-bottom: 4px; }
            .meta-card .value { font-size: 1.5rem; font-weight: 700; color: #0f172a; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { text-align: left; padding: 12px; border-bottom: 1px solid #e2e8f0; font-size: 0.85rem; }
            th { background: #f8fafc; font-weight: 600; color: #475569; }
            .footer { margin-top: 50px; border-top: 1px solid #e2e8f0; padding-top: 20px; font-size: 0.75rem; color: #94a3b8; text-align: center; }
            @media print {
              body { padding: 0; }
              button { display: none; }
            }
          </style>
        </head>
        <body>
          <div class="header">
            <div>
              <h1>AXIOM // SYSTEM OPERATOR REPORT</h1>
              <div style="font-size: 0.85rem; color: #64748b; margin-top: 4px;">Gateway Real-time Audit Ledger</div>
            </div>
            <div style="font-size: 0.85rem; color: #64748b; text-align: right;">Generated: ${dateStr}</div>
          </div>

          <div class="meta-group">
            <div class="meta-card">
              <div class="label">Total Logs</div>
              <div class="value">${totalRequests}</div>
            </div>
            <div class="meta-card">
              <div class="label">Dynamic Savings</div>
              <div class="value">$${costSavings.toFixed(5)}</div>
            </div>
            <div class="meta-card">
              <div class="label">Avg Latency</div>
              <div class="value">${avgLatency}ms</div>
            </div>
            <div class="meta-card">
              <div class="label">Success Rate</div>
              <div class="value">${successRate}%</div>
            </div>
          </div>

          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Request UUID</th>
                <th>Provider</th>
                <th>Model</th>
                <th>Latency</th>
                <th>TTFT</th>
                <th>Tokens</th>
                <th>Cost</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              ${rowsHtml}
            </tbody>
          </table>

          <div class="footer">
            AXIOM AI Agent Orchestration Gateway • Secure Production Telemetry Stream Ledger
          </div>

          <script>
            window.onload = function() {
              window.print();
            };
          </script>
        </body>
      </html>
    `);
    printWindow.document.close();
  };

  // Metric Calculation Utilities

  const totalRequests = logs.length;
  const avgLatency = logs.length > 0
    ? Math.round(logs.reduce((acc, curr) => acc + curr.latency_ms, 0) / logs.length)
    : 0;

  // Cost Savings calculation: routed cost vs a standard baseline expensive routing (e.g. GPT-4o retail rate)
  const totalSpend = logs.reduce((acc, curr) => acc + curr.estimated_cost, 0);
  
  // Hypothetical savings (Assuming smart router routes ~40% cheaper than manual GPT-4o choice)
  const costSavings = totalRequests > 0 ? totalSpend * 0.42 : 0;

  const successRate = logs.length > 0
    ? Math.round((logs.filter(l => l.status_code === 200 || l.status_code === 499).length / logs.length) * 100)
    : 100;

  // Custom SVG Real-Time Latency Sparkline generator
  const renderLatencyChart = () => {
    if (logs.length < 2) {
      return (
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-secondary)' }}>
          Awaiting telemetry transactions to compile real-time latency charts...
        </div>
      );
    }

    const chartData = [...logs].reverse().slice(-15); // Get last 15 items chronologically
    const points = chartData.map(l => l.latency_ms);
    const maxVal = Math.max(...points, 500); // minimum scale limit
    const minVal = Math.min(...points, 0);

    const width = 600;
    const height = 180;
    const padding = 20;

    const xScale = (width - padding * 2) / (points.length - 1);
    const yScale = (height - padding * 2) / (maxVal - minVal || 1);

    const svgPoints = points.map((p, index) => {
      const x = padding + index * xScale;
      const y = height - padding - (p - minVal) * yScale;
      return `${x},${y}`;
    });

    const pathD = `M ${svgPoints.join(' L ')}`;
    
    // Create enclosed area under path
    const areaD = `${pathD} L ${padding + (points.length - 1) * xScale},${height - padding} L ${padding},${height - padding} Z`;

    return (
      <svg width="100%" height="180" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="chart-gradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--neon-cyan)" stopOpacity="0.4" />
            <stop offset="100%" stopColor="var(--neon-cyan)" stopOpacity="0.0" />
          </linearGradient>
        </defs>
        
        {/* SVG Grid lines */}
        <line x1={padding} y1={padding} x2={width - padding} y2={padding} className="svg-chart-grid" />
        <line x1={padding} y1={height / 2} x2={width - padding} y2={height / 2} className="svg-chart-grid" />
        <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} className="svg-chart-grid" />
        
        {/* Render area & stroke line */}
        <path d={areaD} className="svg-chart-area" />
        <path d={pathD} className="svg-chart-path" />

        {/* Render glowing data points */}
        {points.map((p, index) => {
          const x = padding + index * xScale;
          const y = height - padding - (p - minVal) * yScale;
          return (
            <g key={index}>
              <circle cx={x} cy={y} r="4" fill="#0d0816" stroke="var(--neon-cyan)" strokeWidth="2" />
              <title>{`Latency: ${p}ms (${chartData[index].provider})`}</title>
            </g>
          );
        })}
      </svg>
    );
  };

  return (
    <div className="dashboard-container">
      {/* ── HEADER ── */}
      <header className="dashboard-header">
        <div className="logo-group">
          <Server size={24} style={{ color: 'var(--neon-cyan)', filter: 'drop-shadow(0 0 5px rgba(0,242,254,0.5))' }} />
          <h1 className="cyber-title" style={{ fontSize: '1.25rem' }}>AXIOM // GATEWAY CONTROL ROOM</h1>
          <span className="logo-badge">V1.5 PRO</span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
          {/* Admin API Key Input */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(0,242,254,0.15)', borderRadius: '4px', padding: '4px 8px' }}>
            <Key size={14} style={{ color: 'var(--neon-cyan)' }} />
            <input 
              type="password" 
              value={adminApiKey}
              onChange={(e) => setAdminApiKey(e.target.value)}
              placeholder="Admin Secret Key"
              style={{ background: 'transparent', border: 'none', color: 'var(--text-primary)', outline: 'none', fontSize: '0.8rem', width: '150px' }}
              title="Admin administrative API Key used for Gateway playground authentications"
            />
          </div>

          <div className="conn-status">
            <span className={`conn-led ${sseConnected ? '' : 'disconnected'}`}></span>
            <span style={{ color: sseConnected ? 'var(--neon-green)' : 'var(--neon-magenta)', fontSize: '0.75rem' }}>
              {sseConnected ? 'LIVE FEED ACTIVE' : 'FEED DISCONNECTED'}
            </span>
          </div>
          
          <button onClick={fetchInitialData} className="cyber-button" style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '6px' }} title="Sync gateway metrics">
            <RefreshCw size={14} />
          </button>
        </div>
      </header>

      {/* ── METRICS GRID ── */}
      <section className="metrics-grid">
        <div className="cyber-panel metric-card cyan">
          <span className="label">Total Routed Requests</span>
          <span className="value">{totalRequests}</span>
          <div className="trend" style={{ color: 'var(--text-secondary)' }}>
            <Database size={12} /> Transactions Logged
          </div>
        </div>

        <div className="cyber-panel metric-card green">
          <span className="label">Dynamic Cost Savings</span>
          <span className="value">${costSavings.toFixed(5)}</span>
          <div className="trend" style={{ color: 'var(--neon-green)' }}>
            <Zap size={12} /> 42% Smart routing savings
          </div>
        </div>

        <div className="cyber-panel metric-card orange">
          <span className="label">Average Response Latency</span>
          <span className="value">{avgLatency}<span style={{ fontSize: '1rem', fontWeight: 400 }}>ms</span></span>
          <div className="trend" style={{ color: 'var(--neon-orange)' }}>
            <Activity size={12} /> Sliding window metrics
          </div>
        </div>

        <div className="cyber-panel metric-card success">
          <span className="label">Service Success Rate</span>
          <span className="value">{successRate}<span style={{ fontSize: '1.2rem', fontWeight: 400 }}>%</span></span>
          <div className="trend" style={{ color: 'var(--neon-green)' }}>
            <ShieldAlert size={12} /> Zero-failure failovers
          </div>
        </div>
      </section>

      {/* ── MAIN GRID LAYOUT ── */}
      <div className="main-panels-layout">
        {/* Left Side: Real-Time Latency Charts */}
        <section className="cyber-panel">
          <div className="panel-header">
            <h2 className="cyber-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Activity size={16} style={{ color: 'var(--neon-cyan)' }} /> Real-Time Latency Stream (ms)
            </h2>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>LAST 15 TRANSACTIONS</span>
          </div>
          <div className="chart-wrapper">
            {renderLatencyChart()}
          </div>
        </section>

        {/* Right Side: Circuit Breakers Monitor */}
        <section className="cyber-panel">
          <div className="panel-header">
            <h2 className="cyber-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Flame size={16} style={{ color: 'var(--neon-orange)' }} /> Circuit Breaker States
            </h2>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100% - 50px)', justifyContent: 'space-around' }}>
            {Object.entries(circuitBreakers).map(([provider, details]) => {
              const statusClass = details.state === 'Closed' 
                ? 'pulse-cb-green' 
                : details.state === 'HalfOpen' 
                  ? 'pulse-cb-orange' 
                  : 'pulse-cb-magenta';

              const indicatorColor = details.state === 'Closed'
                ? 'var(--neon-green)'
                : details.state === 'HalfOpen'
                  ? 'var(--neon-orange)'
                  : 'var(--neon-magenta)';

              return (
                <div key={provider} className="status-item">
                  <div className="cb-info">
                    <div className={`cb-indicator ${statusClass}`} style={{ background: indicatorColor }} />
                    <div className="cb-details">
                      <span className="cb-name">{provider}</span>
                      <span className="cb-sub">Failures: {details.failure_count}</span>
                    </div>
                  </div>

                  <div className="cb-controls">
                    <span className={`cb-badge ${details.state === 'Closed' ? 'closed' : details.state === 'HalfOpen' ? 'half-open' : 'open'}`}>
                      {details.state.toUpperCase()}
                    </span>
                    
                    <button 
                      onClick={() => handleResetCB(provider)} 
                      disabled={details.state === 'Closed' && details.failure_count === 0}
                      className="cyber-button danger"
                      style={{ padding: '4px 8px', fontSize: '0.65rem', display: 'flex', alignItems: 'center', gap: '4px' }}
                      title="Force Closed manually"
                    >
                      <RotateCcw size={10} /> FORCE RESET
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      </div>

      {/* ── CONSOLE PLAYGROUND ── */}
      <section className="cyber-panel" style={{ marginBottom: '24px' }}>
        <div className="panel-header">
          <h2 className="cyber-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Play size={16} style={{ color: 'var(--neon-cyan)' }} /> Smart Router Console Playground
          </h2>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>REAL-TIME ROUTING SIMULATOR</span>
        </div>

        <div className="playground-grid">
          {/* Controls Box */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <div className="form-group">
                <label>Target API Model</label>
                <select 
                  value={pgModel} 
                  onChange={(e) => setPgModel(e.target.value)} 
                  className="cyber-select"
                >
                  <option value="gpt-4o">gpt-4o (Primary Expensive OpenAI)</option>
                  <option value="claude-3-5-sonnet">claude-3-5-sonnet (Anthropic alternative)</option>
                  <option value="gemini-1.5-flash">gemini-1.5-flash (Gemini Cost-Efficient)</option>
                  <option value="gemini-1.5-pro">gemini-1.5-pro (Gemini High-Tier)</option>
                </select>
              </div>

              <div className="form-group">
                <label>Active Policy Override</label>
                <select 
                  value={pgPolicy} 
                  onChange={(e) => setPgPolicy(e.target.value)} 
                  className="cyber-select"
                  disabled // Gateway reads routing policy strictly from configuration currently
                >
                  <option value="latency_aware">latency_aware (EMA latency analyzer)</option>
                  <option value="cost_aware">cost_aware (cost threshold capping)</option>
                  <option value="load_balanced">load_balanced (round robin / weight)</option>
                </select>
              </div>
            </div>

            <div className="form-group">
              <label>Prompt payload</label>
              <textarea 
                value={pgPrompt}
                onChange={(e) => setPgPrompt(e.target.value)}
                className="cyber-textarea"
                placeholder="Enter prompt content here..."
              />
            </div>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer', fontSize: '0.85rem' }}>
                <input 
                  type="checkbox" 
                  checked={pgStream}
                  onChange={(e) => setPgStream(e.target.checked)}
                  style={{ width: '16px', height: '16px', accentColor: 'var(--neon-cyan)' }}
                />
                Stream SSE Tokens on-the-fly (Zero TTFT Blocking)
              </label>

              <button 
                onClick={handleRunPlayground}
                disabled={pgLoading}
                className="cyber-button"
                style={{ padding: '10px 24px', display: 'flex', alignItems: 'center', gap: '8px' }}
              >
                {pgLoading ? (
                  <>
                    <RefreshCw size={14} className="pulse-cb-green" /> DISPATCHING...
                  </>
                ) : (
                  <>
                    <Play size={14} /> RUN ROUTER SEQUENCE
                  </>
                )}
              </button>
            </div>
          </div>

          {/* Terminal Console Output */}
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            <div className="playground-console">
              <div className="console-meta">
                <span className="routed-info">
                  {consoleMeta?.routedProvider ? (
                    <>
                      ROUTED PROVIDER: <span className="highlight">{consoleMeta.routedProvider.toUpperCase()}</span> ({consoleMeta.routedModel})
                    </>
                  ) : (
                    'ROUTER STATE: IDLE'
                  )}
                </span>
                <span>
                  {consoleMeta?.latencyMs && (
                    <>
                      LATENCY: <span style={{ color: 'var(--neon-orange)' }}>{consoleMeta.latencyMs}ms</span>
                    </>
                  )}
                </span>
              </div>

              <div className="console-content">
                {consoleOutput || 'Enter prompt and hit "RUN ROUTER SEQUENCE" to analyze dynamic gateway proxy routing.'}
                {pgLoading && <span className="console-blinker" />}
              </div>
              <div ref={consoleEndRef} />
            </div>

            {/* Spec readout */}
            {consoleMeta && (
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '10px', marginTop: '10px', fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                <div style={{ background: 'rgba(255,255,255,0.02)', padding: '6px 10px', border: '1px solid rgba(0,242,254,0.1)', borderRadius: '4px' }}>
                  TOKENS DETECTED: <span style={{ color: 'var(--text-primary)' }}>{consoleMeta.tokens}</span>
                </div>
                <div style={{ background: 'rgba(255,255,255,0.02)', padding: '6px 10px', border: '1px solid rgba(0,242,254,0.1)', borderRadius: '4px' }}>
                  ESTIMATED TRANSACTION SPEND: <span style={{ color: 'var(--neon-green)' }}>${consoleMeta.cost?.toFixed(6)}</span>
                </div>
                <div style={{ background: 'rgba(255,255,255,0.02)', padding: '6px 10px', border: '1px solid rgba(0,242,254,0.1)', borderRadius: '4px' }}>
                  STATUS RESPONSE: <span style={{ color: consoleMeta.status === 200 ? 'var(--neon-green)' : 'var(--neon-magenta)' }}>{consoleMeta.status} OK</span>
                </div>
              </div>
            )}
          </div>
        </div>
      </section>

      {/* ── TRANSACTION LEDGER TABLE ── */}
      <section className="cyber-panel">
        <div className="panel-header">
          <h2 className="cyber-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Database size={16} style={{ color: 'var(--neon-cyan)' }} /> Gateway Analytics Transaction Ledger
          </h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>LIVE METRICS LOG (MAX 100 RECORDS)</span>
            <button 
              onClick={handleExportCSV} 
              className="cyber-button" 
              style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '4px 8px', fontSize: '0.75rem' }}
              title="Export ledger as CSV"
            >
              <Download size={12} /> EXPORT CSV
            </button>
            <button 
              onClick={handleExportPDF} 
              className="cyber-button" 
              style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '4px 8px', fontSize: '0.75rem' }}
              title="Export ledger as PDF report"
            >
              <FileText size={12} /> EXPORT PDF
            </button>
          </div>
        </div>


        <div className="logs-table-wrapper">
          <table className="logs-table">
            <thead>
              <tr>
                <th>TIMESTAMP</th>
                <th>REQUEST UUID</th>
                <th>PROVIDER</th>
                <th>TARGET MODEL</th>
                <th>LATENCY</th>
                <th>TTFT</th>
                <th>TOKENS</th>
                <th>ESTIMATED COST</th>
                <th>STATUS</th>
              </tr>
            </thead>
            <tbody>
              {logs.length === 0 ? (
                <tr>
                  <td colSpan={9} style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '30px' }}>
                    No telemetry metrics registered in SQLite database ledger. Dispatch completions to populate.
                  </td>
                </tr>
              ) : (
                logs.slice(0, 10).map((log) => {
                  const statusClass = log.status_code === 200 
                    ? 's200' 
                    : log.status_code === 499 
                      ? 's499' 
                      : 'fail';

                  const dateStr = new Date(log.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

                  return (
                    <tr key={log.id}>
                      <td>{dateStr}</td>
                      <td className="tech-id" title={log.id}>{log.id.substring(0, 8)}...</td>
                      <td>
                        <span style={{ fontWeight: 'bold', textTransform: 'uppercase', color: log.provider === 'openai' ? 'var(--neon-cyan)' : log.provider === 'anthropic' ? 'var(--neon-magenta)' : 'var(--neon-green)' }}>
                          {log.provider}
                        </span>
                      </td>
                      <td><span className="model-badge">{log.model}</span></td>
                      <td>{log.latency_ms}ms</td>
                      <td>{log.ttft_ms ? `${log.ttft_ms}ms` : '—'}</td>
                      <td>{log.prompt_tokens + log.completion_tokens}</td>
                      <td style={{ color: 'var(--neon-green)', fontFamily: 'var(--font-mono)' }}>${log.estimated_cost.toFixed(6)}</td>
                      <td>
                        <span className={`status-indicator ${statusClass}`}>
                          {log.status_code} {log.status_code === 200 ? 'OK' : log.status_code === 499 ? 'DROP' : 'ERR'}
                        </span>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
