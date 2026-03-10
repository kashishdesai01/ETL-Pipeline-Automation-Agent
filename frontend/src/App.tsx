// src/App.tsx
import { useState, useRef, useEffect } from "react";
import { runMigration, checkHealth } from "./api/client";
import type { MigrationResult } from "./api/client";
import Editor from "@monaco-editor/react";
import ReactDiffViewer from "react-diff-viewer-continued";
import {
  Loader2, Zap, CheckCircle, XCircle, Upload,
  Code2, GitCompare, Brain, ListChecks, BarChart3
} from "lucide-react";
import "./index.css";

const SOURCE_DIALECTS = ["oracle", "mysql", "postgres", "bigquery", "spark"];
const TARGET_DIALECTS = ["snowflake", "bigquery", "databricks", "redshift"];
const PIPELINE_STEPS = [
  { label: "Parse", icon: "🔍" },
  { label: "Plan", icon: "📋" },
  { label: "Rewrite", icon: "✨" },
  { label: "Validate", icon: "✅" },
];

type ActiveTab = "diff" | "rewritten" | "transpiled";

export default function App() {
  const [result, setResult] = useState<MigrationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [activeStep, setActiveStep] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [sourceDialect, setSourceDialect] = useState("oracle");
  const [targetDialect, setTargetDialect] = useState("snowflake");
  const [activeTab, setActiveTab] = useState<ActiveTab>("diff");
  const [apiOk, setApiOk] = useState<boolean | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const [selectedFiles, setSelectedFiles] = useState<FileList | null>(null);
  const [entryFilename, setEntryFilename] = useState<string>("");

  // Check backend health on mount
  useEffect(() => {
    checkHealth().then(setApiOk);
  }, []);

  // Simulate pipeline step progression while loading
  useEffect(() => {
    if (!loading) { setActiveStep(0); return; }
    let step = 0;
    const interval = setInterval(() => {
      step = (step + 1) % PIPELINE_STEPS.length;
      setActiveStep(step);
    }, 2500);
    return () => clearInterval(interval);
  }, [loading]);

  async function handleMigrate() {
    if (!selectedFiles || selectedFiles.length === 0) { setError("Please select at least one SQL file"); return; }
    if (!entryFilename) { setError("Please select a primary file to migrate"); return; }
    
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await runMigration(selectedFiles, entryFilename, targetDialect, sourceDialect);
      setResult(data);
      setActiveTab("diff");
    } catch (e: any) {
      const msg = e.response?.data?.detail || e.message || "Migration failed";
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  const val = result?.validation;
  const passed = val?.passed ?? false;

  return (
    <div className="app">
      {/* ── Header ─────────────────────────────── */}
      <header className="header">
        <h1>⚡ ETL Migration Agent</h1>
        <p>LangGraph · GPT-4o · sqlglot · DuckDB</p>
        <div className="badge">
          <span className={`dot ${apiOk === false ? "fail" : ""}`} />
          {apiOk === null ? "Connecting..." : apiOk ? "Backend connected" : "Backend offline — start FastAPI server"}
        </div>
      </header>

      {/* ── Upload Panel ────────────────────────── */}
      <div className="upload-panel">
        <h2>Migration Configuration</h2>
        <div className="controls">
          <div className="control-group file-upload">
            <label>SQL Files</label>
            <input
              ref={fileRef}
              type="file"
              multiple
              accept=".sql,.py"
              onChange={(e) => {
                const files = e.target.files;
                setSelectedFiles(files);
                if (files && files.length > 0) {
                  // Default to first file if none selected or previous selection is no longer valid
                  if (!entryFilename || !Array.from(files).find(f => f.name === entryFilename)) {
                    setEntryFilename(files[0].name);
                  }
                } else {
                  setEntryFilename("");
                }
              }}
            />
          </div>
          
          {selectedFiles && selectedFiles.length > 0 && (
            <div className="control-group">
              <label>Primary File to Migrate</label>
              <select value={entryFilename} onChange={(e) => setEntryFilename(e.target.value)}>
                {Array.from(selectedFiles).map((f) => (
                  <option key={f.name} value={f.name}>{f.name}</option>
                ))}
              </select>
              <small style={{display: 'block', marginTop: '4px', color: 'var(--text-muted)'}}>
                Other files will be used as context.
              </small>
            </div>
          )}

          <div className="control-group">
            <label>Source Dialect</label>
            <select value={sourceDialect} onChange={(e) => setSourceDialect(e.target.value)}>
              {SOURCE_DIALECTS.map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <div className="control-group">
            <label>Target Dialect</label>
            <select value={targetDialect} onChange={(e) => setTargetDialect(e.target.value)}>
              {TARGET_DIALECTS.map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <button className="run-btn" onClick={handleMigrate} disabled={loading || !selectedFiles || selectedFiles.length === 0}>
            {loading ? <Loader2 size={16} className="spin" /> : <Zap size={16} />}
            {loading ? "Migrating..." : "Run Migration"}
          </button>
        </div>
      </div>

      {/* ── Error Banner ─────────────────────────── */}
      {error && (
        <div className="error-banner">
          <XCircle size={16} /> {error}
        </div>
      )}

      {/* ── Loading State ────────────────────────── */}
      {loading && (
        <div className="loading-overlay">
          <div className="pipeline-steps">
            {PIPELINE_STEPS.map((step, i) => (
              <>
                <div key={step.label} className={`pipeline-step ${i === activeStep ? "active" : ""}`}>
                  <div className="step-icon">{step.icon}</div>
                  <div className="step-label">{step.label}</div>
                </div>
                {i < PIPELINE_STEPS.length - 1 && <div className="pipeline-connector" />}
              </>
            ))}
          </div>
          <p style={{ color: "var(--text-muted)", fontSize: "0.9rem" }}>
            Running {PIPELINE_STEPS[activeStep].label} Agent...
          </p>
        </div>
      )}

      {/* ── Empty State ──────────────────────────── */}
      {!loading && !result && !error && (
        <div className="empty-state">
          <div className="icon">
            <Upload size={48} style={{ color: "var(--accent)", opacity: 0.4 }} />
          </div>
          <h3>Upload a legacy SQL file to get started</h3>
          <p>The agent will parse its intent, generate a migration plan, rewrite it in {targetDialect.charAt(0).toUpperCase() + targetDialect.slice(1)} SQL, and validate the output locally.</p>
        </div>
      )}

      {/* ── Results ─────────────────────────────── */}
      {result && !loading && (
        <>
          {/* Validation Banner */}
          <div className={`validation-banner ${passed ? "pass" : "fail"}`}>
            {passed
              ? <><CheckCircle size={18} /> Validation PASSED</>
              : <><XCircle size={18} /> Validation {result.validation ? "FAILED" : "SKIPPED"}</>
            }
            <div className="metrics">
              {val && (
                <>
                  <span className="metric-pill">
                    {val.original_row_count} → {val.rewritten_row_count} rows
                  </span>
                  <span className="metric-pill">
                    Checksum: {val.checksum_match ? "✓" : "✗"}
                  </span>
                  {result.retry_count > 0 && (
                    <span className="metric-pill">
                      {result.retry_count} retr{result.retry_count === 1 ? "y" : "ies"}
                    </span>
                  )}
                </>
              )}
            </div>
          </div>

          {/* Metric Cards */}
          <div className="metrics-row">
            <MetricCard label="Original Rows" value={String(val?.original_row_count ?? "—")} sub="DuckDB run" />
            <MetricCard label="Rewritten Rows" value={String(val?.rewritten_row_count ?? "—")} sub="DuckDB run" />
            <MetricCard label="Retries" value={String(result.retry_count)} sub={`of 3 max`} />
            <MetricCard
              label="Complexity"
              value={(result.intent?.complexity as string)?.toUpperCase() ?? "—"}
              sub="Parsing agent"
            />
          </div>

          {/* Intent */}
          <div className="section">
            <div className="section-header">
              <h2><Brain size={15} /> Pipeline Intent</h2>
              <span className="badge-tag">Parsing Agent</span>
            </div>
            <IntentPanel intent={result.intent} />
          </div>

          {/* Migration Plan */}
          <div className="section">
            <div className="section-header">
              <h2><ListChecks size={15} /> Migration Plan</h2>
              <span className="badge-tag">{result.migration_plan.length} steps</span>
            </div>
            <ol className="steps-list">
              {result.migration_plan.map((s) => (
                <li key={s.step_id}>
                  <span className="step-num">{s.step_id}</span>
                  {s.description}
                </li>
              ))}
            </ol>
          </div>

          {/* SQL Output */}
          <div className="section">
            <div className="section-header">
              <h2><Code2 size={15} /> SQL Output</h2>
            </div>
            <div className="tab-bar">
              <button className={`tab-btn ${activeTab === "diff" ? "active" : ""}`} onClick={() => setActiveTab("diff")}>
                <GitCompare size={13} style={{ display: "inline", marginRight: 4 }} /> Diff View
              </button>
              <button className={`tab-btn ${activeTab === "rewritten" ? "active" : ""}`} onClick={() => setActiveTab("rewritten")}>
                <Code2 size={13} style={{ display: "inline", marginRight: 4 }} /> Rewritten SQL
              </button>
              <button className={`tab-btn ${activeTab === "transpiled" ? "active" : ""}`} onClick={() => setActiveTab("transpiled")}>
                <BarChart3 size={13} style={{ display: "inline", marginRight: 4 }} /> Transpiled (raw)
              </button>
            </div>

            {activeTab === "diff" && (
              <ReactDiffViewer
                oldValue={result.legacy_sql}
                newValue={result.rewritten_sql || ""}
                splitView={true}
                leftTitle={`Legacy (${sourceDialect})`}
                rightTitle={`Rewritten (${targetDialect})`}
                useDarkTheme={true}
              />
            )}

            {activeTab === "rewritten" && (
              <Editor
                height="420px"
                language="sql"
                value={result.rewritten_sql || "-- No output"}
                theme="vs-dark"
                options={{ readOnly: false, minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false }}
              />
            )}

            {activeTab === "transpiled" && (
              <pre className="code-block">{result.transpiled_sql || "— No transpiled output"}</pre>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ── Sub-components ───────────────────────────────────────────────────────────

function MetricCard({ label, value, sub }: { label: string; value: string; sub: string }) {
  return (
    <div className="metric-card">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
      <div className="sub">{sub}</div>
    </div>
  );
}

function IntentPanel({ intent }: { intent: Record<string, unknown> }) {
  if (!intent) return null;
  const entries = Object.entries(intent);

  const renderValue = (key: string, val: unknown) => {
    if (key === "complexity") {
      const cls = String(val).toLowerCase();
      return <span className={`complexity-badge ${cls}`}>{String(val)}</span>;
    }
    if (Array.isArray(val)) {
      return val.length === 0
        ? <span style={{ color: "var(--text-muted)" }}>None</span>
        : (
          <div className="tag-list">
            {val.map((v, i) => <span key={i} className="tag">{String(v)}</span>)}
          </div>
        );
    }
    return <span>{String(val)}</span>;
  };

  return (
    <div className="intent-grid">
      {entries.map(([key, val]) => (
        <div key={key} className="intent-item">
          <div className="key">{key.replace(/_/g, " ")}</div>
          <div className="val">{renderValue(key, val)}</div>
        </div>
      ))}
    </div>
  );
}
