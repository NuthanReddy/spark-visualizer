import { useMemo } from 'react';
import {
  AlertTriangle,
  ArrowRight,
  FlaskConical,
  Info,
  Minus,
  Plus,
  RotateCcw,
  Wrench,
  X,
} from 'lucide-react';
import {
  DEFAULT_SIMULATION_CONFIG,
  FAULT_MAP,
  JOB_TYPE_MAP,
  SIMULATED_DATA_PROFILES,
  SIMULATED_FAULTS,
  SIMULATED_JOBS,
  SIMULATION_PRESETS,
  validateSimulationConfig,
} from '../engine/simulatorTypes';
import type {
  PythonUdfMode,
  SimulatedDataProfile,
  SimulatedFault,
  SimulatedJobType,
  SimulationConfig,
} from '../engine/simulatorTypes';

export interface SimulatorPanelProps {
  id?: string;
  draft: SimulationConfig;
  activeConfig: SimulationConfig | null;
  onDraftChange: (next: SimulationConfig) => void;
  onRun: (config: SimulationConfig) => void;
  onRunPreset: (config: SimulationConfig) => void;
  onResetDraft: () => void;
  onClose: () => void;
}

const CORE_OPTIONS: ReadonlyArray<1 | 2 | 4> = [1, 2, 4];
const PARTITION_OPTIONS: ReadonlyArray<4 | 8 | 12 | 24> = [4, 8, 12, 24];

function configsEqual(a: SimulationConfig, b: SimulationConfig): boolean {
  return (
    a.jobType === b.jobType &&
    a.dataProfile === b.dataProfile &&
    a.deployMode === b.deployMode &&
    a.numExecutors === b.numExecutors &&
    a.executorCores === b.executorCores &&
    a.numPartitions === b.numPartitions &&
    a.externalShuffleService === b.externalShuffleService &&
    a.dynamicAllocation === b.dynamicAllocation &&
    a.aqeEnabled === b.aqeEnabled &&
    a.pythonUdfMode === b.pythonUdfMode &&
    a.fault === b.fault
  );
}

function getFaultRequirementHint(
  faultId: SimulatedFault,
  draft: SimulationConfig,
): string | null {
  const job = JOB_TYPE_MAP[draft.jobType];
  if (faultId === 'shuffle-fetch-failed' && !job.hasShuffle) {
    return '[requires Shuffle job]';
  }
  if (
    (faultId === 'commit-job-failure' || faultId === 'speculation-race') &&
    !job.hasFileCommit
  ) {
    return '[requires Parquet write]';
  }
  if (faultId === 'max-result-size' && !job.hasCollect) {
    return '[requires collect()]';
  }
  if (
    (faultId === 'am-sc-timeout' || faultId === 'am-crash-retry') &&
    draft.deployMode !== 'cluster'
  ) {
    return '[requires Cluster mode]';
  }
  if (faultId === 'client-disconnect' && draft.deployMode !== 'client') {
    return '[requires Client mode]';
  }
  return null;
}

export default function SimulatorPanel({
  id,
  draft,
  activeConfig,
  onDraftChange,
  onRun,
  onRunPreset,
  onResetDraft,
  onClose,
}: SimulatorPanelProps) {
  const validation = useMemo(() => validateSimulationConfig(draft), [draft]);
  const jobMeta = JOB_TYPE_MAP[draft.jobType];
  const faultMeta = FAULT_MAP[draft.fault];
  const dataProfileMeta = useMemo(
    () =>
      SIMULATED_DATA_PROFILES.find((p) => p.id === draft.dataProfile) ??
      SIMULATED_DATA_PROFILES[0],
    [draft.dataProfile],
  );

  const containers = draft.numExecutors + 1;
  const slots = draft.numExecutors * draft.executorCores;
  const idealWaves = Math.ceil(draft.numPartitions / slots);

  const isDefaultDraft = configsEqual(draft, DEFAULT_SIMULATION_CONFIG);
  const hasUnappliedChanges =
    activeConfig !== null && !configsEqual(draft, activeConfig);

  function updateDraft(patch: Partial<SimulationConfig>) {
    onDraftChange({ ...draft, ...patch });
  }

  function handleJobChange(nextJobType: SimulatedJobType) {
    const nextJob = JOB_TYPE_MAP[nextJobType];
    updateDraft({
      jobType: nextJobType,
      dataProfile: nextJob.defaultDataProfile,
    });
  }

  function clampExecutors(next: number): number {
    return Math.max(1, Math.min(6, Math.round(next)));
  }

  return (
    <section
      id={id}
      className="simulator-panel"
      role="region"
      aria-label="Job, data, and fault simulator"
    >
      <div className="sim-panel-header">
        <div className="sim-panel-title-group">
          <span className="sim-panel-title">
            <FlaskConical size={12} aria-hidden="true" />
            <span>JOB, DATA &amp; FAULT SIMULATOR</span>
          </span>
          <span className="sim-panel-subtitle">
            Edit draft parameters below, then run to synthesize an interactive ExecutionRun
          </span>
        </div>
        <div className="sim-panel-header-actions">
          {hasUnappliedChanges && (
            <span className="sim-draft-badge">DRAFT MODIFIED</span>
          )}
          <button
            type="button"
            className="icon-button sim-close-btn"
            aria-label="Close simulator panel"
            title="Close simulator (G or Esc)"
            onClick={onClose}
          >
            <X size={13} />
          </button>
        </div>
      </div>

      <div className="sim-presets-row">
        <span className="sim-presets-label">Quick run presets:</span>
        <div className="sim-presets-list" role="group" aria-label="Quick run scenario presets">
          {SIMULATION_PRESETS.map((preset) => {
            const isMatched = configsEqual(draft, preset.config);
            const isFaultPreset = preset.config.fault !== 'none';
            return (
              <button
                key={preset.id}
                type="button"
                className={[
                  'sim-preset-btn',
                  isMatched ? 'is-matched' : '',
                  isFaultPreset ? 'is-fault-preset' : '',
                ]
                  .filter(Boolean)
                  .join(' ')}
                title={`Load & immediately run "${preset.label}"`}
                onClick={() => onRunPreset({ ...preset.config })}
              >
                <span className="sim-preset-name">{preset.label}</span>
                <span className="sim-preset-badge">{preset.badge}</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="sim-grid">
        {/* Column 1: Workload & Data */}
        <div className="sim-col">
          <div className="sim-col-heading">1. WORKLOAD &amp; DATA</div>

          <div className="sim-field">
            <label className="sim-label" htmlFor="sim-job-type">
              Job Type
            </label>
            <select
              id="sim-job-type"
              className="sim-select"
              value={draft.jobType}
              onChange={(e) => handleJobChange(e.target.value as SimulatedJobType)}
            >
              {SIMULATED_JOBS.map((job) => (
                <option key={job.id} value={job.id}>
                  {job.label}
                </option>
              ))}
            </select>
            <code className="sim-action-preview" title={jobMeta.actionSink}>
              {jobMeta.actionSink}
            </code>
          </div>

          <div className="sim-field">
            <label className="sim-label" htmlFor="sim-data-profile">
              Data Profile
            </label>
            <select
              id="sim-data-profile"
              className="sim-select"
              value={draft.dataProfile}
              onChange={(e) =>
                updateDraft({
                  dataProfile: e.target.value as SimulatedDataProfile,
                })
              }
            >
              {SIMULATED_DATA_PROFILES.map((profile) => (
                <option key={profile.id} value={profile.id}>
                  {profile.label}
                </option>
              ))}
            </select>
            <p className="sim-field-hint">{dataProfileMeta.description}</p>
          </div>

          {draft.jobType === 'python-udf' && (
            <div className="sim-field">
              <span className="sim-label">UDF Mode</span>
              <div className="sim-segmented" role="group" aria-label="Python UDF mode">
                {(
                  [
                    ['pickled', 'Pickled (BatchEval)'],
                    ['arrow', 'Arrow (ArrowEval)'],
                  ] as Array<[PythonUdfMode, string]>
                ).map(([modeId, modeLabel]) => {
                  const isActive = draft.pythonUdfMode === modeId;
                  return (
                    <button
                      key={modeId}
                      type="button"
                      className={isActive ? 'active' : ''}
                      aria-pressed={isActive}
                      onClick={() => updateDraft({ pythonUdfMode: modeId })}
                    >
                      {modeLabel}
                    </button>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Column 2: Cluster & Runtime */}
        <div className="sim-col">
          <div className="sim-col-heading">2. CLUSTER &amp; RUNTIME</div>

          <div className="sim-field">
            <span className="sim-label">Deploy Mode</span>
            <div className="sim-segmented" role="group" aria-label="YARN deploy mode">
              {(['cluster', 'client'] as const).map((modeId) => {
                const isActive = draft.deployMode === modeId;
                return (
                  <button
                    key={modeId}
                    type="button"
                    className={isActive ? 'active' : ''}
                    aria-pressed={isActive}
                    onClick={() => updateDraft({ deployMode: modeId })}
                  >
                    {modeId === 'cluster' ? 'Cluster' : 'Client'}
                  </button>
                );
              })}
            </div>
          </div>

          <div className="sim-field">
            <div className="sim-label-row">
              <label className="sim-label" htmlFor="sim-executors">
                {draft.dynamicAllocation ? 'Initial Executors' : 'Fixed Executors'}
              </label>
              <span className="sim-value-readout">
                {draft.numExecutors} {draft.numExecutors === 1 ? 'exec' : 'execs'}
              </span>
            </div>
            <div className="sim-stepper-row">
              <button
                type="button"
                className="sim-step-btn"
                aria-label="Decrease executors"
                disabled={draft.numExecutors <= 1}
                onClick={() =>
                  updateDraft({ numExecutors: clampExecutors(draft.numExecutors - 1) })
                }
              >
                <Minus size={11} />
              </button>
              <input
                id="sim-executors"
                className="sim-range"
                type="range"
                min={1}
                max={6}
                step={1}
                value={draft.numExecutors}
                aria-valuetext={`${draft.numExecutors} ${
                  draft.dynamicAllocation ? 'initial' : 'fixed'
                } executors`}
                onChange={(e) =>
                  updateDraft({
                    numExecutors: clampExecutors(Number.parseInt(e.target.value, 10)),
                  })
                }
              />
              <button
                type="button"
                className="sim-step-btn"
                aria-label="Increase executors"
                disabled={draft.numExecutors >= 6}
                onClick={() =>
                  updateDraft({ numExecutors: clampExecutors(draft.numExecutors + 1) })
                }
              >
                <Plus size={11} />
              </button>
            </div>
          </div>

          <div className="sim-inline-pair">
            <div className="sim-field">
              <span className="sim-label">Cores / Exec</span>
              <div className="sim-segmented" role="group" aria-label="Executor cores">
                {CORE_OPTIONS.map((cores) => {
                  const isActive = draft.executorCores === cores;
                  return (
                    <button
                      key={cores}
                      type="button"
                      className={isActive ? 'active' : ''}
                      aria-pressed={isActive}
                      onClick={() => updateDraft({ executorCores: cores })}
                    >
                      {cores}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="sim-field">
              <span className="sim-label">Input Partitions</span>
              <div className="sim-segmented" role="group" aria-label="Input partitions">
                {PARTITION_OPTIONS.map((parts) => {
                  const isActive = draft.numPartitions === parts;
                  return (
                    <button
                      key={parts}
                      type="button"
                      className={isActive ? 'active' : ''}
                      aria-pressed={isActive}
                      onClick={() => updateDraft({ numPartitions: parts })}
                    >
                      {parts}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="sim-toggles-grid" role="group" aria-label="Runtime flags">
            <label className="sim-check-row">
              <input
                type="checkbox"
                checked={draft.externalShuffleService}
                onChange={(e) =>
                  updateDraft({ externalShuffleService: e.target.checked })
                }
              />
              <span>External Shuffle Service (:7337)</span>
            </label>
            <label className="sim-check-row">
              <input
                type="checkbox"
                checked={draft.dynamicAllocation}
                onChange={(e) =>
                  updateDraft({ dynamicAllocation: e.target.checked })
                }
              />
              <span>Dynamic Allocation</span>
            </label>
            <label
              className="sim-check-row"
              title="Adaptive Query Execution (spark.sql.adaptive.enabled)"
            >
              <input
                type="checkbox"
                checked={draft.aqeEnabled}
                onChange={(e) => updateDraft({ aqeEnabled: e.target.checked })}
              />
              <span>AQE</span>
            </label>
          </div>
        </div>

        {/* Column 3: Inject Fault / Exception & Live Telemetry */}
        <div className="sim-col">
          <div className="sim-col-heading">3. INJECT FAULT &amp; TELEMETRY</div>

          <div className="sim-field">
            <div className="sim-label-row">
              <label className="sim-label" htmlFor="sim-fault">
                Fault / Exception
              </label>
              <span className={`sim-severity-pill sev-${faultMeta.severity}`}>
                {faultMeta.severity === 'ok'
                  ? 'HAPPY PATH'
                  : faultMeta.severity === 'recoverable'
                    ? 'RECOVERABLE'
                    : 'TERMINAL'}
              </span>
            </div>
            <select
              id="sim-fault"
              className={`sim-select ${draft.fault !== 'none' ? 'is-fault-selected' : ''}`}
              value={draft.fault}
              onChange={(e) =>
                updateDraft({ fault: e.target.value as SimulatedFault })
              }
            >
              {SIMULATED_FAULTS.map((f) => {
                const reqHint = getFaultRequirementHint(f.id, draft);
                return (
                  <option key={f.id} value={f.id}>
                    {reqHint ? `${f.label} ${reqHint}` : f.label}
                  </option>
                );
              })}
            </select>
            <p className="sim-field-hint">{faultMeta.triggerSummary}</p>
          </div>

          <div className="sim-telemetry-chips" aria-label="Live cluster sizing telemetry">
            <span className="sim-telemetry-chip">
              <span className="sim-telemetry-key">Containers:</span>{' '}
              <strong>{containers}</strong>
            </span>
            <span className="sim-telemetry-chip">
              <span className="sim-telemetry-key">Slots:</span>{' '}
              <strong>{slots}</strong>
            </span>
            <span className="sim-telemetry-chip">
              <span className="sim-telemetry-key">
                Input-stage ideal waves ({draft.dynamicAllocation ? 'initial' : 'fixed'}):
              </span>{' '}
              <strong>{idealWaves}</strong>
            </span>
          </div>

          {validation.blockers.length > 0 && (
            <div className="sim-feedback-stack">
              {validation.blockers.map((blocker, idx) => (
                <div
                  key={`${blocker.field}-${idx}`}
                  className="sim-blocker-box"
                  role="alert"
                >
                  <div className="sim-blocker-copy">
                    <AlertTriangle size={13} aria-hidden="true" />
                    <span>{blocker.reason}</span>
                  </div>
                  {blocker.suggestedPatch && (
                    <button
                      type="button"
                      className="sim-fix-btn"
                      onClick={() =>
                        onDraftChange({
                          ...draft,
                          ...blocker.suggestedPatch,
                        })
                      }
                    >
                      <Wrench size={11} aria-hidden="true" />
                      <span>Apply suggested fix</span>
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}

          {validation.notes.length > 0 && (
            <div className="sim-feedback-stack">
              {validation.notes.map((note, idx) => (
                <div key={idx} className="sim-note-box" role="note">
                  <Info size={12} aria-hidden="true" />
                  <span>{note}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="sim-panel-footer">
        <span
          className={[
            'sim-footer-status',
            !validation.valid
              ? 'is-blocked'
              : hasUnappliedChanges
                ? 'is-dirty'
                : '',
          ]
            .filter(Boolean)
            .join(' ')}
          aria-live="polite"
        >
          {!validation.valid
            ? 'Fix compatibility blocker above to run simulation'
            : hasUnappliedChanges
              ? 'Draft updated — run simulation to project new trace'
              : activeConfig !== null
                ? 'Draft matches currently active simulation'
                : `${jobMeta.short} · ${draft.deployMode.toUpperCase()} · ${containers} containers`}
        </span>
        <div className="sim-footer-actions">
          <button
            type="button"
            className="sim-secondary-btn"
            disabled={isDefaultDraft}
            onClick={onResetDraft}
          >
            <RotateCcw size={11} aria-hidden="true" />
            <span>Reset Draft</span>
          </button>
          <button
            type="button"
            className="sim-secondary-btn"
            onClick={onClose}
          >
            <span>Close</span>
          </button>
          <button
            type="button"
            className="sim-run-cta"
            disabled={!validation.valid}
            onClick={() => onRun(draft)}
          >
            <span>Run Simulation</span>
            <ArrowRight size={12} aria-hidden="true" />
          </button>
        </div>
      </div>
    </section>
  );
}
