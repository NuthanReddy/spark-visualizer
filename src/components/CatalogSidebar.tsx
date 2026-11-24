import { useEffect, useMemo, useRef } from 'react';
import {
  ChevronDown,
  ChevronRight,
  Cpu,
  GitBranch,
  Layers,
  X,
} from 'lucide-react';
import type {
  ExecutionRun,
  ProjectedStep,
  ProjectedTimeline,
  RunStep,
} from '../data/runModel';
import { SUBSYSTEM_META } from '../data/types';
import type { SubsystemId, TraceKind, TraceLesson } from '../data/types';

export interface CatalogSidebarProps {
  mode: TraceKind;
  onModeChange: (mode: TraceKind) => void;
  subsystemFilter: SubsystemId | 'all';
  onSubsystemFilterChange: (filter: SubsystemId | 'all') => void;
  lifecycleTraces: TraceLesson[];
  scenarioTraces: TraceLesson[];
  componentTraces: TraceLesson[];
  activeRun: ExecutionRun;
  activeRunId: string;
  projection: ProjectedTimeline;
  expandedStepIds: ReadonlySet<string>;
  activeEventId: string | null;
  deployMode: ExecutionRun['deployMode'];
  playbackStatus: 'PLAYING' | 'INSPECTING' | 'READY';
  onSelectTrace: (id: string) => void;
  onSelectEvent: (eventId: string) => void;
  onToggleExpandStep: (stepId: string) => void;
  onResetFilters: () => void;
  onCloseMobile: () => void;
}

type SidebarTreeRow =
  | {
      kind: 'macro-header';
      step: RunStep;
      depth: number;
      childCount: number;
      hasActiveDescendant: boolean;
      isFilteredEmpty: boolean;
    }
  | {
      kind: 'step';
      projStep: ProjectedStep;
      depth: number;
    };

const SUBSYSTEM_ORDER: SubsystemId[] = [
  'submission',
  'cluster-hdfs',
  'driver-runtime',
  'scheduling',
  'executor-rpc',
  'observability',
  'block-storage',
  'shuffle',
  'sql-execution',
  'python-output',
];

export default function CatalogSidebar({
  mode,
  onModeChange,
  subsystemFilter,
  onSubsystemFilterChange,
  lifecycleTraces,
  scenarioTraces,
  componentTraces,
  activeRun,
  activeRunId,
  projection,
  expandedStepIds,
  activeEventId,
  deployMode,
  playbackStatus,
  onSelectTrace,
  onSelectEvent,
  onToggleExpandStep,
  onResetFilters,
  onCloseMobile,
}: CatalogSidebarProps) {
  const selectedEntryRef = useRef<HTMLDivElement>(null);

  const activeFrameIndex = activeEventId
    ? (projection.frameIndexByEventId.get(activeEventId) ?? -1)
    : -1;
  const activeFrame =
    activeFrameIndex >= 0
      ? (projection.visibleFrames[activeFrameIndex] ?? null)
      : null;
  const activeStepId = activeFrame?.step.id ?? null;

  const sidebarTreeRows = useMemo<SidebarTreeRow[]>(() => {
    if (projection.runId !== activeRun.id) return [];

    const stepById = new Map<string, RunStep>();
    const childrenByParent = new Map<string, RunStep[]>();
    for (const step of activeRun.steps) {
      stepById.set(step.id, step);
      if (step.parentId) {
        const list = childrenByParent.get(step.parentId) ?? [];
        list.push(step);
        childrenByParent.set(step.parentId, list);
      }
    }

    const projectedById = new Map<string, ProjectedStep>();
    for (const projStep of projection.visibleSteps) {
      projectedById.set(projStep.step.id, projStep);
    }

    const activeAncestorIds = new Set(
      (activeFrame?.ancestors ?? []).map((a) => a.id),
    );
    const emptyExpandedSet = new Set(projection.emptyExpandedStepIds ?? []);

    const rows: SidebarTreeRow[] = [];
    const visited = new Set<string>();

    function visit(step: RunStep, depth: number) {
      if (visited.has(step.id)) return;
      visited.add(step.id);

      const children = childrenByParent.get(step.id) ?? [];
      const hasChildren = children.length > 0;
      const isExpanded = hasChildren && expandedStepIds.has(step.id);

      if (isExpanded) {
        rows.push({
          kind: 'macro-header',
          step,
          depth,
          childCount: children.length,
          hasActiveDescendant: activeAncestorIds.has(step.id),
          isFilteredEmpty: emptyExpandedSet.has(step.id),
        });
        for (const child of children) {
          visit(child, depth + 1);
        }
        return;
      }

      const projStep = projectedById.get(step.id);
      if (projStep) {
        rows.push({
          kind: 'step',
          projStep,
          depth,
        });
      }
    }

    for (const step of activeRun.steps) {
      if (!step.parentId || !stepById.has(step.parentId)) {
        visit(step, 0);
      }
    }

    return rows;
  }, [activeRun, projection, expandedStepIds, activeFrame]);

  const subsystemCounts = useMemo(() => {
    const counts: Partial<Record<SubsystemId, number>> = {};
    for (const trace of componentTraces) {
      if (trace.subsystem) {
        counts[trace.subsystem] = (counts[trace.subsystem] ?? 0) + 1;
      }
    }
    return counts;
  }, [componentTraces]);

  const visibleTraces = useMemo(() => {
    if (mode === 'lifecycle') return lifecycleTraces;
    if (mode === 'scenario') return scenarioTraces;
    if (subsystemFilter === 'all') return componentTraces;
    return componentTraces.filter((t) => t.subsystem === subsystemFilter);
  }, [mode, subsystemFilter, lifecycleTraces, scenarioTraces, componentTraces]);

  const totalCount =
    lifecycleTraces.length + scenarioTraces.length + componentTraces.length;

  const totalSkippedEvents = useMemo(
    () => projection.skippedSpans.reduce((sum, span) => sum + span.eventCount, 0),
    [projection.skippedSpans],
  );

  useEffect(() => {
    const entry = selectedEntryRef.current;
    if (!entry) return;
    const target = entry.querySelector('.current-step') ?? entry;
    const container = entry.closest('.catalog-list');
    if (!target || !container) return;
    const rowRect = target.getBoundingClientRect();
    const parentRect = container.getBoundingClientRect();
    if (rowRect.bottom > parentRect.bottom) {
      container.scrollTop += rowRect.bottom - parentRect.bottom + 8;
    } else if (rowRect.top < parentRect.top) {
      container.scrollTop -= parentRect.top - rowRect.top + 8;
    }
  }, [activeRunId, mode, subsystemFilter, activeStepId]);

  return (
    <aside className="catalog-panel panel" aria-label="Spark trace and component catalog">
      <div className="section-heading">
        <h1>TRACE CATALOG</h1>
        <span className="catalog-total" title="Total interactive traces">
          {totalCount}
        </span>
        <button
          type="button"
          className="icon-button mobile-catalog-close"
          aria-label="Close catalog"
          onClick={onCloseMobile}
        >
          <X size={17} />
        </button>
      </div>

      <nav className="catalog-mode-tabs" role="tablist" aria-label="Catalog sections">
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'lifecycle'}
          className={mode === 'lifecycle' ? 'active' : ''}
          onClick={() => onModeChange('lifecycle')}
        >
          <span>Lifecycle</span>
          <span className="tab-count">{lifecycleTraces.length}</span>
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'scenario'}
          className={mode === 'scenario' ? 'active' : ''}
          onClick={() => onModeChange('scenario')}
        >
          <span>Scenarios</span>
          <span className="tab-count">{scenarioTraces.length}</span>
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'component'}
          className={mode === 'component' ? 'active' : ''}
          onClick={() => onModeChange('component')}
        >
          <span>Components</span>
          <span className="tab-count">{componentTraces.length}</span>
        </button>
      </nav>

      {mode === 'component' && (
        <div
          className="subsystem-chips"
          role="group"
          aria-label="Filter components by subsystem"
        >
          <button
            type="button"
            className={`subsystem-chip ${subsystemFilter === 'all' ? 'active' : ''}`}
            aria-pressed={subsystemFilter === 'all'}
            onClick={() => onSubsystemFilterChange('all')}
          >
            All <span>{componentTraces.length}</span>
          </button>
          {SUBSYSTEM_ORDER.map((sub) => (
            <button
              key={sub}
              type="button"
              className={`subsystem-chip ${subsystemFilter === sub ? 'active' : ''}`}
              aria-pressed={subsystemFilter === sub}
              title={SUBSYSTEM_META[sub].label}
              onClick={() => onSubsystemFilterChange(sub)}
            >
              {SUBSYSTEM_META[sub].short}{' '}
              <span>{subsystemCounts[sub] ?? 0}</span>
            </button>
          ))}
        </div>
      )}

      <div className="catalog-list">
        {visibleTraces.map((trace, idx) => {
          const isSelected = trace.id === activeRunId;
          const showProjectedSteps = isSelected && projection.runId === trace.id;
          return (
            <div
              key={trace.id}
              ref={isSelected ? selectedEntryRef : undefined}
              className={`catalog-entry ${isSelected ? 'selected' : ''}`}
            >
              <button
                type="button"
                className="catalog-entry-button"
                aria-expanded={isSelected}
                onClick={() => onSelectTrace(trace.id)}
              >
                <span className="catalog-symbol" aria-hidden="true">
                  {trace.kind === 'lifecycle' ? (
                    <Layers size={14} />
                  ) : trace.kind === 'scenario' ? (
                    <span className="scenario-num">
                      {String(trace.scenarioNumber ?? idx + 1).padStart(2, '0')}
                    </span>
                  ) : isSelected ? (
                    <Cpu size={14} />
                  ) : (
                    <span className="diamond-marker" />
                  )}
                </span>
                <span className="catalog-entry-copy">
                  <span className="catalog-entry-title-row">
                    <strong>{trace.name}</strong>
                    {trace.kind === 'component' && trace.subsystem ? (
                      <span className="entry-mini-tag">
                        {SUBSYSTEM_META[trace.subsystem].short}
                      </span>
                    ) : (
                      <span className={`entry-mode-tag mode-${trace.deployMode}`}>
                        {trace.deployMode.toUpperCase()}
                      </span>
                    )}
                  </span>
                  <small>{trace.subtitle}</small>
                </span>
                {isSelected ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </button>

              {showProjectedSteps && (
                <div className="lesson-steps" role="list" aria-label="Projected run steps">
                  {sidebarTreeRows.length === 0 ? (
                    projection.skippedSpans.length === 0 ? (
                      <p className="sidebar-steps-empty">
                        No steps available in this run.
                      </p>
                    ) : null
                  ) : (
                    sidebarTreeRows.map((row) => {
                      if (row.kind === 'macro-header') {
                        return (
                          <div
                            key={`macro-${row.step.id}`}
                            role="listitem"
                            className="sidebar-macro-header-wrap"
                            style={
                              row.depth > 0
                                ? { paddingLeft: `${row.depth * 12}px` }
                                : undefined
                            }
                          >
                            <button
                              type="button"
                              className={[
                                'sidebar-macro-header',
                                row.hasActiveDescendant ? 'has-active-child' : '',
                                row.isFilteredEmpty ? 'is-filtered-empty' : '',
                              ]
                                .filter(Boolean)
                                .join(' ')}
                              aria-expanded={true}
                              title={
                                row.isFilteredEmpty
                                  ? `Expanded sub-steps hidden by filter — click to collapse ${row.step.title}`
                                  : `Collapse ${row.step.title} (${row.childCount} sub-steps)`
                              }
                              onClick={() => onToggleExpandStep(row.step.id)}
                            >
                              <ChevronDown
                                size={12}
                                className="sidebar-macro-chevron"
                                aria-hidden="true"
                              />
                              <span className="sidebar-macro-title">
                                {row.step.title}
                              </span>
                              <span className="sidebar-macro-meta">
                                {row.isFilteredEmpty ? 'filtered' : `${row.childCount}`}
                              </span>
                            </button>
                          </div>
                        );
                      }

                      const { projStep, depth } = row;
                      const isCurrent = projStep.step.id === activeStepId;
                      const firstEventId =
                        projection.visibleFrames[projStep.firstFrameIndex]?.event.id;
                      const showExpandBtn = projStep.hasChildren && !projStep.isExpanded;

                      return (
                        <div
                          key={projStep.step.id}
                          role="listitem"
                          className={[
                            'sidebar-step-row',
                            isCurrent ? 'is-current-row' : '',
                            depth > 0 ? 'is-substep' : '',
                          ]
                            .filter(Boolean)
                            .join(' ')}
                          style={
                            depth > 0
                              ? { paddingLeft: `${depth * 12}px` }
                              : undefined
                          }
                        >
                          <button
                            type="button"
                            className={[
                              'sidebar-step-select-btn',
                              isCurrent ? 'current-step' : '',
                              depth > 0 ? 'is-substep' : '',
                            ]
                              .filter(Boolean)
                              .join(' ')}
                            aria-current={isCurrent ? 'step' : undefined}
                            onClick={() => {
                              if (firstEventId) onSelectEvent(firstEventId);
                            }}
                          >
                            <span className="step-num">{projStep.displayNumber}</span>
                            <span className="step-title-text">
                              {projStep.step.title}
                            </span>
                            {isCurrent && (
                              <span className="step-active-dot" aria-hidden="true" />
                            )}
                          </button>

                          {showExpandBtn && (
                            <button
                              type="button"
                              className="sidebar-expand-btn"
                              aria-expanded={false}
                              aria-label={`Expand ${projStep.step.title} (${projStep.childCount} sub-steps)`}
                              title={`Go deeper: expand ${projStep.childCount} sub-steps`}
                              onClick={(e) => {
                                e.stopPropagation();
                                onToggleExpandStep(projStep.step.id);
                              }}
                            >
                              <ChevronRight size={11} aria-hidden="true" />
                              <span>+{projStep.childCount}</span>
                            </button>
                          )}
                        </div>
                      );
                    })
                  )}
                  {projection.skippedSpans.length > 0 && (
                    <div className="sidebar-skipped-notice" role="status">
                      <span className="sidebar-skipped-text">
                        {totalSkippedEvents}{' '}
                        {totalSkippedEvents === 1 ? 'event' : 'events'} hidden by
                        category filters —
                      </span>
                      <button
                        type="button"
                        className="sidebar-skipped-reset-btn"
                        onClick={onResetFilters}
                      >
                        Include all
                      </button>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}

        {visibleTraces.length === 0 && (
          <div className="catalog-empty">
            <GitBranch size={18} />
            <p>No traces match this filter.</p>
          </div>
        )}
      </div>

      <div className="catalog-footer">
        <span className="catalog-status">
          <span
            className={`status-dot ${
              playbackStatus === 'PLAYING'
                ? 'playing'
                : playbackStatus === 'INSPECTING'
                  ? 'inspecting'
                  : 'idle'
            }`}
            aria-hidden="true"
          />
          <span>{playbackStatus}</span>
        </span>
        <span className={`deploy-badge mode-${deployMode}`} title="Active run deploy mode">
          {deployMode.toUpperCase()}
        </span>
      </div>
    </aside>
  );
}
