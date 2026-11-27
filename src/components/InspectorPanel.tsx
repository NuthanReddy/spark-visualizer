import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  ArrowDownLeft,
  ArrowRight,
  ArrowUpRight,
  BookOpen,
  Check,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Copy,
  Cpu,
  ExternalLink,
  Eye,
  EyeOff,
  FilterX,
  FlaskConical,
  Layers,
  Map as MapIcon,
  Play,
  RotateCcw,
  Tag,
  Terminal,
  X,
} from 'lucide-react';
import { COMPONENT_LIST, COMPONENTS } from '../data/components';
import { STEP_CATEGORIES, STEP_CATEGORY_MAP } from '../data/runModel';
import type {
  ExecutionRun,
  ProjectedTimeline,
  RunEvent,
  StepCategory,
} from '../data/runModel';
import {
  CHANNEL_META,
  HOST_ZONES,
  SPARK_LANGUAGES,
  SUBSYSTEM_META,
} from '../data/types';
import type { ComponentId, SparkLanguage, TraceField } from '../data/types';
import { computePeerInteractions } from '../engine/peerInteractions';

export type InspectorTab = 'step' | 'component' | 'overview';

export interface InspectorPanelProps {
  tab: InspectorTab;
  onTabChange: (tab: InspectorTab) => void;
  run: ExecutionRun;
  projection: ProjectedTimeline;
  activeEventId: string | null;
  selectedComponentId: ComponentId | null;
  activeLanguage?: SparkLanguage;
  onPrevStep: () => void;
  onNextStep: () => void;
  onSelectStep: (stepId: string) => void;
  onSelectEvent: (eventId: string) => void;
  onSelectComponent: (id: ComponentId) => void;
  onTraceComponent: (id: ComponentId) => void;
  onViewOnClusterMap: (id: ComponentId) => void;
  onToggleExpandStep: (stepId: string) => void;
  onToggleCategory: (category: StepCategory) => void;
  onEnableCategories?: (categories: StepCategory[]) => void;
  onResetFilters: () => void;
  floating?: boolean;
  onCloseFloating?: () => void;
}

function synthesizePayload(event: RunEvent, run: ExecutionRun): string {
  const fromInst = run.instances[event.fromInstance];
  const toInst = run.instances[event.toInstance];
  const fromLabel =
    fromInst?.label ?? COMPONENTS[event.fromComponent]?.name ?? event.fromInstance;
  const toLabel =
    toInst?.label ?? COMPONENTS[event.toComponent]?.name ?? event.toInstance;
  const fieldsObj: Record<string, string> = {};
  for (const field of event.fields) {
    fieldsObj[field.name] = field.value;
  }
  return JSON.stringify(
    {
      id: event.id,
      kind: event.kind,
      protocol: event.protocol,
      channel: event.channel,
      port: event.port,
      from: fromLabel,
      to: toLabel,
      ...(event.peerTag ? { peer: event.peerTag } : {}),
      label: event.label,
      size: event.size,
      ...(event.isError ? { error: true } : {}),
      fields: fieldsObj,
    },
    null,
    2,
  );
}

interface MergedDetailField extends TraceField {
  source: 'step' | 'wire';
}

export default function InspectorPanel({
  tab,
  onTabChange,
  run,
  projection,
  activeEventId,
  selectedComponentId,
  activeLanguage = 'pyspark',
  onPrevStep,
  onNextStep,
  onSelectStep,
  onSelectEvent,
  onSelectComponent,
  onTraceComponent,
  onViewOnClusterMap,
  onToggleExpandStep,
  onToggleCategory,
  onEnableCategories,
  onResetFilters,
  floating = false,
  onCloseFloating,
}: InspectorPanelProps) {
  const [selectedFieldKey, setSelectedFieldKey] = useState<string | null>(null);
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const activeFrameIndex = useMemo(() => {
    if (!activeEventId || projection.visibleFrames.length === 0) return -1;
    const idx = projection.frameIndexByEventId.get(activeEventId);
    return idx !== undefined ? idx : -1;
  }, [activeEventId, projection]);

  const activeFrame =
    activeFrameIndex >= 0 ? projection.visibleFrames[activeFrameIndex] : null;
  const activeEvent = activeFrame?.event ?? null;
  const activeStep = activeFrame?.step ?? null;
  const activeProjectedStep = activeFrame
    ? (projection.visibleSteps[activeFrame.visibleStepIndex] ?? null)
    : null;

  useEffect(() => {
    setSelectedFieldKey(null);
    setCopiedId(null);
  }, [run.id, activeEvent?.id, activeStep?.id]);

  const mergedDetails = useMemo<MergedDetailField[]>(() => {
    const list: MergedDetailField[] = [];
    const seen = new Set<string>();
    if (activeStep?.detailsPassed) {
      for (const f of activeStep.detailsPassed) {
        const key = `${f.name}::${f.value}`;
        if (!seen.has(key)) {
          seen.add(key);
          list.push({ ...f, source: 'step' });
        }
      }
    }
    if (activeEvent?.fields) {
      for (const f of activeEvent.fields) {
        const key = `${f.name}::${f.value}`;
        if (!seen.has(key)) {
          seen.add(key);
          list.push({ ...f, source: 'wire' });
        }
      }
    }
    return list;
  }, [activeStep, activeEvent]);

  const commandEntries = useMemo(() => {
    const items: Array<{ id: string; label: string; text: string }> = [];
    const eventCmd = activeEvent?.command?.trim();
    const stepCmd = activeStep?.command?.trim();

    if (eventCmd) {
      items.push({ id: 'event-cmd', label: 'COMMAND / RPC INVOKED', text: eventCmd });
    }
    if (stepCmd && stepCmd !== eventCmd) {
      items.push({
        id: 'step-cmd',
        label: eventCmd ? 'STEP COMMAND' : 'COMMAND / RPC INVOKED',
        text: stepCmd,
      });
    }
    if (items.length === 0 && activeEvent) {
      const fromShort =
        run.instances[activeEvent.fromInstance]?.shortLabel ??
        COMPONENTS[activeEvent.fromComponent]?.shortName ??
        activeEvent.fromInstance;
      const toShort =
        run.instances[activeEvent.toInstance]?.shortLabel ??
        COMPONENTS[activeEvent.toComponent]?.shortName ??
        activeEvent.toInstance;
      items.push({
        id: 'synth-cmd',
        label: 'COMMAND / RPC INVOKED',
        text: `${activeEvent.protocol} (${activeEvent.port})  ${fromShort} -> ${toShort} :: ${activeEvent.label}`,
      });
    }
    return items;
  }, [activeEvent, activeStep, run.instances]);

  const payloadText = useMemo(() => {
    if (!activeEvent) return null;
    return activeEvent.payload ?? synthesizePayload(activeEvent, run);
  }, [activeEvent, run]);

  const inspectedComponentId: ComponentId =
    selectedComponentId ??
    run.componentId ??
    activeEvent?.fromComponent ??
    (run.focusInstances[0]
      ? run.instances[run.focusInstances[0]]?.componentId
      : undefined) ??
    'spark-driver';

  const inspectedComponent = COMPONENTS[inspectedComponentId];

  const peerInteractions = useMemo(
    () => computePeerInteractions(inspectedComponentId),
    [inspectedComponentId],
  );

  const { runtimeHostOverrides, actualRuntimeHosts } = useMemo(() => {
    if (!inspectedComponent) {
      return { runtimeHostOverrides: [], actualRuntimeHosts: [] as string[] };
    }
    const matching = Object.values(run.instances).filter(
      (inst) => inst.componentId === inspectedComponent.id,
    );
    const overrides = matching.filter(
      (inst) => inst.hostZone !== inspectedComponent.hostZone,
    );
    const hosts = Array.from(new Set(matching.map((inst) => inst.hostLabel)));
    return { runtimeHostOverrides: overrides, actualRuntimeHosts: hosts };
  }, [run.instances, inspectedComponent]);

  const activeInstances = useMemo(() => {
    const orderedIds = new Set<string>(run.focusInstances);
    for (const frame of projection.visibleFrames) {
      orderedIds.add(frame.event.fromInstance);
      orderedIds.add(frame.event.toInstance);
    }
    if (orderedIds.size === 0) {
      for (const id of Object.keys(run.instances)) orderedIds.add(id);
    }
    return Array.from(orderedIds)
      .map((id) => run.instances[id])
      .filter((inst): inst is NonNullable<typeof inst> => Boolean(inst));
  }, [run, projection.visibleFrames]);

  const expandableMacroForComponent = useMemo(() => {
    if (!inspectedComponent) return null;
    const compId = inspectedComponent.id;

    const childrenByParent = new Map<string, string[]>();
    for (const step of run.steps) {
      if (step.parentId) {
        const list = childrenByParent.get(step.parentId) ?? [];
        list.push(step.id);
        childrenByParent.set(step.parentId, list);
      }
    }

    const stepSubtreeInvolvesComponent = (rootStepId: string): boolean => {
      const scopeIds = new Set<string>();
      const stack = [rootStepId];
      while (stack.length > 0) {
        const cur = stack.pop()!;
        if (scopeIds.has(cur)) continue;
        scopeIds.add(cur);
        for (const kidId of childrenByParent.get(cur) ?? []) {
          stack.push(kidId);
        }
      }
      for (const step of run.steps) {
        if (scopeIds.has(step.id) && step.deepDiveComponentId === compId) {
          return true;
        }
      }
      for (const ev of run.events) {
        if (
          scopeIds.has(ev.stepId) &&
          (ev.fromComponent === compId || ev.toComponent === compId)
        ) {
          return true;
        }
      }
      return false;
    };

    if (
      activeProjectedStep?.hasChildren &&
      !activeProjectedStep.isExpanded &&
      stepSubtreeInvolvesComponent(activeProjectedStep.step.id)
    ) {
      return activeProjectedStep;
    }

    for (const projStep of projection.visibleSteps) {
      if (!projStep.hasChildren || projStep.isExpanded) continue;
      if (stepSubtreeInvolvesComponent(projStep.step.id)) {
        return projStep;
      }
    }

    return null;
  }, [
    inspectedComponent,
    run.steps,
    run.events,
    activeProjectedStep,
    projection.visibleSteps,
  ]);

  async function handleCopyCommand(id: string, text: string) {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(id);
      window.setTimeout(
        () => setCopiedId((curr) => (curr === id ? null : curr)),
        1600,
      );
    } catch {
      setCopiedId(null);
    }
  }

  const isAlreadyTracingComponent =
    inspectedComponent && run.id === `comp-${inspectedComponent.id}`;

  const fromInst = activeEvent ? run.instances[activeEvent.fromInstance] : undefined;
  const toInst = activeEvent ? run.instances[activeEvent.toInstance] : undefined;
  const fromShortLabel = activeEvent
    ? (fromInst?.shortLabel ??
      COMPONENTS[activeEvent.fromComponent]?.shortName ??
      activeEvent.fromInstance)
    : '';
  const toShortLabel = activeEvent
    ? (toInst?.shortLabel ??
      COMPONENTS[activeEvent.toComponent]?.shortName ??
      activeEvent.toInstance)
    : '';
  const fromFullLabel = activeEvent
    ? (fromInst?.label ??
      COMPONENTS[activeEvent.fromComponent]?.name ??
      activeEvent.fromInstance)
    : '';
  const toFullLabel = activeEvent
    ? (toInst?.label ??
      COMPONENTS[activeEvent.toComponent]?.name ??
      activeEvent.toInstance)
    : '';

  const whatChanged = activeEvent && activeStep
    ? activeEvent.receiverState ||
      activeEvent.senderState ||
      activeStep.keyState ||
      activeEvent.info
    : '';

  const ancestors = activeFrame?.ancestors ?? [];
  const canGoDeeper = Boolean(
    activeProjectedStep?.hasChildren && !activeProjectedStep.isExpanded,
  );

  const prevFrame =
    activeFrameIndex > 0 ? projection.visibleFrames[activeFrameIndex - 1] : null;
  const nextFrame =
    activeFrameIndex >= 0 && activeFrameIndex < projection.visibleFrames.length - 1
      ? projection.visibleFrames[activeFrameIndex + 1]
      : null;

  const hiddenCount = useMemo(
    () => projection.skippedSpans.reduce((sum, span) => sum + span.eventCount, 0),
    [projection.skippedSpans],
  );
  const skippedCategories = useMemo<StepCategory[]>(() => {
    if (projection.skippedSpans.length === 0) return [];
    const seen = new Set<StepCategory>();
    for (const span of projection.skippedSpans) {
      for (const cat of span.categories) seen.add(cat);
    }
    return STEP_CATEGORIES.map((c) => c.id).filter((id) => seen.has(id));
  }, [projection.skippedSpans]);
  const skippedCategoryNames = useMemo(
    () => skippedCategories.map((c) => STEP_CATEGORY_MAP[c]?.short ?? c).join(', '),
    [skippedCategories],
  );

  return (
    <aside
      className={`inspector-panel panel ${floating ? 'floating-inspector' : ''}`}
      aria-label="Step, component, and run inspector"
    >
      <div className="inspector-tabs-bar">
        <div className="inspector-tabs" role="tablist" aria-label="Inspector mode">
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'step'}
            className={tab === 'step' ? 'active' : ''}
            onClick={() => onTabChange('step')}
          >
            <Terminal size={13} aria-hidden="true" />
            <span>Step Details</span>
            <span className="inspector-tab-pill">
              {activeProjectedStep
                ? String(activeProjectedStep.displayNumber).padStart(2, '0')
                : '--'}
            </span>
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'component'}
            className={tab === 'component' ? 'active' : ''}
            onClick={() => onTabChange('component')}
          >
            <Cpu size={13} aria-hidden="true" />
            <span>Component</span>
            {selectedComponentId && <span className="tab-live-dot" aria-hidden="true" />}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === 'overview'}
            className={tab === 'overview' ? 'active' : ''}
            onClick={() => onTabChange('overview')}
          >
            <BookOpen size={13} aria-hidden="true" />
            <span>Overview</span>
          </button>
        </div>
        {floating && onCloseFloating && (
          <button
            type="button"
            className="icon-button"
            aria-label="Close inspector"
            onClick={onCloseFloating}
          >
            <X size={16} />
          </button>
        )}
      </div>

      <div className="inspector-scroll">
        {tab === 'step' && projection.visibleFrames.length === 0 && (
          <div className="empty-projection" role="status">
            <FilterX size={20} aria-hidden="true" />
            <h2 className="empty-projection-title">
              No matching events — Reset filters
            </h2>
            <p className="empty-projection-sub">
              {hiddenCount > 0
                ? `${hiddenCount} ${hiddenCount === 1 ? 'event' : 'events'} hidden at current depth${
                    skippedCategoryNames ? ` (${skippedCategoryNames})` : ''
                  }.`
                : 'This run contains no events to display.'}
            </p>
            <div
              className="empty-projection-actions"
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '8px',
                justifyContent: 'center',
              }}
            >
              {skippedCategories.length > 0 && onEnableCategories && (
                <button
                  type="button"
                  className="step-nav-btn primary"
                  style={{ whiteSpace: 'normal', textAlign: 'center' }}
                  onClick={() => onEnableCategories(skippedCategories)}
                >
                  <Eye size={12} aria-hidden="true" />
                  <span>Include {skippedCategoryNames}</span>
                </button>
              )}
              <button
                type="button"
                className={`step-nav-btn${
                  skippedCategories.length > 0 && onEnableCategories ? '' : ' primary'
                }`}
                onClick={onResetFilters}
              >
                <RotateCcw size={12} aria-hidden="true" />
                <span>Reset all filters</span>
              </button>
            </div>
          </div>
        )}

        {tab === 'step' &&
          projection.visibleFrames.length > 0 &&
          (!activeFrame || !activeEvent || !activeStep || !activeProjectedStep) && (
            <div className="empty-projection" role="status">
              <Terminal size={20} aria-hidden="true" />
              <h2 className="empty-projection-title">
                Select a step or event to inspect
              </h2>
              <p className="empty-projection-sub">
                Choose a step in the catalog or click any event on the canvas or wire
                trace log to inspect its command, payload, and state transition.
              </p>
              {projection.visibleSteps[0] && (
                <button
                  type="button"
                  className="step-nav-btn primary"
                  onClick={() => onSelectStep(projection.visibleSteps[0].step.id)}
                >
                  <Play size={12} aria-hidden="true" />
                  <span>Inspect Step 01</span>
                </button>
              )}
            </div>
          )}

        {tab === 'step' && activeFrame && activeEvent && activeStep && activeProjectedStep && (
          <div className="step-inspector-body">
            <div className="step-nav-header">
              <span className="step-badge-pill">
                STEP {String(activeProjectedStep.displayNumber).padStart(2, '0')} /{' '}
                {String(projection.visibleSteps.length).padStart(2, '0')}
              </span>
              <div className="step-nav-buttons">
                <button
                  type="button"
                  className="step-nav-btn"
                  disabled={activeFrame.visibleStepIndex <= 0}
                  onClick={onPrevStep}
                  title="Previous step (Left Arrow)"
                >
                  <ChevronLeft size={14} />
                  <span>Prev</span>
                </button>
                <button
                  type="button"
                  className="step-nav-btn primary"
                  disabled={
                    activeFrame.visibleStepIndex >= projection.visibleSteps.length - 1
                  }
                  onClick={onNextStep}
                  title="Next step (Right Arrow)"
                >
                  <span>Next</span>
                  <ChevronRight size={14} />
                </button>
              </div>
            </div>

            {ancestors.length > 0 && (
              <div className="step-deep-dive-bar is-upward" aria-label="Parent step hierarchy">
                <div className="inspector-ancestors-trail">
                  <span className="inspector-ancestors-prefix">Inside:</span>
                  {ancestors.map((ancestor, idx) => (
                    <span key={ancestor.id} className="inspector-ancestor-item">
                      {idx > 0 && (
                        <ChevronRight
                          size={11}
                          className="breadcrumb-sep"
                          aria-hidden="true"
                        />
                      )}
                      <button
                        type="button"
                        className="inspector-ancestor-btn"
                        title={`Collapse back to: ${ancestor.title}`}
                        onClick={() => onToggleExpandStep(ancestor.id)}
                      >
                        {ancestor.title}
                      </button>
                    </span>
                  ))}
                </div>
                <button
                  type="button"
                  className="deep-dive-btn"
                  title={`Collapse ${ancestors[ancestors.length - 1].title}`}
                  onClick={() =>
                    onToggleExpandStep(ancestors[ancestors.length - 1].id)
                  }
                >
                  <ChevronUp size={13} aria-hidden="true" />
                  <span>Collapse</span>
                </button>
              </div>
            )}

            {canGoDeeper && (
              <div className="step-deep-dive-bar is-downward">
                <span className="step-deep-dive-label">
                  Macro summary ({activeProjectedStep.childCount}{' '}
                  {activeProjectedStep.childCount === 1 ? 'sub-step' : 'sub-steps'})
                </span>
                <button
                  type="button"
                  className="go-deeper-cta compact"
                  onClick={() => onToggleExpandStep(activeProjectedStep.step.id)}
                >
                  <Layers size={13} aria-hidden="true" />
                  <span>
                    Go Deeper ({activeProjectedStep.childCount}{' '}
                    {activeProjectedStep.childCount === 1 ? 'sub-step' : 'sub-steps'})
                  </span>
                </button>
              </div>
            )}

            <section className="inspector-section step-summary-section">
              <div className="inspector-category-row">
                <span
                  className="inspector-category-chip"
                  title={STEP_CATEGORY_MAP[activeFrame.effectiveCategory].description}
                >
                  <Tag size={10} aria-hidden="true" />
                  <span>
                    {STEP_CATEGORY_MAP[activeFrame.effectiveCategory].short}
                  </span>
                </span>
                <button
                  type="button"
                  className="inspector-skip-cat-btn"
                  title={`Hide all "${STEP_CATEGORY_MAP[activeFrame.effectiveCategory].label}" events`}
                  onClick={() => onToggleCategory(activeFrame.effectiveCategory)}
                >
                  <EyeOff size={11} aria-hidden="true" />
                  <span>
                    Skip {STEP_CATEGORY_MAP[activeFrame.effectiveCategory].short}
                  </span>
                </button>
              </div>
              <h2 className="inspector-step-title">{activeStep.title}</h2>
              {whatChanged && (
                <div className="what-changed-box">
                  <span className="what-changed-label">WHAT CHANGED</span>
                  <p>{whatChanged}</p>
                </div>
              )}
              <p className="inspector-prose">{activeStep.explanation}</p>
              {activeStep.keyState && activeStep.keyState !== whatChanged && (
                <div className="key-state-box">
                  <Activity size={13} aria-hidden="true" />
                  <div>
                    <span className="micro-label">KEY STATE</span>
                    <p>{activeStep.keyState}</p>
                  </div>
                </div>
              )}
            </section>

            {activeProjectedStep.frameIndices.length > 1 && (
              <section className="inspector-section">
                <div className="section-subhead">
                  <h3>EVENTS IN STEP {activeProjectedStep.displayNumber}</h3>
                  <span>{activeProjectedStep.frameIndices.length} events</span>
                </div>
                <div className="step-messages-list">
                  {activeProjectedStep.frameIndices.map((fIdx) => {
                    const frame = projection.visibleFrames[fIdx];
                    if (!frame) return null;
                    const ev = frame.event;
                    const isActive = ev.id === activeEvent.id;
                    const isErr = Boolean(ev.isError || ev.channel === 'error');
                    const evFrom =
                      run.instances[ev.fromInstance]?.shortLabel ??
                      COMPONENTS[ev.fromComponent]?.shortName ??
                      ev.fromInstance;
                    const evTo =
                      run.instances[ev.toInstance]?.shortLabel ??
                      COMPONENTS[ev.toComponent]?.shortName ??
                      ev.toInstance;
                    return (
                      <button
                        key={ev.id}
                        type="button"
                        className={`step-msg-item ch-${ev.channel} ${isActive ? 'active' : ''} ${isErr ? 'is-error' : ''}`}
                        aria-pressed={isActive}
                        onClick={() => onSelectEvent(ev.id)}
                      >
                        <span className="step-msg-index">
                          #{String(fIdx + 1).padStart(2, '0')}
                        </span>
                        <span className="step-msg-copy">
                          <strong>
                            {isErr && <AlertTriangle size={11} aria-hidden="true" />}
                            {ev.label}
                          </strong>
                          <small>
                            {evFrom} → {evTo} · {ev.protocol}
                          </small>
                        </span>
                      </button>
                    );
                  })}
                </div>
              </section>
            )}

            {commandEntries.length > 0 && (
              <section className="inspector-section">
                <div className="command-stack">
                  {commandEntries.map((entry) => (
                    <div key={entry.id} className="command-block">
                      <div className="section-subhead">
                        <h3>{entry.label}</h3>
                        <button
                          type="button"
                          className="copy-btn"
                          onClick={() => void handleCopyCommand(entry.id, entry.text)}
                          title={`Copy ${entry.label.toLowerCase()}`}
                        >
                          {copiedId === entry.id ? (
                            <Check size={12} />
                          ) : (
                            <Copy size={12} />
                          )}
                          <span>{copiedId === entry.id ? 'Copied' : 'Copy'}</span>
                        </button>
                      </div>
                      <pre className="command-box">
                        <code>{entry.text}</code>
                      </pre>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {mergedDetails.length > 0 && (
              <section className="inspector-section">
                <div className="section-subhead">
                  <h3>DETAILS PASSED (ILLUSTRATIVE)</h3>
                  <span>{mergedDetails.length} fields</span>
                </div>
                <div className="fields-list" role="list">
                  {mergedDetails.map((field, idx) => {
                    const key = `${field.source}-${idx}-${field.name}`;
                    const isSelected = selectedFieldKey === key;
                    return (
                      <button
                        key={key}
                        type="button"
                        role="listitem"
                        className={`detail-field-row ${isSelected ? 'selected' : ''}`}
                        aria-pressed={isSelected}
                        onClick={() =>
                          setSelectedFieldKey((prev) => (prev === key ? null : key))
                        }
                      >
                        <div className="detail-field-top">
                          <span className="detail-field-name">{field.name}</span>
                          <span className="detail-field-source">
                            {field.source === 'step' ? 'ARG' : 'WIRE'}
                          </span>
                        </div>
                        <strong className="detail-field-val">{field.value}</strong>
                        {field.note && (
                          <small className="detail-field-note">{field.note}</small>
                        )}
                      </button>
                    );
                  })}
                </div>
              </section>
            )}

            {payloadText && (
              <section className="inspector-section">
                <details className="payload-details">
                  <summary>
                    <span>ILLUSTRATIVE PAYLOAD / STRUCT</span>
                    <span className="summary-meta">{activeEvent.size}</span>
                  </summary>
                  <p className="illustrative-note">
                    <span className="illustrative-tag">ILLUSTRATIVE</span>
                    <span>
                      Illustrative fields for study — not a live wire capture.
                    </span>
                  </p>
                  <pre className="payload-pre">
                    <code>{payloadText}</code>
                  </pre>
                </details>
              </section>
            )}

            <section className="inspector-section route-section">
              <div className="section-subhead">
                <h3>ROUTE &amp; STATE TRANSITION</h3>
                <div className="msg-mini-nav">
                  <button
                    type="button"
                    className="icon-button tiny"
                    aria-label="Previous projected event"
                    disabled={!prevFrame}
                    onClick={() => prevFrame && onSelectEvent(prevFrame.event.id)}
                  >
                    <ChevronLeft size={13} />
                  </button>
                  <span>
                    {activeFrameIndex + 1}/{projection.visibleFrames.length}
                  </span>
                  <button
                    type="button"
                    className="icon-button tiny"
                    aria-label="Next projected event"
                    disabled={!nextFrame}
                    onClick={() => nextFrame && onSelectEvent(nextFrame.event.id)}
                  >
                    <ChevronRight size={13} />
                  </button>
                </div>
              </div>

              <div
                className={`route-card ch-${activeEvent.channel} ${
                  activeEvent.isError || activeEvent.channel === 'error'
                    ? 'is-error'
                    : ''
                }`}
              >
                <div className="route-endpoints">
                  <button
                    type="button"
                    className="endpoint-chip"
                    title={`Inspect component: ${fromFullLabel}${fromInst?.hostLabel ? ` (${fromInst.hostLabel})` : ''}`}
                    onClick={() => onSelectComponent(activeEvent.fromComponent)}
                  >
                    {fromShortLabel}
                  </button>
                  <ArrowRight size={13} className="route-arrow" aria-hidden="true" />
                  <button
                    type="button"
                    className="endpoint-chip"
                    title={`Inspect component: ${toFullLabel}${toInst?.hostLabel ? ` (${toInst.hostLabel})` : ''}`}
                    onClick={() => onSelectComponent(activeEvent.toComponent)}
                  >
                    {toShortLabel}
                  </button>
                  {activeEvent.peerTag && (
                    <span className="peer-tag-pill" title="Peer-to-peer instance route">
                      {activeEvent.peerTag}
                    </span>
                  )}
                </div>

                <div className="route-meta-row">
                  <span className={`wire-badge ch-${activeEvent.channel}`}>
                    {activeEvent.isError || activeEvent.channel === 'error' ? (
                      <AlertTriangle size={10} aria-hidden="true" />
                    ) : (
                      <span className="channel-dot" aria-hidden="true" />
                    )}
                    <span>{activeEvent.protocol}</span>
                  </span>
                  {activeEvent.kind === 'local' && (
                    <span className="meta-pill">
                      <Layers size={10} aria-hidden="true" /> LOCAL
                    </span>
                  )}
                  <span className="meta-pill">{activeEvent.port}</span>
                  <span className="meta-pill">{activeEvent.size}</span>
                  <span className="meta-pill channel-name">
                    {CHANNEL_META[activeEvent.channel].short}
                  </span>
                </div>

                <p className="route-info-text">{activeEvent.info}</p>
              </div>

              {(activeEvent.senderState || activeEvent.receiverState) && (
                <div className="state-transition-grid route-state-grid">
                  {activeEvent.senderState && (
                    <div className="state-card">
                      <span className="micro-label">
                        SENDER · {fromShortLabel}
                      </span>
                      <p>{activeEvent.senderState}</p>
                    </div>
                  )}
                  {activeEvent.receiverState && (
                    <div className="state-card receiver">
                      <span className="micro-label">
                        RECEIVER · {toShortLabel}
                      </span>
                      <p>{activeEvent.receiverState}</p>
                    </div>
                  )}
                </div>
              )}
            </section>
          </div>
        )}

        {tab === 'component' && inspectedComponent && (
          <div className="component-inspector-body">
            <div className="component-header-card">
              <div className="component-badges-row">
                <span className="meta-pill accent">
                  {SUBSYSTEM_META[inspectedComponent.subsystem].label}
                </span>
                <span className="meta-pill" title="Catalog host zone">
                  {runtimeHostOverrides.length > 0
                    ? `Catalog: ${HOST_ZONES[inspectedComponent.hostZone].label}`
                    : HOST_ZONES[inspectedComponent.hostZone].label}
                </span>
                {runtimeHostOverrides.length > 0 && actualRuntimeHosts.length > 0 && (
                  <span
                    className={`meta-pill mode-${run.deployMode}`}
                    title={`Catalog role (${HOST_ZONES[inspectedComponent.hostZone].label}) running on ${actualRuntimeHosts.join(', ')} in this trace`}
                  >
                    Runtime host in this trace: {actualRuntimeHosts.join(', ')}
                  </span>
                )}
              </div>
              <h2 className="component-name">{inspectedComponent.name}</h2>
              <div className="component-proc-row">
                <span>{inspectedComponent.process}</span>
                {inspectedComponent.port && (
                  <span className="port-tag">{inspectedComponent.port}</span>
                )}
              </div>

              <div className="component-actions">
                {expandableMacroForComponent && (
                  <button
                    type="button"
                    className="go-deeper-cta component-go-deeper-btn"
                    title={`Expand "${expandableMacroForComponent.step.title}" (${expandableMacroForComponent.childCount} sub-steps) inside the current run`}
                    onClick={() => {
                      onToggleExpandStep(expandableMacroForComponent.step.id);
                      onTabChange('step');
                    }}
                  >
                    <Layers size={13} aria-hidden="true" />
                    <span>Go Deeper into this step in current run</span>
                    <span className="go-deeper-count-pill">
                      +{expandableMacroForComponent.childCount}
                    </span>
                  </button>
                )}
                <button
                  type="button"
                  className="primary-action-btn"
                  onClick={() => onTraceComponent(inspectedComponent.id)}
                >
                  <Play size={13} fill="currentColor" />
                  <span>
                    {isAlreadyTracingComponent
                      ? 'Replay this component lesson'
                      : 'Open component lesson →'}
                  </span>
                </button>
                <button
                  type="button"
                  className="secondary-action-btn"
                  onClick={() => onViewOnClusterMap(inspectedComponent.id)}
                >
                  <MapIcon size={13} />
                  <span>Locate on All {COMPONENT_LIST.length} Map</span>
                </button>
              </div>
            </div>

            <section className="inspector-section">
              <h3 className="comp-section-label">ARCHITECTURAL ROLE</h3>
              <p className="inspector-prose">{inspectedComponent.role}</p>
            </section>

            <section className="inspector-section">
              <div className="distinction-card">
                <span className="micro-label">KEY DISTINCTION</span>
                <p>{inspectedComponent.keyDistinction}</p>
              </div>
            </section>

            <section className="inspector-section">
              <h3 className="comp-section-label">LANGUAGES</h3>
              <div className="comp-languages-row">
                {SPARK_LANGUAGES.map((langMeta) => {
                  const isSupported =
                    !inspectedComponent.languages ||
                    inspectedComponent.languages.includes(langMeta.id);
                  const isActiveLang = activeLanguage === langMeta.id;
                  const toneClass =
                    langMeta.id === 'pyspark'
                      ? 'lang-badge-py'
                      : langMeta.id === 'scala'
                        ? 'lang-badge-scala'
                        : 'lang-badge-java';
                  return (
                    <span
                      key={langMeta.id}
                      className={[
                        'lang-badge',
                        toneClass,
                        isSupported ? 'is-supported' : 'is-unsupported',
                        isActiveLang ? 'is-active-lang' : '',
                      ]
                        .filter(Boolean)
                        .join(' ')}
                      title={
                        isSupported
                          ? `${inspectedComponent.shortName} is active in ${langMeta.label} jobs${
                              isActiveLang ? ' (selected language)' : ''
                            }`
                          : `${inspectedComponent.shortName} is not used in pure ${langMeta.label} jobs`
                      }
                    >
                      <span>{langMeta.label}</span>
                      {!isSupported && (
                        <span className="lang-badge-state">N/A</span>
                      )}
                    </span>
                  );
                })}
              </div>
              {inspectedComponent.id === 'dataset-encoder-udf' ? (
                <p className="comp-lang-note">
                  Scala/Java typed Dataset encoders + JVM UDFs; PySpark invokes Java UDFs via spark.udf.registerJavaFunction
                </p>
              ) : inspectedComponent.languages?.length === 1 &&
                inspectedComponent.languages[0] === 'pyspark' ? (
                <p className="comp-lang-note">
                  PySpark-only runtime boundary; absent in pure Scala/Java jobs
                </p>
              ) : inspectedComponent.languages &&
                !inspectedComponent.languages.includes('pyspark') ? (
                <p className="comp-lang-note">
                  Scala/Java JVM entry point; PySpark enters via deploy.PythonRunner and Py4J instead
                </p>
              ) : null}
            </section>

            <section className="inspector-section">
              <div className="section-subhead">
                <h3>PEER INTERACTIONS</h3>
                <span>
                  {peerInteractions.receivesFrom.length} in ·{' '}
                  {peerInteractions.sendsTo.length} out
                </span>
              </div>
              <div className="peer-interactions-grid">
                <div className="peer-group">
                  <div className="peer-group-header">
                    <ArrowDownLeft size={12} aria-hidden="true" />
                    <span>RECEIVES FROM</span>
                    <span className="peer-group-count">
                      {peerInteractions.receivesFrom.length}
                    </span>
                  </div>
                  {peerInteractions.receivesFrom.length === 0 ? (
                    <p className="peer-empty">No inbound peers in catalog traces.</p>
                  ) : (
                    <div className="peer-cards-list">
                      {peerInteractions.receivesFrom.map((peer) => (
                        <button
                          key={`in-${peer.peerId}`}
                          type="button"
                          className="peer-card"
                          title={`Jump to ${peer.fullName} (${peer.hostZoneLabel}) · ${peer.labels.join(', ')}`}
                          onClick={() => onSelectComponent(peer.peerId)}
                        >
                          <div className="peer-card-top">
                            <span
                              className="peer-channel-dot"
                              style={{
                                background:
                                  CHANNEL_META[peer.primaryChannel].cssVar,
                              }}
                              aria-hidden="true"
                            />
                            <strong className="peer-card-name">
                              {peer.shortName}
                            </strong>
                            <span className="peer-card-zone">
                              {peer.hostZoneShort}
                            </span>
                            <ArrowUpRight
                              size={11}
                              className="peer-jump-icon"
                              aria-hidden="true"
                            />
                          </div>
                          <div className="peer-card-proto">
                            {peer.primaryProtocol}
                          </div>
                          <div className="peer-card-labels">
                            <span>{peer.labels.slice(0, 3).join(', ')}</span>
                            {peer.labels.length > 3 && (
                              <span className="peer-more-pill">
                                +{peer.labels.length - 3} more
                              </span>
                            )}
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                <div className="peer-group">
                  <div className="peer-group-header">
                    <ArrowUpRight size={12} aria-hidden="true" />
                    <span>SENDS TO</span>
                    <span className="peer-group-count">
                      {peerInteractions.sendsTo.length}
                    </span>
                  </div>
                  {peerInteractions.sendsTo.length === 0 ? (
                    <p className="peer-empty">No outbound peers in catalog traces.</p>
                  ) : (
                    <div className="peer-cards-list">
                      {peerInteractions.sendsTo.map((peer) => (
                        <button
                          key={`out-${peer.peerId}`}
                          type="button"
                          className="peer-card"
                          title={`Jump to ${peer.fullName} (${peer.hostZoneLabel}) · ${peer.labels.join(', ')}`}
                          onClick={() => onSelectComponent(peer.peerId)}
                        >
                          <div className="peer-card-top">
                            <span
                              className="peer-channel-dot"
                              style={{
                                background:
                                  CHANNEL_META[peer.primaryChannel].cssVar,
                              }}
                              aria-hidden="true"
                            />
                            <strong className="peer-card-name">
                              {peer.shortName}
                            </strong>
                            <span className="peer-card-zone">
                              {peer.hostZoneShort}
                            </span>
                            <ArrowUpRight
                              size={11}
                              className="peer-jump-icon"
                              aria-hidden="true"
                            />
                          </div>
                          <div className="peer-card-proto">
                            {peer.primaryProtocol}
                          </div>
                          <div className="peer-card-labels">
                            <span>{peer.labels.slice(0, 3).join(', ')}</span>
                            {peer.labels.length > 3 && (
                              <span className="peer-more-pill">
                                +{peer.labels.length - 3} more
                              </span>
                            )}
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </section>

            <section className="inspector-section">
              <h3 className="comp-section-label">LIFECYCLE OWNERSHIP</h3>
              <dl className="ownership-dl">
                <div>
                  <dt>Started by</dt>
                  <dd>{inspectedComponent.startedBy}</dd>
                </div>
                <div>
                  <dt>Monitored by</dt>
                  <dd>{inspectedComponent.monitoredBy}</dd>
                </div>
                <div>
                  <dt>Stopped by</dt>
                  <dd>{inspectedComponent.stoppedBy}</dd>
                </div>
              </dl>
            </section>

            <section className="inspector-section">
              <h3 className="comp-section-label">INPUTS &amp; OUTPUTS</h3>
              <div className="io-columns">
                <div className="io-box">
                  <span className="micro-label">INPUTS</span>
                  <ul>
                    {inspectedComponent.inputs.map((item, i) => (
                      <li key={i}>{item}</li>
                    ))}
                  </ul>
                </div>
                <div className="io-box">
                  <span className="micro-label">OUTPUTS</span>
                  <ul>
                    {inspectedComponent.outputs.map((item, i) => (
                      <li key={i}>{item}</li>
                    ))}
                  </ul>
                </div>
              </div>
            </section>

            <section className="inspector-section">
              <h3 className="comp-section-label">INTERNAL STATE</h3>
              <ul className="state-bullet-list">
                {inspectedComponent.internalState.map((item, i) => (
                  <li key={i}>{item}</li>
                ))}
              </ul>
            </section>

            <section className="inspector-section">
              <h3 className="comp-section-label error-heading">
                <AlertTriangle size={12} aria-hidden="true" />
                <span>FAILURE &amp; DIAGNOSTIC CLUES</span>
              </h3>
              <ul className="failure-clues-list">
                {inspectedComponent.failureClues.map((clue, i) => (
                  <li key={i}>{clue}</li>
                ))}
              </ul>
            </section>

            {inspectedComponent.sourceLinks.length > 0 && (
              <section className="inspector-section">
                <div className="section-subhead">
                  <h3>PINNED SOURCE (SPARK dada2a6 / HADOOP 3.4.1)</h3>
                </div>
                <div className="source-links-list">
                  {inspectedComponent.sourceLinks.map((link) => (
                    <a
                      key={link.url}
                      href={link.url}
                      target="_blank"
                      rel="noreferrer"
                      className="source-link-item"
                    >
                      <span>{link.label}</span>
                      <ExternalLink size={12} aria-hidden="true" />
                    </a>
                  ))}
                </div>
                {inspectedComponent.docFile && (
                  <p className="doc-ref-note">Ref: {inspectedComponent.docFile}</p>
                )}
              </section>
            )}
          </div>
        )}

        {tab === 'overview' && (
          <div className="overview-inspector-body">
            <div className="overview-header-card">
              <div className="component-badges-row">
                <span
                  className={
                    run.sourceKind === 'simulation'
                      ? 'meta-pill sim-run-badge'
                      : 'meta-pill accent'
                  }
                >
                  {run.sourceKind === 'simulation' && (
                    <FlaskConical size={10} aria-hidden="true" />
                  )}
                  <span>
                    {run.sourceKind === 'simulation'
                      ? 'SIMULATED RUN'
                      : run.traceKind === 'lifecycle'
                        ? 'LIFECYCLE FLOW'
                        : run.traceKind === 'scenario'
                          ? `SCENARIO #${String(run.scenarioNumber ?? 0).padStart(2, '0')}`
                          : 'COMPONENT LESSON'}
                  </span>
                </span>
                <span className={`deploy-badge mode-${run.deployMode}`}>
                  {run.deployMode.toUpperCase()}
                </span>
                <span className="meta-pill">{run.layerTag}</span>
              </div>
              <h2 className="overview-trace-title">{run.name}</h2>
              <p className="overview-trace-subtitle">{run.subtitle}</p>
              <p className="inspector-prose">{run.summary}</p>
            </div>

            {run.telemetry && (() => {
              const t = run.telemetry;
              const isFaultActive =
                Boolean(t.faultLabel) && !/^none\b/i.test(t.faultLabel.trim());
              return (
                <section
                  className="inspector-section sim-telemetry-section"
                  aria-label="Simulated cluster configuration"
                >
                  <div className="sim-telemetry-card">
                    <div className="section-subhead">
                      <h3>SIMULATED CLUSTER CONFIG</h3>
                      <span>
                        {t.containers} containers · {t.totalSlots} slots
                      </span>
                    </div>

                    <div className="sim-telemetry-action">
                      <span className="micro-label">ACTION</span>
                      <code title={t.actionSummary}>{t.actionSummary}</code>
                    </div>

                    <div className="sim-overview-chips">
                      <div className="sim-overview-chip">
                        <span className="sim-overview-chip-label">
                          Executors × Cores
                        </span>
                        <strong>
                          {t.numExecutors} × {t.executorCores}
                        </strong>
                      </div>
                      <div className="sim-overview-chip">
                        <span className="sim-overview-chip-label">Partitions</span>
                        <strong>{t.numPartitions}</strong>
                      </div>
                      <div className="sim-overview-chip">
                        <span className="sim-overview-chip-label">
                          Input-stage ideal waves ({t.dynamicAllocation ? 'initial' : 'fixed'})
                        </span>
                        <strong>{t.idealWavesPerStage}</strong>
                      </div>
                      <div
                        className={`sim-overview-chip ${
                          t.externalShuffleService ? 'is-on' : ''
                        }`}
                      >
                        <span className="sim-overview-chip-label">ESS</span>
                        <strong>
                          {t.externalShuffleService ? 'ON (:7337)' : 'OFF'}
                        </strong>
                      </div>
                      <div
                        className={`sim-overview-chip ${
                          t.dynamicAllocation ? 'is-on' : ''
                        }`}
                      >
                        <span className="sim-overview-chip-label">
                          Dynamic Alloc
                        </span>
                        <strong>{t.dynamicAllocation ? 'ON' : 'OFF'}</strong>
                      </div>
                      <div
                        className={`sim-overview-chip ${
                          t.aqeEnabled ? 'is-on' : ''
                        }`}
                      >
                        <span className="sim-overview-chip-label">AQE</span>
                        <strong>{t.aqeEnabled ? 'ON' : 'OFF'}</strong>
                      </div>
                      <div
                        className={`sim-overview-chip full-width ${
                          isFaultActive ? 'is-fault' : ''
                        }`}
                      >
                        <span className="sim-overview-chip-label">Fault</span>
                        <strong>
                          {isFaultActive && (
                            <AlertTriangle size={11} aria-hidden="true" />
                          )}
                          <span>{t.faultLabel}</span>
                        </strong>
                      </div>
                    </div>
                  </div>
                </section>
              );
            })()}

            <section className="inspector-section">
              <div className="section-subhead">
                <h3>ACTIVE RUNTIME INSTANCES</h3>
                <span>{activeInstances.length} instances</span>
              </div>
              <div className="component-chips-wrap">
                {activeInstances.map((inst) => {
                  const isPicked = selectedComponentId === inst.componentId;
                  return (
                    <button
                      key={inst.id}
                      type="button"
                      className={`comp-chip ${isPicked ? 'selected' : ''}`}
                      title={`${inst.label} · ${inst.hostLabel} (${inst.processId})`}
                      onClick={() => onSelectComponent(inst.componentId)}
                    >
                      <span>{inst.shortLabel}</span>
                    </button>
                  );
                })}
              </div>
            </section>

            <section className="inspector-section">
              <div className="section-subhead">
                <h3>VISIBLE STEPS</h3>
                <span>{projection.visibleSteps.length} steps</span>
              </div>
              {projection.visibleSteps.length === 0 ? (
                <div className="empty-projection compact" role="status">
                  <p className="empty-projection-sub">
                    No matching events for current category filters.
                  </p>
                  <button
                    type="button"
                    className="step-nav-btn primary"
                    onClick={onResetFilters}
                  >
                    <RotateCcw size={12} aria-hidden="true" />
                    <span>Reset filters</span>
                  </button>
                </div>
              ) : (
                <div className="overview-steps-list">
                  {projection.visibleSteps.map((projStep) => {
                    const isCurrent = activeStep?.id === projStep.step.id;
                    return (
                      <button
                        key={projStep.step.id}
                        type="button"
                        className={[
                          'overview-step-item',
                          isCurrent ? 'active' : '',
                          projStep.depth > 0 ? 'is-substep' : '',
                        ]
                          .filter(Boolean)
                          .join(' ')}
                        style={
                          projStep.depth > 0
                            ? { marginLeft: `${projStep.depth * 14}px` }
                            : undefined
                        }
                        aria-current={isCurrent ? 'step' : undefined}
                        onClick={() => {
                          onSelectStep(projStep.step.id);
                          onTabChange('step');
                        }}
                      >
                        <span className="overview-step-num">
                          {String(projStep.displayNumber).padStart(2, '0')}
                        </span>
                        <span className="overview-step-copy">
                          <strong>{projStep.step.title}</strong>
                          {projStep.step.keyState && (
                            <small>{projStep.step.keyState}</small>
                          )}
                        </span>
                        {projStep.hasChildren && !projStep.isExpanded && (
                          <span className="step-children-badge">
                            +{projStep.childCount}
                          </span>
                        )}
                        <ChevronRight size={13} aria-hidden="true" />
                      </button>
                    );
                  })}
                </div>
              )}
            </section>
          </div>
        )}
      </div>
    </aside>
  );
}
