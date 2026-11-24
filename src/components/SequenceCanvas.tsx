import { useEffect, useMemo, useRef } from 'react';
import type { KeyboardEvent } from 'react';
import { COMPONENTS } from '../data/components';
import { STEP_CATEGORIES, STEP_CATEGORY_MAP } from '../data/runModel';
import type {
  ProjectedFrame,
  RunCanvasProps,
  RunEvent,
  RuntimeInstance,
  SkippedSpan,
  StepCategory,
} from '../data/runModel';
import { CHANNEL_META, SUBSYSTEM_META } from '../data/types';
import type {
  ComponentId,
  HostZoneId,
  SubsystemId,
} from '../data/types';
import { messageTime } from '../playback';

const FLOW_ZONE_RANK: Record<HostZoneId, number> = {
  'submit-host': 0,
  'yarn-control': 1,
  'driver-jvm': 2,
  'worker-node-a': 3,
  'worker-node-b': 4,
  'storage-history': 5,
};

const SHORT_HOST_LABEL: Record<HostZoneId, string> = {
  'submit-host': 'SUBMIT HOST',
  'yarn-control': 'YARN RM',
  'driver-jvm': 'DRIVER JVM',
  'worker-node-a': 'WORKER A',
  'worker-node-b': 'WORKER B',
  'storage-history': 'HDFS / HS',
};

const LEFT_GUTTER = 84;
const RIGHT_GUTTER = 96;
const MIN_COL_STEP = 165;
const HEADER_TOP = 16;
const HEADER_H = 74;
const HEADER_BOX_W = 154;
const BODY_START_Y = 98;
const ROW_H = 64;
const SKIP_BAND_H = 30;

type SequenceLayoutItem =
  | { kind: 'skip'; span: SkippedSpan; y: number; height: number }
  | { kind: 'frame'; frame: ProjectedFrame; y: number; height: number };

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

function truncateText(text: string, maxChars: number): string {
  if (maxChars <= 3) return text.slice(0, Math.max(0, maxChars));
  return text.length > maxChars ? `${text.slice(0, maxChars - 1)}\u2026` : text;
}

function subsystemAccent(subsystem: SubsystemId): string {
  switch (subsystem) {
    case 'submission':
    case 'cluster-hdfs':
      return 'var(--ch-yarn)';
    case 'scheduling':
    case 'executor-rpc':
      return 'var(--ch-rpc)';
    case 'block-storage':
    case 'shuffle':
      return 'var(--ch-data)';
    case 'python-output':
      return 'var(--ch-python)';
    case 'observability':
      return 'var(--ch-internal)';
    case 'driver-runtime':
    case 'sql-execution':
    default:
      return 'var(--accent)';
  }
}

function eventColor(ev: RunEvent): string {
  return ev.isError ? 'var(--ch-error)' : CHANNEL_META[ev.channel].cssVar;
}

/**
 * Samples a compact self-message loop to the right of `x` centered around `y`.
 */
function sampleSelfLoop(x: number, y: number, t: number): [number, number] {
  const loopW = 52;
  const topY = y + 2;
  const botY = y + 20;
  const u = clamp(t, 0, 1);
  if (u <= 0.35) {
    const f = u / 0.35;
    return [x + loopW * f, topY];
  }
  if (u <= 0.65) {
    const f = (u - 0.35) / 0.3;
    return [x + loopW, topY + (botY - topY) * f];
  }
  const f = (u - 0.65) / 0.35;
  return [x + loopW * (1 - f), botY];
}

function selfLoopTrail(x: number, y: number, t: number): string {
  const pts: [number, number][] = [];
  const steps = 18;
  const clamped = clamp(t, 0, 1);
  for (let i = 0; i <= steps; i++) {
    pts.push(sampleSelfLoop(x, y, (clamped * i) / steps));
  }
  return pts.map(([px, py]) => `${px.toFixed(1)},${py.toFixed(1)}`).join(' ');
}

export interface SequenceCanvasProps extends RunCanvasProps {
  onEnableCategories?: (categories: StepCategory[]) => void;
}

export default function SequenceCanvas({
  run,
  projection,
  activeEventId,
  progress,
  reducedMotion,
  selectedComponentId,
  onSelectComponent,
  onSelectEvent,
  onEnableCategories,
}: SequenceCanvasProps) {
  const wrapRef = useRef<HTMLDivElement | null>(null);

  const participants = useMemo(() => {
    const firstMention = new Map<string, number>();
    let seq = 0;
    const record = (instId: string) => {
      const inst = run.instances[instId];
      if (!inst || !COMPONENTS[inst.componentId]) return;
      if (!firstMention.has(instId)) {
        firstMention.set(instId, seq++);
      }
    };

    for (const frame of projection.visibleFrames) {
      record(frame.event.fromInstance);
      record(frame.event.toInstance);
    }
    if (firstMention.size === 0) {
      for (const id of run.focusInstances) record(id);
      for (const ev of run.events) {
        record(ev.fromInstance);
        record(ev.toInstance);
      }
    }

    const instances: RuntimeInstance[] = Array.from(firstMention.keys()).map(
      (id) => run.instances[id],
    );
    instances.sort((a, b) => {
      const rankDiff = FLOW_ZONE_RANK[a.hostZone] - FLOW_ZONE_RANK[b.hostZone];
      if (rankDiff !== 0) return rankDiff;
      return (firstMention.get(a.id) ?? 999) - (firstMention.get(b.id) ?? 999);
    });
    return instances;
  }, [run.instances, run.focusInstances, run.events, projection.visibleFrames]);

  const { layoutItems, yByEventId, contentBottomY } = useMemo(() => {
    const skipsByBefore = new Map<number, SkippedSpan[]>();
    for (const span of projection.skippedSpans) {
      const list = skipsByBefore.get(span.beforeFrameIndex) ?? [];
      list.push(span);
      skipsByBefore.set(span.beforeFrameIndex, list);
    }

    const items: SequenceLayoutItem[] = [];
    const yMap = new Map<string, number>();
    let currY = BODY_START_Y;

    if (projection.visibleFrames.length > 0) {
      for (let i = 0; i < projection.visibleFrames.length; i++) {
        const frame = projection.visibleFrames[i];
        const leadingSkips = skipsByBefore.get(frame.frameIndex);
        if (leadingSkips) {
          for (const span of leadingSkips) {
            items.push({ kind: 'skip', span, y: currY + SKIP_BAND_H / 2, height: SKIP_BAND_H });
            currY += SKIP_BAND_H;
          }
        }
        const centerY = currY + ROW_H / 2;
        items.push({ kind: 'frame', frame, y: centerY, height: ROW_H });
        yMap.set(frame.event.id, centerY);
        currY += ROW_H;
      }

      const trailingSkips = skipsByBefore.get(-1);
      if (trailingSkips) {
        for (const span of trailingSkips) {
          items.push({ kind: 'skip', span, y: currY + SKIP_BAND_H / 2, height: SKIP_BAND_H });
          currY += SKIP_BAND_H;
        }
      }
    }

    return { layoutItems: items, yByEventId: yMap, contentBottomY: currY };
  }, [projection.visibleFrames, projection.skippedSpans]);

  const colStep = useMemo(() => {
    if (participants.length <= 1) return MIN_COL_STEP;
    const targetSpan = 980 - LEFT_GUTTER - RIGHT_GUTTER;
    return Math.max(MIN_COL_STEP, Math.floor(targetSpan / participants.length));
  }, [participants.length]);

  const viewW = Math.max(
    980,
    LEFT_GUTTER + Math.max(1, participants.length) * colStep + RIGHT_GUTTER,
  );
  const viewH = Math.max(420, contentBottomY + 46);

  const xByInstance = useMemo(() => {
    const map = new Map<string, number>();
    const totalColsW = Math.max(1, participants.length) * colStep;
    const usableSpan = viewW - LEFT_GUTTER - RIGHT_GUTTER;
    const startOffset = LEFT_GUTTER + Math.max(0, (usableSpan - totalColsW) / 2);
    participants.forEach((inst, idx) => {
      map.set(inst.id, startOffset + idx * colStep + colStep / 2);
    });
    return map;
  }, [participants, colStep, viewW]);

  const activeFrameIndex = activeEventId
    ? projection.frameIndexByEventId.get(activeEventId) ?? -1
    : -1;
  const activeFrame =
    activeFrameIndex >= 0 && activeFrameIndex < projection.visibleFrames.length
      ? projection.visibleFrames[activeFrameIndex]
      : null;
  const activeEvent = activeFrame?.event ?? null;
  const effectiveProgress = reducedMotion ? 1 : clamp(progress, 0, 1);
  const activeColor = activeEvent ? eventColor(activeEvent) : 'var(--accent)';

  // Gently keep active row in vertical view below the sticky header when activeEventId advances
  useEffect(() => {
    const el = wrapRef.current;
    if (!el || !activeEventId || typeof el.scrollTo !== 'function') return;
    const rowY = yByEventId.get(activeEventId);
    if (rowY === undefined) return;
    const topVisible = el.scrollTop + BODY_START_Y;
    const bottomVisible = el.scrollTop + el.clientHeight;
    if (rowY - ROW_H < topVisible || rowY + ROW_H > bottomVisible) {
      const targetTop = Math.max(0, rowY - BODY_START_Y - (el.clientHeight - BODY_START_Y) * 0.35);
      el.scrollTo({ top: targetTop, behavior: reducedMotion ? 'auto' : 'smooth' });
    }
  }, [activeEventId, yByEventId, reducedMotion]);

  const handleHeaderKey = (id: ComponentId) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectComponent(id);
    }
  };

  const handleRowKey = (eventId: string) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectEvent(eventId);
    }
  };

  const handleSkipBandKey =
    (categories: StepCategory[]) => (e: KeyboardEvent<SVGGElement>) => {
      if (!onEnableCategories || categories.length === 0) return;
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        e.stopPropagation();
        onEnableCategories(categories);
      }
    };

  const visibleCount = projection.visibleFrames.length;
  const hiddenCount = useMemo(
    () => projection.skippedSpans.reduce((sum, span) => sum + span.eventCount, 0),
    [projection.skippedSpans],
  );
  const totalAtDepth = visibleCount + hiddenCount;
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

  const stickyHeaderH = BODY_START_Y;
  const bodyH = Math.max(320, viewH - stickyHeaderH);
  const svgWidth = viewW > 980 ? `${viewW}px` : '100%';
  const svgMinWidth = `${Math.min(viewW, 920)}px`;

  return (
    <div
      ref={wrapRef}
      className="sequence-canvas-wrap"
      style={{
        width: '100%',
        height: '100%',
        overflow: 'auto',
        background: 'var(--page)',
      }}
    >
      <div
        className="sequence-sticky-inner"
        style={{
          width: svgWidth,
          minWidth: svgMinWidth,
          position: 'relative',
          userSelect: 'none',
        }}
      >
        {/* Frozen Sticky Component Header Row */}
        <svg
          className="sequence-canvas-header"
          viewBox={`0 0 ${viewW} ${stickyHeaderH}`}
          role="group"
          aria-label={`Sequence diagram participants for ${run.name}`}
          style={{
            position: 'sticky',
            top: 0,
            zIndex: 4,
            width: '100%',
            height: `${stickyHeaderH}px`,
            display: 'block',
            background: 'var(--page)',
            borderBottom: '1px solid var(--border)',
          }}
        >
          <defs>
            <pattern id="sv-seq-grid-hdr" width="24" height="24" patternUnits="userSpaceOnUse">
              <circle cx="12" cy="12" r="1" fill="var(--grid)" />
            </pattern>
            <style>{`
              .sv-seq-header {
                cursor: pointer;
                outline: none;
              }
              .sv-seq-header:hover .sv-seq-header-box,
              .sv-seq-header:focus-visible .sv-seq-header-box {
                stroke: var(--accent);
                stroke-width: 1.9px;
              }
            `}</style>
          </defs>

          <rect x="0" y="0" width={viewW} height={stickyHeaderH} fill="var(--page)" />
          <rect x="0" y="0" width={viewW} height={stickyHeaderH} fill="url(#sv-seq-grid-hdr)" />

          {/* Lifeline stubs under each frozen header */}
          <g className="sv-seq-header-stubs">
            {participants.map((inst) => {
              const cx = xByInstance.get(inst.id)!;
              const isActiveEnd =
                activeEvent?.fromInstance === inst.id || activeEvent?.toInstance === inst.id;
              const isSelected = selectedComponentId === inst.componentId;
              return (
                <line
                  key={`stub-${inst.id}`}
                  x1={cx}
                  y1={HEADER_TOP + HEADER_H}
                  x2={cx}
                  y2={stickyHeaderH}
                  stroke={isSelected ? 'var(--accent)' : isActiveEnd ? activeColor : 'var(--border)'}
                  strokeWidth={isSelected || isActiveEnd ? 1.5 : 1.1}
                  strokeOpacity={isSelected || isActiveEnd ? 0.75 : 0.78}
                  strokeDasharray="4 5"
                />
              );
            })}
          </g>

          {/* Lifeline Header Boxes */}
          <g className="sv-seq-headers">
            {participants.map((inst) => {
              const spec = COMPONENTS[inst.componentId];
              const cx = xByInstance.get(inst.id)!;
              const x = cx - HEADER_BOX_W / 2;
              const y = HEADER_TOP;
              const isFocal = run.componentId === inst.componentId;
              const isSelected = selectedComponentId === inst.componentId;
              const isSender = activeEvent?.fromInstance === inst.id;
              const isReceiver = activeEvent?.toInstance === inst.id;
              const isActiveEnd = isSender || isReceiver;
              const accent = subsystemAccent(spec.subsystem);
              const hostSubline =
                inst.hostLabel || spec.port || SUBSYSTEM_META[spec.subsystem].short;
              const roleBadge = isSender && isReceiver ? 'SELF' : isSender ? 'TX' : isReceiver ? 'RX' : null;

              return (
                <g
                  key={`hdr-${inst.id}`}
                  className="sv-seq-header"
                  data-instance-id={inst.id}
                  data-component-id={inst.componentId}
                  role="button"
                  tabIndex={0}
                  aria-label={`${inst.label} (${inst.hostLabel})`}
                  onClick={() => onSelectComponent(inst.componentId)}
                  onKeyDown={handleHeaderKey(inst.componentId)}
                >
                  <title>
                    {`${inst.label} \u00B7 ${inst.hostLabel}${inst.containerId ? ` [${inst.containerId}]` : ''}${spec.port ? ` (${spec.port})` : ''}`}
                  </title>
                  {isSelected && (
                    <rect
                      x={x - 3}
                      y={y - 3}
                      width={HEADER_BOX_W + 6}
                      height={HEADER_H + 6}
                      rx="9"
                      fill="var(--selection)"
                      stroke="var(--accent)"
                      strokeWidth="1.4"
                    />
                  )}
                  <rect
                    className="sv-seq-header-box"
                    x={x}
                    y={y}
                    width={HEADER_BOX_W}
                    height={HEADER_H}
                    rx="7"
                    fill="var(--raised)"
                    stroke={
                      isSelected || isFocal
                        ? 'var(--accent)'
                        : isActiveEnd
                          ? activeColor
                          : 'var(--border)'
                    }
                    strokeWidth={isSelected || isActiveEnd ? 1.9 : isFocal ? 1.6 : 1.15}
                  />
                  {/* Top accent bar */}
                  <rect
                    x={x + 1}
                    y={y + 1}
                    width={HEADER_BOX_W - 2}
                    height="3.5"
                    rx="2"
                    fill={isActiveEnd ? activeColor : accent}
                  />
                  {/* Active TX / RX / SELF corner pill */}
                  {roleBadge && (
                    <g transform={`translate(${x + HEADER_BOX_W - (roleBadge === 'SELF' ? 35 : 25)}, ${y - 7})`}>
                      <rect
                        width={roleBadge === 'SELF' ? 32 : 22}
                        height="13"
                        rx="3"
                        fill={activeColor}
                      />
                      <text
                        x={roleBadge === 'SELF' ? 16 : 11}
                        y="9.5"
                        textAnchor="middle"
                        fill="var(--page)"
                        fontSize="7.8"
                        fontWeight="700"
                        fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                      >
                        {roleBadge}
                      </text>
                    </g>
                  )}
                  {/* Host zone tag */}
                  <text
                    x={cx}
                    y={y + 19}
                    textAnchor="middle"
                    fill="var(--faint)"
                    fontSize="8.8"
                    fontWeight="700"
                    letterSpacing="0.09em"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {SHORT_HOST_LABEL[inst.hostZone]}
                  </text>
                  {/* RuntimeInstance shortLabel */}
                  <text
                    x={cx}
                    y={y + 40}
                    textAnchor="middle"
                    fill="var(--text)"
                    fontSize="11.5"
                    fontWeight="700"
                    fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
                  >
                    {truncateText(inst.shortLabel, 20)}
                  </text>
                  {/* RuntimeInstance hostLabel */}
                  <text
                    x={cx}
                    y={y + 58}
                    textAnchor="middle"
                    fill="var(--muted)"
                    fontSize="9.5"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {truncateText(hostSubline, 22)}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>

        {/* Scrollable Sequence Lifelines & Events Body */}
        <svg
          className="sequence-canvas"
          viewBox={`0 ${stickyHeaderH} ${viewW} ${bodyH}`}
          role="group"
          aria-label={`Synchronized sequence diagram for ${run.name}`}
          data-event-id={activeEventId ?? ''}
          data-progress={progress}
          style={{
            width: '100%',
            height: `${bodyH}px`,
            display: 'block',
          }}
        >
          <defs>
            <pattern id="sv-seq-grid" width="24" height="24" patternUnits="userSpaceOnUse">
              <circle cx="12" cy="12" r="1" fill="var(--grid)" />
            </pattern>
            <style>{`
              .sv-seq-row {
                cursor: pointer;
                outline: none;
              }
              .sv-seq-row:hover .sv-seq-row-bg,
              .sv-seq-row:focus-visible .sv-seq-row-bg {
                fill: var(--selection);
                fill-opacity: 0.7;
              }
              .sv-seq-skip-band.is-interactive,
              .sv-seq-empty-include {
                cursor: pointer;
                outline: none;
              }
              .sv-seq-skip-band.is-interactive:hover .sv-seq-skip-pill,
              .sv-seq-skip-band.is-interactive:focus-visible .sv-seq-skip-pill,
              .sv-seq-empty-include:hover .sv-seq-empty-include-box,
              .sv-seq-empty-include:focus-visible .sv-seq-empty-include-box {
                stroke: var(--accent);
                stroke-dasharray: none;
                fill: var(--selection);
              }
              @keyframes sv-seq-pulse {
                0%, 100% { opacity: 0.2; }
                50% { opacity: 0.55; }
              }
              .sv-seq-orb-pulse {
                animation: sv-seq-pulse 1.1s ease-in-out infinite;
              }
              @media (prefers-reduced-motion: reduce) {
                .sv-seq-orb-pulse { animation: none; }
              }
            `}</style>
          </defs>

          {/* Background */}
          <rect x="0" y={stickyHeaderH} width={viewW} height={bodyH} fill="var(--page)" />
          <rect x="0" y={stickyHeaderH} width={viewW} height={bodyH} fill="url(#sv-seq-grid)" />

          {/* Vertical Lifelines */}
          <g className="sv-seq-lifelines">
            {participants.map((inst) => {
              const cx = xByInstance.get(inst.id)!;
              const isActiveEnd =
                activeEvent?.fromInstance === inst.id || activeEvent?.toInstance === inst.id;
              const isSelected = selectedComponentId === inst.componentId;
              return (
                <g key={`life-${inst.id}`} data-instance-id={inst.id}>
                  <line
                    x1={cx}
                    y1={stickyHeaderH}
                    x2={cx}
                    y2={viewH - 28}
                    stroke={isSelected ? 'var(--accent)' : isActiveEnd ? activeColor : 'var(--border)'}
                    strokeWidth={isSelected || isActiveEnd ? 1.5 : 1.1}
                    strokeOpacity={isSelected || isActiveEnd ? 0.65 : 0.78}
                    strokeDasharray="4 5"
                  />
                  {/* Bottom terminal cap */}
                  <rect
                    x={cx - 12}
                    y={viewH - 28}
                    width="24"
                    height="4"
                    rx="2"
                    fill={isSelected ? 'var(--accent)' : 'var(--border)'}
                  />
                </g>
              );
            })}
          </g>

        {/* Empty state */}
        {visibleCount === 0 && (() => {
          const hasHidden = hiddenCount > 0;
          const canIncludeEmpty = Boolean(
            hasHidden && onEnableCategories && skippedCategories.length > 0,
          );
          const boxW = 520;
          const boxH = canIncludeEmpty ? 114 : hasHidden && skippedCategoryNames ? 88 : 68;
          const boxX = viewW / 2 - boxW / 2;
          const boxY = Math.max(HEADER_TOP + HEADER_H + 40, viewH / 2 - boxH / 2);
          const includeLabel = `+ Include ${truncateText(skippedCategoryNames, 46)}`;
          const includePillW = clamp(includeLabel.length * 6.4 + 28, 170, boxW - 40);
          return (
            <g className="sv-seq-empty">
              <g role="status" aria-live="polite">
                <rect
                  x={boxX}
                  y={boxY}
                  width={boxW}
                  height={boxH}
                  rx="8"
                  fill="var(--panel)"
                  fillOpacity="0.95"
                  stroke="var(--border)"
                  strokeWidth="1.4"
                />
                <rect
                  x={boxX}
                  y={boxY}
                  width="4"
                  height={boxH}
                  rx="2"
                  fill="var(--accent)"
                />
                <text
                  x={viewW / 2}
                  y={boxY + 28}
                  textAnchor="middle"
                  fill="var(--text)"
                  fontSize="12.5"
                  fontWeight="600"
                  fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
                >
                  {hasHidden
                    ? 'No matching events for current category filters'
                    : 'No events recorded in this run'}
                </text>
                {hasHidden && (
                  <text
                    x={viewW / 2}
                    y={boxY + 48}
                    textAnchor="middle"
                    fill="var(--muted)"
                    fontSize="10"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {`${hiddenCount} ${hiddenCount === 1 ? 'event' : 'events'} hidden at current depth`}
                  </text>
                )}
                {hasHidden && skippedCategoryNames && (
                  <text
                    x={viewW / 2}
                    y={boxY + 66}
                    textAnchor="middle"
                    fill="var(--faint)"
                    fontSize="9.6"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {`Hidden categories: ${truncateText(skippedCategoryNames, 60)}`}
                  </text>
                )}
              </g>
              {canIncludeEmpty && (
                <g
                  className="sv-seq-empty-include"
                  role="button"
                  tabIndex={0}
                  aria-label={`Include ${skippedCategoryNames}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    onEnableCategories?.(skippedCategories);
                  }}
                  onKeyDown={handleSkipBandKey(skippedCategories)}
                >
                  <title>{`Include ${skippedCategoryNames}`}</title>
                  <rect
                    className="sv-seq-empty-include-box"
                    x={viewW / 2 - includePillW / 2}
                    y={boxY + 78}
                    width={includePillW}
                    height="24"
                    rx="12"
                    fill="var(--raised)"
                    stroke="var(--accent)"
                    strokeWidth="1.2"
                  />
                  <text
                    x={viewW / 2}
                    y={boxY + 93.5}
                    textAnchor="middle"
                    fill="var(--accent)"
                    fontSize="10"
                    fontWeight="600"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {includeLabel}
                  </text>
                </g>
              )}
            </g>
          );
        })()}

        {/* Synchronized Message Rows & Skipped-Span Separator Bands */}
        <g className="sv-seq-rows">
          {layoutItems.map((item) => {
            if (item.kind === 'skip') {
              const { span, y } = item;
              const catLabels = span.categories
                .map((cat) => STEP_CATEGORY_MAP[cat]?.short ?? cat)
                .join(', ');
              const eventWord = span.eventCount === 1 ? 'event' : 'events';
              const canInclude = Boolean(onEnableCategories && span.categories.length > 0);
              const prefixText = `${span.eventCount} skipped ${eventWord}`;
              const includeLabel = catLabels ? `+ Include ${catLabels}` : '+ Include';
              const maxBandChars = Math.max(28, Math.floor((viewW - 96) / 6.1));
              const displayPrefix = canInclude ? `${prefixText} \u00B7 ` : '';
              const displayInclude = canInclude
                ? truncateText(
                    includeLabel,
                    Math.max(12, maxBandChars - displayPrefix.length),
                  )
                : '';
              const fallbackRaw = catLabels
                ? `\u22EF ${prefixText} (${catLabels}) \u22EF`
                : `\u22EF ${prefixText} \u22EF`;
              const bandText = canInclude
                ? `${displayPrefix}${displayInclude}`
                : truncateText(fallbackRaw, maxBandChars);
              const pillW = clamp(bandText.length * 6.05 + 28, 200, viewW - 56);
              const pillH = 22;

              return (
                <g
                  key={span.id}
                  className={`sv-seq-skip-band${canInclude ? ' is-interactive' : ''}`}
                  data-before-frame-index={span.beforeFrameIndex}
                  data-skipped-count={span.eventCount}
                  role={canInclude ? 'button' : undefined}
                  tabIndex={canInclude ? 0 : undefined}
                  aria-label={
                    canInclude
                      ? `Include ${catLabels} (${span.eventCount} skipped ${eventWord})`
                      : undefined
                  }
                  pointerEvents={canInclude ? 'auto' : 'none'}
                  onClick={
                    canInclude
                      ? (e) => {
                          e.stopPropagation();
                          onEnableCategories?.(span.categories);
                        }
                      : undefined
                  }
                  onKeyDown={canInclude ? handleSkipBandKey(span.categories) : undefined}
                >
                  {canInclude && (
                    <title>
                      {`Click to include ${catLabels} (${span.eventCount} skipped ${eventWord})`}
                    </title>
                  )}
                  <line
                    x1="18"
                    y1={y}
                    x2={viewW - 18}
                    y2={y}
                    stroke="var(--border)"
                    strokeOpacity="0.72"
                    strokeWidth="1"
                    strokeDasharray="3 4"
                  />
                  <rect
                    className="sv-seq-skip-pill"
                    x={viewW / 2 - pillW / 2}
                    y={y - pillH / 2}
                    width={pillW}
                    height={pillH}
                    rx="11"
                    fill="var(--page)"
                    fillOpacity="0.96"
                    stroke="var(--border)"
                    strokeOpacity="0.85"
                    strokeWidth="1"
                    strokeDasharray="3 3"
                  />
                  <text
                    x={viewW / 2}
                    y={y + 3.4}
                    textAnchor="middle"
                    fill="var(--faint)"
                    fontSize="9.4"
                    letterSpacing="0.03em"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {canInclude ? (
                      <>
                        <tspan fill="var(--muted)">{displayPrefix}</tspan>
                        <tspan fill="var(--accent)" fontWeight="600">
                          {displayInclude}
                        </tspan>
                      </>
                    ) : (
                      bandText
                    )}
                  </text>
                </g>
              );
            }

            const { frame, y: rowY } = item;
            const ev = frame.event;
            const fromX = xByInstance.get(ev.fromInstance);
            const toX = xByInstance.get(ev.toInstance);
            if (fromX === undefined || toX === undefined) return null;

            const arrowY = rowY + 9;
            const isActive = ev.id === activeEventId;
            const isCompleted = activeFrameIndex >= 0 && frame.frameIndex < activeFrameIndex;
            const isError = Boolean(ev.isError);
            const color = eventColor(ev);
            const isSelf = ev.fromInstance === ev.toInstance;
            const isLocalSelf = isSelf && ev.kind === 'local';
            const dash = isLocalSelf ? '3 3' : CHANNEL_META[ev.channel].dash;
            const timeLabel = `${messageTime(frame.canonicalEventIndex)}s`;
            const hopNum = frame.frameIndex + 1;

            // Label formatting
            const channelShort = isLocalSelf
              ? 'LOCAL'
              : CHANNEL_META[ev.channel].short.toUpperCase();
            const portSuffix = ev.port && !isLocalSelf ? ` \u00B7 ${ev.port}` : '';
            const dir = toX >= fromX ? 1 : -1;
            const span = Math.abs(toX - fromX);
            const maxLabelChars = isSelf
              ? 30
              : clamp(Math.floor((span + 70) / 6.7), 20, 52);
            const fullLabel = truncateText(`${ev.label}${portSuffix}`, maxLabelChars);

            // Progressive active head coordinate
            const currX = isActive
              ? fromX + (toX - fromX) * effectiveProgress
              : toX;

            // Self-message coordinates
            const selfLoopPath = `M ${fromX} ${rowY + 2} H ${fromX + 52} V ${rowY + 20} H ${fromX + 5}`;
            const selfPacketPos = isActive ? sampleSelfLoop(fromX, rowY, effectiveProgress) : null;

            // Label center X clamped safely inside viewBox
            const estLabelW = fullLabel.length * 6.3 + channelShort.length * 6 + 30;
            const rawMidX = isSelf ? fromX + 28 : (fromX + toX) / 2;
            const labelCenterX = clamp(
              rawMidX,
              LEFT_GUTTER + estLabelW / 2 + 8,
              viewW - estLabelW / 2 - 14,
            );

            return (
              <g
                key={ev.id}
                className={`sv-seq-row${isActive ? ' is-active' : ''}${isError ? ' is-error' : ''}`}
                data-event-id={ev.id}
                role="button"
                tabIndex={0}
                aria-label={`Event ${hopNum} at ${timeLabel}: ${ev.label}`}
                onClick={() => onSelectEvent(ev.id)}
                onKeyDown={handleRowKey(ev.id)}
              >
                <title>
                  {`#${hopNum} (${timeLabel}) ${isLocalSelf ? 'LOCAL' : ev.protocol} \u00B7 ${ev.label}${ev.port ? ` [${ev.port}]` : ''}`}
                </title>

                {/* Row clickable highlight band */}
                <rect
                  className="sv-seq-row-bg"
                  x="10"
                  y={rowY - ROW_H / 2 + 3}
                  width={viewW - 20}
                  height={ROW_H - 6}
                  rx="6"
                  fill={
                    isError
                      ? 'var(--ch-error)'
                      : isActive
                        ? 'var(--selection)'
                        : 'var(--panel)'
                  }
                  fillOpacity={
                    isError
                      ? isActive
                        ? 0.16
                        : 0.06
                      : isActive
                        ? 0.92
                        : frame.frameIndex % 2 === 0
                          ? 0.35
                          : 0.16
                  }
                  stroke={isActive ? color : isError ? 'var(--ch-error)' : 'transparent'}
                  strokeOpacity={isActive ? 0.55 : isError ? 0.3 : 0}
                  strokeWidth="1.2"
                />

                {/* Active left indicator rail */}
                {isActive && (
                  <rect
                    x="10"
                    y={rowY - ROW_H / 2 + 6}
                    width="3.5"
                    height={ROW_H - 12}
                    rx="1.75"
                    fill={color}
                  />
                )}

                {/* Left Gutter: Visible Frame Number + Canonical Timestamp */}
                <circle
                  cx="30"
                  cy={rowY + 2}
                  r="9.5"
                  fill={isActive ? color : 'var(--panel)'}
                  stroke={isActive || isCompleted || isError ? color : 'var(--border)'}
                  strokeWidth="1.3"
                />
                <text
                  x="30"
                  y={rowY + 5.3}
                  textAnchor="middle"
                  fill={isActive ? 'var(--page)' : isCompleted ? color : 'var(--muted)'}
                  fontSize={hopNum >= 10 ? '8.4' : '9.2'}
                  fontWeight="700"
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {hopNum}
                </text>
                <text
                  x="76"
                  y={rowY + 5.5}
                  textAnchor="end"
                  fill={isActive ? 'var(--text)' : 'var(--faint)'}
                  fontSize="10"
                  fontWeight={isActive ? '600' : '400'}
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {timeLabel}
                </text>

                {/* Active lifeline execution bars */}
                {isActive && (
                  <>
                    <rect
                      x={fromX - 3.5}
                      y={rowY - 12}
                      width="7"
                      height="30"
                      rx="2.5"
                      fill={color}
                      fillOpacity="0.85"
                    />
                    {!isSelf && (
                      <rect
                        x={toX - 3.5}
                        y={rowY - 12}
                        width="7"
                        height="30"
                        rx="2.5"
                        fill={color}
                        fillOpacity={effectiveProgress > 0.85 ? 0.85 : 0.28}
                      />
                    )}
                  </>
                )}

                {/* Message Arrow (Horizontal or Self-Loop) */}
                {isSelf ? (
                  <g>
                    <path
                      d={selfLoopPath}
                      fill="none"
                      stroke={color}
                      strokeWidth={isActive ? 1.8 : 1.7}
                      strokeOpacity={isActive ? 0.28 : isCompleted ? 0.85 : 0.42}
                      strokeDasharray={dash}
                      strokeLinejoin="round"
                    />
                    {isActive && (
                      <>
                        <polyline
                          points={selfLoopTrail(fromX, rowY, effectiveProgress)}
                          fill="none"
                          stroke={color}
                          strokeWidth="6"
                          strokeOpacity="0.2"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                        <polyline
                          points={selfLoopTrail(fromX, rowY, effectiveProgress)}
                          fill="none"
                          stroke={color}
                          strokeWidth="2.6"
                          strokeDasharray={dash}
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </>
                    )}
                    {/* Return arrowhead into lifeline */}
                    <polygon
                      points="7,-3.8 -1,0 7,3.8"
                      fill={color}
                      fillOpacity={isActive ? (effectiveProgress > 0.88 ? 1 : 0.38) : isCompleted ? 0.9 : 0.55}
                      transform={`translate(${fromX + 4}, ${rowY + 20})`}
                    />
                    {isActive && selfPacketPos && (
                      <g>
                        <circle
                          className={reducedMotion ? undefined : 'sv-seq-orb-pulse'}
                          cx={selfPacketPos[0]}
                          cy={selfPacketPos[1]}
                          r="11"
                          fill={color}
                          fillOpacity="0.24"
                        />
                        <circle
                          cx={selfPacketPos[0]}
                          cy={selfPacketPos[1]}
                          r="4.8"
                          fill={color}
                          stroke="var(--panel)"
                          strokeWidth="1.6"
                        />
                      </g>
                    )}
                  </g>
                ) : (
                  <g>
                    {/* Base / completed line */}
                    <line
                      x1={fromX}
                      y1={arrowY}
                      x2={toX - dir * 5}
                      y2={arrowY}
                      stroke={color}
                      strokeWidth={isActive ? 1.8 : isCompleted ? 1.9 : 1.5}
                      strokeOpacity={isActive ? 0.26 : isCompleted ? 0.84 : 0.42}
                      strokeDasharray={dash}
                    />
                    {/* Progressive active glow & line */}
                    {isActive && (
                      <>
                        <line
                          x1={fromX}
                          y1={arrowY}
                          x2={currX}
                          y2={arrowY}
                          stroke={color}
                          strokeWidth="6.5"
                          strokeOpacity="0.2"
                          strokeLinecap="round"
                        />
                        <line
                          x1={fromX}
                          y1={arrowY}
                          x2={currX}
                          y2={arrowY}
                          stroke={color}
                          strokeWidth="2.7"
                          strokeDasharray={dash}
                          strokeLinecap="round"
                        />
                      </>
                    )}
                    {/* Destination arrowhead */}
                    <polygon
                      points={dir > 0 ? '-7,-4 1,0 -7,4' : '7,-4 -1,0 7,4'}
                      fill={color}
                      fillOpacity={isActive ? (effectiveProgress > 0.9 ? 1 : 0.4) : isCompleted ? 0.92 : 0.52}
                      transform={`translate(${toX - dir * 4}, ${arrowY})`}
                    />
                    {/* Traveling packet dot */}
                    {isActive && (
                      <g>
                        <circle
                          className={reducedMotion ? undefined : 'sv-seq-orb-pulse'}
                          cx={currX}
                          cy={arrowY}
                          r="12"
                          fill={color}
                          fillOpacity="0.22"
                        />
                        {isError && (
                          <circle
                            cx={currX}
                            cy={arrowY}
                            r="15"
                            fill="none"
                            stroke="var(--ch-error)"
                            strokeWidth="1.5"
                            strokeDasharray="3 2"
                          />
                        )}
                        <circle
                          cx={currX}
                          cy={arrowY}
                          r="5"
                          fill={color}
                          stroke="var(--panel)"
                          strokeWidth="1.7"
                        />
                      </g>
                    )}
                  </g>
                )}

                {/* Crisp Label + Channel Tag Pill above arrow */}
                <g transform={`translate(${(labelCenterX - estLabelW / 2).toFixed(1)}, ${(rowY - 22).toFixed(1)})`}>
                  <rect
                    x="0"
                    y="0"
                    width={estLabelW}
                    height="20"
                    rx="4"
                    fill="var(--panel)"
                    fillOpacity={isActive ? 0.96 : 0.86}
                    stroke={isActive ? color : 'var(--border)'}
                    strokeOpacity={isActive ? 0.9 : 0.65}
                    strokeWidth={isActive ? 1.15 : 0.85}
                  />
                  <rect
                    x="3"
                    y="3"
                    width={channelShort.length * 6 + 10}
                    height="14"
                    rx="2.5"
                    fill={color}
                    fillOpacity="0.18"
                  />
                  <text
                    x={3 + (channelShort.length * 6 + 10) / 2}
                    y="12.8"
                    textAnchor="middle"
                    fill={color}
                    fontSize="8.2"
                    fontWeight="700"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {channelShort}
                  </text>
                  <text
                    x={channelShort.length * 6 + 18}
                    y="13"
                    fill={isActive ? 'var(--text)' : isCompleted ? 'var(--text)' : 'var(--muted)'}
                    fontSize="10"
                    fontWeight={isActive ? '600' : '500'}
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {fullLabel}
                  </text>
                </g>
              </g>
            );
          })}
        </g>

        {/* Bottom Blueprint Coordinate Footer */}
        <g pointerEvents="none">
          <text
            x="16"
            y={viewH - 8}
            fill="var(--faint)"
            fontSize="9.4"
            letterSpacing="0.08em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            SEQUENCE DIAGRAM {'\u00B7'} CLICK ANY LIFELINE HEADER TO INSPECT COMPONENT {'\u00B7'} CLICK ANY ROW TO JUMP
          </text>
          <text
            x={viewW - 16}
            y={viewH - 8}
            textAnchor="end"
            fill="var(--faint)"
            fontSize="9.4"
            letterSpacing="0.06em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            {`${participants.length} LIFELINES \u00B7 ${
              hiddenCount === 0
                ? `${visibleCount} EVENTS`
                : `${visibleCount}/${totalAtDepth} EVENTS`
            }`}
          </text>
        </g>
        </svg>
      </div>
    </div>
  );
}
