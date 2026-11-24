import { Fragment, useEffect, useMemo, useRef } from 'react';
import {
  AlertTriangle,
  ArrowRight,
  Eye,
  FilterX,
  Pause,
  Play,
  RotateCcw,
  SkipBack,
  StepForward,
} from 'lucide-react';
import { COMPONENTS } from '../data/components';
import { STEP_CATEGORIES, STEP_CATEGORY_MAP } from '../data/runModel';
import type {
  ExecutionRun,
  ProjectedTimeline,
  SkippedSpan,
  StepCategory,
} from '../data/runModel';
import { CHANNEL_META } from '../data/types';
import type { ComponentId } from '../data/types';
import { messageTime } from '../playback';

export interface MessageTableProps {
  run: ExecutionRun;
  projection: ProjectedTimeline;
  activeEventId: string | null;
  progress?: number;
  playing: boolean;
  speed: number;
  onSelectEvent: (eventId: string) => void;
  onSelectComponent: (id: ComponentId) => void;
  onTogglePlay: () => void;
  onReset: () => void;
  onPrevEvent: () => void;
  onNextEvent: () => void;
  onSpeedChange: (speed: number) => void;
  onEnableCategories: (categories: StepCategory[]) => void;
  onResetFilters: () => void;
}

const SPEEDS = [0.5, 1, 1.5, 2] as const;

export default function MessageTable({
  run,
  projection,
  activeEventId,
  progress = 1,
  playing,
  speed,
  onSelectEvent,
  onSelectComponent,
  onTogglePlay,
  onReset,
  onPrevEvent,
  onNextEvent,
  onSpeedChange,
  onEnableCategories,
  onResetFilters,
}: MessageTableProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const total = projection.visibleFrames.length;
  const activeFrameIndex = activeEventId
    ? (projection.frameIndexByEventId.get(activeEventId) ?? -1)
    : -1;

  const skippedByBeforeIndex = useMemo(() => {
    const map = new Map<number, SkippedSpan[]>();
    for (const span of projection.skippedSpans) {
      const list = map.get(span.beforeFrameIndex) ?? [];
      list.push(span);
      map.set(span.beforeFrameIndex, list);
    }
    return map;
  }, [projection.skippedSpans]);

  const totalHiddenCount = useMemo(
    () => projection.skippedSpans.reduce((sum, s) => sum + s.eventCount, 0),
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

  useEffect(() => {
    const container = scrollRef.current;
    if (!container || activeFrameIndex < 0) return;
    const row = container.querySelector<HTMLElement>(
      `[data-frame-index="${activeFrameIndex}"]`,
    );
    if (!row) return;
    const parentRect = container.getBoundingClientRect();
    const rowRect = row.getBoundingClientRect();
    const headerHeight =
      container.querySelector('thead')?.getBoundingClientRect().height ?? 0;
    if (rowRect.bottom > parentRect.bottom) {
      container.scrollTop += rowRect.bottom - parentRect.bottom;
    } else if (rowRect.top < parentRect.top + headerHeight) {
      container.scrollTop -= parentRect.top + headerHeight - rowRect.top;
    }
  }, [activeFrameIndex, run.id]);

  const progressRatio =
    total === 0 || activeFrameIndex < 0
      ? 0
      : Math.min(1, Math.max(0, (activeFrameIndex + progress) / total));

  const displayIndex = total === 0 || activeFrameIndex < 0 ? 0 : activeFrameIndex + 1;

  function renderSkippedRow(span: SkippedSpan) {
    const catLabels = span.categories
      .map((c) => STEP_CATEGORY_MAP[c]?.short ?? c)
      .join(', ');
    return (
      <tr key={span.id} className="skipped-span-row">
        <td colSpan={6}>
          <div className="skipped-span-cell">
            <span className="skipped-span-copy">
              <Eye size={12} aria-hidden="true" />
              <span>
                {span.eventCount} {span.eventCount === 1 ? 'event' : 'events'} skipped (
                {catLabels})
              </span>
            </span>
            <button
              type="button"
              className="skipped-span-btn"
              onClick={(e) => {
                e.stopPropagation();
                onEnableCategories(span.categories);
              }}
            >
              Include
            </button>
          </div>
        </td>
      </tr>
    );
  }

  const trailingSpans = skippedByBeforeIndex.get(-1) ?? [];

  return (
    <section className="flow-panel panel" aria-label="Synchronized message trace log">
      <div className="flow-toolbar">
        <div className="flow-heading">
          <h2>WIRE TRACE LOG</h2>
          <span className="flow-counter" aria-live="polite">
            {String(displayIndex).padStart(2, '0')} / {String(total).padStart(2, '0')}
          </span>
          <span className="flow-note">
            {totalHiddenCount > 0
              ? `${totalHiddenCount} filtered out · click any row to inspect`
              : 'Click any row to inspect wire fields'}
          </span>
        </div>

        <div className="playback-controls">
          <label className="speed-select" title="Playback speed ([ / ])">
            <span className="sr-only">Playback speed</span>
            <select
              aria-label="Playback speed"
              value={speed}
              onChange={(e) => onSpeedChange(Number(e.target.value))}
            >
              {SPEEDS.map((s) => (
                <option key={s} value={s}>
                  {s}x
                </option>
              ))}
            </select>
          </label>

          <button
            type="button"
            className="control-button"
            title="Reset trace to Step 1 (R)"
            aria-label="Reset trace"
            onClick={onReset}
          >
            <RotateCcw size={13} />
            <span className="control-label">RESET</span>
          </button>

          <button
            type="button"
            className="icon-button"
            title="Previous event"
            aria-label="Previous event"
            disabled={activeFrameIndex <= 0}
            onClick={onPrevEvent}
          >
            <SkipBack size={14} />
          </button>

          <button
            type="button"
            className="play-button"
            title="Play or pause full trace (Space)"
            disabled={total === 0}
            onClick={onTogglePlay}
          >
            {playing ? <Pause size={13} /> : <Play size={13} fill="currentColor" />}
            <span>{playing ? 'PAUSE' : 'PLAY'}</span>
          </button>

          <button
            type="button"
            className="control-button"
            title="Step forward one event (S)"
            aria-label="Step forward one event"
            disabled={total === 0 || activeFrameIndex >= total - 1}
            onClick={onNextEvent}
          >
            <StepForward size={14} />
            <span className="control-label">STEP</span>
          </button>
        </div>
      </div>

      <div
        className="message-table-scroll"
        ref={scrollRef}
        role="region"
        aria-label="Message trace table"
        tabIndex={0}
      >
        {total === 0 ? (
          <div className="empty-projection compact" role="status">
            <FilterX size={18} aria-hidden="true" />
            <strong className="empty-projection-title">No matching events</strong>
            <span className="empty-projection-sub">
              {totalHiddenCount > 0
                ? `${totalHiddenCount} ${totalHiddenCount === 1 ? 'event' : 'events'} hidden at current depth${
                    skippedCategoryNames ? ` (${skippedCategoryNames})` : ''
                  }.`
                : 'No events available in this run.'}
            </span>
            <div
              className="empty-projection-actions"
              style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '8px',
                justifyContent: 'center',
              }}
            >
              {skippedCategories.length > 0 && (
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
                className={`step-nav-btn${skippedCategories.length > 0 ? '' : ' primary'}`}
                onClick={onResetFilters}
              >
                <RotateCcw size={12} aria-hidden="true" />
                <span>Reset all filters</span>
              </button>
            </div>
          </div>
        ) : (
          <table className="message-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Time</th>
                <th>From → To</th>
                <th>Protocol &amp; Port</th>
                <th>Message</th>
                <th>Size</th>
              </tr>
            </thead>
            <tbody>
              {projection.visibleFrames.map((frame) => {
                const ev = frame.event;
                const index = frame.frameIndex;
                const isCurrent = ev.id === activeEventId;
                const isTransmitted = activeFrameIndex >= index;
                const isErr = Boolean(ev.isError || ev.channel === 'error');
                const fromInst = run.instances[ev.fromInstance];
                const toInst = run.instances[ev.toInstance];
                const fromShort =
                  fromInst?.shortLabel ??
                  COMPONENTS[ev.fromComponent]?.shortName ??
                  ev.fromInstance;
                const toShort =
                  toInst?.shortLabel ??
                  COMPONENTS[ev.toComponent]?.shortName ??
                  ev.toInstance;
                const fromTitle =
                  fromInst?.label ??
                  COMPONENTS[ev.fromComponent]?.name ??
                  ev.fromInstance;
                const toTitle =
                  toInst?.label ??
                  COMPONENTS[ev.toComponent]?.name ??
                  ev.toInstance;
                const chMeta = CHANNEL_META[ev.channel];
                const leadingSpans = skippedByBeforeIndex.get(index) ?? [];

                return (
                  <Fragment key={ev.id}>
                    {leadingSpans.map(renderSkippedRow)}
                    <tr
                      data-frame-index={index}
                      className={[
                        isCurrent ? 'current-message' : '',
                        isTransmitted ? 'transmitted' : '',
                        isErr ? 'is-error' : '',
                        `ch-${ev.channel}`,
                      ]
                        .filter(Boolean)
                        .join(' ')}
                      onClick={() => onSelectEvent(ev.id)}
                    >
                      <td className="col-num">
                        <button
                          type="button"
                          className="msg-select-btn"
                          aria-current={isCurrent ? 'true' : undefined}
                          aria-label={`Inspect event ${index + 1}: ${ev.label}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            onSelectEvent(ev.id);
                          }}
                        >
                          <span>{String(index + 1).padStart(2, '0')}</span>
                          <span className="msg-step-pill">
                            S{frame.displayStepNumber}
                          </span>
                        </button>
                      </td>
                      <td className="col-time">
                        {messageTime(frame.canonicalEventIndex)}
                        <small>s</small>
                      </td>
                      <td className="col-route">
                        <button
                          type="button"
                          className="table-comp-link"
                          title={`Inspect ${fromTitle}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            onSelectComponent(ev.fromComponent);
                          }}
                        >
                          {fromShort}
                        </button>
                        <ArrowRight
                          size={11}
                          className="route-arrow"
                          aria-hidden="true"
                        />
                        <button
                          type="button"
                          className="table-comp-link"
                          title={`Inspect ${toTitle}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            onSelectComponent(ev.toComponent);
                          }}
                        >
                          {toShort}
                        </button>
                        {ev.peerTag && (
                          <span className="peer-tag-pill">{ev.peerTag}</span>
                        )}
                      </td>
                      <td className="col-proto">
                        <span
                          className={`wire-badge ch-${ev.channel}`}
                          title={chMeta.label}
                        >
                          {isErr ? (
                            <AlertTriangle size={10} aria-hidden="true" />
                          ) : (
                            <span className="channel-dot" aria-hidden="true" />
                          )}
                          <span>{ev.protocol}</span>
                        </span>
                        {ev.port && <span className="port-tag">{ev.port}</span>}
                      </td>
                      <td className="col-info">
                        <span className="msg-label-strong">
                          {isErr && (
                            <AlertTriangle
                              size={12}
                              className="inline-err-icon"
                              aria-label="Error event"
                            />
                          )}
                          {ev.label}
                        </span>
                        {ev.info && <span className="msg-info-muted"> — {ev.info}</span>}
                      </td>
                      <td className="col-size">{ev.size}</td>
                    </tr>
                  </Fragment>
                );
              })}
              {trailingSpans.map(renderSkippedRow)}
            </tbody>
          </table>
        )}
      </div>

      <div className="flow-footer">
        <span>
          {String(displayIndex).padStart(2, '0')} / {String(total).padStart(2, '0')} EVENTS
          {totalHiddenCount > 0 ? ` · ${totalHiddenCount} HIDDEN` : ''}
        </span>
        <div className="progress-track" aria-hidden="true">
          <div
            style={{
              transform: `scaleX(${progressRatio})`,
              transition: playing ? 'none' : undefined,
            }}
          />
        </div>
        <span>{run.layerTag}</span>
      </div>
    </section>
  );
}
