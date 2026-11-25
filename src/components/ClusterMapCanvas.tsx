import { useMemo, useRef, useState } from 'react';
import type { KeyboardEvent, PointerEvent, WheelEvent } from 'react';
import { COMPONENT_LIST, COMPONENTS } from '../data/components';
import type { RunCanvasProps, RunEvent } from '../data/runModel';
import {
  CHANNEL_META,
  DRIVER_SUB_ZONES,
  HOST_ZONES,
  MASTER_CANVAS,
  SUBSYSTEM_META,
} from '../data/types';
import type {
  ChannelKind,
  ComponentId,
  HostZoneId,
  SubsystemId,
} from '../data/types';
import { buildEdgeCurve } from '../playback';

export type ClusterMapCanvasProps = RunCanvasProps & {
  zoom?: number;
  onZoomChange?: (z: number) => void;
};

const MIN_ZOOM = 0.55;
const MAX_ZOOM = 2.6;

const HOST_ZONE_ORDER: HostZoneId[] = [
  'submit-host',
  'yarn-control',
  'storage-history',
  'driver-jvm',
  'worker-node-a',
  'worker-node-b',
];

interface MapRoute {
  key: string;
  from: ComponentId;
  to: ComponentId;
  isPeerTransfer: boolean;
  isLocalOnly: boolean;
  frameIndices: number[];
  curve: ReturnType<typeof buildEdgeCurve>;
  arrowEnd: [number, number];
  arrowAngle: number;
}

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

function isPeerSameComponentEvent(ev: RunEvent): boolean {
  return ev.fromComponent === ev.toComponent && ev.fromInstance !== ev.toInstance;
}

function isLocalSelfEvent(ev: RunEvent): boolean {
  return ev.kind === 'local' && ev.fromInstance === ev.toInstance;
}

/**
 * Distinct wide dashed peer-transfer arc for cross-executor same-component traffic
 * (e.g. Exec-1 BlockManager -> Exec-2 BlockManager) projected onto one map card.
 */
function buildPeerTransferCurve(
  center: [number, number],
  hw: number,
  hh: number,
): ReturnType<typeof buildEdgeCurve> {
  const [cx, cy] = center;
  const rx = Math.max(58, hw * 0.72);
  const ry = 48;
  const baseY = cy - hh - 2;

  const sample = (t: number): [number, number] => {
    const u = clamp(t, 0, 1);
    const a = Math.PI * (1 - u);
    return [cx + Math.cos(a) * rx, baseY - Math.sin(a) * ry];
  };

  const trail = (t: number, segments = 24): [number, number][] => {
    const clamped = clamp(t, 0, 1);
    const pts: [number, number][] = [];
    for (let i = 0; i <= segments; i++) {
      pts.push(sample((clamped * i) / segments));
    }
    return pts;
  };

  return {
    path: `M ${(cx - rx).toFixed(1)} ${baseY.toFixed(1)} A ${rx.toFixed(1)} ${ry.toFixed(1)} 0 1 1 ${(cx + rx).toFixed(1)} ${baseY.toFixed(1)}`,
    sample,
    trail,
    midpoint: [cx, baseY - ry],
  };
}

function computeEdgeAnchors(
  fromCenter: [number, number],
  toCenter: [number, number],
  hw: number,
  hh: number,
  pad: number,
  lateralShift: number,
): { from: [number, number]; to: [number, number] } {
  const dx = toCenter[0] - fromCenter[0];
  const dy = toCenter[1] - fromCenter[1];
  const dist = Math.hypot(dx, dy);
  if (dist < 1) {
    return { from: fromCenter, to: toCenter };
  }

  const nx = -dy / dist;
  const ny = dx / dist;
  const shiftFrom: [number, number] = [
    fromCenter[0] + nx * lateralShift,
    fromCenter[1] + ny * lateralShift,
  ];
  const shiftTo: [number, number] = [
    toCenter[0] + nx * lateralShift,
    toCenter[1] + ny * lateralShift,
  ];

  const rx = hw + pad;
  const ry = hh + pad;
  const denom = Math.max(Math.abs(dx) / rx, Math.abs(dy) / ry);
  const frac = denom > 0 ? Math.min(0.4, 1 / denom) : 0;

  const p1: [number, number] = [shiftFrom[0] + dx * frac, shiftFrom[1] + dy * frac];
  const p2: [number, number] = [shiftTo[0] - dx * frac, shiftTo[1] - dy * frac];

  if (Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) < 14) {
    return { from: fromCenter, to: toCenter };
  }
  return { from: p1, to: p2 };
}

function MapPacketGlyph({
  channel,
  isError,
  color,
}: {
  channel: ChannelKind;
  isError?: boolean;
  color: string;
}) {
  if (isError || channel === 'error') {
    return (
      <g>
        <polygon
          points="0,-9.5 8.8,6.5 -8.8,6.5"
          fill="var(--panel)"
          stroke="var(--ch-error)"
          strokeWidth="2.4"
          strokeLinejoin="round"
        />
        <line x1="0" y1="-3.5" x2="0" y2="1.2" stroke="var(--ch-error)" strokeWidth="2.1" strokeLinecap="round" />
        <circle cx="0" cy="4" r="1.2" fill="var(--ch-error)" />
      </g>
    );
  }
  switch (channel) {
    case 'yarn':
      return (
        <polygon
          points="0,-8.5 7.8,-4.3 7.8,4.3 0,8.5 -7.8,4.3 -7.8,-4.3"
          fill={color}
          stroke="var(--panel)"
          strokeWidth="1.8"
        />
      );
    case 'rpc':
      return (
        <g>
          <polygon points="0,-9 9,0 0,9 -9,0" fill={color} stroke="var(--panel)" strokeWidth="1.8" />
          <circle cx="0" cy="0" r="2.4" fill="var(--panel)" />
        </g>
      );
    case 'data':
      return (
        <g>
          <rect x="-7.5" y="-7.5" width="15" height="15" rx="2.5" fill={color} stroke="var(--panel)" strokeWidth="1.8" />
          <line x1="-4" y1="-2" x2="4" y2="-2" stroke="var(--panel)" strokeWidth="1.6" />
          <line x1="-4" y1="2.2" x2="4" y2="2.2" stroke="var(--panel)" strokeWidth="1.6" />
        </g>
      );
    case 'python':
      return (
        <g>
          <circle cx="0" cy="0" r="8" fill="var(--panel)" stroke={color} strokeWidth="2.4" />
          <circle cx="-2.4" cy="-1.6" r="1.6" fill={color} />
          <circle cx="2.4" cy="1.6" r="1.6" fill={color} />
        </g>
      );
    case 'internal':
    default:
      return (
        <g>
          <circle cx="0" cy="0" r="7.2" fill="var(--panel)" stroke={color} strokeWidth="2.2" strokeDasharray="3 2" />
          <circle cx="0" cy="0" r="2.8" fill={color} />
        </g>
      );
  }
}

export default function ClusterMapCanvas({
  run,
  projection,
  activeEventId,
  progress,
  reducedMotion,
  selectedComponentId,
  onSelectComponent,
  onSelectEvent,
  zoom,
  onZoomChange,
}: ClusterMapCanvasProps) {
  const [internalZoom, setInternalZoom] = useState(1);
  const [pan, setPan] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [isPanning, setIsPanning] = useState(false);

  const svgRef = useRef<SVGSVGElement | null>(null);
  const dragRef = useRef<{
    active: boolean;
    moved: boolean;
    pointerId: number;
    startClientX: number;
    startClientY: number;
    startPanX: number;
    startPanY: number;
  }>({
    active: false,
    moved: false,
    pointerId: -1,
    startClientX: 0,
    startClientY: 0,
    startPanX: 0,
    startPanY: 0,
  });

  const effectiveZoom = clamp(zoom ?? internalZoom, MIN_ZOOM, MAX_ZOOM);

  const setZoomValue = (next: number) => {
    const clamped = Number(clamp(next, MIN_ZOOM, MAX_ZOOM).toFixed(2));
    if (onZoomChange) {
      onZoomChange(clamped);
    } else {
      setInternalZoom(clamped);
    }
  };

  // Compute safe card dimensions from actual clusterPos spacings so cards never overlap
  const { cardW, cardH } = useMemo(() => {
    let minSameRowDx = 190;
    let minSameColDy = 60;
    for (let i = 0; i < COMPONENT_LIST.length; i++) {
      const a = COMPONENT_LIST[i].clusterPos;
      for (let j = i + 1; j < COMPONENT_LIST.length; j++) {
        const b = COMPONENT_LIST[j].clusterPos;
        const dx = Math.abs(a[0] - b[0]);
        const dy = Math.abs(a[1] - b[1]);
        if (dy < 38 && dx > 4) {
          minSameRowDx = Math.min(minSameRowDx, dx);
        }
        if (dx < 140 && dy > 4) {
          minSameColDy = Math.min(minSameColDy, dy);
        }
      }
    }
    return {
      cardW: clamp(Math.floor(minSameRowDx - 14), 132, 166),
      cardH: clamp(Math.floor(minSameColDy - 10), 36, 44),
    };
  }, []);

  const participantSet = useMemo(() => {
    const set = new Set<ComponentId>();
    if (projection.visibleFrames.length > 0) {
      for (const frame of projection.visibleFrames) {
        if (COMPONENTS[frame.event.fromComponent]) set.add(frame.event.fromComponent);
        if (COMPONENTS[frame.event.toComponent]) set.add(frame.event.toComponent);
      }
    } else {
      for (const ev of run.events) {
        if (COMPONENTS[ev.fromComponent]) set.add(ev.fromComponent);
        if (COMPONENTS[ev.toComponent]) set.add(ev.toComponent);
      }
    }
    if (run.componentId && COMPONENTS[run.componentId]) {
      set.add(run.componentId);
    }
    return set;
  }, [projection.visibleFrames, run.events, run.componentId]);

  const activeFrameIndex = activeEventId
    ? projection.frameIndexByEventId.get(activeEventId) ?? -1
    : -1;
  const activeFrame =
    activeFrameIndex >= 0 && activeFrameIndex < projection.visibleFrames.length
      ? projection.visibleFrames[activeFrameIndex]
      : null;
  const activeEvent = activeFrame?.event ?? null;
  const activeIsPeerTransfer = activeEvent ? isPeerSameComponentEvent(activeEvent) : false;
  const activeIsLocalSelf = activeEvent ? isLocalSelfEvent(activeEvent) : false;

  const { routes, routeByKey } = useMemo(() => {
    const grouped = new Map<
      string,
      { from: ComponentId; to: ComponentId; isPeerTransfer: boolean; indices: number[] }
    >();
    projection.visibleFrames.forEach((frame, index) => {
      const ev = frame.event;
      if (!COMPONENTS[ev.fromComponent] || !COMPONENTS[ev.toComponent]) return;
      const peer = isPeerSameComponentEvent(ev);
      const key = `${ev.fromComponent}::${ev.toComponent}${peer ? '::peer' : ''}`;
      const existing = grouped.get(key);
      if (existing) {
        existing.indices.push(index);
      } else {
        grouped.set(key, {
          from: ev.fromComponent,
          to: ev.toComponent,
          isPeerTransfer: peer,
          indices: [index],
        });
      }
    });

    const list: MapRoute[] = [];
    const byKey = new Map<string, MapRoute>();
    const hw = cardW / 2;
    const hh = cardH / 2;

    for (const [key, item] of grouped.entries()) {
      const fromPos = COMPONENTS[item.from].clusterPos;
      const toPos = COMPONENTS[item.to].clusterPos;
      const isSelf = item.from === item.to;
      const isLocalOnly =
        isSelf &&
        !item.isPeerTransfer &&
        item.indices.every((idx) => {
          const ev = projection.visibleFrames[idx]?.event;
          return ev ? isLocalSelfEvent(ev) : false;
        });
      const hasReverse = !isSelf && grouped.has(`${item.to}::${item.from}`);

      let curve: ReturnType<typeof buildEdgeCurve>;
      if (item.isPeerTransfer) {
        curve = buildPeerTransferCurve(fromPos, hw, hh);
      } else {
        const anchors = isSelf
          ? { from: fromPos, to: toPos }
          : computeEdgeAnchors(fromPos, toPos, hw, hh, 4, hasReverse ? 7 : 0);
        curve = buildEdgeCurve(anchors.from, anchors.to, hasReverse ? 0.21 : 0.14);
      }

      const pEnd = curve.sample(1);
      const pPrev = curve.sample(0.95);
      const arrowAngle = (Math.atan2(pEnd[1] - pPrev[1], pEnd[0] - pPrev[0]) * 180) / Math.PI;

      const entry: MapRoute = {
        key,
        from: item.from,
        to: item.to,
        isPeerTransfer: item.isPeerTransfer,
        isLocalOnly,
        frameIndices: item.indices,
        curve,
        arrowEnd: pEnd,
        arrowAngle,
      };
      list.push(entry);
      byKey.set(key, entry);
    }

    return { routes: list, routeByKey: byKey };
  }, [projection.visibleFrames, cardW, cardH]);

  const activeRouteKey = activeEvent
    ? `${activeEvent.fromComponent}::${activeEvent.toComponent}${activeIsPeerTransfer ? '::peer' : ''}`
    : null;
  const activeRoute = activeRouteKey ? routeByKey.get(activeRouteKey) ?? null : null;
  const effectiveProgress = reducedMotion ? 1 : clamp(progress, 0, 1);
  const activeColor = activeEvent ? eventColor(activeEvent) : 'var(--accent)';

  const packetState = useMemo(() => {
    if (!activeEvent || !activeRoute) return null;
    const pos = activeRoute.curve.sample(effectiveProgress);
    const trailPts = activeRoute.curve.trail(effectiveProgress, 28);
    const trailStr = trailPts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');

    const peerTransfer = isPeerSameComponentEvent(activeEvent);
    const localSelf = isLocalSelfEvent(activeEvent);

    const peerPrefix = peerTransfer
      ? activeEvent.peerTag ||
        `${run.instances[activeEvent.fromInstance]?.shortLabel ?? activeEvent.fromInstance} -> ${run.instances[activeEvent.toInstance]?.shortLabel ?? activeEvent.toInstance}`
      : null;

    const channelTag = peerTransfer
      ? activeEvent.channel === 'internal'
        ? 'PEER'
        : CHANNEL_META[activeEvent.channel].short.toUpperCase()
      : localSelf
        ? 'LOCAL'
        : CHANNEL_META[activeEvent.channel].short.toUpperCase();

    const rawLabel = peerPrefix
      ? `${peerPrefix} \u00B7 ${activeEvent.label}`
      : localSelf
        ? `LOCAL \u00B7 ${activeEvent.label}`
        : activeEvent.label;
    const labelText = truncateText(rawLabel, peerPrefix ? 48 : 38);
    const pillH = 30;
    const tagW = channelTag.length * 7 + 16;
    const textW = labelText.length * 7.4 + 22;
    const pillW = localSelf
      ? clamp(labelText.length * 7.4 + 38, 160, 440)
      : clamp(tagW + textW + 12, 160, 520);
    const preferBelow = pos[1] < 80;
    const rawY = preferBelow ? pos[1] + 24 : pos[1] - pillH - 20;
    const pillX = clamp(pos[0] - pillW / 2, 18, MASTER_CANVAS.width - pillW - 18);
    const pillY = clamp(rawY, 16, MASTER_CANVAS.height - pillH - 36);

    return {
      pos,
      trailStr,
      peerTransfer,
      localSelf,
      channelTag,
      labelText,
      tagW,
      pillW,
      pillH,
      pillX,
      pillY,
    };
  }, [activeEvent, activeRoute, effectiveProgress, run.instances]);

  const vw = MASTER_CANVAS.width / effectiveZoom;
  const vh = MASTER_CANVAS.height / effectiveZoom;
  const vx = (MASTER_CANVAS.width - vw) / 2 - pan.x;
  const vy = (MASTER_CANVAS.height - vh) / 2 - pan.y;

  const handleResetView = () => {
    setPan({ x: 0, y: 0 });
    setZoomValue(1);
  };

  const handleFocusTrace = () => {
    const pts = Array.from(participantSet)
      .map((id) => COMPONENTS[id]?.clusterPos)
      .filter((p): p is [number, number] => Boolean(p));
    if (pts.length === 0) {
      handleResetView();
      return;
    }
    const minX = Math.min(...pts.map((p) => p[0])) - 180;
    const maxX = Math.max(...pts.map((p) => p[0])) + 180;
    const minY = Math.min(...pts.map((p) => p[1])) - 120;
    const maxY = Math.max(...pts.map((p) => p[1])) + 120;
    const spanW = Math.max(460, maxX - minX);
    const spanH = Math.max(300, maxY - minY);
    const nextZoom = clamp(
      Math.min(MASTER_CANVAS.width / spanW, MASTER_CANVAS.height / spanH),
      1,
      2.1,
    );
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    setZoomValue(nextZoom);
    setPan({
      x: MASTER_CANVAS.width / 2 - centerX,
      y: MASTER_CANVAS.height / 2 - centerY,
    });
  };

  const handlePointerDown = (e: PointerEvent<SVGSVGElement>) => {
    if (e.button !== 0) return;
    dragRef.current = {
      active: true,
      moved: false,
      pointerId: e.pointerId,
      startClientX: e.clientX,
      startClientY: e.clientY,
      startPanX: pan.x,
      startPanY: pan.y,
    };
  };

  const handlePointerMove = (e: PointerEvent<SVGSVGElement>) => {
    const drag = dragRef.current;
    if (!drag.active || drag.pointerId !== e.pointerId) return;
    const dxPx = e.clientX - drag.startClientX;
    const dyPx = e.clientY - drag.startClientY;

    if (!drag.moved && Math.hypot(dxPx, dyPx) > 4) {
      drag.moved = true;
      setIsPanning(true);
      e.currentTarget.setPointerCapture(e.pointerId);
    }
    if (!drag.moved) return;

    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0 || rect.height === 0) return;
    const unitScale = Math.max(vw / rect.width, vh / rect.height);
    const maxPanX = MASTER_CANVAS.width * 0.72;
    const maxPanY = MASTER_CANVAS.height * 0.72;

    setPan({
      x: clamp(drag.startPanX + dxPx * unitScale, -maxPanX, maxPanX),
      y: clamp(drag.startPanY + dyPx * unitScale, -maxPanY, maxPanY),
    });
  };

  const endDrag = (e: PointerEvent<SVGSVGElement>) => {
    const drag = dragRef.current;
    if (!drag.active || drag.pointerId !== e.pointerId) return;
    if (drag.moved && e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
    drag.active = false;
    setIsPanning(false);
  };

  const handleWheel = (e: WheelEvent<SVGSVGElement>) => {
    if (!e.ctrlKey && !e.metaKey) return;
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.12 : 0.9;
    setZoomValue(effectiveZoom * factor);
  };

  const handleNodeClick = (id: ComponentId) => {
    if (dragRef.current.moved) {
      dragRef.current.moved = false;
      return;
    }
    onSelectComponent(id);
  };

  const handleNodeKey = (id: ComponentId) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectComponent(id);
    }
  };

  const handleHopClick = (eventId: string) => {
    if (dragRef.current.moved) {
      dragRef.current.moved = false;
      return;
    }
    onSelectEvent(eventId);
  };

  const handleHopKey = (eventId: string) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectEvent(eventId);
    }
  };

  const hudBtnStyle = {
    background: 'var(--panel)',
    color: 'var(--text)',
    border: '1px solid var(--border)',
    borderRadius: '5px',
    padding: '4px 8px',
    fontSize: '11px',
    fontFamily: "var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)",
    cursor: 'pointer',
    lineHeight: 1.2,
  } as const;

  const visibleCount = projection.visibleFrames.length;
  const hiddenCount = projection.skippedSpans.reduce(
    (sum, span) => sum + span.eventCount,
    0,
  );
  const totalAtDepth = visibleCount + hiddenCount;

  return (
    <div
      className="cluster-map-wrap"
      style={{
        width: '100%',
        height: '100%',
        position: 'relative',
        overflow: 'hidden',
        background: 'var(--page)',
      }}
    >
      {/* Floating pan/zoom HUD */}
      <div
        className="cluster-map-controls"
        style={{
          position: 'absolute',
          top: '10px',
          right: '12px',
          zIndex: 4,
          display: 'flex',
          alignItems: 'center',
          gap: '6px',
          padding: '4px 6px',
          borderRadius: '7px',
          background: 'var(--panel)',
          border: '1px solid var(--border)',
          opacity: 0.94,
        }}
      >
        <button
          type="button"
          style={hudBtnStyle}
          aria-label="Zoom out cluster map"
          onClick={() => setZoomValue(effectiveZoom - 0.2)}
        >
          -
        </button>
        <span
          style={{
            minWidth: '42px',
            textAlign: 'center',
            fontSize: '11px',
            color: 'var(--muted)',
            fontFamily: "var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)",
          }}
        >
          {Math.round(effectiveZoom * 100)}%
        </span>
        <button
          type="button"
          style={hudBtnStyle}
          aria-label="Zoom in cluster map"
          onClick={() => setZoomValue(effectiveZoom + 0.2)}
        >
          +
        </button>
        <button
          type="button"
          style={hudBtnStyle}
          onClick={handleFocusTrace}
          title="Center and zoom onto active trace participants"
        >
          Focus trace
        </button>
        <button
          type="button"
          style={hudBtnStyle}
          onClick={handleResetView}
          title={`Reset pan and zoom to show all ${COMPONENT_LIST.length} components`}
        >
          Reset view
        </button>
      </div>

      <svg
        ref={svgRef}
        className="cluster-map-canvas"
        viewBox={`${vx.toFixed(1)} ${vy.toFixed(1)} ${vw.toFixed(1)} ${vh.toFixed(1)}`}
        preserveAspectRatio="xMidYMid meet"
        role="group"
        aria-label={`All-${COMPONENT_LIST.length} PySpark-on-YARN cluster architecture map for ${run.name}`}
        data-event-id={activeEventId ?? ''}
        data-progress={progress}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onWheel={handleWheel}
        style={{
          width: '100%',
          height: '100%',
          display: 'block',
          cursor: isPanning ? 'grabbing' : 'grab',
          touchAction: 'none',
          userSelect: 'none',
        }}
      >
        <defs>
          <pattern id="sv-map-grid" width="28" height="28" patternUnits="userSpaceOnUse">
            <circle cx="14" cy="14" r="1.15" fill="var(--grid)" />
          </pattern>
          <style>{`
            .cluster-node {
              cursor: pointer;
              outline: none;
              opacity: 0.44;
              transition: opacity 150ms ease;
            }
            .cluster-node.is-participant,
            .cluster-node.is-selected,
            .cluster-node:hover,
            .cluster-node:focus-visible {
              opacity: 1;
            }
            .cluster-node:hover .sv-map-card,
            .cluster-node:focus-visible .sv-map-card {
              stroke: var(--accent);
              stroke-width: 2.2px;
            }
            .sv-map-hop {
              cursor: pointer;
              outline: none;
            }
            .sv-map-hop:hover circle,
            .sv-map-hop:focus-visible circle {
              stroke: var(--accent);
              stroke-width: 2.2px;
            }
            .sv-map-packet {
              cursor: pointer;
            }
            @keyframes sv-map-halo {
              0%, 100% { opacity: 0.28; }
              50% { opacity: 0.78; }
            }
            .sv-map-pulse {
              animation: sv-map-halo 1.4s ease-in-out infinite;
            }
            @media (prefers-reduced-motion: reduce) {
              .sv-map-pulse { animation: none; }
            }
          `}</style>
        </defs>

        {/* Infinite-feel blueprint backdrop */}
        <rect
          x={-MASTER_CANVAS.width}
          y={-MASTER_CANVAS.height}
          width={MASTER_CANVAS.width * 3}
          height={MASTER_CANVAS.height * 3}
          fill="var(--page)"
        />
        <rect
          x={-MASTER_CANVAS.width}
          y={-MASTER_CANVAS.height}
          width={MASTER_CANVAS.width * 3}
          height={MASTER_CANVAS.height * 3}
          fill="url(#sv-map-grid)"
        />

        {/* 6 Host Zones */}
        {HOST_ZONE_ORDER.map((zoneId) => {
          const zone = HOST_ZONES[zoneId];
          const { x, y, w, h } = zone.rect;
          return (
            <g key={zoneId} className="sv-map-host-zone">
              <rect
                x={x}
                y={y}
                width={w}
                height={h}
                rx="10"
                fill="var(--panel)"
                fillOpacity="0.8"
                stroke="var(--border)"
                strokeWidth="1.25"
              />
              <line
                x1={x}
                y1={y + 34}
                x2={x + w}
                y2={y + 34}
                stroke="var(--border)"
                strokeOpacity="0.6"
                strokeWidth="1"
              />
              <circle cx={x + 16} cy={y + 17} r="3.2" fill="var(--accent)" fillOpacity="0.85" />
              <text
                x={x + 28}
                y={y + 21}
                fill="var(--muted)"
                fontSize="12.5"
                fontWeight="700"
                letterSpacing="0.09em"
                fontFamily="var(--font-display, 'Barlow Condensed', system-ui, sans-serif)"
              >
                {zone.label}
              </text>
              {zoneId === 'driver-jvm' && run.deployMode === 'client' && (
                <g className="sv-map-client-runtime-badge">
                  <title>
                    Client deploy mode: Spark driver components execute on submit host edge-01 while the AM runs in YARN
                  </title>
                  <rect
                    x={x + 276}
                    y={y + 7}
                    width="134"
                    height="20"
                    rx="4"
                    fill="var(--selection)"
                    fillOpacity="0.92"
                    stroke="var(--accent)"
                    strokeOpacity="0.55"
                    strokeWidth="1"
                  />
                  <text
                    x={x + 276 + 67}
                    y={y + 20.4}
                    textAnchor="middle"
                    fill="var(--accent)"
                    fontSize="9.8"
                    fontWeight="700"
                    letterSpacing="0.05em"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    RUNTIME: edge-01
                  </text>
                </g>
              )}
              <text
                x={x + w - 14}
                y={y + 21}
                textAnchor="end"
                fill="var(--faint)"
                fontSize="10.5"
                fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
              >
                {truncateText(zone.subtitle, Math.max(12, Math.floor((w - zone.label.length * 8.4 - 46) / 6.8)))}
              </text>
            </g>
          );
        })}

        {/* 4 Driver Sub-Zones */}
        {DRIVER_SUB_ZONES.map((sub) => {
          const { x, y, w, h } = sub.rect;
          return (
            <g key={sub.id} className="sv-map-sub-zone">
              <rect
                x={x}
                y={y}
                width={w}
                height={h}
                rx="7"
                fill="var(--page)"
                fillOpacity="0.34"
                stroke="var(--border)"
                strokeOpacity="0.72"
                strokeWidth="1"
                strokeDasharray="4 4"
              />
              <text
                x={x + 12}
                y={y + 18}
                fill="var(--faint)"
                fontSize="10"
                fontWeight="700"
                letterSpacing="0.09em"
                fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
              >
                {sub.label}
              </text>
            </g>
          );
        })}

        {/* Trace Routes (under nodes so labels stay legible; active trail renders on top) */}
        <g className="sv-map-routes">
          {routes.map((route) => {
            const isActive = activeRoute?.key === route.key;
            const hasCompleted =
              activeFrameIndex >= 0 && route.frameIndices.some((i) => i < activeFrameIndex);
            const latestCompletedIdx = route.frameIndices
              .filter((i) => i < activeFrameIndex)
              .slice(-1)[0];
            const completedColor =
              latestCompletedIdx !== undefined
                ? eventColor(projection.visibleFrames[latestCompletedIdx].event)
                : 'var(--line)';
            const strokeDash = route.isPeerTransfer
              ? '8 4'
              : route.isLocalOnly
                ? '2 4'
                : isActive && activeEvent
                  ? CHANNEL_META[activeEvent.channel].dash ?? 'none'
                  : '6 5';

            return (
              <g
                key={route.key}
                className={route.isPeerTransfer ? 'sv-map-peer-arc' : undefined}
                data-peer-transfer={route.isPeerTransfer ? 'true' : undefined}
              >
                <path
                  d={route.curve.path}
                  fill="none"
                  stroke={isActive ? activeColor : hasCompleted ? completedColor : 'var(--line)'}
                  strokeWidth={
                    route.isPeerTransfer
                      ? isActive
                        ? 3.1
                        : 2.3
                      : isActive
                        ? 2.8
                        : hasCompleted
                          ? 2.1
                          : 1.7
                  }
                  strokeOpacity={isActive ? 0.42 : hasCompleted ? 0.62 : 0.78}
                  strokeDasharray={strokeDash}
                  strokeLinecap="round"
                />
                {!route.isLocalOnly && (
                  <polygon
                    points="-8,-4.2 1,0 -8,4.2"
                    fill={isActive ? activeColor : hasCompleted ? completedColor : 'var(--line)'}
                    fillOpacity={isActive ? 0.95 : hasCompleted ? 0.8 : 0.85}
                    transform={`translate(${route.arrowEnd[0].toFixed(1)}, ${route.arrowEnd[1].toFixed(1)}) rotate(${route.arrowAngle.toFixed(1)})`}
                  />
                )}
              </g>
            );
          })}
        </g>

        {/* All Components */}
        <g className="sv-map-nodes">
          {COMPONENT_LIST.map((spec) => {
            const [cx, cy] = spec.clusterPos;
            const x = cx - cardW / 2;
            const y = cy - cardH / 2;
            const isParticipant = participantSet.has(spec.id);
            const isFocal = run.componentId === spec.id;
            const isSelected = selectedComponentId === spec.id;
            const isSender = activeEvent?.fromComponent === spec.id;
            const isReceiver = activeEvent?.toComponent === spec.id;
            const isActiveEndpoint = isSender || isReceiver;
            const accent = subsystemAccent(spec.subsystem);
            const subline = spec.port ? spec.port : SUBSYSTEM_META[spec.subsystem].short;
            const maxTitleChars = Math.max(15, Math.floor((cardW - 18) / 6.9));
            const maxSubChars = Math.max(15, Math.floor((cardW - 18) / 6.0));

            const endpointBadgeText =
              isSender && isReceiver
                ? activeIsPeerTransfer
                  ? truncateText(activeEvent?.peerTag || 'PEER', 11)
                  : activeIsLocalSelf
                    ? 'LOCAL'
                    : 'SELF'
                : isSender
                  ? 'TX'
                  : 'RX';
            const endpointBadgeW =
              endpointBadgeText === 'TX' || endpointBadgeText === 'RX'
                ? 24
                : Math.max(38, Math.ceil(endpointBadgeText.length * 6 + 10));

            const nodeClasses = [
              'cluster-node',
              isParticipant ? 'is-participant' : '',
              isSelected ? 'is-selected' : '',
              isFocal ? 'is-focal' : '',
              isActiveEndpoint ? 'is-active' : '',
            ]
              .filter(Boolean)
              .join(' ');

            return (
              <g
                key={spec.id}
                className={nodeClasses}
                role="button"
                tabIndex={0}
                aria-label={`${spec.name}${spec.port ? ` (${spec.port})` : ''}`}
                onClick={() => handleNodeClick(spec.id)}
                onKeyDown={handleNodeKey(spec.id)}
              >
                <title>
                  {`${spec.name} \u00B7 ${SUBSYSTEM_META[spec.subsystem].label}${spec.port ? ` (${spec.port})` : ''}`}
                </title>

                {/* Active endpoint pulse halo */}
                {isActiveEndpoint && (
                  <rect
                    className={reducedMotion ? undefined : 'sv-map-pulse'}
                    x={x - 5}
                    y={y - 5}
                    width={cardW + 10}
                    height={cardH + 10}
                    rx="9"
                    fill="none"
                    stroke={activeColor}
                    strokeWidth="2.8"
                    strokeDasharray={activeIsPeerTransfer && isSender && isReceiver ? '6 3' : undefined}
                  />
                )}

                {/* Selection tint frame */}
                {isSelected && (
                  <rect
                    x={x - 3}
                    y={y - 3}
                    width={cardW + 6}
                    height={cardH + 6}
                    rx="8"
                    fill="var(--selection)"
                    stroke="var(--accent)"
                    strokeWidth="1.6"
                  />
                )}

                {/* Component card */}
                <rect
                  className="sv-map-card"
                  x={x}
                  y={y}
                  width={cardW}
                  height={cardH}
                  rx="6"
                  fill="var(--raised)"
                  stroke={
                    isSelected || isFocal
                      ? 'var(--accent)'
                      : isActiveEndpoint
                        ? activeColor
                        : isParticipant
                          ? 'var(--text)'
                          : 'var(--border)'
                  }
                  strokeOpacity={
                    isSelected || isFocal || isActiveEndpoint
                      ? 1
                      : isParticipant
                        ? 0.42
                        : 0.9
                  }
                  strokeWidth={isSelected || isFocal || isActiveEndpoint ? 2.2 : isParticipant ? 1.4 : 1}
                />

                {/* Left subsystem color rail */}
                <rect
                  x={x + 1}
                  y={y + 5}
                  width="3.5"
                  height={cardH - 10}
                  rx="1.5"
                  fill={isActiveEndpoint ? activeColor : accent}
                />

                <text
                  x={x + 11}
                  y={cy - 3}
                  fill="var(--text)"
                  fontSize="11.6"
                  fontWeight="700"
                  fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
                >
                  {truncateText(spec.shortName, maxTitleChars)}
                </text>
                <text
                  x={x + 11}
                  y={cy + 12}
                  fill="var(--muted)"
                  fontSize="9.6"
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {truncateText(subline, maxSubChars)}
                </text>

                {/* Focal / active corner badge */}
                {isActiveEndpoint ? (
                  <g transform={`translate(${x + cardW - endpointBadgeW - 4}, ${y - 8})`}>
                    <rect x="0" y="0" width={endpointBadgeW} height="14" rx="3" fill={activeColor} />
                    <text
                      x={endpointBadgeW / 2}
                      y="10"
                      textAnchor="middle"
                      fill="var(--page)"
                      fontSize="8.5"
                      fontWeight="700"
                      fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                    >
                      {endpointBadgeText}
                    </text>
                  </g>
                ) : isFocal ? (
                  <g transform={`translate(${x + cardW - 42}, ${y - 8})`}>
                    <rect x="0" y="0" width="38" height="14" rx="3" fill="var(--accent)" />
                    <text
                      x="19"
                      y="10"
                      textAnchor="middle"
                      fill="var(--page)"
                      fontSize="8.2"
                      fontWeight="700"
                      fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                    >
                      FOCUS
                    </text>
                  </g>
                ) : null}
              </g>
            );
          })}
        </g>

        {/* Active Glowing Trail across the cluster map */}
        {activeEvent && activeRoute && packetState && (
          <g
            className="sv-map-active-trail"
            data-peer-transfer={packetState.peerTransfer ? 'true' : undefined}
            pointerEvents="none"
          >
            <polyline
              points={packetState.trailStr}
              fill="none"
              stroke={activeColor}
              strokeWidth="9"
              strokeOpacity="0.22"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <polyline
              points={packetState.trailStr}
              fill="none"
              stroke={activeColor}
              strokeWidth="3.4"
              strokeDasharray={
                packetState.peerTransfer
                  ? '8 4'
                  : packetState.localSelf
                    ? '3 3'
                    : CHANNEL_META[activeEvent.channel].dash
              }
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </g>
        )}

        {/* Clickable Hop Badges */}
        <g className="sv-map-hops">
          {routes.map((route) => {
            const count = route.frameIndices.length;
            return route.frameIndices.map((frameIdx, posIdx) => {
              const frame = projection.visibleFrames[frameIdx];
              if (!frame) return null;
              const ev = frame.event;
              const t =
                count === 1
                  ? 0.5
                  : clamp(0.24 + (0.52 * posIdx) / (count - 1), 0.22, 0.78);
              const [hx, hy] = route.curve.sample(t);
              const isCurrent = frameIdx === activeFrameIndex;
              const isPast = activeFrameIndex >= 0 && frameIdx < activeFrameIndex;
              const hopColor = eventColor(ev);
              const hopNum = frame.frameIndex + 1;

              return (
                <g
                  key={`map-hop-${ev.id}`}
                  className="sv-map-hop"
                  role="button"
                  tabIndex={0}
                  aria-label={`Jump to hop ${hopNum}: ${ev.label}`}
                  onClick={() => handleHopClick(ev.id)}
                  onKeyDown={handleHopKey(ev.id)}
                >
                  <title>
                    {`#${hopNum} \u00B7 ${ev.peerTag ? `${ev.peerTag} \u00B7 ` : ''}${ev.label} (${ev.protocol})`}
                  </title>
                  <circle
                    cx={hx}
                    cy={hy}
                    r={isCurrent ? 11.5 : 9.5}
                    fill={isCurrent ? hopColor : 'var(--panel)'}
                    stroke={isCurrent || isPast ? hopColor : 'var(--border)'}
                    strokeWidth={isCurrent ? 2 : 1.4}
                  />
                  <text
                    x={hx}
                    y={hy + 3.6}
                    textAnchor="middle"
                    fill={isCurrent ? 'var(--page)' : isPast ? hopColor : 'var(--muted)'}
                    fontSize={hopNum >= 10 ? '9.2' : '10'}
                    fontWeight="700"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {hopNum}
                  </text>
                </g>
              );
            });
          })}
        </g>

        {/* Active Packet Orb + Floating Label Pill */}
        {activeEvent && packetState && (
          <g
            className="sv-map-packet"
            data-peer-transfer={packetState.peerTransfer ? 'true' : undefined}
            role="button"
            tabIndex={0}
            aria-label={`Active packet #${activeFrameIndex + 1}: ${packetState.labelText}`}
            onClick={() => handleHopClick(activeEvent.id)}
            onKeyDown={handleHopKey(activeEvent.id)}
          >
            <title>{`#${activeFrameIndex + 1} ${activeEvent.protocol} \u00B7 ${packetState.labelText}`}</title>
            <circle
              className={reducedMotion ? undefined : 'sv-map-pulse'}
              cx={packetState.pos[0]}
              cy={packetState.pos[1]}
              r="18"
              fill={activeColor}
              fillOpacity="0.22"
            />
            {packetState.peerTransfer && (
              <circle
                cx={packetState.pos[0]}
                cy={packetState.pos[1]}
                r="14"
                fill="none"
                stroke={activeColor}
                strokeWidth="1.8"
                strokeDasharray="5 3"
              />
            )}
            {activeEvent.isError && (
              <circle
                cx={packetState.pos[0]}
                cy={packetState.pos[1]}
                r="22"
                fill="none"
                stroke="var(--ch-error)"
                strokeWidth="2"
                strokeDasharray="4 3"
              />
            )}
            <g transform={`translate(${packetState.pos[0].toFixed(1)}, ${packetState.pos[1].toFixed(1)})`}>
              <MapPacketGlyph
                channel={packetState.localSelf ? 'internal' : activeEvent.channel}
                isError={activeEvent.isError}
                color={activeColor}
              />
            </g>

            {/* Clamped floating label pill */}
            <g transform={`translate(${packetState.pillX.toFixed(1)}, ${packetState.pillY.toFixed(1)})`}>
              <rect
                x="0"
                y="0"
                width={packetState.pillW}
                height={packetState.pillH}
                rx="6"
                fill="var(--panel)"
                fillOpacity="0.96"
                stroke={activeColor}
                strokeWidth="1.5"
                strokeDasharray={packetState.peerTransfer ? '6 3' : packetState.localSelf ? '4 2' : undefined}
              />
              {packetState.localSelf ? (
                <>
                  <circle
                    cx="14"
                    cy={packetState.pillH / 2}
                    r="4.5"
                    fill="none"
                    stroke={activeColor}
                    strokeWidth="1.4"
                    strokeDasharray="2 1.5"
                  />
                  <circle cx="14" cy={packetState.pillH / 2} r="2" fill={activeColor} />
                  <text
                    x="25"
                    y={packetState.pillH / 2 + 4}
                    fill="var(--text)"
                    fontSize="11.8"
                    fontWeight="600"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {packetState.labelText}
                  </text>
                </>
              ) : (
                <>
                  <rect
                    x="5"
                    y="5"
                    width={packetState.tagW}
                    height={packetState.pillH - 10}
                    rx="3"
                    fill={activeColor}
                    fillOpacity="0.18"
                  />
                  <text
                    x={5 + packetState.tagW / 2}
                    y={packetState.pillH / 2 + 3.8}
                    textAnchor="middle"
                    fill={activeColor}
                    fontSize="10"
                    fontWeight="700"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {packetState.channelTag}
                  </text>
                  <text
                    x={packetState.tagW + 14}
                    y={packetState.pillH / 2 + 4}
                    fill="var(--text)"
                    fontSize="11.8"
                    fontWeight="600"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {packetState.labelText}
                  </text>
                </>
              )}
            </g>
          </g>
        )}

        {/* Calm Empty-State Filter Banner */}
        {visibleCount === 0 && (
          <g className="sv-map-empty" role="status" aria-live="polite" pointerEvents="none">
            <rect
              x={MASTER_CANVAS.width / 2 - 280}
              y={MASTER_CANVAS.height / 2 - 40}
              width="560"
              height="80"
              rx="10"
              fill="var(--panel)"
              fillOpacity="0.95"
              stroke="var(--border)"
              strokeWidth="1.6"
            />
            <rect
              x={MASTER_CANVAS.width / 2 - 280}
              y={MASTER_CANVAS.height / 2 - 40}
              width="5"
              height="80"
              rx="2.5"
              fill="var(--accent)"
            />
            <text
              x={MASTER_CANVAS.width / 2}
              y={MASTER_CANVAS.height / 2 - 4}
              textAnchor="middle"
              fill="var(--text)"
              fontSize="15"
              fontWeight="600"
              fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
            >
              No matching events for current category filters
            </text>
            <text
              x={MASTER_CANVAS.width / 2}
              y={MASTER_CANVAS.height / 2 + 20}
              textAnchor="middle"
              fill="var(--muted)"
              fontSize="12"
              fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
            >
              {hiddenCount > 0
                ? `${hiddenCount} ${hiddenCount === 1 ? 'event' : 'events'} hidden at current depth`
                : 'No events recorded in this run'}
            </text>
          </g>
        )}

        {/* Blueprint coordinate footer */}
        <g pointerEvents="none">
          <text
            x="24"
            y={MASTER_CANVAS.height - 6}
            fill="var(--faint)"
            fontSize="10.5"
            letterSpacing="0.08em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            {`ALL-${COMPONENT_LIST.length} CLUSTER MAP \u00B7 DRAG TO PAN \u00B7 CTRL+SCROLL OR BUTTONS TO ZOOM \u00B7 CLICK ANY COMPONENT TO INSPECT`}
          </text>
          <text
            x={MASTER_CANVAS.width - 24}
            y={MASTER_CANVAS.height - 6}
            textAnchor="end"
            fill="var(--faint)"
            fontSize="10.5"
            letterSpacing="0.06em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            {`${COMPONENT_LIST.length} COMPONENTS \u00B7 ${participantSet.size} ACTIVE IN TRACE \u00B7 ${
              hiddenCount === 0
                ? `${visibleCount} HOPS`
                : `${visibleCount}/${totalAtDepth} HOPS`
            }`}
          </text>
        </g>
      </svg>
    </div>
  );
}
