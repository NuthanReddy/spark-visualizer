import { useMemo } from 'react';
import type { KeyboardEvent } from 'react';
import { COMPONENTS } from '../data/components';
import type {
  ExecutionRun,
  ProjectedFrame,
  RunCanvasProps,
  RunEvent,
  RuntimeInstance,
} from '../data/runModel';
import { CHANNEL_META, HOST_ZONES, SUBSYSTEM_META } from '../data/types';
import type {
  ChannelKind,
  ComponentId,
  ComponentSpec,
  HostZoneId,
  SubsystemId,
} from '../data/types';
import { buildEdgeCurve } from '../playback';

export type { RunCanvasProps } from '../data/runModel';

export interface TopologyCanvasProps extends RunCanvasProps {
  onToggleExpandStep?: (stepId: string) => void;
}

const VIEW_W = 1120;
const VIEW_H = 620;
const MARGIN_X = 22;
const MARGIN_TOP = 16;
const FOOTER_RESERVE = 36;
const USABLE_W = VIEW_W - MARGIN_X * 2;
const USABLE_H = VIEW_H - MARGIN_TOP - FOOTER_RESERVE;
const ROW_GAP = 16;
const ZONE_GAP = 16;

const ARCHITECTURAL_ROWS: HostZoneId[][] = [
  ['submit-host', 'yarn-control', 'storage-history'],
  ['driver-jvm'],
  ['worker-node-a', 'worker-node-b'],
];

interface ZoneBox {
  id: HostZoneId;
  x: number;
  y: number;
  w: number;
  h: number;
  headerH: number;
  nodes: RuntimeInstance[];
}

interface PlacedNode {
  id: string;
  inst: RuntimeInstance;
  spec: ComponentSpec;
  cx: number;
  cy: number;
  w: number;
  h: number;
}

interface LayoutResult {
  zones: ZoneBox[];
  nodes: PlacedNode[];
  nodeById: Map<string, PlacedNode>;
  componentInstanceCounts: Map<ComponentId, number>;
  nodeW: number;
  nodeH: number;
}

interface RouteEntry {
  key: string;
  from: string;
  to: string;
  isSelf: boolean;
  isLocalOnly: boolean;
  hasReverse: boolean;
  frameIndices: number[];
  curve: ReturnType<typeof buildEdgeCurve>;
  arrowEnd: [number, number];
  arrowAngle: number;
}

interface HopBadgePlacement {
  frameIdx: number;
  frame: ProjectedFrame;
  curve: ReturnType<typeof buildEdgeCurve>;
  defaultT: number;
  baseX: number;
  baseY: number;
  x: number;
  y: number;
}

const HOP_BADGE_MIN_DIST = 22;
const HOP_NODE_CLEARANCE = 14;
const HOP_CANDIDATE_TS = [0.35, 0.65, 0.25, 0.75, 0.18, 0.82];
const HOP_PERP_OFFSETS = [0, 18, -18, 30, -30, 42, -42];

function hitsNodeCard(
  x: number,
  y: number,
  nodes: readonly PlacedNode[],
  clearance = HOP_NODE_CLEARANCE,
): boolean {
  for (const node of nodes) {
    const hw = node.w / 2 + clearance;
    const hh = node.h / 2 + clearance;
    if (
      x >= node.cx - hw &&
      x <= node.cx + hw &&
      y >= node.cy - hh &&
      y <= node.cy + hh
    ) {
      return true;
    }
  }
  return false;
}

function sampleCurveWithNormal(
  curve: ReturnType<typeof buildEdgeCurve>,
  t: number,
): { x: number; y: number; nx: number; ny: number } {
  const [x, y] = curve.sample(t);
  const dt = 0.02;
  const [x0, y0] = curve.sample(Math.max(0, t - dt));
  const [x1, y1] = curve.sample(Math.min(1, t + dt));
  const dx = x1 - x0;
  const dy = y1 - y0;
  const len = Math.hypot(dx, dy);
  if (len < 1e-4) {
    return { x, y, nx: 0, ny: -1 };
  }
  return { x, y, nx: -dy / len, ny: dx / len };
}

function resolveHopBadgeCollisions(
  badges: HopBadgePlacement[],
  nodes: readonly PlacedNode[],
  minX: number,
  maxX: number,
  minY: number,
  maxY: number,
  minDist = HOP_BADGE_MIN_DIST,
  nodeClearance = HOP_NODE_CLEARANCE,
): HopBadgePlacement[] {
  const sorted = [...badges].sort((a, b) => a.frameIdx - b.frameIdx);
  const minDistSq = minDist * minDist;

  const isClearOfPlaced = (x: number, y: number, placedCount: number) => {
    for (let k = 0; k < placedCount; k++) {
      const dx = sorted[k].x - x;
      const dy = sorted[k].y - y;
      if (dx * dx + dy * dy < minDistSq) return false;
    }
    return true;
  };

  const isValidSpot = (x: number, y: number, placedCount: number) =>
    !hitsNodeCard(x, y, nodes, nodeClearance) && isClearOfPlaced(x, y, placedCount);

  for (let i = 0; i < sorted.length; i++) {
    const cur = sorted[i];
    const startX = clamp(cur.baseX, minX, maxX);
    const startY = clamp(cur.baseY, minY, maxY);

    if (isValidSpot(startX, startY, i)) {
      cur.x = startX;
      cur.y = startY;
      continue;
    }

    // Search along the curve and perpendicular offsets for the nearest collision-free spot
    let bestX = startX;
    let bestY = startY;
    let bestDistSq = Number.POSITIVE_INFINITY;
    let found = false;

    const candidateTs = [cur.defaultT, ...HOP_CANDIDATE_TS];
    for (const t of candidateTs) {
      const { x: px, y: py, nx, ny } = sampleCurveWithNormal(cur.curve, t);
      for (const offset of HOP_PERP_OFFSETS) {
        const cx = clamp(px + nx * offset, minX, maxX);
        const cy = clamp(py + ny * offset, minY, maxY);
        if (!isValidSpot(cx, cy, i)) continue;
        const dx = cx - cur.baseX;
        const dy = cy - cur.baseY;
        const d2 = dx * dx + dy * dy;
        if (d2 < bestDistSq) {
          bestDistSq = d2;
          bestX = cx;
          bestY = cy;
          found = true;
        }
      }
    }

    // Fallback ring search guarantees node-card clearance and >= minDist even in crowded areas
    if (!found) {
      for (let r = minDist; r <= 360 && !found; r += 6) {
        let ringBestDistSq = Number.POSITIVE_INFINITY;
        for (let s = 0; s < 36; s++) {
          const angle = (s / 36) * Math.PI * 2;
          const cx = clamp(cur.baseX + Math.cos(angle) * r, minX, maxX);
          const cy = clamp(cur.baseY + Math.sin(angle) * r, minY, maxY);
          if (!isValidSpot(cx, cy, i)) continue;
          const dx = cx - cur.baseX;
          const dy = cy - cur.baseY;
          const d2 = dx * dx + dy * dy;
          if (d2 < ringBestDistSq) {
            ringBestDistSq = d2;
            bestX = cx;
            bestY = cy;
            found = true;
          }
        }
      }
    }

    cur.x = bestX;
    cur.y = bestY;
  }

  return sorted;
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

function isLocalSelfEvent(ev: RunEvent): boolean {
  return ev.kind === 'local' && ev.fromInstance === ev.toInstance;
}

function SubsystemGlyph({
  subsystem,
  x,
  y,
  color,
}: {
  subsystem: SubsystemId;
  x: number;
  y: number;
  color: string;
}) {
  return (
    <g transform={`translate(${x}, ${y})`} stroke={color} fill="none" strokeWidth="1.35" strokeLinecap="round" strokeLinejoin="round">
      {subsystem === 'submission' && (
        <>
          <polyline points="-5,-3 -1,0 -5,3" />
          <line x1="0" y1="3" x2="5" y2="3" />
        </>
      )}
      {subsystem === 'cluster-hdfs' && (
        <>
          <ellipse cx="0" cy="-3.5" rx="5" ry="2" />
          <path d="M -5 -3.5 V 3.5 C -5 4.8 5 4.8 5 3.5 V -3.5" />
          <path d="M -5 0 C -5 1.3 5 1.3 5 0" />
        </>
      )}
      {subsystem === 'driver-runtime' && (
        <>
          <rect x="-4.5" y="-4.5" width="9" height="9" rx="1.5" />
          <rect x="-1.8" y="-1.8" width="3.6" height="3.6" fill={color} stroke="none" />
        </>
      )}
      {subsystem === 'scheduling' && (
        <>
          <circle cx="-4" cy="-3.5" r="1.5" fill={color} stroke="none" />
          <circle cx="4" cy="0" r="1.5" fill={color} stroke="none" />
          <circle cx="-4" cy="3.5" r="1.5" fill={color} stroke="none" />
          <polyline points="-2.5,-3 2.5,0 -2.5,3" />
        </>
      )}
      {subsystem === 'executor-rpc' && (
        <polygon points="1,-5.5 -4.5,0.5 0,0.5 -1,5.5 4.5,-0.5 0,-0.5" fill={color} fillOpacity="0.2" />
      )}
      {subsystem === 'observability' && (
        <polyline points="-5.5,1 -3,1 -1.5,-4 1,4 2.8,-1 5.5,-1" />
      )}
      {subsystem === 'block-storage' && (
        <>
          <rect x="-5" y="-5" width="4" height="4" rx="0.8" />
          <rect x="1" y="-5" width="4" height="4" rx="0.8" />
          <rect x="-5" y="1" width="4" height="4" rx="0.8" />
          <rect x="1" y="1" width="4" height="4" rx="0.8" fill={color} fillOpacity="0.3" />
        </>
      )}
      {subsystem === 'shuffle' && (
        <>
          <path d="M -5 -3 H -1 L 2 3 H 5" />
          <path d="M -5 3 H -1 L 2 -3 H 5" />
        </>
      )}
      {subsystem === 'sql-execution' && (
        <>
          <polygon points="-5,-4.5 5,-4.5 1.5,0 1.5,4.5 -1.5,4.5 -1.5,0" fill={color} fillOpacity="0.16" />
        </>
      )}
      {subsystem === 'python-output' && (
        <>
          <polyline points="-2.5,-4 -5.5,0 -2.5,4" />
          <polyline points="2.5,-4 5.5,0 2.5,4" />
          <circle cx="0" cy="0" r="1.3" fill={color} stroke="none" />
        </>
      )}
    </g>
  );
}

function PacketGlyph({
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
          points="0,-8 7.5,5.5 -7.5,5.5"
          fill="var(--panel)"
          stroke="var(--ch-error)"
          strokeWidth="2"
          strokeLinejoin="round"
        />
        <line x1="0" y1="-3" x2="0" y2="1" stroke="var(--ch-error)" strokeWidth="1.8" strokeLinecap="round" />
        <circle cx="0" cy="3.4" r="1" fill="var(--ch-error)" />
      </g>
    );
  }
  switch (channel) {
    case 'yarn':
      return (
        <polygon
          points="0,-7.5 6.8,-3.7 6.8,3.7 0,7.5 -6.8,3.7 -6.8,-3.7"
          fill={color}
          stroke="var(--panel)"
          strokeWidth="1.6"
        />
      );
    case 'rpc':
      return (
        <g>
          <polygon
            points="0,-8 8,0 0,8 -8,0"
            fill={color}
            stroke="var(--panel)"
            strokeWidth="1.6"
          />
          <circle cx="0" cy="0" r="2.1" fill="var(--panel)" />
        </g>
      );
    case 'data':
      return (
        <g>
          <rect
            x="-6.5"
            y="-6.5"
            width="13"
            height="13"
            rx="2.5"
            fill={color}
            stroke="var(--panel)"
            strokeWidth="1.6"
          />
          <line x1="-3.5" y1="-1.5" x2="3.5" y2="-1.5" stroke="var(--panel)" strokeWidth="1.4" />
          <line x1="-3.5" y1="2" x2="3.5" y2="2" stroke="var(--panel)" strokeWidth="1.4" />
        </g>
      );
    case 'python':
      return (
        <g>
          <circle cx="0" cy="0" r="7" fill="var(--panel)" stroke={color} strokeWidth="2.2" />
          <circle cx="-2.1" cy="-1.4" r="1.4" fill={color} />
          <circle cx="2.1" cy="1.4" r="1.4" fill={color} />
        </g>
      );
    case 'internal':
    default:
      return (
        <g>
          <circle cx="0" cy="0" r="6.2" fill="var(--panel)" stroke={color} strokeWidth="2" strokeDasharray="3 2" />
          <circle cx="0" cy="0" r="2.4" fill={color} />
        </g>
      );
  }
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
  const frac = denom > 0 ? Math.min(0.38, 1 / denom) : 0;

  const p1: [number, number] = [shiftFrom[0] + dx * frac, shiftFrom[1] + dy * frac];
  const p2: [number, number] = [shiftTo[0] - dx * frac, shiftTo[1] - dy * frac];

  if (Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) < 16) {
    return { from: fromCenter, to: toCenter };
  }
  return { from: p1, to: p2 };
}

function computeTopologyLayout(
  run: ExecutionRun,
  visibleFrames: ProjectedFrame[],
): LayoutResult {
  const seen = new Set<string>();
  const orderedInstances: RuntimeInstance[] = [];
  const componentInstanceCounts = new Map<ComponentId, number>();

  const addInstance = (instId: string) => {
    if (seen.has(instId)) return;
    const inst = run.instances[instId];
    if (!inst || !COMPONENTS[inst.componentId]) return;
    seen.add(instId);
    orderedInstances.push(inst);
    componentInstanceCounts.set(
      inst.componentId,
      (componentInstanceCounts.get(inst.componentId) ?? 0) + 1,
    );
  };

  for (const id of run.focusInstances) addInstance(id);
  for (const frame of visibleFrames) {
    addInstance(frame.event.fromInstance);
    addInstance(frame.event.toInstance);
  }

  const selfLoopInstances = new Set<string>();
  for (const frame of visibleFrames) {
    if (frame.event.fromInstance === frame.event.toInstance) {
      selfLoopInstances.add(frame.event.fromInstance);
    }
  }

  const byZone = new Map<HostZoneId, RuntimeInstance[]>();
  for (const inst of orderedInstances) {
    const hz = inst.hostZone;
    const list = byZone.get(hz) ?? [];
    list.push(inst);
    byZone.set(hz, list);
  }

  // Sort instances inside each host zone in natural left-to-right architectural order
  for (const [zoneId, list] of byZone.entries()) {
    list.sort((a, b) => {
      const specA = COMPONENTS[a.componentId];
      const specB = COMPONENTS[b.componentId];
      const nativeDiff =
        (specA.hostZone === zoneId ? 0 : 1) - (specB.hostZone === zoneId ? 0 : 1);
      if (nativeDiff !== 0) return nativeDiff;
      const pa = specA.clusterPos;
      const pb = specB.clusterPos;
      if (pa[0] !== pb[0]) return pa[0] - pb[0];
      if (pa[1] !== pb[1]) return pa[1] - pb[1];
      const execDiff = (a.executorNumber ?? 0) - (b.executorNumber ?? 0);
      if (execDiff !== 0) return execDiff;
      return a.id.localeCompare(b.id);
    });
  }

  const activeRows = ARCHITECTURAL_ROWS.map((row) =>
    row.filter((zoneId) => (byZone.get(zoneId)?.length ?? 0) > 0),
  ).filter((row) => row.length > 0);

  if (activeRows.length === 0) {
    return {
      zones: [],
      nodes: [],
      nodeById: new Map(),
      componentInstanceCounts,
      nodeW: 164,
      nodeH: 54,
    };
  }

  // Pick card dimensions, compacting gracefully only if an unusually dense run needs extra room
  const presets = [
    { nodeW: 164, nodeH: 54, headerH: 32, padX: 18, padY: 16, gapX: 18, gapY: 18 },
    { nodeW: 156, nodeH: 48, headerH: 29, padX: 14, padY: 12, gapX: 14, gapY: 12 },
    { nodeW: 146, nodeH: 44, headerH: 26, padX: 12, padY: 10, gapX: 10, gapY: 10 },
    { nodeW: 136, nodeH: 40, headerH: 24, padX: 10, padY: 8, gapX: 8, gapY: 8 },
  ];

  let chosenPreset = presets[presets.length - 1];
  let rowZoneSpecs: Array<
    Array<{
      id: HostZoneId;
      w: number;
      nodes: RuntimeInstance[];
      cols: number;
      rows: number;
      minH: number;
    }>
  > = [];

  for (const preset of presets) {
    const candidateRows = activeRows.map((rowZones) => {
      const availableW = USABLE_W - ZONE_GAP * (rowZones.length - 1);
      const minZoneW = Math.min(preset.nodeW + preset.padX * 2 + 24, availableW / rowZones.length);
      const weights = rowZones.map((zid) => {
        const count = byZone.get(zid)?.length ?? 1;
        return Math.max(1, Math.min(8, count));
      });
      const weightSum = weights.reduce((acc, w) => acc + w, 0);
      const extraW = Math.max(0, availableW - minZoneW * rowZones.length);

      return rowZones.map((zid, idx) => {
        const nodes = byZone.get(zid) ?? [];
        const w = minZoneW + (extraW * weights[idx]) / weightSum;
        const innerW = Math.max(preset.nodeW, w - preset.padX * 2);
        const maxCols = Math.max(1, Math.floor((innerW + preset.gapX) / (preset.nodeW + preset.gapX)));
        const gridRows = Math.max(1, Math.ceil(nodes.length / maxCols));
        const cols = Math.max(1, Math.ceil(nodes.length / gridRows));
        const hasTopSelfLoop = nodes.slice(0, cols).some((inst) => selfLoopInstances.has(inst.id));
        const loopClearance = hasTopSelfLoop ? 26 : 0;
        const minH =
          preset.headerH +
          preset.padY * 2 +
          loopClearance +
          gridRows * preset.nodeH +
          (gridRows - 1) * preset.gapY;
        return { id: zid, w, nodes, cols, rows: gridRows, minH };
      });
    });

    const totalMinH =
      candidateRows.reduce((sum, r) => sum + Math.max(...r.map((z) => z.minH)), 0) +
      ROW_GAP * (candidateRows.length - 1);

    chosenPreset = preset;
    rowZoneSpecs = candidateRows;
    if (totalMinH <= USABLE_H) break;
  }

  const rowMinHeights = rowZoneSpecs.map((r) => Math.max(...r.map((z) => z.minH)));
  const totalMinH =
    rowMinHeights.reduce((sum, h) => sum + h, 0) + ROW_GAP * (rowZoneSpecs.length - 1);

  const rowHeights = rowMinHeights.map((minH) => {
    if (totalMinH <= USABLE_H) {
      const extra = USABLE_H - totalMinH;
      return minH + (extra * minH) / Math.max(1, totalMinH - ROW_GAP * (rowZoneSpecs.length - 1));
    }
    const scale =
      (USABLE_H - ROW_GAP * (rowZoneSpecs.length - 1)) /
      Math.max(1, totalMinH - ROW_GAP * (rowZoneSpecs.length - 1));
    return minH * scale;
  });

  // Normalize rounding so rows fill USABLE_H exactly
  const heightSum = rowHeights.reduce((s, h) => s + h, 0) + ROW_GAP * (rowHeights.length - 1);
  if (rowHeights.length > 0 && Math.abs(heightSum - USABLE_H) > 0.5) {
    rowHeights[rowHeights.length - 1] += USABLE_H - heightSum;
  }

  const zones: ZoneBox[] = [];
  const placedNodes: PlacedNode[] = [];
  const nodeById = new Map<string, PlacedNode>();

  let currY = MARGIN_TOP;
  rowZoneSpecs.forEach((rowZones, rIdx) => {
    const rh = rowHeights[rIdx];
    let currX = MARGIN_X;

    rowZones.forEach((zs) => {
      zones.push({
        id: zs.id,
        x: currX,
        y: currY,
        w: zs.w,
        h: rh,
        headerH: chosenPreset.headerH,
        nodes: zs.nodes,
      });

      const bodyTop = currY + chosenPreset.headerH + chosenPreset.padY;
      const bodyH = Math.max(chosenPreset.nodeH, rh - chosenPreset.headerH - chosenPreset.padY * 2);
      const bodyLeft = currX + chosenPreset.padX;
      const bodyW = Math.max(chosenPreset.nodeW, zs.w - chosenPreset.padX * 2);

      const hasTopSelfLoop = zs.nodes.slice(0, zs.cols).some((inst) => selfLoopInstances.has(inst.id));
      const topExtra = hasTopSelfLoop
        ? Math.min(24, Math.max(0, bodyH - zs.rows * chosenPreset.nodeH) * 0.45)
        : 0;

      const availYForGaps = Math.max(0, bodyH - topExtra - zs.rows * chosenPreset.nodeH);
      const maxFitGapY = zs.rows > 1 ? availYForGaps / (zs.rows - 1) : 0;
      const gapY =
        zs.rows > 1
          ? clamp(
              availYForGaps / (zs.rows + 0.4),
              Math.min(chosenPreset.gapY * 0.7, maxFitGapY),
              Math.min(54, maxFitGapY),
            )
          : 0;
      const totalGridH = zs.rows * chosenPreset.nodeH + (zs.rows - 1) * gapY;
      const gridStartY = bodyTop + topExtra + Math.max(0, (bodyH - topExtra - totalGridH) / 2);

      for (let row = 0; row < zs.rows; row++) {
        const startIdx = row * zs.cols;
        const rowInstances = zs.nodes.slice(startIdx, startIdx + zs.cols);
        const countInRow = rowInstances.length;
        const availXForGaps = Math.max(0, bodyW - countInRow * chosenPreset.nodeW);
        const maxFitGapX = countInRow > 1 ? availXForGaps / (countInRow - 1) : 0;
        const gapX =
          countInRow > 1
            ? clamp(
                availXForGaps / (countInRow + 0.5),
                Math.min(chosenPreset.gapX * 0.75, maxFitGapX),
                Math.min(68, maxFitGapX),
              )
            : 0;
        const totalRowW = countInRow * chosenPreset.nodeW + (countInRow - 1) * gapX;
        const rowStartX = bodyLeft + Math.max(0, (bodyW - totalRowW) / 2);
        const cy = gridStartY + row * (chosenPreset.nodeH + gapY) + chosenPreset.nodeH / 2;

        rowInstances.forEach((inst, colIdx) => {
          const cx = rowStartX + colIdx * (chosenPreset.nodeW + gapX) + chosenPreset.nodeW / 2;
          const placed: PlacedNode = {
            id: inst.id,
            inst,
            spec: COMPONENTS[inst.componentId],
            cx,
            cy,
            w: chosenPreset.nodeW,
            h: chosenPreset.nodeH,
          };
          placedNodes.push(placed);
          nodeById.set(inst.id, placed);
        });
      }

      currX += zs.w + ZONE_GAP;
    });

    currY += rh + ROW_GAP;
  });

  return {
    zones,
    nodes: placedNodes,
    nodeById,
    componentInstanceCounts,
    nodeW: chosenPreset.nodeW,
    nodeH: chosenPreset.nodeH,
  };
}

export default function TopologyCanvas({
  run,
  projection,
  activeEventId,
  progress,
  reducedMotion,
  selectedComponentId,
  onSelectComponent,
  onSelectEvent,
  onToggleExpandStep,
}: TopologyCanvasProps) {
  const layout = useMemo(
    () => computeTopologyLayout(run, projection.visibleFrames),
    [run, projection.visibleFrames],
  );

  const activeFrameIndex = activeEventId
    ? projection.frameIndexByEventId.get(activeEventId) ?? -1
    : -1;
  const activeFrame =
    activeFrameIndex >= 0 && activeFrameIndex < projection.visibleFrames.length
      ? projection.visibleFrames[activeFrameIndex]
      : null;
  const activeEvent = activeFrame?.event ?? null;
  const activeIsLocalSelf = activeEvent ? isLocalSelfEvent(activeEvent) : false;

  const expandableInfoByInstance = useMemo(() => {
    const map = new Map<
      string,
      { stepId: string; stepTitle: string; childCount: number; isCurrentStep: boolean }
    >();
    for (const frame of projection.visibleFrames) {
      if (!frame.isSummaryFrame) continue;
      const projStep = projection.visibleSteps[frame.visibleStepIndex];
      if (!projStep || !projStep.hasChildren || projStep.isExpanded) continue;
      const isCurrentStep = activeFrame?.step.id === projStep.step.id;
      const preferredInstId =
        projStep.step.deepDiveComponentId &&
        run.instances[frame.event.toInstance]?.componentId ===
          projStep.step.deepDiveComponentId
          ? frame.event.toInstance
          : projStep.step.deepDiveComponentId &&
              run.instances[frame.event.fromInstance]?.componentId ===
                projStep.step.deepDiveComponentId
            ? frame.event.fromInstance
            : frame.event.toInstance;

      const existing = map.get(preferredInstId);
      if (!existing || (isCurrentStep && !existing.isCurrentStep)) {
        map.set(preferredInstId, {
          stepId: projStep.step.id,
          stepTitle: projStep.step.title,
          childCount: projStep.childCount,
          isCurrentStep,
        });
      }
    }
    return map;
  }, [projection.visibleFrames, projection.visibleSteps, activeFrame, run.instances]);

  const { routes, routeByKey } = useMemo(() => {
    const grouped = new Map<string, { from: string; to: string; indices: number[] }>();
    projection.visibleFrames.forEach((frame, index) => {
      const ev = frame.event;
      if (!layout.nodeById.has(ev.fromInstance) || !layout.nodeById.has(ev.toInstance)) return;
      const key = `${ev.fromInstance}::${ev.toInstance}`;
      const existing = grouped.get(key);
      if (existing) {
        existing.indices.push(index);
      } else {
        grouped.set(key, { from: ev.fromInstance, to: ev.toInstance, indices: [index] });
      }
    });

    const list: RouteEntry[] = [];
    const byKey = new Map<string, RouteEntry>();
    const hw = layout.nodeW / 2;
    const hh = layout.nodeH / 2;

    for (const [key, item] of grouped.entries()) {
      const fromNode = layout.nodeById.get(item.from)!;
      const toNode = layout.nodeById.get(item.to)!;
      const isSelf = item.from === item.to;
      const isLocalOnly =
        isSelf &&
        item.indices.every((idx) => {
          const ev = projection.visibleFrames[idx]?.event;
          return ev ? isLocalSelfEvent(ev) : false;
        });
      const hasReverse = !isSelf && grouped.has(`${item.to}::${item.from}`);

      // Detect whether a straight horizontal hop skips across another node on the same row
      let skipsInlineNode = false;
      if (!isSelf && Math.abs(fromNode.cy - toNode.cy) < 22) {
        const minX = Math.min(fromNode.cx, toNode.cx);
        const maxX = Math.max(fromNode.cx, toNode.cx);
        skipsInlineNode = layout.nodes.some(
          (n) =>
            n.id !== item.from &&
            n.id !== item.to &&
            Math.abs(n.cy - fromNode.cy) < 26 &&
            n.cx > minX + 20 &&
            n.cx < maxX - 20,
        );
      }

      const anchors = isSelf
        ? {
            from: [fromNode.cx, fromNode.cy] as [number, number],
            to: [toNode.cx, toNode.cy] as [number, number],
          }
        : computeEdgeAnchors(
            [fromNode.cx, fromNode.cy],
            [toNode.cx, toNode.cy],
            hw,
            hh,
            5,
            hasReverse ? 8 : 0,
          );

      const curvature = hasReverse ? 0.22 : skipsInlineNode ? 0.26 : 0.14;
      const curve = buildEdgeCurve(anchors.from, anchors.to, curvature);
      const pEnd = curve.sample(1);
      const pPrev = curve.sample(0.95);
      const arrowAngle = (Math.atan2(pEnd[1] - pPrev[1], pEnd[0] - pPrev[0]) * 180) / Math.PI;

      const entry: RouteEntry = {
        key,
        from: item.from,
        to: item.to,
        isSelf,
        isLocalOnly,
        hasReverse,
        frameIndices: item.indices,
        curve,
        arrowEnd: pEnd,
        arrowAngle,
      };
      list.push(entry);
      byKey.set(key, entry);
    }

    return { routes: list, routeByKey: byKey };
  }, [projection.visibleFrames, layout]);

  const activeRoute = activeEvent
    ? routeByKey.get(`${activeEvent.fromInstance}::${activeEvent.toInstance}`) ?? null
    : null;

  const hopBadges = useMemo(() => {
    const raw: HopBadgePlacement[] = [];
    const minX = 14;
    const maxX = VIEW_W - 14;
    const minY = 14;
    const maxY = VIEW_H - FOOTER_RESERVE - 12;

    for (const route of routes) {
      const count = route.frameIndices.length;
      route.frameIndices.forEach((frameIdx, posIdx) => {
        const frame = projection.visibleFrames[frameIdx];
        if (!frame) return;
        const t =
          count === 1
            ? 0.5
            : clamp(0.25 + (0.5 * posIdx) / (count - 1), 0.25, 0.75);
        const [hx, hy] = route.curve.sample(t);
        raw.push({
          frameIdx,
          frame,
          curve: route.curve,
          defaultT: t,
          baseX: hx,
          baseY: hy,
          x: clamp(hx, minX, maxX),
          y: clamp(hy, minY, maxY),
        });
      });
    }

    return resolveHopBadgeCollisions(raw, layout.nodes, minX, maxX, minY, maxY);
  }, [routes, projection.visibleFrames, layout.nodes]);
  const effectiveProgress = reducedMotion ? 1 : clamp(progress, 0, 1);
  const activeColor = activeEvent ? eventColor(activeEvent) : 'var(--accent)';

  // Compute traveling packet position & trail (or local self-arc position)
  const packetState = useMemo(() => {
    if (!activeEvent || !activeRoute) return null;
    const pos = activeRoute.curve.sample(effectiveProgress);
    const trailPts = activeRoute.curve.trail(effectiveProgress, 24);
    const trailStr = trailPts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
    const localSelf = isLocalSelfEvent(activeEvent);
    const channelTag = localSelf
      ? 'LOCAL'
      : CHANNEL_META[activeEvent.channel].short.toUpperCase();
    const labelText = truncateText(activeEvent.label, 34);
    const pillH = 26;
    const tagW = channelTag.length * 6.2 + 12;
    const localFullText = `LOCAL \u00B7 ${labelText}`;
    const textW = localSelf ? localFullText.length * 6.45 + 30 : labelText.length * 6.45 + 18;
    const pillW = localSelf
      ? clamp(textW, 140, 380)
      : clamp(tagW + textW + 10, 128, 360);
    const preferBelow = pos[1] < 66;
    const rawY = preferBelow ? pos[1] + 22 : pos[1] - pillH - 18;
    const pillX = clamp(pos[0] - pillW / 2, 14, VIEW_W - pillW - 14);
    const pillY = clamp(rawY, 12, VIEW_H - FOOTER_RESERVE - pillH - 6);

    return {
      pos,
      trailStr,
      localSelf,
      channelTag,
      labelText,
      tagW,
      pillW,
      pillH,
      pillX,
      pillY,
    };
  }, [activeEvent, activeRoute, effectiveProgress]);

  const handleNodeKey = (id: ComponentId) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectComponent(id);
    }
  };

  const handleHopKey = (eventId: string) => (e: KeyboardEvent<SVGGElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      e.stopPropagation();
      onSelectEvent(eventId);
    }
  };

  const visibleCount = projection.visibleFrames.length;
  const hiddenCount = projection.skippedSpans.reduce(
    (sum, span) => sum + span.eventCount,
    0,
  );
  const totalAtDepth = visibleCount + hiddenCount;

  return (
    <div
      className="topology-canvas-wrap"
      style={{
        width: '100%',
        height: '100%',
        position: 'relative',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'var(--page)',
      }}
    >
      <svg
        className="topology-canvas"
        viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
        preserveAspectRatio="xMidYMid meet"
        role="group"
        aria-label={`Focused topology diagram for ${run.name}`}
        data-event-id={activeEventId ?? ''}
        data-progress={progress}
        style={{
          width: '100%',
          height: '100%',
          display: 'block',
          userSelect: 'none',
        }}
      >
        <defs>
          <pattern id="sv-topo-grid" width="24" height="24" patternUnits="userSpaceOnUse">
            <circle cx="12" cy="12" r="1" fill="var(--grid)" />
          </pattern>
          <style>{`
            .sv-topo-node {
              cursor: pointer;
              outline: none;
              transition: opacity 160ms ease;
            }
            .sv-topo-node:hover .sv-topo-card,
            .sv-topo-node:focus-visible .sv-topo-card {
              stroke: var(--accent);
              stroke-width: 1.9px;
            }
            .sv-topo-hop {
              cursor: pointer;
              outline: none;
            }
            .sv-topo-hop:hover circle,
            .sv-topo-hop:focus-visible circle {
              stroke: var(--accent);
              stroke-width: 2px;
            }
            .sv-topo-packet {
              cursor: pointer;
            }
            .sv-topo-expand-badge {
              cursor: pointer;
              outline: none;
            }
            .sv-topo-expand-badge:hover rect,
            .sv-topo-expand-badge:focus-visible rect {
              stroke: var(--accent);
              fill: var(--selection);
            }
            .sv-topo-expand-badge:hover text,
            .sv-topo-expand-badge:focus-visible text {
              fill: var(--accent);
            }
            @keyframes sv-topo-halo {
              0%, 100% { opacity: 0.24; transform: scale(1); }
              50% { opacity: 0.62; transform: scale(1.04); }
            }
            @keyframes sv-topo-orb {
              0%, 100% { opacity: 0.16; }
              50% { opacity: 0.42; }
            }
            .sv-topo-pulse {
              transform-box: fill-box;
              transform-origin: center;
              animation: sv-topo-halo 1.5s ease-in-out infinite;
            }
            .sv-topo-orb-pulse {
              animation: sv-topo-orb 1.1s ease-in-out infinite;
            }
            @media (prefers-reduced-motion: reduce) {
              .sv-topo-pulse, .sv-topo-orb-pulse { animation: none; }
            }
          `}</style>
        </defs>

        {/* Blueprint background & subtle frame */}
        <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="var(--page)" />
        <rect x="0" y="0" width={VIEW_W} height={VIEW_H} fill="url(#sv-topo-grid)" />

        {/* Subtle architectural corner registration ticks */}
        <g stroke="var(--border)" strokeWidth="1" opacity="0.7">
          <path d="M 8 20 V 8 H 20" fill="none" />
          <path d={`M ${VIEW_W - 20} 8 H ${VIEW_W - 8} V 20`} fill="none" />
          <path d={`M 8 ${VIEW_H - 20} V ${VIEW_H - 8} H 20`} fill="none" />
          <path d={`M ${VIEW_W - 20} ${VIEW_H - 8} H ${VIEW_W - 8} V ${VIEW_H - 20}`} fill="none" />
        </g>

        {/* Host Zone Enclosures */}
        {layout.zones.map((zone) => {
          const meta = HOST_ZONES[zone.id];
          const maxLabelChars = Math.max(14, Math.floor((zone.w - 28) / 7.2));
          const maxSubChars = Math.max(0, Math.floor((zone.w - meta.label.length * 7.4 - 44) / 6.3));
          return (
            <g key={zone.id} className="sv-topo-zone" data-host-zone={zone.id}>
              <rect
                x={zone.x}
                y={zone.y}
                width={zone.w}
                height={zone.h}
                rx="8"
                fill="var(--panel)"
                fillOpacity="0.82"
                stroke="var(--border)"
                strokeWidth="1"
              />
              {/* Zone header hairline */}
              <line
                x1={zone.x}
                y1={zone.y + zone.headerH}
                x2={zone.x + zone.w}
                y2={zone.y + zone.headerH}
                stroke="var(--border)"
                strokeOpacity="0.65"
                strokeWidth="1"
              />
              {/* Corner status dot */}
              <circle
                cx={zone.x + 14}
                cy={zone.y + zone.headerH / 2}
                r="2.8"
                fill="var(--accent)"
                fillOpacity="0.8"
              />
              <text
                x={zone.x + 24}
                y={zone.y + zone.headerH / 2 + 3.5}
                fill="var(--muted)"
                fontSize="10.5"
                fontWeight="700"
                letterSpacing="0.09em"
                fontFamily="var(--font-display, 'Barlow Condensed', system-ui, sans-serif)"
              >
                {truncateText(meta.label, maxLabelChars)}
              </text>
              {maxSubChars >= 10 && (
                <text
                  x={zone.x + zone.w - 12}
                  y={zone.y + zone.headerH / 2 + 3.5}
                  textAnchor="end"
                  fill="var(--faint)"
                  fontSize="9.5"
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {truncateText(meta.subtitle, maxSubChars)}
                </text>
              )}
            </g>
          );
        })}

        {/* Static Dashed Route Paths */}
        <g className="sv-topo-routes">
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
            const strokeDash =
              isActive && activeIsLocalSelf
                ? '3 3'
                : route.isLocalOnly
                  ? '2 4'
                  : isActive && activeEvent
                    ? CHANNEL_META[activeEvent.channel].dash ?? 'none'
                    : '5 5';

            return (
              <g
                key={route.key}
                data-local-route={route.isLocalOnly ? 'true' : undefined}
              >
                <path
                  d={route.curve.path}
                  fill="none"
                  stroke={isActive ? activeColor : hasCompleted ? completedColor : 'var(--line)'}
                  strokeWidth={isActive ? 2.2 : hasCompleted ? 1.8 : 1.5}
                  strokeOpacity={isActive ? 0.36 : hasCompleted ? 0.55 : 0.78}
                  strokeDasharray={strokeDash}
                  strokeLinecap="round"
                />
                {!route.isLocalOnly && (
                  <polygon
                    points="-7,-3.6 1,0 -7,3.6"
                    fill={isActive ? activeColor : hasCompleted ? completedColor : 'var(--line)'}
                    fillOpacity={isActive ? 0.92 : hasCompleted ? 0.72 : 0.85}
                    transform={`translate(${route.arrowEnd[0].toFixed(1)}, ${route.arrowEnd[1].toFixed(1)}) rotate(${route.arrowAngle.toFixed(1)})`}
                  />
                )}
              </g>
            );
          })}
        </g>

        {/* Runtime Instance Node Cards */}
        <g className="sv-topo-nodes">
          {layout.nodes.map((node) => {
            const x = node.cx - node.w / 2;
            const y = node.cy - node.h / 2;
            const isFocal = run.componentId === node.inst.componentId;
            const isSelected = selectedComponentId === node.inst.componentId;
            const isSender = activeEvent?.fromInstance === node.id;
            const isReceiver = activeEvent?.toInstance === node.id;
            const isActiveEndpoint = isSender || isReceiver;
            const isActiveLocalNode = activeIsLocalSelf && isSender && isReceiver;
            const accent = subsystemAccent(node.spec.subsystem);
            const hasPeerInstances =
              (layout.componentInstanceCounts.get(node.inst.componentId) ?? 0) > 1;
            const subtitle =
              node.inst.executorNumber !== undefined ||
              hasPeerInstances ||
              node.inst.hostZone !== node.spec.hostZone
                ? node.inst.hostLabel ||
                  node.spec.port ||
                  SUBSYSTEM_META[node.spec.subsystem].short
                : node.spec.port ||
                  node.inst.hostLabel ||
                  SUBSYSTEM_META[node.spec.subsystem].short;
            const maxTitleChars = Math.max(13, Math.floor((node.w - 36) / 6.35));
            const maxSubChars = Math.max(13, Math.floor((node.w - 36) / 6.0));
            const endpointBadgeText = isActiveLocalNode
              ? 'LOCAL'
              : isSender && isReceiver
                ? 'SELF'
                : isSender
                  ? 'TX'
                  : 'RX';
            const endpointBadgeW =
              endpointBadgeText === 'LOCAL'
                ? 42
                : endpointBadgeText === 'SELF'
                  ? 38
                  : 24;

            return (
              <g
                key={node.id}
                className={`sv-topo-node${isSelected ? ' is-selected' : ''}${isFocal ? ' is-focal' : ''}${isActiveEndpoint ? ' is-active' : ''}`}
                data-instance-id={node.id}
                data-component-id={node.inst.componentId}
                role="button"
                tabIndex={0}
                aria-label={`${node.inst.label} (${node.inst.hostLabel})`}
                onClick={() => onSelectComponent(node.inst.componentId)}
                onKeyDown={handleNodeKey(node.inst.componentId)}
              >
                <title>
                  {`${node.inst.label} (${node.inst.hostLabel}) - ${node.inst.processId}${node.inst.containerId ? ` [${node.inst.containerId}]` : ''}${node.spec.port ? ` (${node.spec.port})` : ''}`}
                </title>

                {/* Crisp local pulse ring for in-process local self events */}
                {isActiveLocalNode && (
                  <g className="sv-topo-local-ring" pointerEvents="none">
                    <rect
                      className={reducedMotion ? undefined : 'sv-topo-pulse'}
                      x={x - 9}
                      y={y - 9}
                      width={node.w + 18}
                      height={node.h + 18}
                      rx="12"
                      fill="none"
                      stroke={activeColor}
                      strokeWidth="1.8"
                      strokeDasharray="4 3"
                      strokeOpacity="0.82"
                    />
                  </g>
                )}

                {/* Active TX/RX pulsing halo */}
                {isActiveEndpoint && (
                  <rect
                    className={reducedMotion ? undefined : 'sv-topo-pulse'}
                    x={x - 5}
                    y={y - 5}
                    width={node.w + 10}
                    height={node.h + 10}
                    rx="10"
                    fill="none"
                    stroke={activeColor}
                    strokeWidth={isActiveLocalNode ? 1.8 : 2.2}
                    strokeDasharray={isActiveLocalNode ? '3 3' : undefined}
                    strokeOpacity="0.65"
                  />
                )}

                {/* Selected tint halo */}
                {isSelected && (
                  <rect
                    x={x - 3}
                    y={y - 3}
                    width={node.w + 6}
                    height={node.h + 6}
                    rx="8.5"
                    fill="var(--selection)"
                    stroke="var(--accent)"
                    strokeWidth="1.4"
                    strokeOpacity="0.6"
                  />
                )}

                {/* Main architectural node card */}
                <rect
                  className="sv-topo-card"
                  x={x}
                  y={y}
                  width={node.w}
                  height={node.h}
                  rx="6"
                  fill="var(--raised)"
                  stroke={
                    isSelected
                      ? 'var(--accent)'
                      : isActiveEndpoint
                        ? activeColor
                        : isFocal
                          ? 'var(--accent)'
                          : 'var(--border)'
                  }
                  strokeWidth={isSelected || isActiveEndpoint ? 1.9 : isFocal ? 1.6 : 1.15}
                />

                {/* Left subsystem accent bar */}
                <rect
                  x={x + 1}
                  y={y + 6}
                  width="3"
                  height={node.h - 12}
                  rx="1.5"
                  fill={isActiveEndpoint ? activeColor : accent}
                />

                {/* Subtle subsystem glyph */}
                <SubsystemGlyph
                  subsystem={node.spec.subsystem}
                  x={x + 17}
                  y={node.cy}
                  color={isActiveEndpoint ? activeColor : accent}
                />

                {/* Instance shortLabel */}
                <text
                  x={x + 30}
                  y={node.cy - 4}
                  fill="var(--text)"
                  fontSize="11.2"
                  fontWeight="700"
                  fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
                >
                  {truncateText(node.inst.shortLabel, maxTitleChars)}
                </text>

                {/* Host label or component port subtitle */}
                <text
                  x={x + 30}
                  y={node.cy + 12}
                  fill="var(--muted)"
                  fontSize="9.5"
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {truncateText(subtitle, maxSubChars)}
                </text>

                {/* Focal badge (top-right) */}
                {isFocal && !isActiveEndpoint && (
                  <g transform={`translate(${x + node.w - 42}, ${y - 8})`}>
                    <rect
                      x="0"
                      y="0"
                      width="38"
                      height="14"
                      rx="3"
                      fill="var(--accent)"
                    />
                    <text
                      x="19"
                      y="10"
                      textAnchor="middle"
                      fill="var(--page)"
                      fontSize="8.2"
                      fontWeight="700"
                      letterSpacing="0.06em"
                      fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                    >
                      FOCUS
                    </text>
                  </g>
                )}

                {/* Active TX / RX / SELF / LOCAL direction micro-badge */}
                {isActiveEndpoint && (
                  <g transform={`translate(${x + node.w - endpointBadgeW - 4}, ${y - 8})`}>
                    <rect
                      x="0"
                      y="0"
                      width={endpointBadgeW}
                      height="14"
                      rx="3"
                      fill={activeColor}
                    />
                    <text
                      x={endpointBadgeW / 2}
                      y="10"
                      textAnchor="middle"
                      fill="var(--page)"
                      fontSize="8.5"
                      fontWeight="700"
                      letterSpacing="0.05em"
                      fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                    >
                      {endpointBadgeText}
                    </text>
                  </g>
                )}

                {/* Subtle bottom-right expandable macro step micro-badge */}
                {(() => {
                  const expandInfo = expandableInfoByInstance.get(node.id);
                  if (!expandInfo) return null;
                  const label = `+${expandInfo.childCount}`;
                  const badgeW = label.length * 5.8 + 14;
                  const isHighlighted = expandInfo.isCurrentStep;
                  return (
                    <g
                      className="sv-topo-expand-badge"
                      data-expandable-step={expandInfo.stepId}
                      transform={`translate(${x + node.w - badgeW - 4}, ${y + node.h - 7})`}
                      role={onToggleExpandStep ? 'button' : undefined}
                      tabIndex={onToggleExpandStep ? 0 : undefined}
                      aria-label={
                        onToggleExpandStep
                          ? `Expand ${expandInfo.stepTitle} (${expandInfo.childCount} sub-steps)`
                          : undefined
                      }
                      onClick={(e) => {
                        if (!onToggleExpandStep) return;
                        e.stopPropagation();
                        onToggleExpandStep(expandInfo.stepId);
                      }}
                      onKeyDown={(e) => {
                        if (!onToggleExpandStep) return;
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          e.stopPropagation();
                          onToggleExpandStep(expandInfo.stepId);
                        }
                      }}
                    >
                      <title>{`Go deeper: ${expandInfo.stepTitle} (${expandInfo.childCount} sub-steps)`}</title>
                      <rect
                        x="0"
                        y="0"
                        width={badgeW}
                        height="13"
                        rx="3"
                        fill={isHighlighted ? 'var(--selection)' : 'var(--panel)'}
                        stroke={isHighlighted ? 'var(--accent)' : 'var(--border)'}
                        strokeWidth={isHighlighted ? '1.15' : '1'}
                      />
                      <text
                        x={badgeW / 2}
                        y="9.4"
                        textAnchor="middle"
                        fill={isHighlighted ? 'var(--accent)' : 'var(--muted)'}
                        fontSize="8.2"
                        fontWeight="700"
                        letterSpacing="0.04em"
                        fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                      >
                        {label}
                      </text>
                    </g>
                  );
                })()}
              </g>
            );
          })}
        </g>

        {/* Active Glowing Animated Trail (wire trail or local self-arc) */}
        {activeEvent && activeRoute && packetState && (
          <g className="sv-topo-active-trail" pointerEvents="none">
            <polyline
              points={packetState.trailStr}
              fill="none"
              stroke={activeColor}
              strokeWidth={packetState.localSelf ? '6.5' : '7.5'}
              strokeOpacity="0.2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <polyline
              points={packetState.trailStr}
              fill="none"
              stroke={activeColor}
              strokeWidth={packetState.localSelf ? '2.4' : '2.8'}
              strokeDasharray={
                packetState.localSelf ? '3 3' : CHANNEL_META[activeEvent.channel].dash
              }
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </g>
        )}

        {/* Clickable Hop Number Badges */}
        <g className="sv-topo-hops">
          {hopBadges.map(({ frameIdx, frame, x: hx, y: hy }) => {
            const ev = frame.event;
            const isCurrent = frameIdx === activeFrameIndex;
            const isPast = activeFrameIndex >= 0 && frameIdx < activeFrameIndex;
            const hopColor = eventColor(ev);
            const hopNum = frame.frameIndex + 1;

            return (
              <g
                key={`hop-${ev.id}`}
                className="sv-topo-hop"
                role="button"
                tabIndex={0}
                aria-label={`Jump to hop ${hopNum}: ${ev.label}`}
                onClick={() => onSelectEvent(ev.id)}
                onKeyDown={handleHopKey(ev.id)}
              >
                <title>{`#${hopNum} \u00B7 ${ev.label} (${ev.protocol})`}</title>
                <circle
                  cx={hx}
                  cy={hy}
                  r={isCurrent ? 10 : 8.5}
                  fill={isCurrent ? hopColor : 'var(--panel)'}
                  stroke={isCurrent || isPast ? hopColor : 'var(--border)'}
                  strokeWidth={isCurrent ? 1.8 : 1.3}
                />
                <text
                  x={hx}
                  y={hy + 3.2}
                  textAnchor="middle"
                  fill={isCurrent ? 'var(--page)' : isPast ? hopColor : 'var(--muted)'}
                  fontSize={hopNum >= 10 ? '8.5' : '9.2'}
                  fontWeight="700"
                  fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                >
                  {hopNum}
                </text>
              </g>
            );
          })}
        </g>

        {/* Traveling Packet Marker (or Local Self-Arc Pulse) & Floating Clamped Protocol Pill */}
        {activeEvent && packetState && (
          <g
            className="sv-topo-packet"
            role="button"
            tabIndex={0}
            aria-label={`Active event #${activeFrameIndex + 1}: ${
              packetState.localSelf ? `LOCAL \u00B7 ${activeEvent.label}` : activeEvent.label
            }`}
            onClick={() => onSelectEvent(activeEvent.id)}
            onKeyDown={handleHopKey(activeEvent.id)}
          >
            <title>
              {`#${activeFrameIndex + 1} ${packetState.localSelf ? 'LOCAL' : activeEvent.protocol} \u00B7 ${activeEvent.label}${activeEvent.port ? ` (${activeEvent.port})` : ''}`}
            </title>

            {/* Outer halo ring */}
            <circle
              className={reducedMotion ? undefined : 'sv-topo-orb-pulse'}
              cx={packetState.pos[0]}
              cy={packetState.pos[1]}
              r={packetState.localSelf ? '13' : '15'}
              fill={activeColor}
              fillOpacity="0.22"
            />
            {packetState.localSelf && (
              <circle
                cx={packetState.pos[0]}
                cy={packetState.pos[1]}
                r="10"
                fill="none"
                stroke={activeColor}
                strokeWidth="1.4"
                strokeDasharray="3 2"
              />
            )}
            {activeEvent.isError && (
              <circle
                cx={packetState.pos[0]}
                cy={packetState.pos[1]}
                r="18"
                fill="none"
                stroke="var(--ch-error)"
                strokeWidth="1.6"
                strokeDasharray="3 2"
              />
            )}

            {/* Distinct channel payload glyph */}
            <g transform={`translate(${packetState.pos[0].toFixed(1)}, ${packetState.pos[1].toFixed(1)})`}>
              <PacketGlyph
                channel={packetState.localSelf ? 'internal' : activeEvent.channel}
                isError={activeEvent.isError}
                color={activeColor}
              />
            </g>

            {/* Floating clamped protocol/label pill */}
            <g transform={`translate(${packetState.pillX.toFixed(1)}, ${packetState.pillY.toFixed(1)})`}>
              <rect
                x="0"
                y="0"
                width={packetState.pillW}
                height={packetState.pillH}
                rx="5"
                fill="var(--panel)"
                fillOpacity="0.96"
                stroke={activeColor}
                strokeWidth="1.25"
                strokeDasharray={packetState.localSelf ? '4 2' : undefined}
              />
              {packetState.localSelf ? (
                <>
                  <circle
                    cx="12"
                    cy={packetState.pillH / 2}
                    r="4.2"
                    fill="none"
                    stroke={activeColor}
                    strokeWidth="1.3"
                    strokeDasharray="2 1.5"
                  />
                  <circle cx="12" cy={packetState.pillH / 2} r="1.8" fill={activeColor} />
                  <text
                    x="22"
                    y={packetState.pillH / 2 + 3.5}
                    fontSize="10.4"
                    fontWeight="600"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    <tspan fill={activeColor} fontWeight="700">
                      LOCAL
                    </tspan>
                    <tspan fill="var(--muted)">{' \u00B7 '}</tspan>
                    <tspan fill="var(--text)">{packetState.labelText}</tspan>
                  </text>
                </>
              ) : (
                <>
                  {/* Channel micro-tag */}
                  <rect
                    x="4"
                    y="4"
                    width={packetState.tagW}
                    height={packetState.pillH - 8}
                    rx="3"
                    fill={activeColor}
                    fillOpacity="0.18"
                  />
                  <text
                    x={4 + packetState.tagW / 2}
                    y={packetState.pillH / 2 + 3.2}
                    textAnchor="middle"
                    fill={activeColor}
                    fontSize="8.8"
                    fontWeight="700"
                    letterSpacing="0.04em"
                    fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
                  >
                    {packetState.channelTag}
                  </text>
                  <text
                    x={packetState.tagW + 12}
                    y={packetState.pillH / 2 + 3.5}
                    fill="var(--text)"
                    fontSize="10.4"
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

        {/* Calm Empty-State Filter Banner (zones & nodes remain visible underneath) */}
        {visibleCount === 0 && (
          <g className="sv-topo-empty" role="status" aria-live="polite">
            <rect
              x={VIEW_W / 2 - 236}
              y={VIEW_H / 2 - 34}
              width="472"
              height="68"
              rx="8"
              fill="var(--panel)"
              fillOpacity="0.95"
              stroke="var(--border)"
              strokeWidth="1.4"
            />
            <rect
              x={VIEW_W / 2 - 236}
              y={VIEW_H / 2 - 34}
              width="4"
              height="68"
              rx="2"
              fill="var(--accent)"
            />
            <text
              x={VIEW_W / 2}
              y={VIEW_H / 2 - 4}
              textAnchor="middle"
              fill="var(--text)"
              fontSize="12.5"
              fontWeight="600"
              fontFamily="var(--font-sans, 'IBM Plex Sans', system-ui, sans-serif)"
            >
              No matching events for current category filters
            </text>
            <text
              x={VIEW_W / 2}
              y={VIEW_H / 2 + 16}
              textAnchor="middle"
              fill="var(--muted)"
              fontSize="10"
              fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
            >
              {hiddenCount > 0
                ? `${hiddenCount} ${hiddenCount === 1 ? 'event' : 'events'} hidden at current depth`
                : 'No events recorded in this run'}
            </text>
          </g>
        )}

        {/* Bottom Blueprint Coordinate Footer */}
        <g pointerEvents="none">
          <line
            x1={MARGIN_X}
            y1={VIEW_H - FOOTER_RESERVE + 8}
            x2={VIEW_W - MARGIN_X}
            y2={VIEW_H - FOOTER_RESERVE + 8}
            stroke="var(--border)"
            strokeOpacity="0.55"
            strokeWidth="1"
          />
          <text
            x={MARGIN_X}
            y={VIEW_H - 12}
            fill="var(--faint)"
            fontSize="9.5"
            letterSpacing="0.08em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            TOPOLOGY CANVAS {'\u00B7'} CLICK ANY COMPONENT TO INSPECT {'\u00B7'} CLICK PACKET OR ROW TO DECODE
          </text>
          <text
            x={VIEW_W - MARGIN_X}
            y={VIEW_H - 12}
            textAnchor="end"
            fill="var(--faint)"
            fontSize="9.5"
            letterSpacing="0.06em"
            fontFamily="var(--font-mono, 'IBM Plex Mono', ui-monospace, monospace)"
          >
            {`${run.layerTag.toUpperCase()} \u00B7 ${layout.nodes.length} NODES \u00B7 ${
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
