export interface Timeline {
  cursor: number;
  progress: number;
}

export const MESSAGE_DURATION_MS = 1400;

export function advanceTimeline(
  timeline: Timeline,
  elapsedMs: number,
  speed: number,
  count: number,
): Timeline {
  if (count <= 0) return { cursor: -1, progress: 0 };
  const base = Math.max(0, timeline.cursor) + timeline.progress;
  const nextPos = base + (elapsedMs * speed) / MESSAGE_DURATION_MS;
  if (nextPos >= count) return { cursor: count - 1, progress: 1 };
  return { cursor: Math.floor(nextPos), progress: nextPos % 1 };
}

export function messageTime(index: number): string {
  return ((index + 1) * 0.14).toFixed(2);
}

/**
 * Computes a clean quadratic curve between two spatial coordinates.
 * Self-edges (from === to) return a compact loop arc above the node so local
 * in-component transitions still visibly animate without collapsing to a dot.
 */
export function buildEdgeCurve(
  from: [number, number],
  to: [number, number],
  curvature = 0.16,
): {
  path: string;
  sample: (t: number) => [number, number];
  trail: (t: number, segments?: number) => [number, number][];
  midpoint: [number, number];
} {
  const [x1, y1] = from;
  const [x2, y2] = to;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const dist = Math.hypot(dx, dy);

  if (dist < 1) {
    // Self-loop arc above the node
    const rx = 38;
    const ry = 36;
    const sample = (t: number): [number, number] => {
      const a = Math.PI * (1 - t);
      return [x1 + Math.cos(a) * rx, y1 - 24 - Math.sin(a) * ry];
    };
    const trail = (t: number, segments = 16): [number, number][] => {
      const clamped = Math.max(0, Math.min(1, t));
      const pts: [number, number][] = [];
      for (let i = 0; i <= segments; i++) {
        pts.push(sample((clamped * i) / segments));
      }
      return pts;
    };
    return {
      path: `M ${x1 - rx} ${y1 - 24} A ${rx} ${ry} 0 1 1 ${x1 + rx} ${y1 - 24}`,
      sample,
      trail,
      midpoint: [x1, y1 - 24 - ry],
    };
  }

  // Perpendicular control point for smooth arc separation (distinct forward/reverse lanes)
  const nx = -dy / dist;
  const ny = dx / dist;
  const offset = Math.min(90, Math.max(24, dist * curvature));
  const cx = (x1 + x2) / 2 + nx * offset;
  const cy = (y1 + y2) / 2 + ny * offset;

  const sample = (t: number): [number, number] => {
    const u = Math.max(0, Math.min(1, t));
    const inv = 1 - u;
    return [
      inv * inv * x1 + 2 * inv * u * cx + u * u * x2,
      inv * inv * y1 + 2 * inv * u * cy + u * u * y2,
    ];
  };

  const trail = (t: number, segments = 20): [number, number][] => {
    const clamped = Math.max(0, Math.min(1, t));
    const pts: [number, number][] = [];
    for (let i = 0; i <= segments; i++) {
      pts.push(sample((clamped * i) / segments));
    }
    return pts;
  };

  return {
    path: `M ${x1.toFixed(1)} ${y1.toFixed(1)} Q ${cx.toFixed(1)} ${cy.toFixed(1)} ${x2.toFixed(1)} ${y2.toFixed(1)}`,
    sample,
    trail,
    midpoint: sample(0.5),
  };
}
