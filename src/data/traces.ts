import { LIFECYCLE_TRACES, SCENARIO_TRACES } from './tracesCore.ts';
import { COMPONENT_TRACES_A } from './tracesCompA.ts';
import { COMPONENT_TRACES_B } from './tracesCompB.ts';
import type { ComponentId, TraceLesson } from './types.ts';

export const COMPONENT_TRACES: TraceLesson[] = [
  ...COMPONENT_TRACES_A,
  ...COMPONENT_TRACES_B,
];

export const ALL_TRACES: TraceLesson[] = [
  ...LIFECYCLE_TRACES,
  ...SCENARIO_TRACES,
  ...COMPONENT_TRACES,
];

export const TRACE_BY_ID: Record<string, TraceLesson> = Object.fromEntries(
  ALL_TRACES.map(trace => [trace.id, trace]),
);

export const COMPONENT_TRACE_BY_ID: Partial<Record<ComponentId, TraceLesson>> = Object.fromEntries(
  COMPONENT_TRACES.filter(t => t.componentId).map(t => [t.componentId!, t]),
);

export { LIFECYCLE_TRACES, SCENARIO_TRACES };
