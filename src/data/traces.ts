import {
  CATALYST_JVM_COMPONENT_TRACES,
  CATALYST_JVM_LIFECYCLE_TRACES,
  CATALYST_JVM_SCENARIO_TRACES,
} from './tracesCatalystJvm.ts';
import {
  LIFECYCLE_TRACES as BASE_LIFECYCLE_TRACES,
  SCENARIO_TRACES as BASE_SCENARIO_TRACES,
} from './tracesCore.ts';
import { COMPONENT_TRACES_A } from './tracesCompA.ts';
import { COMPONENT_TRACES_B } from './tracesCompB.ts';
import { COMPONENT_TRACES_C } from './tracesCompC.ts';
import type { ComponentId, TraceLesson } from './types.ts';

export const LIFECYCLE_TRACES: TraceLesson[] = [
  ...BASE_LIFECYCLE_TRACES,
  ...CATALYST_JVM_LIFECYCLE_TRACES,
];

export const SCENARIO_TRACES: TraceLesson[] = [
  ...BASE_SCENARIO_TRACES,
  ...CATALYST_JVM_SCENARIO_TRACES,
];

export const COMPONENT_TRACES: TraceLesson[] = [
  ...COMPONENT_TRACES_A,
  ...COMPONENT_TRACES_B,
  ...COMPONENT_TRACES_C,
  ...CATALYST_JVM_COMPONENT_TRACES,
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
