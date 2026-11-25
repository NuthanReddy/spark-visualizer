import { COMPONENTS } from '../data/components.ts';
import { DEEP_STEP_ENRICHMENTS } from '../data/deepSteps.ts';
import type {
  DeepStepEnrichment,
  ExecutionRun,
  RunEvent,
  RunStep,
  RuntimeInstance,
  StepCategory,
} from '../data/runModel';
import type { ComponentId, HostZoneId, TraceLesson, TraceMessage } from '../data/types';

const LOCAL_PORT_PATTERN =
  /(?:^|\s)(?:in-jvm|in-process|memory|local|local-dir|local-disk|os)(?:\s|$)/i;

export function isLocalPort(port: string): boolean {
  return LOCAL_PORT_PATTERN.test(port.trim());
}

export function inferStepCategory(
  from: ComponentId,
  to: ComponentId,
  msg: Pick<TraceMessage, 'channel' | 'label' | 'protocol' | 'info'>,
): StepCategory {
  const hay = `${from} ${to} ${msg.label} ${msg.protocol} ${msg.info}`.toLowerCase();

  if (
    from === 'broadcast-manager' ||
    to === 'broadcast-manager' ||
    from === 'broadcast-exchange' ||
    to === 'broadcast-exchange' ||
    from === 'broadcast-hash-join' ||
    to === 'broadcast-hash-join' ||
    hay.includes('torrent') ||
    hay.includes('broadcast')
  ) {
    return 'broadcast-torrent';
  }
  if (
    from === 'output-commit-coordinator' ||
    to === 'output-commit-coordinator' ||
    from === 'file-commit-protocol' ||
    to === 'file-commit-protocol' ||
    from === 'file-format-writer' ||
    to === 'file-format-writer' ||
    hay.includes('commitjob') ||
    hay.includes('committask') ||
    hay.includes('cancommit') ||
    hay.includes('abortjob')
  ) {
    return 'output-commit';
  }
  if (
    from === 'python-worker-factory' ||
    to === 'python-worker-factory' ||
    from === 'python-worker' ||
    to === 'python-worker' ||
    from === 'python-runner' ||
    to === 'python-runner' ||
    from === 'python-udf-operator' ||
    to === 'python-udf-operator'
  ) {
    return 'python-worker-ipc';
  }
  if (
    from === 'python-driver' ||
    to === 'python-driver' ||
    from === 'py4j-gateway' ||
    to === 'py4j-gateway' ||
    hay.includes('py4j') ||
    hay.includes('pythonrunner')
  ) {
    return 'driver-python-bridge';
  }
  if (
    from === 'shuffle-writer' ||
    to === 'shuffle-writer' ||
    from === 'shuffle-reader' ||
    to === 'shuffle-reader' ||
    from === 'shuffle-block-resolver' ||
    to === 'shuffle-block-resolver' ||
    from === 'external-shuffle-service' ||
    to === 'external-shuffle-service' ||
    from === 'map-output-tracker-master' ||
    to === 'map-output-tracker-master' ||
    from === 'map-output-tracker-worker' ||
    to === 'map-output-tracker-worker' ||
    hay.includes('fetchfailed')
  ) {
    return 'shuffle-internals';
  }
  if (
    from === 'executor-allocation-manager' ||
    to === 'executor-allocation-manager' ||
    from === 'executor-monitor' ||
    to === 'executor-monitor'
  ) {
    return 'yarn-lifecycle';
  }
  if (
    from === 'heartbeat-receiver' ||
    to === 'heartbeat-receiver' ||
    from === 'listener-bus' ||
    to === 'listener-bus' ||
    from === 'event-log' ||
    to === 'event-log' ||
    from === 'spark-ui' ||
    to === 'spark-ui' ||
    from === 'history-server' ||
    to === 'history-server' ||
    from === 'metrics-system' ||
    to === 'metrics-system' ||
    from === 'executor-metrics-poller' ||
    to === 'executor-metrics-poller' ||
    hay.includes('heartbeat')
  ) {
    return 'heartbeats-metrics';
  }
  if (
    hay.includes('staging') ||
    hay.includes('localiz') ||
    hay.includes('.sparkstaging') ||
    hay.includes('preparelocalresources')
  ) {
    return 'staging-localization';
  }
  if (
    from === 'sql-planner' ||
    to === 'sql-planner' ||
    from === 'adaptive-query-execution' ||
    to === 'adaptive-query-execution' ||
    from === 'dag-scheduler' ||
    to === 'dag-scheduler' ||
    from === 'task-scheduler' ||
    to === 'task-scheduler' ||
    from === 'task-set-manager' ||
    to === 'task-set-manager'
  ) {
    return 'sql-planning-aqe';
  }
  if (hay.includes('shuffle')) {
    return 'shuffle-internals';
  }
  return 'yarn-lifecycle';
}

const CLIENT_HOST_DRIVER_COMPONENTS = new Set<ComponentId>([
  'spark-driver',
  'python-driver',
  'py4j-gateway',
  'spark-session',
  'spark-context',
  'spark-env',
  'dag-scheduler',
  'task-scheduler',
  'task-set-manager',
  'driver-scheduler-backend',
  'yarn-scheduler-backend',
  'task-result-getter',
  'sql-planner',
  'adaptive-query-execution',
  'broadcast-exchange',
  'executor-allocation-manager',
  'executor-monitor',
  'heartbeat-receiver',
  'block-manager-master',
  'map-output-tracker-master',
  'broadcast-manager',
  'output-commit-coordinator',
  'listener-bus',
  'event-log',
  'spark-ui',
  'metrics-system',
]);

function hostLabelForZone(zone: HostZoneId): string {
  switch (zone) {
    case 'submit-host':
      return 'edge-01';
    case 'yarn-control':
      return 'rm01';
    case 'storage-history':
      return 'hdfs / history';
    case 'driver-jvm':
      return 'driver-1 (AM)';
    case 'worker-node-a':
      return 'worker-a';
    case 'worker-node-b':
      return 'worker-b';
  }
}

function createDefaultInstance(compId: ComponentId, isClientMode: boolean): RuntimeInstance {
  const spec = COMPONENTS[compId];
  const hostZone: HostZoneId =
    isClientMode && CLIENT_HOST_DRIVER_COMPONENTS.has(compId)
      ? 'submit-host'
      : spec.hostZone;
  return {
    id: compId,
    componentId: compId,
    label: spec.name,
    shortLabel: spec.shortName,
    hostZone,
    hostLabel:
      isClientMode && hostZone === 'submit-host'
        ? 'edge-01 (client driver)'
        : hostLabelForZone(hostZone),
    processId: spec.process,
  };
}

function normalizeExtraInstance(
  extra: RuntimeInstance,
  isClientMode: boolean,
): RuntimeInstance {
  const defaultSpec = COMPONENTS[extra.componentId];
  if (
    isClientMode &&
    extra.hostZone === 'driver-jvm' &&
    defaultSpec?.hostZone === 'driver-jvm' &&
    CLIENT_HOST_DRIVER_COMPONENTS.has(extra.componentId)
  ) {
    return {
      ...extra,
      hostZone: 'submit-host',
      hostLabel: 'edge-01 (client driver)',
    };
  }
  return { ...extra };
}

export function adaptTraceToRun(trace: TraceLesson): ExecutionRun {
  const isClientMode = trace.deployMode === 'client';
  const participantIds = Array.from(
    new Set<ComponentId>([
      ...trace.focusNodes,
      ...trace.messages.flatMap(m => [m.from, m.to]),
    ]),
  );

  const instances: Record<string, RuntimeInstance> = {};
  const customByComponent = new Map<ComponentId, RuntimeInstance[]>();

  for (const custom of trace.customInstances ?? []) {
    const inst: RuntimeInstance = {
      id: custom.id,
      componentId: custom.componentId,
      label: custom.label,
      shortLabel: custom.shortLabel,
      hostZone: custom.hostZone,
      hostLabel: custom.hostLabel,
      processId: custom.processId,
      containerId: custom.containerId,
      executorNumber: custom.executorNumber,
      attemptNumber: custom.attemptNumber,
    };
    instances[inst.id] = inst;
    const list = customByComponent.get(inst.componentId);
    if (list) {
      list.push(inst);
    } else {
      customByComponent.set(inst.componentId, [inst]);
    }
  }

  const referencedInstanceIds = new Set<string>();
  for (const m of trace.messages) {
    referencedInstanceIds.add(m.fromInstance ?? m.from);
    referencedInstanceIds.add(m.toInstance ?? m.to);
  }

  const focusInstances: string[] = [];
  const focusInstanceSet = new Set<string>();
  const addFocus = (id: string) => {
    if (!focusInstanceSet.has(id)) {
      focusInstanceSet.add(id);
      focusInstances.push(id);
    }
  };

  for (const compId of participantIds) {
    const customList = customByComponent.get(compId);
    const hasCustomOverride = Boolean(customList && customList.length > 0);
    const referencedDirectly = referencedInstanceIds.has(compId);

    if (!hasCustomOverride || referencedDirectly) {
      if (!instances[compId]) {
        instances[compId] = createDefaultInstance(compId, isClientMode);
      }
      addFocus(compId);
    }

    if (customList) {
      for (const customInst of customList) {
        addFocus(customInst.id);
      }
    }
  }

  for (const custom of trace.customInstances ?? []) {
    addFocus(custom.id);
  }

  const enrichment: DeepStepEnrichment | undefined = DEEP_STEP_ENRICHMENTS[trace.id];
  if (enrichment) {
    // Register extra deep-only instances in `instances` without adding them to
    // `focusInstances`, so collapsed views stay minimal and expanded frames surface them.
    for (const extra of enrichment.extraInstances ?? []) {
      if (!instances[extra.id]) {
        instances[extra.id] = normalizeExtraInstance(extra, isClientMode);
      }
    }
    for (const expansion of Object.values(enrichment.expansions)) {
      for (const ev of expansion.events) {
        if (
          !instances[ev.fromInstance] &&
          ev.fromInstance === ev.fromComponent &&
          COMPONENTS[ev.fromComponent]
        ) {
          instances[ev.fromInstance] = createDefaultInstance(ev.fromComponent, isClientMode);
        }
        if (
          !instances[ev.toInstance] &&
          ev.toInstance === ev.toComponent &&
          COMPONENTS[ev.toComponent]
        ) {
          instances[ev.toInstance] = createDefaultInstance(ev.toComponent, isClientMode);
        }
      }
    }
  }

  const baseSteps: RunStep[] = trace.steps.map((s, idx) => {
    const firstMsg = trace.messages.find(m => m.step === idx);
    const category: StepCategory = firstMsg
      ? inferStepCategory(firstMsg.from, firstMsg.to, firstMsg)
      : 'yarn-lifecycle';
    return {
      id: `${trace.id}:step-${idx}`,
      title: s.title,
      explanation: s.explanation,
      category,
      keyState: s.keyState,
      command: s.command,
      detailsPassed: s.detailsPassed,
      deepDiveComponentId: firstMsg?.to ?? trace.componentId,
    };
  });

  const baseEvents: RunEvent[] = trace.messages.map(m => {
    const stepIdx = Math.max(0, Math.min(baseSteps.length - 1, m.step));
    const stepId = baseSteps[stepIdx].id;
    const inferredCat = inferStepCategory(m.from, m.to, m);
    const fromInst = m.fromInstance ?? m.from;
    const toInst = m.toInstance ?? m.to;
    const localPort = isLocalPort(m.port);
    return {
      id: `${trace.id}:${m.id}`,
      stepId,
      kind:
        m.kind ??
        (localPort || (fromInst === toInst && m.channel === 'internal') ? 'local' : 'wire'),
      categoryOverride: inferredCat !== baseSteps[stepIdx].category ? inferredCat : undefined,
      fromInstance: fromInst,
      toInstance: toInst,
      fromComponent: m.from,
      toComponent: m.to,
      channel: m.channel,
      protocol: m.protocol,
      port: m.port,
      label: m.label,
      info: m.info,
      size: m.size,
      command: m.command,
      payload: m.payload,
      fields: m.fields,
      senderState: m.senderState,
      receiverState: m.receiverState,
      isError: m.isError,
      peerTag: m.peerTag,
    };
  });

  const steps: RunStep[] = [];
  const stepIndexById = new Map<string, number>();

  for (let idx = 0; idx < baseSteps.length; idx++) {
    const baseStep = baseSteps[idx];
    stepIndexById.set(baseStep.id, idx);
    const expansion = enrichment?.expansions[idx];
    if (expansion && expansion.steps.length > 0) {
      steps.push({ ...baseStep, isMacro: true });
      const nestedParentIds = new Set<string>();
      for (const childStep of expansion.steps) {
        if (childStep.parentId) nestedParentIds.add(childStep.parentId);
      }
      for (const childStep of expansion.steps) {
        const hasNestedChildren = nestedParentIds.has(childStep.id);
        steps.push({
          ...childStep,
          parentId: childStep.parentId ?? baseStep.id,
          ...(hasNestedChildren ? { isMacro: true } : {}),
        });
      }
    } else {
      steps.push(baseStep);
    }
  }

  let events: RunEvent[];
  if (!enrichment) {
    events = baseEvents;
  } else {
    const lastBaseEventIdxByStep = new Map<number, number>();
    for (let i = 0; i < baseEvents.length; i++) {
      const sIdx = stepIndexById.get(baseEvents[i].stepId);
      if (sIdx !== undefined) {
        lastBaseEventIdxByStep.set(sIdx, i);
      }
    }

    const insertAfterBaseIdx = new Map<number, RunEvent[]>();
    const leadingEvents: RunEvent[] = [];

    const normalizeEnrichedEvent = (ev: RunEvent): RunEvent => ({
      ...ev,
      kind: isLocalPort(ev.port) ? 'local' : ev.kind,
    });

    const appendAfter = (baseIdx: number, toAppend: RunEvent[]) => {
      const copies = toAppend.map(normalizeEnrichedEvent);
      const existing = insertAfterBaseIdx.get(baseIdx);
      if (existing) {
        existing.push(...copies);
      } else {
        insertAfterBaseIdx.set(baseIdx, copies);
      }
    };

    for (let idx = 0; idx < baseSteps.length; idx++) {
      const expansion = enrichment.expansions[idx];
      if (!expansion || expansion.events.length === 0) continue;

      const directAnchor = lastBaseEventIdxByStep.get(idx);
      if (directAnchor !== undefined) {
        appendAfter(directAnchor, expansion.events);
      } else {
        let prevAnchor: number | undefined;
        for (let p = idx - 1; p >= 0; p--) {
          if (lastBaseEventIdxByStep.has(p)) {
            prevAnchor = lastBaseEventIdxByStep.get(p);
            break;
          }
        }
        if (prevAnchor !== undefined) {
          appendAfter(prevAnchor, expansion.events);
        } else {
          leadingEvents.push(...expansion.events.map(normalizeEnrichedEvent));
        }
      }
    }

    events = [...leadingEvents];
    for (let i = 0; i < baseEvents.length; i++) {
      events.push(baseEvents[i]);
      const extraEvs = insertAfterBaseIdx.get(i);
      if (extraEvs) {
        events.push(...extraEvs);
      }
    }
  }

  return {
    id: trace.id,
    sourceKind: 'catalog',
    traceKind: trace.kind,
    name: trace.name,
    subtitle: trace.subtitle,
    summary: trace.summary,
    deployMode: trace.deployMode,
    layerTag: trace.layerTag,
    componentId: trace.componentId,
    scenarioNumber: trace.scenarioNumber,
    subsystem: trace.subsystem,
    instances,
    focusInstances,
    steps,
    events,
  };
}
