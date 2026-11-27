import { COMPONENTS } from '../data/components.ts';
import { resolveDeepStepEnrichment } from '../data/deepSteps.ts';
import { resolveTraceForLanguage } from '../data/jvmTraceOverrides.ts';
import type {
  DeepStepEnrichment,
  ExecutionRun,
  RunEvent,
  RunStep,
  RuntimeInstance,
  StepCategory,
} from '../data/runModel.ts';
import type {
  ComponentId,
  SparkLanguage,
  TraceLesson,
  TraceMessage,
} from '../data/types.ts';
import {
  CLIENT_HOST_DRIVER_COMPONENTS,
  resolveDefaultHostLabel,
  resolveDefaultHostZone,
} from './hostPlacement.ts';

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
    from === 'datasource-v2' ||
    to === 'datasource-v2' ||
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
    from === 'push-based-shuffle' ||
    to === 'push-based-shuffle' ||
    from === 'map-output-tracker-master' ||
    to === 'map-output-tracker-master' ||
    from === 'map-output-tracker-worker' ||
    to === 'map-output-tracker-worker' ||
    hay.includes('fetchfailed')
  ) {
    return 'shuffle-internals';
  }
  if (
    from === 'block-manager-decommissioner' ||
    to === 'block-manager-decommissioner'
  ) {
    return hay.includes('decommission') &&
      (hay.includes('yarn') || hay.includes('nodemanager') || hay.includes('resourcemanager') || hay.includes('allocator'))
      ? 'yarn-lifecycle'
      : 'shuffle-internals';
  }
  if (
    from === 'memory-manager' ||
    to === 'memory-manager'
  ) {
    return hay.includes('spill') ||
      hay.includes('shuffle') ||
      hay.includes('storage') ||
      hay.includes('execution') ||
      hay.includes('evict') ||
      hay.includes('oom')
      ? 'shuffle-internals'
      : 'yarn-lifecycle';
  }
  if (
    from === 'executor-allocation-manager' ||
    to === 'executor-allocation-manager' ||
    from === 'executor-monitor' ||
    to === 'executor-monitor' ||
    from === 'resource-profile-manager' ||
    to === 'resource-profile-manager' ||
    from === 'health-tracker' ||
    to === 'health-tracker' ||
    from === 'delegation-token-manager' ||
    to === 'delegation-token-manager' ||
    from === 'security-manager' ||
    to === 'security-manager' ||
    from === 'jvm-user-app' ||
    to === 'jvm-user-app' ||
    from === 'closure-cleaner' ||
    to === 'closure-cleaner' ||
    from === 'scala-repl-compiler' ||
    to === 'scala-repl-compiler' ||
    from === 'java-spark-api' ||
    to === 'java-spark-api'
  ) {
    return 'yarn-lifecycle';
  }
  if (
    from === 'heartbeat-receiver' ||
    to === 'heartbeat-receiver' ||
    from === 'listener-bus' ||
    to === 'listener-bus' ||
    from === 'app-status-store' ||
    to === 'app-status-store' ||
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
    from === 'rpc-file-server' ||
    to === 'rpc-file-server' ||
    hay.includes('staging') ||
    hay.includes('localiz') ||
    hay.includes('.sparkstaging') ||
    hay.includes('preparelocalresources')
  ) {
    return 'staging-localization';
  }
  if (
    from === 'session-catalog' ||
    to === 'session-catalog' ||
    from === 'catalyst-analyzer' ||
    to === 'catalyst-analyzer' ||
    from === 'catalyst-optimizer' ||
    to === 'catalyst-optimizer' ||
    from === 'shuffle-exchange' ||
    to === 'shuffle-exchange' ||
    from === 'columnar-cache' ||
    to === 'columnar-cache' ||
    from === 'sql-planner' ||
    to === 'sql-planner' ||
    from === 'adaptive-query-execution' ||
    to === 'adaptive-query-execution' ||
    from === 'whole-stage-codegen' ||
    to === 'whole-stage-codegen' ||
    from === 'janino-code-generator' ||
    to === 'janino-code-generator' ||
    from === 'tungsten-unsafe-row' ||
    to === 'tungsten-unsafe-row' ||
    from === 'hash-aggregate' ||
    to === 'hash-aggregate' ||
    from === 'sort-merge-join' ||
    to === 'sort-merge-join' ||
    from === 'dataset-encoder-udf' ||
    to === 'dataset-encoder-udf' ||
    from === 'typed-dataset-ops' ||
    to === 'typed-dataset-ops' ||
    from === 'scala-java-udf' ||
    to === 'scala-java-udf' ||
    from === 'typed-aggregator' ||
    to === 'typed-aggregator' ||
    from === 'dag-scheduler' ||
    to === 'dag-scheduler' ||
    from === 'task-scheduler' ||
    to === 'task-scheduler' ||
    from === 'task-set-manager' ||
    to === 'task-set-manager'
  ) {
    return 'sql-planning-aqe';
  }
  if (
    from === 'serializer-manager' ||
    to === 'serializer-manager' ||
    from === 'kryo-registrator' ||
    to === 'kryo-registrator' ||
    from === 'user-classloader' ||
    to === 'user-classloader' ||
    from === 'context-cleaner' ||
    to === 'context-cleaner' ||
    from === 'transport-context' ||
    to === 'transport-context'
  ) {
    if (hay.includes('shuffle') || hay.includes('block') || hay.includes('fetch') || hay.includes('chunk')) {
      return 'shuffle-internals';
    }
    if (hay.includes('closure') || hay.includes('kryo') || hay.includes('task')) {
      return 'sql-planning-aqe';
    }
    return 'yarn-lifecycle';
  }
  if (hay.includes('shuffle')) {
    return 'shuffle-internals';
  }
  return 'yarn-lifecycle';
}

function createDefaultInstance(compId: ComponentId, isClientMode: boolean): RuntimeInstance {
  const spec = COMPONENTS[compId];
  const hostZone = resolveDefaultHostZone(compId, isClientMode);
  return {
    id: compId,
    componentId: compId,
    label: spec.name,
    shortLabel: spec.shortName,
    hostZone,
    hostLabel: resolveDefaultHostLabel(compId, hostZone, isClientMode),
    processId: spec.process,
  };
}

function normalizeExtraInstance(
  extra: RuntimeInstance,
  isClientMode: boolean,
): RuntimeInstance {
  const defaultSpec = COMPONENTS[extra.componentId];
  // Only relocate auto-generated default-ID driver instances in client mode; never
  // overwrite an extra instance explicitly pinned to a non-default host or distinct role ID.
  if (
    isClientMode &&
    extra.id === extra.componentId &&
    extra.hostZone === 'driver-jvm' &&
    defaultSpec?.hostZone === 'driver-jvm' &&
    CLIENT_HOST_DRIVER_COMPONENTS.has(extra.componentId)
  ) {
    return {
      ...extra,
      hostZone: 'submit-host',
      hostLabel: resolveDefaultHostLabel(extra.componentId, 'submit-host', true),
    };
  }
  return { ...extra };
}

export function adaptTraceToRun(
  trace: TraceLesson,
  language?: SparkLanguage,
): ExecutionRun {
  const effectiveTrace = resolveTraceForLanguage(trace, language);
  const isClientMode = effectiveTrace.deployMode === 'client';
  const participantIds = Array.from(
    new Set<ComponentId>([
      ...effectiveTrace.focusNodes,
      ...effectiveTrace.messages.flatMap(m => [m.from, m.to]),
    ]),
  );

  const instances: Record<string, RuntimeInstance> = {};
  const customByComponent = new Map<ComponentId, RuntimeInstance[]>();

  for (const custom of effectiveTrace.customInstances ?? []) {
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
  for (const m of effectiveTrace.messages) {
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

  for (const custom of effectiveTrace.customInstances ?? []) {
    addFocus(custom.id);
  }

  // Resolve language-specific or pure-JVM deep-step enrichments cleanly.
  const enrichment: DeepStepEnrichment | undefined = resolveDeepStepEnrichment(
    effectiveTrace.id,
    language,
    effectiveTrace !== trace,
  );
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

  const baseSteps: RunStep[] = effectiveTrace.steps.map((s, idx) => {
    const firstMsg = effectiveTrace.messages.find(m => m.step === idx);
    const category: StepCategory = firstMsg
      ? inferStepCategory(firstMsg.from, firstMsg.to, firstMsg)
      : 'yarn-lifecycle';
    return {
      id: `${effectiveTrace.id}:step-${idx}`,
      title: s.title,
      explanation: s.explanation,
      category,
      keyState: s.keyState,
      command: s.command,
      detailsPassed: s.detailsPassed,
      deepDiveComponentId: firstMsg?.to ?? effectiveTrace.componentId,
    };
  });

  const baseEvents: RunEvent[] = effectiveTrace.messages.map(m => {
    const stepIdx = Math.max(0, Math.min(baseSteps.length - 1, m.step));
    const stepId = baseSteps[stepIdx].id;
    const inferredCat = inferStepCategory(m.from, m.to, m);
    const fromInst = m.fromInstance ?? m.from;
    const toInst = m.toInstance ?? m.to;
    const localPort = isLocalPort(m.port);
    return {
      id: `${effectiveTrace.id}:${m.id}`,
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
    id: effectiveTrace.id,
    sourceKind: 'catalog',
    traceKind: effectiveTrace.kind,
    name: effectiveTrace.name,
    subtitle: effectiveTrace.subtitle,
    summary: effectiveTrace.summary,
    deployMode: effectiveTrace.deployMode,
    layerTag: effectiveTrace.layerTag,
    componentId: effectiveTrace.componentId,
    scenarioNumber: effectiveTrace.scenarioNumber,
    subsystem: effectiveTrace.subsystem,
    instances,
    focusInstances,
    steps,
    events,
  };
}
