import { COMPONENTS } from '../data/components.ts';
import type { RunEvent, RuntimeInstance } from '../data/runModel.ts';
import type { ComponentId, HostZoneId } from '../data/types.ts';

/**
 * Driver-JVM components that execute inside the client driver process on `submit-host`
 * (edge-01) in YARN client deploy mode. `application-master` and `yarn-allocator` NEVER
 * belong here because the YARN AM container always runs on a cluster node (`driver-jvm`).
 */
export const CLIENT_HOST_DRIVER_COMPONENTS: ReadonlySet<ComponentId> = new Set<ComponentId>([
  'spark-driver',
  'python-driver',
  'py4j-gateway',
  'jvm-user-app',
  'delegation-token-manager',
  'spark-session',
  'spark-context',
  'spark-env',
  'rpc-env',
  'rpc-file-server',
  'serializer-manager',
  'session-catalog',
  'sql-planner',
  'adaptive-query-execution',
  'broadcast-exchange',
  'whole-stage-codegen',
  'dag-scheduler',
  'task-scheduler',
  'task-set-manager',
  'driver-scheduler-backend',
  'yarn-scheduler-backend',
  'task-result-getter',
  'executor-allocation-manager',
  'executor-monitor',
  'heartbeat-receiver',
  'health-tracker',
  'output-commit-coordinator',
  'block-manager-master',
  'map-output-tracker-master',
  'broadcast-manager',
  'context-cleaner',
  'listener-bus',
  'app-status-store',
  'event-log',
  'spark-ui',
  'metrics-system',
]);

const WORKER_NON_EXEC_JVM_COMPONENTS: ReadonlySet<ComponentId> = new Set<ComponentId>([
  'node-manager',
  'external-shuffle-service',
  'hdfs-datanode',
  'python-worker',
  'yarn-container',
]);

const LOCAL_PORT_RE =
  /(?:^|\s)(?:in-jvm|in-process|memory|local|local-dir|local-disk|os)(?:\s|$)/i;
const SAME_HOST_PORT_RE =
  /127\.0\.0\.1|\blocalhost\b|loopback|in-?jvm|\bjvm\b|os proc|unix socket|\blocal\b/i;
const SAME_HOST_PROTOCOL_RE =
  /127\.0\.0\.1|\blocalhost\b|loopback|in-?jvm|\bjvm\b|os proc|unix socket|\blocal\b|in-?memory|local[- ]disk|\bpipe\b/i;
const SHORT_CIRCUIT_RE = /short-circuit|BlockReaderLocal/i;

const IN_JVM_RE = /\b(?:in-?jvm|in-?process|in-?memory|jvm\s+main)\b/i;
const NON_IN_JVM_IPC_RE =
  /\b(?:processbuilder|fork\/exec|os\s+fork|os\s+exit|127\.0\.0\.1|localhost|loopback|unix\s+socket|local\s+socket|local[- ]disk|containerexecutor)\b/i;

/**
 * Resolves a stable physical-host + OS-process key for a RuntimeInstance without
 * comparing freeform prose `processId` strings verbatim.
 */
export function resolveProcessKey(inst: RuntimeInstance): string {
  const zone = inst.hostZone;
  const comp = inst.componentId;
  const attempt = inst.attemptNumber ?? 1;

  switch (zone) {
    case 'submit-host':
      if (comp === 'python-driver') {
        return 'submit-host:python-driver';
      }
      return 'submit-host:driver-jvm';
    case 'driver-jvm':
      if (comp === 'python-driver') {
        return `driver-jvm:python-driver-a${attempt}`;
      }
      if (comp === 'node-manager') {
        return 'driver-jvm:nm-jvm';
      }
      if (comp === 'yarn-container') {
        return 'driver-jvm:container-proc';
      }
      return `driver-jvm:am-jvm-a${attempt}`;
    case 'worker-node-a':
    case 'worker-node-b':
      // Hadoop AuxServices.java:200-211 hosts External Shuffle Service inside the NM JVM.
      if (comp === 'node-manager' || comp === 'external-shuffle-service') {
        return `${zone}:nm-jvm`;
      }
      if (comp === 'hdfs-datanode') {
        return `${zone}:datanode-jvm`;
      }
      if (comp === 'python-worker') {
        return `${zone}:python-worker-proc`;
      }
      if (comp === 'yarn-container') {
        return `${zone}:container-proc`;
      }
      return `${zone}:exec-jvm-${inst.executorNumber ?? 1}-a${attempt}`;
    case 'yarn-control':
      return 'yarn-control:rm-jvm';
    case 'storage-history':
      if (comp === 'hdfs-namenode') {
        return 'storage-history:nn-jvm';
      }
      if (comp === 'hdfs-datanode') {
        return 'storage-history:dn-jvm';
      }
      return 'storage-history:hs-jvm';
  }
}

/**
 * True when an event claims direct in-JVM / in-process / in-memory execution,
 * distinct from OS child fork/exec, 127.0.0.1 loopback TCP, Unix sockets, or local disk.
 */
export function isInJvmEvent(ev: Pick<RunEvent, 'port' | 'protocol'>): boolean {
  const combined = `${ev.port} ${ev.protocol}`;
  if (NON_IN_JVM_IPC_RE.test(combined)) {
    return false;
  }
  return IN_JVM_RE.test(ev.port) || IN_JVM_RE.test(ev.protocol);
}

/**
 * Validates both host-level locality (all local/loopback/OS/short-circuit/internal
 * events must share one hostZone) and process-level locality (all in-JVM events must
 * share one OS process key, except YARN container startup handoff to its hosted AM or
 * executor JVM). Returns null when valid, or a diagnostic violation message.
 */
export function validateEventLocality(
  ev: Pick<
    RunEvent,
    'id' | 'kind' | 'channel' | 'port' | 'protocol' | 'fromInstance' | 'toInstance' | 'command'
  >,
  instances: Record<string, RuntimeInstance>,
): string | null {
  const fromInst = instances[ev.fromInstance];
  const toInst = instances[ev.toInstance];
  if (!fromInst || !toInst) {
    return `Missing runtime instance for event ${ev.id}: from=${ev.fromInstance}, to=${ev.toInstance}`;
  }

  const requiresSameHost =
    ev.kind === 'local' ||
    LOCAL_PORT_RE.test(ev.port.trim()) ||
    SAME_HOST_PORT_RE.test(ev.port) ||
    SAME_HOST_PROTOCOL_RE.test(ev.protocol) ||
    SHORT_CIRCUIT_RE.test(`${ev.protocol} ${ev.command ?? ''}`) ||
    ev.channel === 'internal';

  if (requiresSameHost && fromInst.hostZone !== toInst.hostZone) {
    return `Cross-host local event forbidden (${ev.id}): ${ev.fromInstance} (${fromInst.hostZone}) -> ${ev.toInstance} (${toInst.hostZone}) via port "${ev.port}", protocol "${ev.protocol}"`;
  }

  if (isInJvmEvent(ev)) {
    if (
      fromInst.containerId &&
      toInst.containerId &&
      fromInst.containerId !== toInst.containerId
    ) {
      return `Cross-process in-JVM event forbidden (${ev.id}): ${ev.fromInstance} [container=${fromInst.containerId}] -> ${ev.toInstance} [container=${toInst.containerId}] via port "${ev.port}", protocol "${ev.protocol}"`;
    }
    const fromKey = resolveProcessKey(fromInst);
    const toKey = resolveProcessKey(toInst);
    if (fromKey !== toKey) {
      const sameAttempt = (fromInst.attemptNumber ?? 1) === (toInst.attemptNumber ?? 1);
      const isContainerStartupHandoff =
        fromInst.hostZone === toInst.hostZone &&
        sameAttempt &&
        fromInst.componentId === 'yarn-container' &&
        (toInst.componentId === 'application-master' ||
          toInst.componentId === 'executor-backend');
      const sameContainerIfBothKnown =
        !fromInst.containerId ||
        !toInst.containerId ||
        fromInst.containerId === toInst.containerId;
      const isWorkerExecHostSingletonMatch =
        fromInst.hostZone === toInst.hostZone &&
        sameAttempt &&
        sameContainerIfBothKnown &&
        (fromInst.hostZone === 'worker-node-a' || fromInst.hostZone === 'worker-node-b') &&
        !WORKER_NON_EXEC_JVM_COMPONENTS.has(fromInst.componentId) &&
        !WORKER_NON_EXEC_JVM_COMPONENTS.has(toInst.componentId) &&
        (fromInst.executorNumber === undefined || toInst.executorNumber === undefined);
      if (!isContainerStartupHandoff && !isWorkerExecHostSingletonMatch) {
        return `Cross-process in-JVM event forbidden (${ev.id}): ${ev.fromInstance} [${fromKey}] -> ${ev.toInstance} [${toKey}] via port "${ev.port}", protocol "${ev.protocol}"`;
      }
    }
  }

  return null;
}

export function isDriverRuntimeInstance(inst: RuntimeInstance | undefined): boolean {
  return Boolean(
    inst &&
      (inst.hostZone === 'driver-jvm' ||
        (inst.hostZone === 'submit-host' &&
          CLIENT_HOST_DRIVER_COMPONENTS.has(inst.componentId))),
  );
}

export function resolveDefaultHostZone(
  compId: ComponentId,
  isClientMode: boolean,
): HostZoneId {
  const spec = COMPONENTS[compId];
  if (isClientMode && CLIENT_HOST_DRIVER_COMPONENTS.has(compId)) {
    return 'submit-host';
  }
  return spec.hostZone;
}

export function resolveDefaultHostLabel(
  compId: ComponentId,
  zone: HostZoneId,
  isClientMode: boolean,
): string {
  if (isClientMode && zone === 'submit-host') {
    return 'edge-01 (client driver)';
  }
  switch (zone) {
    case 'submit-host':
      return 'edge-01';
    case 'yarn-control':
      return 'rm01';
    case 'storage-history':
      return COMPONENTS[compId]?.port && compId !== 'history-server'
        ? `hdfs ${COMPONENTS[compId].port}`
        : 'hdfs / history';
    case 'driver-jvm':
      return 'driver-1 (AM)';
    case 'worker-node-a':
      return 'worker-a';
    case 'worker-node-b':
      return 'worker-b';
  }
}
