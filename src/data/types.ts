export type ComponentId =
  // 1. Submission & Allocation (4)
  | 'spark-submit'
  | 'yarn-client'
  | 'application-master'
  | 'yarn-allocator'
  // 2. Cluster & HDFS Services (5)
  | 'resource-manager'
  | 'node-manager'
  | 'yarn-container'
  | 'hdfs-namenode'
  | 'hdfs-datanode'
  // 3. Driver Runtime (6)
  | 'spark-driver'
  | 'python-driver'
  | 'py4j-gateway'
  | 'spark-session'
  | 'spark-context'
  | 'spark-env'
  // 4. Scheduling (5)
  | 'dag-scheduler'
  | 'task-scheduler'
  | 'task-set-manager'
  | 'driver-scheduler-backend'
  | 'yarn-scheduler-backend'
  // 5. Executor & RPC (5)
  | 'executor-backend'
  | 'executor'
  | 'task-attempt'
  | 'task-result-getter'
  | 'rpc-env'
  // 6. Capacity & Observability (9)
  | 'executor-allocation-manager'
  | 'executor-monitor'
  | 'heartbeat-receiver'
  | 'listener-bus'
  | 'event-log'
  | 'spark-ui'
  | 'history-server'
  | 'metrics-system'
  | 'executor-metrics-poller'
  // 7. Block Storage (5)
  | 'block-manager'
  | 'block-manager-master'
  | 'block-transfer-service'
  | 'rdd-cache'
  | 'broadcast-manager'
  // 8. Shuffle (6)
  | 'map-output-tracker-master'
  | 'map-output-tracker-worker'
  | 'shuffle-writer'
  | 'shuffle-reader'
  | 'shuffle-block-resolver'
  | 'external-shuffle-service'
  // 9. SQL Execution (5)
  | 'sql-planner'
  | 'adaptive-query-execution'
  | 'broadcast-exchange'
  | 'broadcast-hash-join'
  | 'python-udf-operator'
  // 10. Python Workers & Output Commits (6)
  | 'python-worker-factory'
  | 'python-worker'
  | 'python-runner'
  | 'file-format-writer'
  | 'file-commit-protocol'
  | 'output-commit-coordinator';

export type HostZoneId =
  | 'submit-host'
  | 'yarn-control'
  | 'storage-history'
  | 'driver-jvm'
  | 'worker-node-a'
  | 'worker-node-b';

export type SubsystemId =
  | 'submission'
  | 'cluster-hdfs'
  | 'driver-runtime'
  | 'scheduling'
  | 'executor-rpc'
  | 'observability'
  | 'block-storage'
  | 'shuffle'
  | 'sql-execution'
  | 'python-output';

export type ChannelKind = 'yarn' | 'rpc' | 'data' | 'python' | 'internal' | 'error';

export interface SourceLink {
  label: string;
  url: string;
}

export interface ComponentSpec {
  id: ComponentId;
  name: string;
  shortName: string;
  subsystem: SubsystemId;
  hostZone: HostZoneId;
  subZone?: string;
  clusterPos: [number, number];
  port?: string;
  process: string;
  role: string;
  keyDistinction: string;
  startedBy: string;
  monitoredBy: string;
  stoppedBy: string;
  inputs: string[];
  outputs: string[];
  internalState: string[];
  failureClues: string[];
  sourceLinks: SourceLink[];
  docFile: string;
}

export interface TraceField {
  name: string;
  value: string;
  note?: string;
}

export interface TraceMessage {
  id: string;
  step: number;
  from: ComponentId;
  to: ComponentId;
  fromInstance?: string;
  toInstance?: string;
  peerTag?: string;
  kind?: 'wire' | 'local';
  channel: ChannelKind;
  protocol: string;
  port: string;
  label: string;
  info: string;
  size: string;
  command?: string;
  payload?: string;
  fields: TraceField[];
  senderState?: string;
  receiverState?: string;
  isError?: boolean;
}

export interface TraceStep {
  title: string;
  explanation: string;
  keyState?: string;
  command?: string;
  detailsPassed?: TraceField[];
}

export type TraceKind = 'lifecycle' | 'scenario' | 'component';

export interface CustomInstanceSpec {
  id: string;
  componentId: ComponentId;
  label: string;
  shortLabel: string;
  hostZone: HostZoneId;
  hostLabel: string;
  processId: string;
  containerId?: string;
  executorNumber?: number;
  attemptNumber?: number;
}

export interface TraceLesson {
  id: string;
  kind: TraceKind;
  componentId?: ComponentId;
  scenarioNumber?: number;
  subsystem?: SubsystemId;
  name: string;
  subtitle: string;
  summary: string;
  deployMode: 'cluster' | 'client' | 'both';
  layerTag: string;
  focusNodes: ComponentId[];
  customInstances?: CustomInstanceSpec[];
  steps: TraceStep[];
  messages: TraceMessage[];
}

export interface HostZoneMeta {
  id: HostZoneId;
  label: string;
  subtitle: string;
  rect: { x: number; y: number; w: number; h: number };
}

export interface SubZoneMeta {
  id: string;
  label: string;
  rect: { x: number; y: number; w: number; h: number };
}

export const MASTER_CANVAS = { width: 1800, height: 1040 } as const;

export const HOST_ZONES: Record<HostZoneId, HostZoneMeta> = {
  'submit-host': {
    id: 'submit-host',
    label: 'SUBMIT / CLIENT HOST',
    subtitle: 'edge-01 · CLI & staging',
    rect: { x: 24, y: 20, w: 420, h: 148 },
  },
  'yarn-control': {
    id: 'yarn-control',
    label: 'YARN CONTROL PLANE',
    subtitle: 'rm01 · :8032 / :8030 / :8031',
    rect: { x: 468, y: 20, w: 380, h: 148 },
  },
  'storage-history': {
    id: 'storage-history',
    label: 'HDFS & EXTERNAL HISTORY',
    subtitle: 'nn01:8020 · dn:9866 · hs:18080',
    rect: { x: 872, y: 20, w: 904, h: 148 },
  },
  'driver-jvm': {
    id: 'driver-jvm',
    label: 'AM / SPARK DRIVER CONTAINER',
    subtitle: 'driver-1 · Spark RPC :41000 · BlockMgr :41001 · UI :4040',
    rect: { x: 24, y: 196, w: 1752, h: 440 },
  },
  'worker-node-a': {
    id: 'worker-node-a',
    label: 'WORKER NODE A · MAP / PRODUCER EXECUTOR',
    subtitle: 'worker-a · NM dynamic · BlockMgr :42001 · ESS :7337',
    rect: { x: 24, y: 664, w: 1032, h: 356 },
  },
  'worker-node-b': {
    id: 'worker-node-b',
    label: 'WORKER NODE B · REDUCE / PYTHON / WRITE EXECUTOR',
    subtitle: 'worker-b · BlockMgr :42002 · Local Python sockets',
    rect: { x: 1080, y: 664, w: 696, h: 356 },
  },
};

export const DRIVER_SUB_ZONES: SubZoneMeta[] = [
  { id: 'driver-core', label: 'AM BRIDGE & DRIVER CORE', rect: { x: 40, y: 246, w: 412, h: 374 } },
  { id: 'driver-sched', label: 'SQL PLANNING & SCHEDULERS', rect: { x: 476, y: 246, w: 412, h: 374 } },
  { id: 'driver-masters', label: 'CAPACITY, LIVENESS & MASTERS', rect: { x: 912, y: 246, w: 412, h: 374 } },
  { id: 'driver-obs', label: 'OBSERVABILITY & LIVE UI', rect: { x: 1348, y: 246, w: 412, h: 374 } },
];

export const SUBSYSTEM_META: Record<SubsystemId, { label: string; short: string }> = {
  submission: { label: 'Submission & Allocation', short: 'Submit / AM' },
  'cluster-hdfs': { label: 'YARN & HDFS Services', short: 'YARN / HDFS' },
  'driver-runtime': { label: 'Driver Runtime', short: 'Driver JVM' },
  scheduling: { label: 'DAG & Task Scheduling', short: 'Schedulers' },
  'executor-rpc': { label: 'Executor & Netty RPC', short: 'Executor / RPC' },
  observability: { label: 'Capacity & Observability', short: 'Alloc / UI' },
  'block-storage': { label: 'Block Storage & Broadcast', short: 'BlockMgr' },
  shuffle: { label: 'Shuffle Plane', short: 'Shuffle' },
  'sql-execution': { label: 'SQL Physical Execution', short: 'Spark SQL' },
  'python-output': { label: 'Python Workers & Commits', short: 'Python / Commit' },
};

export const CHANNEL_META: Record<
  ChannelKind,
  { label: string; short: string; cssVar: string; dash?: string }
> = {
  yarn: { label: 'YARN / Hadoop RPC', short: 'YARN RPC', cssVar: 'var(--ch-yarn)' },
  rpc: { label: 'Spark Netty RPC', short: 'Spark RPC', cssVar: 'var(--ch-rpc)' },
  data: { label: 'Block / Shuffle / HDFS Data', short: 'Data Plane', cssVar: 'var(--ch-data)' },
  python: { label: 'Py4J / Local Python Socket', short: 'Python IPC', cssVar: 'var(--ch-python)', dash: '6 4' },
  internal: { label: 'In-JVM Handoff', short: 'In-JVM', cssVar: 'var(--ch-internal)', dash: '3 4' },
  error: { label: 'Failure / Abort / Preempt', short: 'Failure', cssVar: 'var(--ch-error)', dash: '8 4' },
};
