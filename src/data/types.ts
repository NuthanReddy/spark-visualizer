export type SparkLanguage = 'pyspark' | 'scala' | 'java';

export interface SparkLanguageMeta {
  id: SparkLanguage;
  label: string;
  short: string;
  description: string;
}

export const SPARK_LANGUAGES: readonly SparkLanguageMeta[] = [
  {
    id: 'pyspark',
    label: 'PySpark',
    short: 'PySpark',
    description:
      'Python driver via deploy.PythonRunner and Py4J loopback gateway; Python UDFs execute over local sockets to python3 workers.',
  },
  {
    id: 'scala',
    label: 'Scala',
    short: 'Scala',
    description:
      'Native JVM --class entry from __app__.jar with typed Dataset[T], ExpressionEncoder, ScalaUDF, and zero Python worker IPC.',
  },
  {
    id: 'java',
    label: 'Java',
    short: 'Java',
    description:
      'Native JVM --class main(String[]) with JavaSparkContext, Encoders.bean(), Java UDF1..22, and zero Python worker IPC.',
  },
] as const;

export type ComponentId =
  // 1. Submission & Allocation (5)
  | 'spark-submit'
  | 'yarn-client'
  | 'application-master'
  | 'yarn-allocator'
  | 'security-manager'
  // 2. Cluster & HDFS Services (5)
  | 'resource-manager'
  | 'node-manager'
  | 'yarn-container'
  | 'hdfs-namenode'
  | 'hdfs-datanode'
  // 3. Driver Runtime & Language Entry (13)
  | 'spark-driver'
  | 'python-driver'
  | 'py4j-gateway'
  | 'jvm-user-app'
  | 'closure-cleaner'
  | 'scala-repl-compiler'
  | 'java-spark-api'
  | 'delegation-token-manager'
  | 'spark-session'
  | 'spark-context'
  | 'spark-env'
  | 'serializer-manager'
  | 'kryo-registrator'
  // 4. Scheduling (6)
  | 'dag-scheduler'
  | 'task-scheduler'
  | 'task-set-manager'
  | 'health-tracker'
  | 'driver-scheduler-backend'
  | 'yarn-scheduler-backend'
  // 5. Executor & RPC (8)
  | 'executor-backend'
  | 'executor'
  | 'user-classloader'
  | 'task-attempt'
  | 'task-result-getter'
  | 'rpc-env'
  | 'rpc-file-server'
  | 'transport-context'
  // 6. Capacity & Observability (11)
  | 'executor-allocation-manager'
  | 'executor-monitor'
  | 'resource-profile-manager'
  | 'heartbeat-receiver'
  | 'listener-bus'
  | 'app-status-store'
  | 'event-log'
  | 'spark-ui'
  | 'history-server'
  | 'metrics-system'
  | 'executor-metrics-poller'
  // 7. Block Storage & Memory (8)
  | 'block-manager'
  | 'block-manager-master'
  | 'block-transfer-service'
  | 'memory-manager'
  | 'rdd-cache'
  | 'broadcast-manager'
  | 'context-cleaner'
  | 'block-manager-decommissioner'
  // 8. Shuffle (7)
  | 'map-output-tracker-master'
  | 'map-output-tracker-worker'
  | 'shuffle-writer'
  | 'shuffle-reader'
  | 'shuffle-block-resolver'
  | 'external-shuffle-service'
  | 'push-based-shuffle'
  // 9. SQL, Catalyst, Codegen, Tungsten & Typed Datasets (19)
  | 'session-catalog'
  | 'catalyst-analyzer'
  | 'catalyst-optimizer'
  | 'sql-planner'
  | 'adaptive-query-execution'
  | 'shuffle-exchange'
  | 'broadcast-exchange'
  | 'columnar-cache'
  | 'broadcast-hash-join'
  | 'sort-merge-join'
  | 'hash-aggregate'
  | 'whole-stage-codegen'
  | 'janino-code-generator'
  | 'tungsten-unsafe-row'
  | 'dataset-encoder-udf'
  | 'typed-dataset-ops'
  | 'scala-java-udf'
  | 'typed-aggregator'
  | 'python-udf-operator'
  // 10. Python Workers, Connectors & Output Commits (7)
  | 'python-worker-factory'
  | 'python-worker'
  | 'python-runner'
  | 'file-format-writer'
  | 'file-commit-protocol'
  | 'output-commit-coordinator'
  | 'datasource-v2';

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
  languages?: readonly SparkLanguage[];
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
  languages?: readonly SparkLanguage[];
  languageVariantGroup?: string;
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

export const MASTER_CANVAS = { width: 2160, height: 1260 } as const;

export const HOST_ZONES: Record<HostZoneId, HostZoneMeta> = {
  'submit-host': {
    id: 'submit-host',
    label: 'SUBMIT / CLIENT HOST',
    subtitle: 'edge-01 \u00B7 CLI, REPL, Java API & staging',
    rect: { x: 24, y: 20, w: 740, h: 148 },
  },
  'yarn-control': {
    id: 'yarn-control',
    label: 'YARN CONTROL PLANE',
    subtitle: 'rm01 \u00B7 :8032 / :8030 / :8031',
    rect: { x: 788, y: 20, w: 316, h: 148 },
  },
  'storage-history': {
    id: 'storage-history',
    label: 'HDFS & EXTERNAL HISTORY',
    subtitle: 'nn01:8020 \u00B7 dn:9866 \u00B7 hs:18080',
    rect: { x: 1128, y: 20, w: 1008, h: 148 },
  },
  'driver-jvm': {
    id: 'driver-jvm',
    label: 'AM / SPARK DRIVER CONTAINER',
    subtitle: 'driver-1 \u00B7 Spark RPC :41000 \u00B7 BlockMgr :41001 \u00B7 UI :4040',
    rect: { x: 24, y: 192, w: 2112, h: 588 },
  },
  'worker-node-a': {
    id: 'worker-node-a',
    label: 'WORKER NODE A \u00B7 MAP, MEMORY, SERDE & SHUFFLE PRODUCER',
    subtitle: 'worker-a \u00B7 NM dynamic \u00B7 BlockMgr :42001 \u00B7 ESS :7337',
    rect: { x: 24, y: 804, w: 1044, h: 436 },
  },
  'worker-node-b': {
    id: 'worker-node-b',
    label: 'WORKER NODE B \u00B7 CODEGEN, TUNGSTEN, UDF, PYTHON & WRITE',
    subtitle: 'worker-b \u00B7 BlockMgr :42002 \u00B7 Janino Codegen / Python IPC',
    rect: { x: 1092, y: 804, w: 1044, h: 436 },
  },
};

export const DRIVER_SUB_ZONES: SubZoneMeta[] = [
  { id: 'driver-core', label: 'AM BRIDGE & DRIVER CORE', rect: { x: 40, y: 236, w: 440, h: 532 } },
  { id: 'driver-sched', label: 'SQL PLANNING & SCHEDULERS', rect: { x: 500, y: 236, w: 636, h: 532 } },
  { id: 'driver-masters', label: 'CAPACITY, LIVENESS & MASTERS', rect: { x: 1156, y: 236, w: 440, h: 532 } },
  { id: 'driver-obs', label: 'OBSERVABILITY & LIVE UI', rect: { x: 1616, y: 236, w: 504, h: 532 } },
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
