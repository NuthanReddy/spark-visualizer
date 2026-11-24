export type SimulatedJobType =
  | 'narrow-filter'
  | 'wide-shuffle'
  | 'broadcast-join'
  | 'sort-merge-skew'
  | 'python-udf'
  | 'cached-rdd'
  | 'collect-action';

export type SimulatedDataProfile =
  | 'uniform'
  | 'skewed'
  | 'small-dim'
  | 'large-result'
  | 'corrupt-record';

export type SimulatedFault =
  | 'none'
  | 'oversized-request'
  | 'am-sc-timeout'
  | 'oom-pmem'
  | 'yarn-preemption'
  | 'shuffle-fetch-failed'
  | 'task-exception'
  | 'heartbeat-timeout'
  | 'am-crash-retry'
  | 'client-disconnect'
  | 'commit-job-failure'
  | 'max-result-size'
  | 'speculation-race';

export type PythonUdfMode = 'pickled' | 'arrow';

export interface SimulationConfig {
  jobType: SimulatedJobType;
  dataProfile: SimulatedDataProfile;
  deployMode: 'cluster' | 'client';
  /** Fixed executor count when dynamicAllocation is false; initial executor target when true (1..6). */
  numExecutors: number;
  executorCores: 1 | 2 | 4;
  numPartitions: 4 | 8 | 12 | 24;
  externalShuffleService: boolean;
  dynamicAllocation: boolean;
  aqeEnabled: boolean;
  pythonUdfMode: PythonUdfMode;
  fault: SimulatedFault;
}

export interface JobTypeMeta {
  id: SimulatedJobType;
  label: string;
  short: string;
  actionSink: string;
  hasShuffle: boolean;
  hasFileCommit: boolean;
  hasCollect: boolean;
  summary: string;
  defaultDataProfile: SimulatedDataProfile;
}

export interface DataProfileMeta {
  id: SimulatedDataProfile;
  label: string;
  short: string;
  description: string;
}

export interface FaultMeta {
  id: SimulatedFault;
  label: string;
  short: string;
  severity: 'ok' | 'recoverable' | 'terminal';
  triggerSummary: string;
}

export interface SimulationValidationIssue {
  field: keyof SimulationConfig;
  reason: string;
  suggestedPatch?: Partial<SimulationConfig>;
}

export interface SimulationValidation {
  valid: boolean;
  blockers: SimulationValidationIssue[];
  notes: string[];
}

export interface SimulationPreset {
  id: string;
  label: string;
  badge: string;
  config: SimulationConfig;
}

export const DEFAULT_SIMULATION_CONFIG: SimulationConfig = {
  jobType: 'wide-shuffle',
  dataProfile: 'uniform',
  deployMode: 'cluster',
  numExecutors: 3,
  executorCores: 2,
  numPartitions: 12,
  externalShuffleService: false,
  dynamicAllocation: false,
  aqeEnabled: true,
  pythonUdfMode: 'pickled',
  fault: 'none',
};

export const SIMULATED_JOBS: JobTypeMeta[] = [
  {
    id: 'narrow-filter',
    label: 'Narrow Filter & Parquet Write (No Shuffle)',
    short: 'No Shuffle (Narrow)',
    actionSink: 'orders.where("status = \'PAID\'").write.parquet("hdfs:///out/paid")',
    hasShuffle: false,
    hasFileCommit: true,
    hasCollect: false,
    summary: 'Single ResultStage pipelining HDFS scan, predicate filter, and Parquet write with 0 shuffle exchanges.',
    defaultDataProfile: 'uniform',
  },
  {
    id: 'wide-shuffle',
    label: 'Shuffle Aggregation (groupBy + Parquet Write)',
    short: 'Shuffle Aggregation',
    actionSink: 'orders.where("status = \'PAID\'").groupBy("region").count().write.parquet(...)',
    hasShuffle: true,
    hasFileCommit: true,
    hasCollect: false,
    summary: 'Two stages separated by 1 ShuffleDependency: map writes .data/.index, reducers fetch remote shuffle slices.',
    defaultDataProfile: 'uniform',
  },
  {
    id: 'broadcast-join',
    label: 'Broadcast Hash Join (TorrentBroadcast)',
    short: 'Broadcast Join',
    actionSink: 'orders.join(broadcast(regions), "region_id").write.parquet(...)',
    hasShuffle: false,
    hasFileCommit: true,
    hasCollect: false,
    summary: 'Driver builds HashedRelation, TorrentBroadcast distributes 4 MiB pieces, streamed fact table probes with 0 large shuffle.',
    defaultDataProfile: 'small-dim',
  },
  {
    id: 'sort-merge-skew',
    label: 'Skewed Sort-Merge Join (AQE Skew Split)',
    short: 'Skewed SMJ + AQE',
    actionSink: 'orders.join(customers, "cust_id").write.parquet(...)',
    hasShuffle: true,
    hasFileCommit: true,
    hasCollect: false,
    summary: 'Two materialized shuffle stages; when AQE is ON and skew exceeds both 5x median & 256 MiB, OptimizeSkewedJoin splits into PartialReducerPartitionSpec.',
    defaultDataProfile: 'skewed',
  },
  {
    id: 'python-udf',
    label: 'PySpark UDF Pipeline (JVM <-> Python Worker)',
    short: 'Python UDF',
    actionSink: 'orders.withColumn("tier", score_udf("amount")).write.parquet(...)',
    hasShuffle: false,
    hasFileCommit: true,
    hasCollect: false,
    summary: 'Extracts scalar PythonUDF into EvalPythonExec; executors stream batches over local sockets to pyspark.daemon workers.',
    defaultDataProfile: 'uniform',
  },
  {
    id: 'cached-rdd',
    label: 'Persisted RDD Multi-Action (Hit vs Eviction)',
    short: 'Cached RDD (2 Actions)',
    actionSink: 'rdd.persist(MEMORY_ONLY); rdd.count(); rdd.count()',
    hasShuffle: false,
    hasFileCommit: false,
    hasCollect: false,
    summary: 'Action 1 computes and caches partitions in executor MemoryStore (MEMORY_ONLY); Action 2 reads cache hits or recomputes evicted partitions via RDD.getOrCompute.',
    defaultDataProfile: 'uniform',
  },
  {
    id: 'collect-action',
    label: 'Driver collect() Action (Result Transport)',
    short: 'Driver collect()',
    actionSink: 'orders.where("status = \'PAID\'").rdd.collect()',
    hasShuffle: false,
    hasFileCommit: false,
    hasCollect: true,
    summary: 'Gathers partition arrays back to driver via direct StatusUpdate or IndirectTaskResult (TaskResultBlock), guarded by spark.driver.maxResultSize.',
    defaultDataProfile: 'large-result',
  },
];

export const SIMULATED_DATA_PROFILES: DataProfileMeta[] = [
  {
    id: 'uniform',
    label: 'Uniform Partitions',
    short: 'Uniform',
    description: 'Balanced ~48 MiB partitions evenly distributed across executor slots.',
  },
  {
    id: 'skewed',
    label: 'Hot-Key Skew (640 MiB vs 48 MiB median)',
    short: 'Skewed (640 MiB)',
    description: 'Partition 0 holds 640 MiB (> 5x 48 MiB median AND > 256 MiB default AQE threshold).',
  },
  {
    id: 'small-dim',
    label: 'Small Dimension Table (10.8 MiB / 3 pieces)',
    short: 'Small Dim (10.8 MiB)',
    description: 'Broadcastable dimension table split into 3 serialized TorrentBroadcast pieces (4 + 4 + 2.8 MiB; 4 MiB blockSize).',
  },
  {
    id: 'large-result',
    label: 'Large Serialized Results (600 MiB total)',
    short: 'Large Result (600 MiB)',
    description: 'Serialized partition results (>= 600 MiB total) use IndirectTaskResult; aborts at 512 MiB maxResultSize or succeeds at 1 GiB.',
  },
  {
    id: 'corrupt-record',
    label: 'Malformed Row in Target Partition',
    short: 'Corrupt Row',
    description: 'Malformed row in target partition triggering repeatable task exceptions.',
  },
];

export const SIMULATED_FAULTS: FaultMeta[] = [
  {
    id: 'none',
    label: 'None (Happy Path)',
    short: 'None',
    severity: 'ok',
    triggerSummary: 'All stages, transfers, and cleanup succeed normally.',
  },
  {
    id: 'oversized-request',
    label: 'Pre-Submit Container Limit Exceeded',
    short: 'Oversized Request',
    severity: 'terminal',
    triggerSummary: 'Client.verifyClusterResources throws BEFORE submitApplication() after querying RM max capability.',
  },
  {
    id: 'am-sc-timeout',
    label: 'AM SparkContext Init Timeout (100s)',
    short: 'AM SC Timeout',
    severity: 'terminal',
    triggerSummary: 'Cluster-mode orders.py stalls before getOrCreate(); AM exits locally with EXIT_SC_NOT_INITED (13) without registering with RM.',
  },
  {
    id: 'oom-pmem',
    label: 'YARN Physical Memory Kill (PMEM / -104)',
    short: 'YARN PMEM (-104)',
    severity: 'recoverable',
    triggerSummary: 'NM kills executor for exceeding container RSS (KILLED_EXCEEDED_PMEM=-104; exitCausedByApp=true; counts toward task failures).',
  },
  {
    id: 'yarn-preemption',
    label: 'YARN Container Preemption (PREEMPTED)',
    short: 'YARN Preemption',
    severity: 'recoverable',
    triggerSummary: 'YARN reclaims a healthy executor (exitCausedByApp=false; retried without charging task failure budget).',
  },
  {
    id: 'shuffle-fetch-failed',
    label: 'Shuffle FetchFailed (Lost Producer / Block)',
    short: 'FetchFailed',
    severity: 'recoverable',
    triggerSummary: 'Tests ESS ON (producer exit survives via :7337) vs ESS OFF (FetchFailed -> invalidate -> rerun map -> retry stage).',
  },
  {
    id: 'task-exception',
    label: 'Deterministic Task Exception (maxFailures=4)',
    short: 'Task Exception',
    severity: 'terminal',
    triggerSummary: 'Same partition fails 4 counted attempts on TaskSetManager -> aborts TaskSet and fails job.',
  },
  {
    id: 'heartbeat-timeout',
    label: 'Executor Heartbeat Timeout (120s)',
    short: 'Heartbeat Timeout',
    severity: 'recoverable',
    triggerSummary: 'HeartbeatReceiver.expireDeadHosts triggers killAndReplaceExecutor + RemoveExecutor independently of YARN NM.',
  },
  {
    id: 'am-crash-retry',
    label: 'Cluster-Mode AM Crash -> Attempt 2 Restart',
    short: 'AM Crash Retry',
    severity: 'recoverable',
    triggerSummary: 'AM Attempt 1 dies; YARN spawns Attempt 2 with a fresh driver rerunning orders.py from line 1.',
  },
  {
    id: 'client-disconnect',
    label: 'Client-Mode External Driver Disconnect',
    short: 'Client Disconnect',
    severity: 'terminal',
    triggerSummary: 'External driver on edge-01 dies; AM onDisconnected runs (default SUCCEEDED vs clientModeTreatDisconnectAsFailed=true).',
  },
  {
    id: 'commit-job-failure',
    label: 'V1 commitJob Failure After All Tasks Succeed',
    short: 'commitJob Fail',
    severity: 'terminal',
    triggerSummary: 'All output tasks commit cleanly, then driver commitJob throws -> FileFormatWriter invokes best-effort abortJob.',
  },
  {
    id: 'max-result-size',
    label: 'collect() Exceeds spark.driver.maxResultSize',
    short: 'maxResultSize Abort',
    severity: 'terminal',
    triggerSummary: 'IndirectTaskResult transport succeeds individually, but cumulative size exceeds 512 MiB -> TaskSetManager aborts.',
  },
  {
    id: 'speculation-race',
    label: 'Speculative Copy + OutputCommitCoordinator Race',
    short: 'Speculation Race',
    severity: 'recoverable',
    triggerSummary: 'Speculative Attempt 1 wins canCommit; Attempt 0 gets TaskCommitDenied (not charged to task failures).',
  },
];

export const JOB_TYPE_MAP: Record<SimulatedJobType, JobTypeMeta> = Object.fromEntries(
  SIMULATED_JOBS.map(j => [j.id, j]),
) as Record<SimulatedJobType, JobTypeMeta>;

export const FAULT_MAP: Record<SimulatedFault, FaultMeta> = Object.fromEntries(
  SIMULATED_FAULTS.map(f => [f.id, f]),
) as Record<SimulatedFault, FaultMeta>;

export const SIMULATION_PRESETS: SimulationPreset[] = [
  {
    id: 'preset-narrow',
    label: 'No-Shuffle Narrow Write',
    badge: '0 SHUFFLES',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'narrow-filter', numExecutors: 3, numPartitions: 12 },
  },
  {
    id: 'preset-shuffle-ess',
    label: 'Shuffle + ESS (:7337)',
    badge: '4 EXECS',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 4, externalShuffleService: true },
  },
  {
    id: 'preset-broadcast',
    label: 'Broadcast + TorrentP2P',
    badge: 'TORRENT',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', dataProfile: 'small-dim', numExecutors: 3 },
  },
  {
    id: 'preset-aqe-skew',
    label: 'AQE Skew Split',
    badge: '640 MiB SKEW',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'sort-merge-skew', dataProfile: 'skewed', numExecutors: 4, aqeEnabled: true },
  },
  {
    id: 'preset-fetch-failed',
    label: 'Shuffle FetchFailed',
    badge: 'RECOVERY',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 3, externalShuffleService: false, fault: 'shuffle-fetch-failed' },
  },
  {
    id: 'preset-pmem-kill',
    label: 'YARN PMEM Kill (-104)',
    badge: 'FAULT',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'python-udf', numExecutors: 3, fault: 'oom-pmem' },
  },
  {
    id: 'preset-am-retry',
    label: 'AM Crash -> Attempt 2',
    badge: 'RESTART',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', deployMode: 'cluster', numExecutors: 2, fault: 'am-crash-retry' },
  },
  {
    id: 'preset-max-result',
    label: 'Oversized collect()',
    badge: 'LIMIT',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'collect-action', dataProfile: 'large-result', numExecutors: 3, fault: 'max-result-size' },
  },
];

export function validateSimulationConfig(config: SimulationConfig): SimulationValidation {
  const job = JOB_TYPE_MAP[config.jobType];
  const blockers: SimulationValidationIssue[] = [];
  const notes: string[] = [];

  if (config.fault === 'shuffle-fetch-failed' && !job.hasShuffle) {
    blockers.push({
      field: 'fault',
      reason: `${job.short} has no ShuffleDependency; switch to Shuffle Aggregation or Skewed SMJ to trigger FetchFailed.`,
      suggestedPatch: { jobType: 'wide-shuffle' },
    });
  }
  if (config.fault === 'shuffle-fetch-failed' && config.numExecutors < 2) {
    blockers.push({
      field: 'numExecutors',
      reason: 'Remote shuffle FetchFailed + surviving-host recomputation requires >= 2 executors.',
      suggestedPatch: { numExecutors: 2 },
    });
  }
  if (config.fault === 'commit-job-failure' && !job.hasFileCommit) {
    blockers.push({
      field: 'fault',
      reason: `${job.short} does not run a V1 file commit; switch to a Parquet write job to simulate commitJob failure.`,
      suggestedPatch: { jobType: 'wide-shuffle' },
    });
  }
  if (config.fault === 'speculation-race' && !job.hasFileCommit) {
    blockers.push({
      field: 'fault',
      reason: `OutputCommitCoordinator.canCommit applies to file-output commits; switch to a Parquet write job.`,
      suggestedPatch: { jobType: 'narrow-filter' },
    });
  }
  if (config.fault === 'speculation-race' && config.numExecutors < 2) {
    blockers.push({
      field: 'numExecutors',
      reason: 'Spark TaskSetManager excludes hosts that already ran the speculative task; requires >= 2 executors.',
      suggestedPatch: { numExecutors: 2 },
    });
  }
  if (config.fault === 'max-result-size' && !job.hasCollect) {
    blockers.push({
      field: 'fault',
      reason: `spark.driver.maxResultSize aborts result-gathering actions like collect(), not executor-to-HDFS writes.`,
      suggestedPatch: { jobType: 'collect-action', dataProfile: 'large-result' },
    });
  }
  if (
    (config.fault === 'am-sc-timeout' || config.fault === 'am-crash-retry') &&
    config.deployMode !== 'cluster'
  ) {
    blockers.push({
      field: 'deployMode',
      reason: `${FAULT_MAP[config.fault].short} applies to cluster mode where the driver runs inside the AM container.`,
      suggestedPatch: { deployMode: 'cluster' },
    });
  }
  if (config.fault === 'client-disconnect' && config.deployMode !== 'client') {
    blockers.push({
      field: 'deployMode',
      reason: `Client-mode driver disconnect requires deployMode = client (external driver on edge-01).`,
      suggestedPatch: { deployMode: 'client' },
    });
  }

  // Nuanced behavioral notes (valid combinations with important Spark semantics)
  if (config.jobType === 'broadcast-join' && config.numExecutors === 1) {
    notes.push(
      'With 1 executor, TorrentBroadcast fetches all 3 pieces directly from Driver BM (no executor-to-executor P2P hop).',
    );
  }
  if (config.fault === 'shuffle-fetch-failed' && config.externalShuffleService) {
    notes.push(
      'ESS (:7337) is ON: producer executor exit alone still serves intact shuffle files. This simulation demonstrates how ESS survives executor exit versus node/disk loss.',
    );
  }
  if (config.jobType === 'sort-merge-skew' && (!config.aqeEnabled || config.dataProfile !== 'skewed')) {
    notes.push(
      config.aqeEnabled
        ? 'Data profile is not skewed (>5x median AND >256 MiB), so AQE OptimizeSkewedJoin does not split partitions.'
        : config.dataProfile === 'skewed'
          ? 'AQE is OFF: skewed 640 MiB partition runs as a single straggler reduce task without splitting.'
          : 'AQE is OFF: uniform 48 MiB partitions run without runtime AQE re-optimization.',
    );
  }
  if (config.dynamicAllocation) {
    notes.push(
      `Dynamic allocation ON: ${config.numExecutors} is the initial executor target (ramps up on pending task backlog, scales down on idle timeout).`,
    );
  }

  return {
    valid: blockers.length === 0,
    blockers,
    notes,
  };
}
