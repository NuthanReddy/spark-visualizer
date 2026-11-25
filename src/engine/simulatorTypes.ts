import {
  FAULT_CATEGORIES,
  FAULT_CATEGORY_MAP,
  FAULT_MAP,
  FAULTS_BY_CATEGORY,
  SIMULATED_FAULTS,
} from './faultCatalog.ts';
import type {
  FaultCategoryId,
  FaultCategoryMeta,
  FaultMeta,
  FaultRequirements,
  SimulatedFault,
} from './faultCatalog.ts';

export {
  FAULT_CATEGORIES,
  FAULT_CATEGORY_MAP,
  FAULT_MAP,
  FAULTS_BY_CATEGORY,
  SIMULATED_FAULTS,
};
export type {
  FaultCategoryId,
  FaultCategoryMeta,
  FaultMeta,
  FaultRequirements,
  SimulatedFault,
};

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
  hasBroadcast: boolean;
  hasPythonUdf: boolean;
  hasSqlCodegen: boolean;
  summary: string;
  defaultDataProfile: SimulatedDataProfile;
}

export interface DataProfileMeta {
  id: SimulatedDataProfile;
  label: string;
  short: string;
  description: string;
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
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: true,
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
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: true,
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
    hasBroadcast: true,
    hasPythonUdf: false,
    hasSqlCodegen: true,
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
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: true,
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
    hasBroadcast: false,
    hasPythonUdf: true,
    hasSqlCodegen: true,
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
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: false,
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
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: true,
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

export const JOB_TYPE_MAP: Record<SimulatedJobType, JobTypeMeta> = Object.fromEntries(
  SIMULATED_JOBS.map((j) => [j.id, j]),
) as Record<SimulatedJobType, JobTypeMeta>;

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
  {
    id: 'preset-kernel-oom',
    label: 'Kernel Node OOM (SIGKILL 137)',
    badge: 'SIGKILL 137',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 3, fault: 'kernel-node-oom' },
  },
  {
    id: 'preset-jvm-heap-oom',
    label: 'JVM Heap OOM (Exit 52 != 137)',
    badge: 'EXIT 52',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 3, fault: 'jvm-heap-oom' },
  },
  {
    id: 'preset-sigterm-sigkill',
    label: 'SIGTERM -> SIGKILL Escalation',
    badge: 'TERM -> 137',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 3, fault: 'sigterm-escalated-sigkill' },
  },
  {
    id: 'preset-broadcast-8gb',
    label: 'Broadcast > 8 GiB Guard',
    badge: '8 GiB LIMIT',
    config: { ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', dataProfile: 'small-dim', numExecutors: 3, fault: 'broadcast-over-8gb' },
  },
];

export function validateSimulationConfig(config: SimulationConfig): SimulationValidation {
  const job = JOB_TYPE_MAP[config.jobType];
  const fault = FAULT_MAP[config.fault];
  const req = fault.requires;
  const blockers: SimulationValidationIssue[] = [];
  const notes: string[] = [];

  if (req) {
    if (req.shuffle && !job.hasShuffle) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} has no ShuffleDependency; switch to Shuffle Aggregation or Skewed SMJ to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'wide-shuffle' },
      });
    }
    if (req.fileCommit && !job.hasFileCommit) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} does not run a file-output commit; switch to a Parquet write job to trigger ${fault.short}.`,
        suggestedPatch: {
          jobType: config.fault === 'speculation-race' ? 'narrow-filter' : 'wide-shuffle',
        },
      });
    }
    if (req.collect && !job.hasCollect) {
      blockers.push({
        field: 'fault',
        reason: `${fault.short} applies to driver result-gathering actions like collect(), not executor-to-HDFS writes.`,
        suggestedPatch: { jobType: 'collect-action', dataProfile: 'large-result' },
      });
    }
    if (req.broadcast && !job.hasBroadcast) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} has no BroadcastExchangeExec / TorrentBroadcast; switch to Broadcast Join to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'broadcast-join', dataProfile: 'small-dim' },
      });
    }
    if (req.pythonUdf && !job.hasPythonUdf) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} does not spawn pyspark.daemon / Python workers; switch to Python UDF to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'python-udf', dataProfile: 'uniform' },
      });
    }
    if (req.sqlCodegen && !job.hasSqlCodegen) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} uses the RDD API rather than Catalyst WholeStageCodegenExec; switch to a SQL DataFrame job to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'wide-shuffle', dataProfile: 'uniform' },
      });
    }
    if (req.clusterMode && config.deployMode !== 'cluster') {
      blockers.push({
        field: 'deployMode',
        reason: `${fault.short} applies to cluster mode where the driver runs inside the AM container.`,
        suggestedPatch: { deployMode: 'cluster' },
      });
    }
    if (req.clientMode && config.deployMode !== 'client') {
      blockers.push({
        field: 'deployMode',
        reason: `${fault.short} requires deployMode = client (external driver on edge-01 / yarn-client AM).`,
        suggestedPatch: { deployMode: 'client' },
      });
    }
    if (req.minExecutors !== undefined && config.numExecutors < req.minExecutors) {
      blockers.push({
        field: 'numExecutors',
        reason: `${fault.short} requires >= ${req.minExecutors} executors for peer or surviving-host execution.`,
        suggestedPatch: { numExecutors: req.minExecutors },
      });
    }
    if (req.essEnabled === true && !config.externalShuffleService) {
      blockers.push({
        field: 'externalShuffleService',
        reason: `${fault.short} requires External Shuffle Service (:7337) to be enabled.`,
        suggestedPatch: { externalShuffleService: true },
      });
    }
    if (req.essEnabled === false && config.externalShuffleService) {
      blockers.push({
        field: 'externalShuffleService',
        reason: `${fault.short} requires External Shuffle Service (:7337) to be disabled (with shuffle tracking, decommission shuffle fallback, and reliable ShuffleDataIO storage also off) so ExecutorAllocationManager rejects startup.`,
        suggestedPatch: { externalShuffleService: false },
      });
    }
    if (req.dynamicAllocation === true && !config.dynamicAllocation) {
      blockers.push({
        field: 'dynamicAllocation',
        reason: `${fault.short} requires spark.dynamicAllocation.enabled = true.`,
        suggestedPatch: { dynamicAllocation: true },
      });
    }
    if (req.dynamicAllocation === false && config.dynamicAllocation) {
      blockers.push({
        field: 'dynamicAllocation',
        reason: `${fault.short} requires spark.dynamicAllocation.enabled = false.`,
        suggestedPatch: { dynamicAllocation: false },
      });
    }
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
  if (config.fault === 'exec-intentional-remove' && !config.dynamicAllocation) {
    notes.push(
      'Dynamic allocation is OFF: simulates explicit SparkContext.killExecutors -> YarnAllocator.internalReleaseContainer -> YARN ContainerExitStatus.ABORTED (-100), Spark exitCausedByApp=false, while surviving executor(s) finish the job.',
    );
  }
  if (config.fault === 'jvm-heap-oom') {
    notes.push(
      'Distinction: Java heap OutOfMemoryError exits via SparkUncaughtExceptionHandler calling System.exit(52) (or Runtime.halt(52) if the handler itself hits OOM), not SIGKILL 137 / YARN -104.',
    );
  }
  if (config.fault === 'node-crash-power-loss') {
    notes.push(
      'Distinction: Hard worker node crash leaves NO OS process exit code (not 137 or 52); RM NMLivelinessMonitor (:8031) later synthesizes ContainerExitStatus.ABORTED (-100, exitCausedByApp=false).',
    );
  }

  return {
    valid: blockers.length === 0,
    blockers,
    notes,
  };
}
