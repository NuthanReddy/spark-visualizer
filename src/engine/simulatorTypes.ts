import { SPARK_LANGUAGES } from '../data/types.ts';
import type { SparkLanguage } from '../data/types.ts';
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
  SPARK_LANGUAGES,
};
export type {
  FaultCategoryId,
  FaultCategoryMeta,
  FaultMeta,
  FaultRequirements,
  SimulatedFault,
  SparkLanguage,
};

export type SimulatedJobType =
  | 'narrow-filter'
  | 'wide-shuffle'
  | 'broadcast-join'
  | 'sort-merge-skew'
  | 'python-udf'
  | 'jvm-udf-dataset'
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
  language: SparkLanguage;
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
  languages: readonly SparkLanguage[];
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
  language: 'pyspark',
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
    languages: ['pyspark', 'scala', 'java'],
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
    languages: ['pyspark', 'scala', 'java'],
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
    languages: ['pyspark', 'scala', 'java'],
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
    languages: ['pyspark', 'scala', 'java'],
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
    languages: ['pyspark'],
    summary: 'Extracts scalar PythonUDF into EvalPythonExec; executors stream batches over local sockets to pyspark.daemon workers.',
    defaultDataProfile: 'uniform',
  },
  {
    id: 'jvm-udf-dataset',
    label: 'Typed Dataset[T] + JVM UDF (Scala / Java)',
    short: 'Typed Dataset + JVM UDF',
    actionSink: 'orders.as[Order].map(score).withColumn("n", norm_amt($"amount")).write.parquet(...)',
    hasShuffle: false,
    hasFileCommit: true,
    hasCollect: false,
    hasBroadcast: false,
    hasPythonUdf: false,
    hasSqlCodegen: true,
    languages: ['scala', 'java'],
    summary: 'Pure-JVM typed Dataset[Order].map (ExpressionEncoder / Encoders.bean) + scalar ScalaUDF/Java UDF1 executing directly on Executor JVM task threads with 0 Python workers.',
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
    languages: ['pyspark', 'scala', 'java'],
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
    languages: ['pyspark', 'scala', 'java'],
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
  {
    id: 'preset-scala-shuffle',
    label: 'Scala Cluster Shuffle (Zero Python)',
    badge: 'SCALA',
    config: { ...DEFAULT_SIMULATION_CONFIG, language: 'scala', jobType: 'wide-shuffle', fault: 'none' },
  },
  {
    id: 'preset-java-dataset',
    label: 'Java Typed Dataset + Encoders.bean',
    badge: 'JAVA JVM',
    config: { ...DEFAULT_SIMULATION_CONFIG, language: 'java', jobType: 'jvm-udf-dataset', fault: 'none' },
  },
  {
    id: 'preset-scala-cnf',
    label: 'Scala ClassNotFound (101 / 13 / 15)',
    badge: 'EXIT 13/101',
    config: { ...DEFAULT_SIMULATION_CONFIG, language: 'scala', jobType: 'wide-shuffle', fault: 'class-not-found-jar' },
  },
  {
    id: 'preset-kryo-overflow',
    label: 'Kryo Buffer Overflow (> 64 MiB)',
    badge: 'KRYO 64M',
    config: { ...DEFAULT_SIMULATION_CONFIG, language: 'scala', jobType: 'wide-shuffle', fault: 'kryo-buffer-overflow' },
  },
];

const VALID_LANGUAGES: ReadonlySet<string> = new Set(SPARK_LANGUAGES.map((l) => l.id));
const VALID_DATA_PROFILES: ReadonlySet<string> = new Set(
  SIMULATED_DATA_PROFILES.map((p) => p.id),
);
const VALID_PYTHON_UDF_MODES: ReadonlySet<string> = new Set(['pickled', 'arrow']);
const VALID_DEPLOY_MODES: ReadonlySet<string> = new Set(['cluster', 'client']);
const VALID_EXECUTOR_CORES: ReadonlySet<number> = new Set([1, 2, 4]);
const VALID_NUM_PARTITIONS: ReadonlySet<number> = new Set([4, 8, 12, 24]);

function hasOwn<T extends object>(obj: T, key: PropertyKey): boolean {
  return typeof key === 'string' && Object.prototype.hasOwnProperty.call(obj, key);
}

export function validateSimulationConfig(config: SimulationConfig): SimulationValidation {
  const blockers: SimulationValidationIssue[] = [];
  const notes: string[] = [];

  const hasValidLanguage = typeof config.language === 'string' && VALID_LANGUAGES.has(config.language);
  const hasValidJob = hasOwn(JOB_TYPE_MAP, config.jobType);
  const hasValidDataProfile =
    typeof config.dataProfile === 'string' && VALID_DATA_PROFILES.has(config.dataProfile);
  const hasValidPythonUdfMode =
    typeof config.pythonUdfMode === 'string' && VALID_PYTHON_UDF_MODES.has(config.pythonUdfMode);
  const hasValidFault = hasOwn(FAULT_MAP, config.fault);
  const hasValidDeployMode =
    typeof config.deployMode === 'string' && VALID_DEPLOY_MODES.has(config.deployMode);
  const hasValidCores =
    typeof config.executorCores === 'number' && VALID_EXECUTOR_CORES.has(config.executorCores);
  const hasValidPartitions =
    typeof config.numPartitions === 'number' && VALID_NUM_PARTITIONS.has(config.numPartitions);
  const hasValidExecutors =
    typeof config.numExecutors === 'number' &&
    Number.isInteger(config.numExecutors) &&
    config.numExecutors >= 1 &&
    config.numExecutors <= 6;
  const hasValidEssFlag = typeof config.externalShuffleService === 'boolean';
  const hasValidDaFlag = typeof config.dynamicAllocation === 'boolean';
  const hasValidAqeFlag = typeof config.aqeEnabled === 'boolean';

  if (!hasValidLanguage) {
    blockers.push({
      field: 'language',
      reason: `Unknown Spark language "${String(config.language)}"; expected one of pyspark, scala, or java.`,
      suggestedPatch: { language: DEFAULT_SIMULATION_CONFIG.language },
    });
  }
  if (!hasValidJob) {
    blockers.push({
      field: 'jobType',
      reason: `Unknown job type "${String(config.jobType)}"; choose a supported Spark job template.`,
      suggestedPatch: { jobType: DEFAULT_SIMULATION_CONFIG.jobType },
    });
  }
  if (!hasValidDataProfile) {
    blockers.push({
      field: 'dataProfile',
      reason: `Unknown data profile "${String(config.dataProfile)}"; choose a supported data profile.`,
      suggestedPatch: {
        dataProfile: hasValidJob
          ? JOB_TYPE_MAP[config.jobType].defaultDataProfile
          : DEFAULT_SIMULATION_CONFIG.dataProfile,
      },
    });
  }
  if (!hasValidPythonUdfMode) {
    blockers.push({
      field: 'pythonUdfMode',
      reason: `Invalid pythonUdfMode "${String(config.pythonUdfMode)}"; expected "pickled" or "arrow".`,
      suggestedPatch: { pythonUdfMode: DEFAULT_SIMULATION_CONFIG.pythonUdfMode },
    });
  }
  if (!hasValidFault) {
    blockers.push({
      field: 'fault',
      reason: `Unknown fault "${String(config.fault)}"; reset fault to none or pick a cataloged fault.`,
      suggestedPatch: { fault: DEFAULT_SIMULATION_CONFIG.fault },
    });
  }
  if (!hasValidDeployMode) {
    blockers.push({
      field: 'deployMode',
      reason: `Invalid deploy mode "${String(config.deployMode)}"; expected "cluster" or "client".`,
      suggestedPatch: { deployMode: DEFAULT_SIMULATION_CONFIG.deployMode },
    });
  }
  if (!hasValidExecutors) {
    const suggestedExecutors =
      typeof config.numExecutors === 'number' && Number.isFinite(config.numExecutors)
        ? Math.max(1, Math.min(6, Math.round(config.numExecutors)))
        : DEFAULT_SIMULATION_CONFIG.numExecutors;
    blockers.push({
      field: 'numExecutors',
      reason: `Invalid executor count "${String(config.numExecutors)}"; expected an integer in 1..6.`,
      suggestedPatch: { numExecutors: suggestedExecutors },
    });
  }
  if (!hasValidCores) {
    blockers.push({
      field: 'executorCores',
      reason: `Invalid executorCores "${String(config.executorCores)}"; expected 1, 2, or 4.`,
      suggestedPatch: { executorCores: DEFAULT_SIMULATION_CONFIG.executorCores },
    });
  }
  if (!hasValidPartitions) {
    blockers.push({
      field: 'numPartitions',
      reason: `Invalid numPartitions "${String(config.numPartitions)}"; expected 4, 8, 12, or 24.`,
      suggestedPatch: { numPartitions: DEFAULT_SIMULATION_CONFIG.numPartitions },
    });
  }
  if (!hasValidEssFlag) {
    blockers.push({
      field: 'externalShuffleService',
      reason: `Invalid externalShuffleService flag "${String(config.externalShuffleService)}"; expected boolean.`,
      suggestedPatch: {
        externalShuffleService:
          String(config.externalShuffleService).toLowerCase() === 'true'
            ? true
            : String(config.externalShuffleService).toLowerCase() === 'false'
              ? false
              : DEFAULT_SIMULATION_CONFIG.externalShuffleService,
      },
    });
  }
  if (!hasValidDaFlag) {
    blockers.push({
      field: 'dynamicAllocation',
      reason: `Invalid dynamicAllocation flag "${String(config.dynamicAllocation)}"; expected boolean.`,
      suggestedPatch: {
        dynamicAllocation:
          String(config.dynamicAllocation).toLowerCase() === 'true'
            ? true
            : String(config.dynamicAllocation).toLowerCase() === 'false'
              ? false
              : DEFAULT_SIMULATION_CONFIG.dynamicAllocation,
      },
    });
  }
  if (!hasValidAqeFlag) {
    blockers.push({
      field: 'aqeEnabled',
      reason: `Invalid aqeEnabled flag "${String(config.aqeEnabled)}"; expected boolean.`,
      suggestedPatch: {
        aqeEnabled:
          String(config.aqeEnabled).toLowerCase() === 'true'
            ? true
            : String(config.aqeEnabled).toLowerCase() === 'false'
              ? false
              : DEFAULT_SIMULATION_CONFIG.aqeEnabled,
      },
    });
  }

  const job = hasValidJob ? JOB_TYPE_MAP[config.jobType] : undefined;
  const fault = hasValidFault ? FAULT_MAP[config.fault] : undefined;
  const req = fault?.requires;

  if (fault && req) {
    if (hasValidLanguage && req.jvmApp && config.language === 'pyspark') {
      blockers.push({
        field: 'language',
        reason: `${fault.short} applies to Scala/Java JVM applications; switch language to Scala or Java.`,
        suggestedPatch:
          config.jobType === 'python-udf'
            ? { language: 'scala', jobType: 'wide-shuffle' }
            : { language: 'scala' },
      });
    }
    if (hasValidLanguage && req.pysparkOnly && config.language !== 'pyspark') {
      blockers.push({
        field: 'language',
        reason: `${fault.short} applies only to PySpark Python workers; switch language to PySpark with Python UDF.`,
        suggestedPatch: {
          language: 'pyspark',
          jobType: 'python-udf',
          dataProfile: 'uniform',
        },
      });
    }
    if (job && req.shuffle && !job.hasShuffle) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} has no ShuffleDependency; switch to Shuffle Aggregation or Skewed SMJ to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'wide-shuffle' },
      });
    }
    if (job && req.fileCommit && !job.hasFileCommit) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} does not run a file-output commit; switch to a Parquet write job to trigger ${fault.short}.`,
        suggestedPatch: {
          jobType: config.fault === 'speculation-race' ? 'narrow-filter' : 'wide-shuffle',
        },
      });
    }
    if (job && req.collect && !job.hasCollect) {
      blockers.push({
        field: 'fault',
        reason: `${fault.short} applies to driver result-gathering actions like collect(), not executor-to-HDFS writes.`,
        suggestedPatch: { jobType: 'collect-action', dataProfile: 'large-result' },
      });
    }
    if (job && req.broadcast && !job.hasBroadcast) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} has no BroadcastExchangeExec / TorrentBroadcast; switch to Broadcast Join to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'broadcast-join', dataProfile: 'small-dim' },
      });
    }
    if (job && req.pythonUdf && !job.hasPythonUdf) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} does not spawn pyspark.daemon / Python workers; switch to Python UDF to trigger ${fault.short}.`,
        suggestedPatch:
          config.language !== 'pyspark'
            ? { language: 'pyspark', jobType: 'python-udf', dataProfile: 'uniform' }
            : { jobType: 'python-udf', dataProfile: 'uniform' },
      });
    }
    if (job && req.sqlCodegen && !job.hasSqlCodegen) {
      blockers.push({
        field: 'fault',
        reason: `${job.short} uses the RDD API rather than Catalyst WholeStageCodegenExec; switch to a SQL DataFrame job to trigger ${fault.short}.`,
        suggestedPatch: { jobType: 'wide-shuffle', dataProfile: 'uniform' },
      });
    }
    if (hasValidDeployMode && req.clusterMode && config.deployMode !== 'cluster') {
      blockers.push({
        field: 'deployMode',
        reason: `${fault.short} applies to cluster mode where the driver runs inside the AM container.`,
        suggestedPatch: { deployMode: 'cluster' },
      });
    }
    if (hasValidDeployMode && req.clientMode && config.deployMode !== 'client') {
      blockers.push({
        field: 'deployMode',
        reason: `${fault.short} requires deployMode = client (external driver on edge-01 / yarn-client AM).`,
        suggestedPatch: { deployMode: 'client' },
      });
    }
    if (hasValidExecutors && req.minExecutors !== undefined && config.numExecutors < req.minExecutors) {
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

  if (job && hasValidLanguage && !job.languages.includes(config.language)) {
    if (config.jobType === 'python-udf' && config.language !== 'pyspark') {
      blockers.push({
        field: 'jobType',
        reason: `${job.short} requires PySpark; switch to Typed Dataset + JVM UDF for ${config.language === 'scala' ? 'Scala' : 'Java'}.`,
        suggestedPatch: { jobType: 'jvm-udf-dataset', dataProfile: 'uniform' },
      });
    } else if (config.jobType === 'jvm-udf-dataset' && config.language === 'pyspark') {
      blockers.push({
        field: 'jobType',
        reason: `${job.short} requires Scala or Java; switch to Python UDF for PySpark.`,
        suggestedPatch: { jobType: 'python-udf', dataProfile: 'uniform' },
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

export function adaptSimConfigForLanguage(
  cfg: SimulationConfig,
  nextLang: SparkLanguage,
): SimulationConfig {
  const safeExecutors = Number.isFinite(cfg.numExecutors)
    ? Math.max(1, Math.min(6, Math.round(cfg.numExecutors)))
    : DEFAULT_SIMULATION_CONFIG.numExecutors;

  let next: SimulationConfig = {
    ...cfg,
    language: nextLang,
    numExecutors: safeExecutors,
  };

  if (nextLang !== 'pyspark' && next.jobType === 'python-udf') {
    next = { ...next, jobType: 'jvm-udf-dataset', dataProfile: 'uniform' };
  } else if (nextLang === 'pyspark' && next.jobType === 'jvm-udf-dataset') {
    next = { ...next, jobType: 'python-udf', dataProfile: 'uniform' };
  }

  const faultReq = hasOwn(FAULT_MAP, next.fault) ? FAULT_MAP[next.fault].requires : undefined;
  if ((faultReq?.pysparkOnly || faultReq?.pythonUdf) && nextLang !== 'pyspark') {
    next = {
      ...next,
      fault: 'none',
    };
  } else if (faultReq?.jvmApp && nextLang === 'pyspark') {
    next = { ...next, fault: 'none' };
  }

  for (let i = 0; i < 12; i++) {
    const check = validateSimulationConfig(next);
    if (check.valid) break;
    const patch = check.blockers[0]?.suggestedPatch;
    if (!patch) break;
    if (patch.language !== undefined && patch.language !== nextLang) {
      next = { ...next, fault: 'none', language: nextLang };
      continue;
    }
    const patchedExecutors =
      patch.numExecutors !== undefined && Number.isFinite(patch.numExecutors)
        ? Math.max(1, Math.min(6, Math.round(patch.numExecutors)))
        : next.numExecutors;
    next = {
      ...next,
      ...patch,
      language: nextLang,
      numExecutors: patchedExecutors,
    };
  }

  return next;
}
