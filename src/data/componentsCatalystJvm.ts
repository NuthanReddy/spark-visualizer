import type { ComponentSpec } from './types';

export type CatalystJvmComponentId =
  // 13 Shared Core / Catalyst / Codegen / Tungsten / SQL components
  | 'security-manager'
  | 'user-classloader'
  | 'resource-profile-manager'
  | 'catalyst-analyzer'
  | 'catalyst-optimizer'
  | 'shuffle-exchange'
  | 'columnar-cache'
  | 'janino-code-generator'
  | 'tungsten-unsafe-row'
  | 'hash-aggregate'
  | 'sort-merge-join'
  | 'window-sort-exec'
  | 'dynamic-partition-pruning'
  // 12 Scala & Java Native JVM components (NOT in PySpark)
  | 'closure-cleaner'
  | 'kryo-registrator'
  | 'typed-dataset-ops'
  | 'key-value-grouped-dataset'
  | 'scala-java-udf'
  | 'typed-aggregator'
  | 'spark-session-extensions'
  | 'jvm-partitioner-pair-rdd'
  | 'jvm-accumulator-v2'
  | 'scala-repl-compiler'
  | 'scala-reflection-implicits'
  | 'java-spark-api'
  // 4 PySpark-Specific Engine Components (NOT in Scala/Java)
  | 'cloudpickle-serializer'
  | 'python-accumulator-v2'
  | 'arrow-pandas-udf-engine'
  | 'python-udtf-exec';

const SPARK_SHA = 'dada2a6db5d73b152d6ba16e4fe21406f86abfe3';
const SPARK_BLOB = `https://github.com/apache/spark/blob/${SPARK_SHA}`;
const ALL_LANGS = ['pyspark', 'scala', 'java'] as const;

export const CATALYST_JVM_COMPONENTS: Record<CatalystJvmComponentId, ComponentSpec> = {
  // =========================================================================
  // 11 Shared Core, Catalyst, Codegen & Tungsten Components
  // =========================================================================
  'security-manager': {
    id: 'security-manager',
    name: 'SecurityManager (SASL, ACLs & AES)',
    shortName: 'SecurityManager',
    subsystem: 'submission',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1476, 542],
    languages: ALL_LANGS,
    port: 'SASL / AES',
    process: 'Per-JVM org.apache.spark.SecurityManager (SASL SecretKeyHolder, UI ACLs, I/O encryption)',
    role: 'Enforces Spark RPC SASL shared-secret authentication, generates per-app AES-CTR ioEncryptionKey for CryptoStreamUtils shuffle/spill/cache encryption, and gates Spark UI / REST API access via viewAcls, modifyAcls, and adminAcls.',
    keyDistinction:
      'SecurityManager governs Spark internal RPC auth, disk/network encryption, and Spark UI ACLs, whereas HadoopDelegationTokenManager renews Kerberos HDFS/Hive/HBase delegation tokens.',
    startedBy: 'Instantiated inside driver and executor SparkEnv.create() before NettyRpcEnv and TransportContext bind',
    monitoredBy: 'SASL handshake logs on TransportContext and HTTP 403 UI ACL rejection logs',
    stoppedBy: 'Exits with SparkEnv shutdown; keys reside only in JVM memory',
    inputs: [
      'spark.authenticate, spark.authenticate.secret, spark.io.encryption.enabled, and UI ACL configs',
      'Inbound SASL authentication challenges and SparkUI Jetty HTTP user identity checks',
    ],
    outputs: [
      'SecretKeyHolder credentials for Netty RPC / BlockTransfer / ESS SASL handshakes',
      'AES-CTR ioEncryptionKey passed to SerializerManager -> CryptoStreamUtils',
    ],
    internalState: [
      'authOn, aclsOn, viewAcls / modifyAcls / adminAcls user & group sets',
      'Optional 128/256-bit ioEncryptionKey: Option[Array[Byte]]',
      'SecretKeyHolder per-app SASL secret lookup',
    ],
    failureClues: [
      'SASL authentication failure between executor and driver/ESS indicates mismatched spark.authenticate or missing shared secret',
      'HTTP 403 Forbidden on SparkUI :4040 indicates SecurityManager viewAcls / adminAcls denied the user',
      'Enabling spark.io.encryption.enabled adds AES-CTR encryption to local shuffle spills and cached disk blocks',
    ],
    sourceLinks: [
      {
        label: 'SecurityManager SASL auth, ACLs & encryption',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/SecurityManager.scala#L42-L120`,
      },
      {
        label: 'CryptoStreamUtils AES-CTR I/O stream encryption',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/security/CryptoStreamUtils.scala#L1-L70`,
      },
    ],
    docFile: 'security-manager.md',
  },

  'user-classloader': {
    id: 'user-classloader',
    name: 'User & Executor ClassLoader (JARs)',
    shortName: 'User ClassLoader',
    subsystem: 'executor-rpc',
    hostZone: 'worker-node-a',
    clusterPos: [156, 1196],
    languages: ALL_LANGS,
    port: 'in-JVM loader',
    process: 'AM/Driver MutableURLClassLoader or ChildFirstURLClassLoader + Executor URLClassLoader / ExecutorClassLoader',
    role: 'Loads user Scala/Java JARs (__app__.jar, --jars, sc.addJar) and connector JARs (e.g. JDBC/Iceberg/Delta in any language) in isolated class loaders, honors spark.{driver,executor}.userClassPathFirst precedence, and dynamically downloads new JARs via Executor.updateDependencies.',
    keyDistinction:
      'Parent-first MutableURLClassLoader resolves Spark system classes first, whereas ChildFirstURLClassLoader (userClassPathFirst=true) prioritizes user JAR classes to avoid Guava/Jackson/Netty shading collisions.',
    startedBy: 'SparkSubmit / ApplicationMaster creates driver userClassLoader; Executor builds urlClassLoader & replClassLoader on startup',
    monitoredBy: 'Executor.updateDependencies fetch logs and JVM ClassNotFoundException / NoSuchMethodError traces',
    stoppedBy: 'Closed when driver or executor JVM exits',
    inputs: [
      'Localized __app__.jar and --jars URLs in YARN container working directory',
      'TaskDescription.addedJars version timestamps fetched from driver RpcEnvFileServer',
    ],
    outputs: [
      'Loaded user main classes, Scala/Java UDFs, domain POJOs, and connector classes',
      'Thread context ClassLoader installed on Driver and every Executor TaskRunner thread',
    ],
    internalState: [
      'MutableURLClassLoader (default) or ChildFirstURLClassLoader',
      'Executor currentJars map guarded by dependenciesLock',
      'Optional ExecutorClassLoader for dynamic Scala REPL bytecode over Spark RPC',
    ],
    failureClues: [
      'ClassNotFoundException on executors while driver succeeds means a JAR on edge-01 classpath was omitted from --jars / fat-JAR',
      'NoSuchMethodError on Guava/Jackson points to Spark dependency conflict (shade JAR or set userClassPathFirst=true)',
      'Missing static main(String[]) on --class fails cluster AM immediately with EXIT_EXCEPTION_USER_CLASS (15)',
    ],
    sourceLinks: [
      {
        label: 'MutableURLClassLoader & ChildFirstURLClassLoader',
        url: `${SPARK_BLOB}/core/src/main/java/org/apache/spark/util/MutableURLClassLoader.java#L24-L41`,
      },
      {
        label: 'ExecutorClassLoader RPC & filesystem class fetch',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L42-L95`,
      },
    ],
    docFile: 'user-classloader.md',
  },

  'resource-profile-manager': {
    id: 'resource-profile-manager',
    name: 'ResourceProfileManager',
    shortName: 'ResourceProfileMgr',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1276, 414],
    languages: ALL_LANGS,
    port: 'in-JVM / YARN prio',
    process: 'Driver JVM ResourceProfileManager owned by SparkContext',
    role: 'Registers the default ResourceProfile (rpId=0) and stage-level custom ResourceProfiles (heterogeneous executor memory, overhead, off-heap, PySpark memory, cores, GPUs, and per-task resources), emits SparkListenerResourceProfileAdded, and maps ResourceProfile IDs to YARN ContainerRequest priorities.',
    keyDistinction:
      'Stage-level ResourceProfiles allow a single Spark application to schedule CPU-heavy and GPU/high-memory stages on differently sized YARN containers under separate ResourceProfile IDs.',
    startedBy: 'SparkContext creates ResourceProfileManager with defaultProfile (rpId=0) at startup',
    monitoredBy: 'SparkListenerResourceProfileAdded events on LiveListenerBus and Spark UI Resource Profile columns',
    stoppedBy: 'Append-only registry cleared when SparkContext stops',
    inputs: [
      'Default SparkConf resource keys and rdd.withResources(ResourceProfile) builder calls',
      'YarnAllocator and TaskSchedulerImpl queries mapping rpId -> container & task resource requirements',
    ],
    outputs: [
      'Monotonic ResourceProfile IDs (0 = default, 1+ = custom stage profiles)',
      'Per-profile executor targets mapped to distinct YARN ContainerRequest priorities in YarnAllocator',
    ],
    internalState: [
      'resourceProfileIdToResourceProfile HashMap guarded by ReentrantReadWriteLock',
      'defaultProfile built from spark.executor.{memory,cores,memoryOverhead,offHeap,pyspark.memory}',
      'Cluster-manager dynamic allocation compatibility validation',
    ],
    failureClues: [
      'Custom executor ResourceProfiles on YARN require spark.dynamicAllocation.enabled=true',
      'Requesting more task GPUs/cores than the executor profile provides rejects stage submission',
      'PySpark profiles add spark.executor.pyspark.memory to YARN container capability (0 MiB in Scala/Java)',
    ],
    sourceLinks: [
      {
        label: 'ResourceProfileManager registry & YARN gates',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/resource/ResourceProfileManager.scala#L39-L115`,
      },
      {
        label: 'YarnAllocator per-ResourceProfile target maps',
        url: `${SPARK_BLOB}/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala#L94-L148`,
      },
    ],
    docFile: 'resource-profile-manager.md',
  },

  'catalyst-analyzer': {
    id: 'catalyst-analyzer',
    name: 'Catalyst Parser & Analyzer',
    shortName: 'Catalyst Analyzer',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [618, 350],
    languages: ALL_LANGS,
    port: 'in-JVM AST',
    process: 'Driver JVM SessionState.sqlParser (SparkSqlParser) + SessionState.analyzer (Analyzer)',
    role: 'Parses SQL text and DataFrame/Dataset expressions into an UnresolvedLogicalPlan, resolves table/view/column/function identifiers against SessionCatalog, runs ANSI/default type coercion, expands CTEs and subqueries, and enforces CheckAnalysis.',
    keyDistinction:
      'Analysis fails fast on the driver before optimization, planning, or any YARN executor task runs — unresolved column, table, or type errors never reach workers.',
    startedBy: 'QueryExecution.analyzed evaluated lazily on DataFrame/Dataset creation, explain(), or actions',
    monitoredBy: 'df.explain("extended") Parsed vs Analyzed Logical Plan and QueryPlanningTracker timings',
    stoppedBy: 'Per-query fixed-point rule batch completes once plan.resolved is true and CheckAnalysis passes',
    inputs: [
      'SQL strings parsed by ANTLR SparkSqlParser or DataFrame/Dataset LogicalPlan trees',
      'Catalog relations, schemas, temp views, and FunctionRegistry entries from SessionCatalog',
    ],
    outputs: [
      'Fully typed, ExprId-bound Resolved LogicalPlan passed to Catalyst Optimizer',
      'AnalysisException (UNRESOLVED_COLUMN, TABLE_OR_VIEW_NOT_FOUND, DATATYPE_MISMATCH)',
    ],
    internalState: [
      'Ordered Catalyst RuleExecutor batches (Substitution, Resolution, TypeCoercion, View)',
      'Max-iteration guard (spark.sql.analyzer.maxIterations, default 100)',
      'CheckAnalysis verifier asserting zero unresolved attributes',
    ],
    failureClues: [
      'AnalysisException fails strictly on the driver with 0 stages or tasks launched',
      'Ambiguous reference after self-joins points to duplicate AttributeReference exprIds prior to aliasing',
      'Slow analysis before Stage 0 indicates expensive catalog/Metastore view expansion or deeply nested CTEs',
    ],
    sourceLinks: [
      {
        label: 'Catalyst Analyzer rule batches',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala#L220-L330`,
      },
      {
        label: 'SparkSqlParser ANTLR AST builder',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala#L1-L70`,
      },
    ],
    docFile: 'catalyst-analyzer.md',
  },

  'catalyst-optimizer': {
    id: 'catalyst-optimizer',
    name: 'Catalyst Optimizer (RBO & CBO)',
    shortName: 'Catalyst Optimizer',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [618, 414],
    languages: ALL_LANGS,
    port: 'in-JVM rules',
    process: 'Driver JVM SessionState.optimizer (SparkOptimizer extending Catalyst Optimizer)',
    role: 'Rewrites the analyzed LogicalPlan through fixed-point Rule-Based (predicate pushdown, column pruning, constant folding, filter combining, subquery decorrelation, runtime filter injection) and Cost-Based (CostBasedJoinReorder) batches to minimize scanned and shuffled bytes.',
    keyDistinction:
      'Catalyst Optimizer transforms LogicalPlan -> LogicalPlan on the driver before SparkPlanner picks physical operators; AQE later re-runs a subset of rules using real shuffle stats.',
    startedBy: 'QueryExecution.optimizedPlan executed on driver after Analyzer and CacheManager substitution',
    monitoredBy: 'df.explain("extended" | "cost") Optimized Logical Plan and QueryPlanningTracker metrics',
    stoppedBy: 'Fixed-point convergence or spark.sql.optimizer.maxIterations ceiling',
    inputs: [
      'Analyzed LogicalPlan (with optional CachedData replacements from CacheManager)',
      'Table/column histograms and row-count stats when spark.sql.cbo.enabled=true',
    ],
    outputs: [
      'Optimized LogicalPlan with pushed predicates, pruned projections, folded constants, and reordered joins',
      'Injected DynamicPruningSubquery / BloomFilter runtime filter nodes when eligible',
    ],
    internalState: [
      'RuleExecutor batches (Finish Analysis, Union, Operator Optimization, CostBasedJoinReorder)',
      'spark.sql.optimizer.excludedRules and structural plan-change check',
      'Pushdown barriers around opaque UDFs (PythonUDF, ScalaUDF, Typed MapElements)',
    ],
    failureClues: [
      'Opaque typed Dataset.filter(o => ...) or Python/Scala UDFs block filter pushdown into Parquet/ORC scans',
      'Long driver planning pauses before Stage 0 can indicate large CASE/OR trees hitting rule iterations',
      'Stale ANALYZE TABLE statistics under spark.sql.cbo.enabled=true can misorder multi-way joins',
    ],
    sourceLinks: [
      {
        label: 'Catalyst Optimizer rule batches',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L100-L220`,
      },
      {
        label: 'SparkOptimizer SQL-core extensions',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala#L30-L115`,
      },
    ],
    docFile: 'catalyst-optimizer.md',
  },

  'shuffle-exchange': {
    id: 'shuffle-exchange',
    name: 'ShuffleExchangeExec (SQL Exchange)',
    shortName: 'ShuffleExchange',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [818, 414],
    languages: ALL_LANGS,
    port: 'SQL -> ShuffleDep',
    process: 'Driver JVM ShuffleExchangeExec inserted by EnsureRequirements',
    role: 'Bridges Spark SQL physical plans into Spark Core shuffles: satisfies required child distributions (HashPartitioning, RangePartitioning, RoundRobinPartitioning), serializes UnsafeRows via UnsafeRowSerializer, constructs ShuffleDependency, and exposes ShuffledRowRDD and MapOutputStatistics to AQE.',
    keyDistinction:
      'ShuffleExchangeExec is the SQL physical node that constructs ShuffleDependency on the driver; core DAGScheduler splits stages at that dependency and SortShuffleWriter / ShuffleReader move bytes on workers.',
    startedBy: 'EnsureRequirements inserts ShuffleExchangeExec wherever child.outputPartitioning misses requiredChildDistribution',
    monitoredBy: 'SQL UI Exchange node (shuffle records/bytes written, partition count) and AQE ShuffleQueryStageExec',
    stoppedBy: 'ShuffleDependency completes when map stage finishes; ReuseExchangeAndSubquery caches duplicate exchanges',
    inputs: [
      'Child SparkPlan RDD[InternalRow], target Partitioning (e.g. HashPartitioning(keys, 200)), and ShuffleOrigin',
      'UnsafeRowSerializer configured with schema field count and optional checksums',
    ],
    outputs: [
      'ShuffleDependency[Int, InternalRow, InternalRow] registered with DAGScheduler & MapOutputTrackerMaster',
      'ShuffledRowRDD reading partition slices (or AQE coalesced/skew-split PartitionSpecs)',
    ],
    internalState: [
      'Cached inputRDD and shuffleDependency enabling Exchange reuse',
      'SQLShuffleWriteMetricsReporter & SQLShuffleReadMetricsReporter accumulators',
      'Murmur3Hash partitioner computing partition IDs on map side',
    ],
    failureClues: [
      'Unexpected Exchange nodes in df.explain() show where distribution mismatches forced an extra shuffle stage',
      'Defaults to spark.sql.shuffle.partitions=200 unless AQE coalesces or user overrides',
      'Bucketed joins avoid ShuffleExchangeExec only when bucket counts, bucket keys, and sort order match',
    ],
    sourceLinks: [
      {
        label: 'ShuffleExchangeExec dependency & ShuffledRowRDD',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L65-L180`,
      },
      {
        label: 'EnsureRequirements exchange insertion',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L179-L235`,
      },
    ],
    docFile: 'shuffle-exchange.md',
  },

  'columnar-cache': {
    id: 'columnar-cache',
    name: 'SQL CacheManager & InMemoryRelation',
    shortName: 'SQL Columnar Cache',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [618, 478],
    languages: ALL_LANGS,
    port: 'in-JVM / Storage',
    process: 'Driver SharedState.cacheManager + Executor InMemoryTableScanExec / ColumnBuilder',
    role: 'Manages df.cache()/persist() and CACHE TABLE across sibling SparkSessions: encodes rows into compressed columnar CachedBatch blocks (RLE, dictionary, delta encoding) in BlockManager and rewrites matching sub-plans to InMemoryTableScanExec with per-batch min/max stats pruning.',
    keyDistinction:
      'SQL df.cache() stores compressed columnar batches via CacheManager/InMemoryRelation (default MEMORY_AND_DISK) with column pruning and stats skipping, unlike raw RDD.cache() (row/object storage in MEMORY_ONLY).',
    startedBy: 'df.cache() / spark.catalog.cacheTable() registers CachedData in SharedState.cacheManager',
    monitoredBy: 'Spark UI Storage tab (In-memory table entries) and InMemoryTableScanExec in df.explain()',
    stoppedBy: 'df.unpersist() / uncacheTable() / clearCache() evicts CachedData and calls BlockManagerMaster.removeRdd',
    inputs: [
      'Dataset/DataFrame LogicalPlan, target StorageLevel (default MEMORY_AND_DISK), and optional table name',
      'Incoming query plans checked for canonical sub-plan equivalence',
    ],
    outputs: [
      'InMemoryRelation / InMemoryTableScanExec substituting cached sub-trees during Optimizer lookup',
      'Compressed columnar CachedBatch RDD blocks persisted in executor BlockManagers',
    ],
    internalState: [
      'SharedState.cacheManager cachedData indexed list guarded by read/write lock',
      'InMemoryRelation CachedRDDBuilder + DefaultCachedBatchSerializer',
      'Per-batch column min/max/nullCount PartitionStatistics for predicate skipping',
    ],
    failureClues: [
      'df.cache() is lazy; storage stays 0 B until an action materializes the DataFrame',
      'Non-matching canonical plan prevents CacheManager sub-plan substitution',
      'Tune spark.sql.inMemoryColumnarStorage.batchSize (default 10000) for very wide tables',
    ],
    sourceLinks: [
      {
        label: 'CacheManager lookup & cacheQuery',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala#L60-L155`,
      },
      {
        label: 'InMemoryRelation columnar CachedRDDBuilder',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala#L110-L195`,
      },
    ],
    docFile: 'columnar-cache.md',
  },

  'janino-code-generator': {
    id: 'janino-code-generator',
    name: 'Janino CodeGenerator & CodeCompiler',
    shortName: 'Janino CodeGen',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1200, 950],
    languages: ALL_LANGS,
    port: 'in-JVM Janino',
    process: 'Executor JVM CodeGenerator + CodeCompiler (Janino SimpleCompiler + Guava compile cache)',
    role: 'Compiles generated Java source for WholeStageCodegenExec pipelines, UnsafeProjection, MutableProjection, Predicates, and Sort Orderings into JVM bytecode via Janino on executors, caching compiled classes by source SHA-256 hash and enforcing method-size/constant-pool limits.',
    keyDistinction:
      'WholeStageCodegenExec fuses operators into Java source on the driver, while Janino CodeGenerator/CodeCompiler compiles and caches the bytecode on executors and also compiles expression projections even when whole-stage fusion is off.',
    startedBy: 'Called lazily on executors by WholeStageCodegenExec.doExecute() and expression generators (GenerateUnsafeProjection, GenerateOrdering)',
    monitoredBy: 'SQL UI Codegen compilation time metrics, CodeGenerator.cache hit rate, and Janino fallback logs',
    stoppedBy: 'Generated classes stay resident in Guava compile cache (spark.sql.codegen.cache.maxEntries) until evicted or executor exit',
    inputs: [
      'CodeAndComment (generated Java source string + operator comments) and CodegenContext expressions',
      'spark.sql.codegen.maxFields, hugeMethodLimit (default 65535 B), and splitExpressionsThreshold',
    ],
    outputs: [
      'Compiled GeneratedClass (BufferedRowIterator, UnsafeProjection, BaseOrdering, Predicate) loaded into executor JVM',
      'Fallback trigger when CompileException or maxMethodCodeSize limit hits',
    ],
    internalState: [
      'CodeGenerator.cache: LoadingCache[CodeAndComment, (GeneratedClass, Int)] keyed by source hash',
      'Janino SimpleCompiler bound to Executor ClassLoader with split-method helper generation',
      'Bytecode length verifier enforcing spark.sql.codegen.hugeMethodLimit',
    ],
    failureClues: [
      'Higher compile time on Task 0 reflects cold Janino compilation before Guava cache warms',
      'Janino CompileException falls back cleanly only when spark.sql.codegen.fallback=true',
      'High Metaspace churn across thousands of dynamically varying literal queries points to CodeGenerator cache pressure',
    ],
    sourceLinks: [
      {
        label: 'CodeGenerator & Guava Janino cache',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala#L1370-L1475`,
      },
      {
        label: 'CodeCompiler Janino compilation & bytecode checks',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeCompiler.scala#L1-L95`,
      },
    ],
    docFile: 'janino-code-generator.md',
  },

  'tungsten-unsafe-row': {
    id: 'tungsten-unsafe-row',
    name: 'Tungsten UnsafeRow & UnsafeSorter',
    shortName: 'Tungsten UnsafeRow',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1406, 950],
    languages: ALL_LANGS,
    port: 'in-JVM binary',
    process: 'Executor JVM Tungsten binary engine (UnsafeRow, UnsafeExternalSorter, BytesToBytesMap, Platform)',
    role: 'Encodes SQL rows as compact 8-byte-aligned binary byte arrays (null bitset + fixed-width 8-byte words + variable-length tail) in TaskMemoryManager pages, enabling pointer-based binary sorting, prefix comparisons, raw byte hashing, and spillable maps with near-zero Java GC overhead.',
    keyDistinction:
      'UnsafeRow avoids allocating Java objects per column/row (eliminating GC pauses and 3-5x object header bloat); typed Scala/Java Dataset.map lambdas temporarily materialize JVM objects via ExpressionEncoder.',
    startedBy: 'Allocated per task inside WholeStageCodegenExec projections, HashAggregateExec maps, SortExec, and UnsafeShuffleWriter',
    monitoredBy: 'TaskMetrics peakExecutionMemory, memoryBytesSpilled, diskBytesSpilled, and JVM GC time',
    stoppedBy: 'Memory pages reclaimed via TaskMemoryManager.cleanUpAllAllocatedMemory() on task exit',
    inputs: [
      'InternalRow fields written via GenerateUnsafeProjection into fixed + variable binary regions',
      '64-bit page pointers (13-bit page# + 51-bit offset) from TaskMemoryManager / UnifiedMemoryManager',
    ],
    outputs: [
      'Zero-copy binary UnsafeRow views passed between codegen operators and serialized directly by UnsafeRowSerializer',
      'Spillable sorted runs from UnsafeExternalSorter and hash buckets from BytesToBytesMap',
    ],
    internalState: [
      'UnsafeRow baseObject (null for off-heap), baseOffset, sizeInBytes, and bitSetWidthInBytes',
      'UnsafeExternalSorter Radix/TimSort pointer array + DiskBlockObjectWriter spill readers',
      'UnsafeFixedWidthAggregationMap / BytesToBytesMap backed by TaskMemoryManager pages',
    ],
    failureClues: [
      'spark.memory.offHeap.enabled=true moves Tungsten pages off JVM heap — must include spark.memory.offHeap.size in YARN container budget or hit PMEM -104',
      'TooLargePageException or SparkOutOfMemoryError indicates a huge row/array exceeded page size or spillable execution memory exhausted',
      'DeserializeToObject / SerializeFromObject around typed Dataset lambdas indicates rows leaving Tungsten binary format',
    ],
    sourceLinks: [
      {
        label: 'UnsafeRow binary layout & Platform accessors',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java#L48-L135`,
      },
      {
        label: 'UnsafeExternalSorter pointer sort & disk spill',
        url: `${SPARK_BLOB}/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java#L65-L155`,
      },
    ],
    docFile: 'tungsten-unsafe-row.md',
  },

  'hash-aggregate': {
    id: 'hash-aggregate',
    name: 'HashAggregateExec & SortAggregate',
    shortName: 'HashAggregate',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1818, 868],
    languages: ALL_LANGS,
    port: 'in-JVM agg map',
    process: 'Executor JVM HashAggregateExec (TungstenAggregationIterator) / ObjectHashAggregateExec / SortAggregateExec',
    role: 'Executes two-phase (map-side Partial + reduce-side Final) SQL aggregations using generated two-level Tungsten hash maps (fast vectorized hash map -> row-based UnsafeFixedWidthAggregationMap), automatically falling back to spillable external sort-based aggregation when execution memory fills.',
    keyDistinction:
      'HashAggregateExec handles fixed-width primitive Tungsten buffers at maximum speed; when an aggregation uses arbitrary JVM objects (like Typed Aggregator UDAFs or collect_list), Spark selects ObjectHashAggregateExec or SortAggregateExec.',
    startedBy: 'SQL Planner Aggregation strategy selects HashAggregateExec (or ObjectHashAggregateExec / SortAggregateExec)',
    monitoredBy: 'SQL UI HashAggregate metrics (numOutputRows, peakMemory, spillSize, avgHashProbe, aggTime)',
    stoppedBy: 'Releases UnsafeFixedWidthAggregationMap & external sorter pages on iterator drain or task end',
    inputs: [
      'Upstream partition InternalRow / UnsafeRow stream (Partial before ShuffleExchange, Final after ShuffleReader)',
      'Grouping key expressions, Imperative/Declarative AggregateExpressions, and execution memory pages',
    ],
    outputs: [
      'Partially aggregated (groupingKey, aggBuffer) rows to ShuffleWriter, or final aggregated output rows downstream',
      'Automatic spill-to-sort fallback runs when BytesToBytesMap cannot acquire more execution pages',
    ],
    internalState: [
      'Two-level codegen VectorizedHashMap (level-1) + UnsafeFixedWidthAggregationMap (level-2)',
      'TungstenAggregationIterator switch to UnsafeExternalSorter on memory exhaustion',
      'ObjectHashAggregateExec ObjectAggregationMap fallback threshold (default 128 keys)',
    ],
    failureClues: [
      'High avgHashProbe or spillSize on HashAggregateExec indicates high-cardinality grouping keys or key skew',
      'SortAggregate in df.explain() occurs when mutable variable-length buffer fields cannot fit fixed-width Tungsten slots',
      'Missing Partial HashAggregate before ShuffleExchange means raw rows crossed the network without pre-combining',
    ],
    sourceLinks: [
      {
        label: 'HashAggregateExec codegen & two-level hash map',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala#L60-L150`,
      },
      {
        label: 'ObjectHashAggregateExec for typed/object buffers',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ObjectHashAggregateExec.scala#L35-L115`,
      },
    ],
    docFile: 'hash-aggregate.md',
  },

  'sort-merge-join': {
    id: 'sort-merge-join',
    name: 'SortMergeJoinExec',
    shortName: 'SortMergeJoin',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1612, 868],
    languages: ALL_LANGS,
    port: 'in-JVM merge',
    process: 'Executor JVM SortMergeJoinExec + SortExec + ExternalAppendOnlyUnsafeRowArray',
    role: 'Joins two large equi-join tables after ShuffleExchangeExec co-partitions both sides and SortExec sorts each partition by join keys, streaming matched key groups in O(N + M) memory using a spillable ExternalAppendOnlyUnsafeRowArray buffer and cooperating with AQE OptimizeSkewedJoin.',
    keyDistinction:
      'Unlike BroadcastHashJoin (which collects the small table to the driver), SortMergeJoin scales to multi-terabyte joins on both sides by shuffling, sorting, and streaming key-matched groups.',
    startedBy: 'SQL Planner JoinSelection selects SortMergeJoinExec when neither side qualifies for broadcast or shuffled hash join',
    monitoredBy: 'SQL UI SortMergeJoin metrics (numOutputRows, spillSize) plus upstream SortExec & ShuffleExchange',
    stoppedBy: 'Closes streamed & buffered iterators and cleans ExternalAppendOnlyUnsafeRowArray spill files on task end',
    inputs: [
      'Co-partitioned, key-sorted left and right InternalRow iterators from ShuffleReader -> SortExec',
      'JoinType (Inner, LeftOuter, RightOuter, FullOuter, Semi, Anti), bound keys, and optional residual condition',
    ],
    outputs: [
      'Joined InternalRow stream (participates in WholeStageCodegenExec for inner/outer/semi/anti joins)',
      'Spilled buffered-side key-group runs if a hot key has huge multiplicity',
    ],
    internalState: [
      'SMJScanner / codegen two-pointer cursor advancing left and right sorted iterators in lockstep',
      'ExternalAppendOnlyUnsafeRowArray buffering duplicate right-side matches for one key',
      'isSkewJoin flag set by AQE OptimizeSkewedJoin when reading PartialReducerPartitionSpecs',
    ],
    failureClues: [
      'One straggler SortMergeJoin task with huge spillSize points to a hot join key; enable AQE OptimizeSkewedJoin',
      'Check df.explain() for SortExec + ShuffleExchange on both sides — bucketing can eliminate both exchanges',
      'Key explosion on duplicate keys in both tables overflows ExternalAppendOnlyUnsafeRowArray to disk',
    ],
    sourceLinks: [
      {
        label: 'SortMergeJoinExec operator & codegen',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinExec.scala#L45-L140`,
      },
      {
        label: 'ExternalAppendOnlyUnsafeRowArray buffered match spill',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/ExternalAppendOnlyUnsafeRowArray.scala#L35-L115`,
      },
    ],
    docFile: 'sort-merge-join.md',
  },

  // =========================================================================
  // 7 Scala & Java Native JVM Components (NOT in PySpark!)
  // =========================================================================
  'closure-cleaner': {
    id: 'closure-cleaner',
    name: 'SparkClosureCleaner (ASM Bytecode)',
    shortName: 'ClosureCleaner',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [160, 670],
    languages: ['scala', 'java'],
    port: 'ASM / JavaSer',
    process: 'Driver JVM SparkClosureCleaner + ClosureCleaner (ASM9) + SerializationDebugger',
    role: 'Analyzes Scala anonymous function classes and Java SerializedLambda bytecode via ASM on the driver, clones and nulls out unreferenced enclosing $outer pointers transitively, and validates serializability with SparkEnv.closureSerializer (JavaSerializer) before DAGScheduler submits any task.',
    keyDistinction:
      'ClosureCleaner is strictly a Scala/Java JVM bytecode cleaner (PySpark serializes Python functions via cloudpickle instead) and ALWAYS validates using JavaSerializer regardless of spark.serializer.',
    startedBy: 'Called automatically on the driver by SparkContext.clean() inside every RDD/Dataset transformation and action',
    monitoredBy: 'Driver TaskNotSerializableException + SerializationDebugger object-reference chain diagnostics',
    stoppedBy: 'Synchronous per-transformation driver check; throws SparkException immediately on non-serializable captures',
    inputs: [
      'Scala FunctionN / anonymous inner classes or Java Serializable lambdas (SerializedLambda)',
      'ASM ClassReader bytecode inspection of enclosing classes and captured fields',
    ],
    outputs: [
      'Cleaned closure with unreachable outer-object fields nulled so tasks ship cleanly to executors',
      'SparkException("Task not serializable") with full SerializationDebugger field chain on invalid capture',
    ],
    internalState: [
      'ASM InnerClosureFinder, FieldAccessFinder, and IndylambdaScalaClosures bytecode visitors',
      'Transitive visited map tracking cleaned enclosing closures and outer-class clones',
      'SparkEnv.get.closureSerializer (hardcoded JavaSerializer) validation pass',
    ],
    failureClues: [
      'TaskNotSerializableException happens strictly on the driver (0 tasks ever launched); read SerializationDebugger bottom-to-top',
      'Capturing SparkSession, SparkContext, or a JDBC Connection inside rdd.map / ds.map fails ClosureCleaner immediately',
      'Fix by moving helpers to a Scala object / Java static method, extracting local vals, marking fields @transient, or using sc.broadcast()',
    ],
    sourceLinks: [
      {
        label: 'SparkClosureCleaner.clean & JavaSerializer check',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/util/SparkClosureCleaner.scala#L23-L56`,
      },
      {
        label: 'ClosureCleaner ASM bytecode outer-field stripping',
        url: `${SPARK_BLOB}/common/utils/src/main/scala/org/apache/spark/util/ClosureCleaner.scala#L55-L150`,
      },
    ],
    docFile: 'closure-cleaner.md',
  },

  'kryo-registrator': {
    id: 'kryo-registrator',
    name: 'KryoSerializer & User KryoRegistrator',
    shortName: 'KryoRegistrator',
    subsystem: 'driver-runtime',
    hostZone: 'worker-node-a',
    clusterPos: [414, 1196],
    languages: ['scala', 'java'],
    port: 'in-JVM Kryo',
    process: 'Per-JVM KryoSerializer + KryoPool + AllScalaRegistrar + user KryoRegistrator',
    role: 'Serializes Scala case classes, Scala collections, and Java POJOs into compact binary streams up to 10x faster and smaller than JavaSerializer across RDD shuffles, cached partitions, broadcasts, and Encoders.kryo — enforcing strict numeric class ID registration when spark.kryo.registrationRequired=true.',
    keyDistinction:
      'PySpark user objects serialize via Python pickle/Arrow rather than user KryoRegistrators; in Scala and Java, registering domain classes replaces full class-name strings with compact numeric IDs.',
    startedBy: 'Configured via spark.serializer=org.apache.spark.serializer.KryoSerializer and conf.registerKryoClasses / spark.kryo.registrator',
    monitoredBy: 'Task serialization/deserialization metrics, shuffle size reduction, and KryoException diagnostics',
    stoppedBy: 'KryoPool instances released when executor threads or SparkEnv stop',
    inputs: [
      'Scala case classes, Twitter Chill AllScalaRegistrar types, and Java domain POJOs',
      'spark.kryo.registrationRequired, spark.kryoserializer.buffer, spark.kryoserializer.buffer.max, and KryoRegistrator list',
    ],
    outputs: [
      'Compact binary Kryo byte streams for RDD shuffles, MEMORY_AND_DISK_SER cache, Broadcast, and Encoders.kryo',
      'KryoException("Class is not registered") immediately if registrationRequired=true hits an unregistered class',
    ],
    internalState: [
      'KryoPool reusing pre-configured Kryo instances across tasks on each executor JVM',
      'Numeric class ID registry (Spark built-ins + AllScalaRegistrar + user registerKryoClasses)',
      'Configurable reference tracking and buffer growth cap (default 64m)',
    ],
    failureClues: [
      'KryoException: "Class is not registered" means registrationRequired=true caught an unregistered Scala case class or Java POJO',
      'KryoException: "Buffer overflow" points to spark.kryoserializer.buffer.max (default 64m) being too small for one object',
      'Task closures still use JavaSerializer (SparkEnv.closureSerializer) even when KryoSerializer is enabled',
    ],
    sourceLinks: [
      {
        label: 'KryoSerializer pool, registrators & strict mode',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/serializer/KryoSerializer.scala#L63-L165`,
      },
      {
        label: 'Encoders.kryo Dataset binary encoder',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/encoders/KryoSerializationCodecImpl.scala#L1-L60`,
      },
    ],
    docFile: 'kryo-registrator.md',
  },

  'typed-dataset-ops': {
    id: 'typed-dataset-ops',
    name: 'Typed Dataset Operators (MapElements)',
    shortName: 'Typed Dataset Ops',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1818, 950],
    languages: ['scala', 'java'],
    port: 'in-JVM Dataset[T]',
    process: 'Executor JVM MapElementsExec, MapPartitionsExec, DeserializeToObjectExec, SerializeFromObjectExec, MapGroupsExec',
    role: 'Executes strongly-typed Scala and Java Dataset[T] functional operators (ds.map, ds.flatMap, ds.mapPartitions, ds.groupByKey().mapGroups) directly inside executor JVM threads by converting Tungsten InternalRows into typed domain objects via ExpressionEncoder, running the user JVM lambda, and re-encoding to UnsafeRow.',
    keyDistinction:
      'Typed Dataset object operators are exclusive to Scala and Java (PySpark only has untyped DataFrame); they run natively in-JVM without Python IPC, but DeserializeToObject -> SerializeFromObject acts as a Catalyst pushdown barrier.',
    startedBy: 'Planned by SparkStrategies BasicOperators for typed Dataset[T] logical nodes (MapElements, MapPartitions, MapGroups)',
    monitoredBy: 'SQL UI DeserializeToObject / MapElements / SerializeFromObject nodes inside WholeStageCodegen stages',
    stoppedBy: 'Finishes when partition iterator completes; fused into WholeStageCodegenExec wherever supported',
    inputs: [
      'Upstream InternalRow / UnsafeRow stream decoded via DeserializeToObjectExec (ExpressionEncoder)',
      'Cleaned Scala Function1 / Java MapFunction / FlatMapFunction / MapGroupsFunction JVM closure',
    ],
    outputs: [
      'Typed Scala/Java domain object results re-encoded into Tungsten UnsafeRow by SerializeFromObjectExec',
      'Compile-time type safety without leaving the executor JVM process',
    ],
    internalState: [
      'ObjectProducerExec & ObjectConsumerExec codegen adapters fusing into WholeStageCodegenExec',
      'CatalystDeserializer -> user lambda invocation -> CatalystSerializer expression chain',
      'Keyed group iterators for MapGroupsExec / CoGroupExec on KeyValueGroupedDataset',
    ],
    failureClues: [
      'DeserializeToObject + SerializeFromObject in df.explain() explains higher CPU/GC vs pure Column expressions',
      'Typed ds.filter(o => o.amount > 100) cannot be pushed down to Parquet unlike df.filter($"amount" > 100)',
      'Non-serializable captured state in a Dataset.map lambda fails driver SparkClosureCleaner before execution',
    ],
    sourceLinks: [
      {
        label: 'MapElementsExec, DeserializeToObjectExec & SerializeFromObjectExec',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/objects.scala#L60-L210`,
      },
      {
        label: 'Dataset typed transformations (Scala/Java)',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L1-L90`,
      },
    ],
    docFile: 'typed-dataset-ops.md',
  },

  'scala-java-udf': {
    id: 'scala-java-udf',
    name: 'In-JVM ScalaUDF & Java UDF1..22',
    shortName: 'Scala/Java UDF',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [2024, 950],
    languages: ['scala', 'java'],
    port: 'in-JVM UDF',
    process: 'Executor JVM ScalaUDF expression inside WholeStageCodegenExec / GenerateUnsafeProjection',
    role: 'Executes Scala udf(...) closures and Java UDF0..UDF22 functional interfaces inline inside Catalyst generated Java bytecode on the executor task thread via ScalaUDF.doGenCode(), converting arguments with CatalystTypeConverters and returning results with zero Python workers or local sockets.',
    keyDistinction:
      'Unlike PySpark EvalPythonExec (which splits WholeStageCodegen and streams pickle/Arrow batches over local sockets), ScalaUDF and Java UDF1..22 inline directly inside WholeStageCodegenExec *(N) loops.',
    startedBy: 'Registered via spark.udf.register(...) or functions.udf(...) and invoked in SQL/DataFrame expressions',
    monitoredBy: 'SQL UI fused WholeStageCodegenExec stage (* prefix preserved!) and task CPU metrics',
    stoppedBy: 'Evaluated inline per row inside the compiled BufferedRowIterator loop until partition drains',
    inputs: [
      'Bound Catalyst argument expressions evaluated directly from Tungsten UnsafeRow registers',
      'Cleaned Scala FunctionN or Java org.apache.spark.sql.api.java.UDF1..UDF22 instance',
    ],
    outputs: [
      'Converted Catalyst return value written straight into the output UnsafeRow buffer in the same CPU loop',
      'Zero external worker processes, zero loopback sockets, zero cross-process memory overhead',
    ],
    internalState: [
      'ScalaUDF expression node with optional inputEncoders / outputEncoder and CatalystTypeConverters',
      'doGenCode() emitting direct Java method call inside Janino WholeStageCodegen',
      'udfDeterministic flag governing Catalyst folding/deduplication',
    ],
    failureClues: [
      'Scala/Java UDFs remain opaque to Catalyst predicate pushdown — prefer built-in SQL functions when available',
      'Primitive Scala UDF inputs (Int, Double) automatically return null on null inputs unless boxed/Option is used',
      'Unhandled exception wraps in SparkException ("Failed to execute user defined function") with exact JVM stack trace',
    ],
    sourceLinks: [
      {
        label: 'ScalaUDF codegen & inline execution',
        url: `${SPARK_BLOB}/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/ScalaUDF.scala#L39-L125`,
      },
      {
        label: 'Java UDF1 functional interface',
        url: `${SPARK_BLOB}/sql/api/src/main/java/org/apache/spark/sql/api/java/UDF1.java#L1-L31`,
      },
    ],
    docFile: 'scala-java-udf.md',
  },

  'typed-aggregator': {
    id: 'typed-aggregator',
    name: 'Typed Aggregator (JVM UDAF)',
    shortName: 'Typed Aggregator',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [2024, 868],
    languages: ['scala', 'java'],
    port: 'in-JVM UDAF',
    process: 'Executor JVM Aggregator[IN, BUF, OUT] + TypedAggregateExpression + ObjectHashAggregateExec',
    role: 'Executes strongly-typed Scala and Java user-defined aggregate functions (Aggregator[IN, BUF, OUT] implementing zero, reduce, merge, finish, bufferEncoder, outputEncoder) with map-side partial combining and reduce-side merging inside ObjectHashAggregateExec or HashAggregateExec.',
    keyDistinction:
      'Typed Aggregator UDAFs are exclusive to Scala and Java and combine rows map-side before shuffle, slashing network bytes compared to groupByKey().mapGroups or PySpark grouped-map Pandas UDFs.',
    startedBy: 'Registered via functions.udaf(myAggregator, inputEncoder) or ds.groupByKey(...).agg(myAggregator.toColumn)',
    monitoredBy: 'SQL UI ObjectHashAggregate / HashAggregate metrics (partial & final stages, spill fallback)',
    stoppedBy: 'Calls finish(finalBuf) per group key during final reduce aggregation and frees ObjectAggregationMap',
    inputs: [
      'Typed input records IN (from Dataset[IN] or udaf column inputs) and intermediate buffer BUF',
      'User-supplied bufferEncoder: Encoder[BUF] and outputEncoder: Encoder[OUT]',
    ],
    outputs: [
      'Serialized partial BUF buffers written to shuffle (map-side combine), merged via merge(b1, b2) on reducers',
      'Final typed aggregate result OUT emitted by finish(buf) per grouping key',
    ],
    internalState: [
      'TypedAggregateExpression / ScalaAggregator binding zero(), reduce(), merge(), and finish() hooks',
      'ObjectHashAggregateExec ObjectAggregationMap (falls back to SortAggregate past threshold)',
      'ExpressionEncoder codecs serializing BUF across ShuffleExchangeExec boundaries',
    ],
    failureClues: [
      'Exceeding spark.sql.objectHashAggregate.sortBased.fallbackThreshold (128) switches ObjectHashAggregateExec to SortAggregateExec',
      'Prefer Aggregator[IN, BUF, OUT] over deprecated untyped UserDefinedAggregateFunction and over non-combining mapGroups',
      'Non-associative merge(b1, b2) yields nondeterministic results across partition orderings',
    ],
    sourceLinks: [
      {
        label: 'Aggregator[IN, BUF, OUT] UDAF contract',
        url: `${SPARK_BLOB}/sql/api/src/main/scala/org/apache/spark/sql/expressions/Aggregator.scala#L55-L120`,
      },
      {
        label: 'TypedAggregateExpression & ScalaAggregator',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/TypedAggregateExpression.scala#L30-L125`,
      },
    ],
    docFile: 'typed-aggregator.md',
  },

  'scala-repl-compiler': {
    id: 'scala-repl-compiler',
    name: 'Scala REPL (SparkILoop) & ClassServer',
    shortName: 'Scala REPL',
    subsystem: 'driver-runtime',
    hostZone: 'submit-host',
    clusterPos: [484, 106],
    languages: ['scala'],
    port: 'spark://.../classes',
    process: 'Client-host Scala REPL JVM (org.apache.spark.repl.SparkILoop) + ExecutorClassLoader',
    role: 'Powers interactive bin/spark-shell sessions in Scala: compiles entered Scala expressions on the fly into synthetic wrapper classes ($lineN.$read$$iw) inside a temp class directory served by driver RpcEnvFileServer, while worker ExecutorClassLoader fetches those dynamic .class bytes on demand over Spark RPC.',
    keyDistinction:
      'Interactive Scala spark-shell compiles real JVM bytecode dynamically per REPL line and ships .class files to worker ExecutorClassLoaders over Spark RPC — completely distinct from PySpark interactive shell (cloudpickle over Py4J).',
    startedBy: 'bin/spark-shell -> spark-submit --class org.apache.spark.repl.Main (client deploy mode on YARN)',
    monitoredBy: 'REPL compilation output, driver spark.repl.class.uri, and ExecutorClassLoader fetch logs',
    stoppedBy: ':quit / EOF stops SparkILoop, cleans temp REPL class directory, and stops SparkSession/SparkContext',
    inputs: [
      'Interactive Scala statements, case class definitions, and UDFs entered in spark-shell',
      'ExecutorClassLoader RPC requests for synthetic $lineN.$read... .class files',
    ],
    outputs: [
      'Compiled JVM .class bytecode served from spark.repl.class.outputDir via Netty RpcEnvFileServer',
      'Dynamically loaded user classes inside executor ExecutorClassLoader',
    ],
    internalState: [
      'SparkILoop Scala compiler (IMain) wrapping each REPL command into line objects',
      'spark.repl.class.uri advertised to executors via SparkAppConfig',
      'ExecutorClassLoader parent-null isolation fetching REPL bytecode via Spark RPC',
    ],
    failureClues: [
      'NotSerializableException on $iw wrapper in spark-shell means a REPL line captured an earlier non-serializable REPL val',
      'ClassNotFoundException for $line3.$read on an executor points to unreachable driver spark.repl.class.uri',
      'spark-shell on YARN runs in client mode only; cluster mode requires a compiled JAR via spark-submit --class',
    ],
    sourceLinks: [
      {
        label: 'SparkILoop interactive Scala REPL',
        url: `${SPARK_BLOB}/repl/src/main/scala/org/apache/spark/repl/SparkILoop.scala#L30-L115`,
      },
      {
        label: 'ExecutorClassLoader dynamic RPC class fetch',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/executor/ExecutorClassLoader.scala#L42-L120`,
      },
    ],
    docFile: 'scala-repl-compiler.md',
  },

  'java-spark-api': {
    id: 'java-spark-api',
    name: 'JavaSparkContext, JavaRDD & Bean API',
    shortName: 'Java Spark API',
    subsystem: 'driver-runtime',
    hostZone: 'submit-host',
    clusterPos: [664, 106],
    languages: ['java'],
    port: 'in-JVM Java API',
    process: 'Driver JVM Java interoperability facade (JavaSparkContext, JavaRDD, JavaPairRDD, JavaTypeInference)',
    role: 'Provides the idiomatic Java entry layer over Spark Scala core and SQL: wraps SparkContext in Closeable JavaSparkContext, bridges Java lambdas (Function, FlatMapFunction, PairFunction) and java.util.List/Optional into Scala RDDs, and introspects JavaBeans via Encoders.bean() and JavaTypeInference.',
    keyDistinction:
      'Java Spark jobs compile straight to JVM bytecode and share the exact same in-JVM Driver/Executor runtime as Scala, using JavaSparkContext / JavaRDD / Encoders.bean / UDF1..22 adapters with zero Py4J or Python processes.',
    startedBy: 'User Java main(String[] args) calling SparkSession.builder().getOrCreate() or new JavaSparkContext(sparkConf)',
    monitoredBy: 'Java compile-time type checks, JavaSparkStatusTracker, and SparkClosureCleaner SerializedLambda checks',
    stoppedBy: 'JavaSparkContext.close() (java.io.Closeable for try-with-resources) delegates to sc.stop()',
    inputs: [
      'Java SparkConf, java.util.List collections (jsc.parallelize), and JavaBean POJOs (Encoders.bean(Order.class))',
      'Java Serializable functional interfaces (Function, PairFunction, FlatMapFunction, UDF1..UDF22)',
    ],
    outputs: [
      'Underlying Scala SparkContext / RDD[T] / Dataset[T] submitted identically to DAGScheduler & Catalyst',
      'Java-typed collect() lists, JavaPairRDD Tuple2<K, V> streams, and Encoders.bean ExpressionEncoders',
    ],
    internalState: [
      'JavaSparkContext wrapping val sc: SparkContext (single active context per JVM still enforced)',
      'JavaRDD / JavaPairRDD wrapping Scala RDD with fakeClassTag bridges',
      'JavaTypeInference BeanInfo property inspector deriving Catalyst StructTypes from getters/setters',
    ],
    failureClues: [
      'NotSerializableException on Java lambda (SerializedLambda) indicates the lambda captured a non-Serializable outer Java instance field',
      'Encoders.bean(Order.class) requires public no-arg constructor and getter/setter pairs',
      'Calling jsc.close() inside try-with-resources sends default stop(0) in client mode unless sc.stop(1) is invoked on failure',
    ],
    sourceLinks: [
      {
        label: 'JavaSparkContext Closeable wrapper over SparkContext',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/api/java/JavaSparkContext.scala#L48-L120`,
      },
      {
        label: 'JavaRDD functional adapter over Scala RDD',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/api/java/JavaRDD.scala#L25-L105`,
      },
      {
        label: 'JavaTypeInference JavaBean schema reflection',
        url: `${SPARK_BLOB}/sql/api/src/main/scala/org/apache/spark/sql/catalyst/JavaTypeInference.scala#L1-L95`,
      },
    ],
    docFile: 'java-spark-api.md',
  },

  'spark-session-extensions': {
    id: 'spark-session-extensions',
    name: 'SparkSessionExtensions (Catalyst Hooks)',
    shortName: 'SessionExtensions',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [618, 606],
    languages: ['scala', 'java'],
    port: 'in-JVM plugin',
    process: 'Driver JVM SparkSessionExtensions builder hook (Analyzer / Optimizer / Strategy / Columnar / Parser)',
    role: 'Exposes the JVM-only Catalyst extension API (SparkSession.builder.withExtensions / spark.sql.extensions) allowing Scala and Java libraries (Delta Lake, Iceberg, Photon, custom engines) to inject custom Catalyst Analyzer rules, Optimizer rules, physical SparkPlanner Strategies, SQL parsers, columnar rules, and AQE query-stage prep rules directly into SessionState.',
    keyDistinction:
      'Custom Catalyst Rule[LogicalPlan], Strategy, and ColumnarRule extensions run strictly inside the Driver JVM and must be authored in Scala or Java (PySpark cannot register Python Catalyst rules).',
    startedBy: 'Invoked during SparkSession.builder.withExtensions(...) or via spark.sql.extensions class names at SessionState creation',
    monitoredBy: 'df.explain("extended") showing injected custom rules/operators and QueryPlanningTracker timings',
    stoppedBy: 'Bound for the lifetime of the SparkSession and cloned into child sessions',
    inputs: [
      'Scala Function1[SparkSessionExtensions, Unit] or Java Consumer<SparkSessionExtensions>',
      'Custom Resolution/Check/Optimizer rules, Planner Strategies, FunctionDescriptions, and ColumnarRules',
    ],
    outputs: [
      'Extended Catalyst Analyzer, SparkOptimizer, SparkPlanner, and Columnar transition pipeline inside SessionState',
      'Custom physical operators (e.g., MERGE INTO, custom scans, GPU columnar transitions)',
    ],
    internalState: [
      'resolutionRuleBuilders, postHocResolutionRuleBuilders, optimizerRules, plannerStrategyBuilders',
      'parserBuilders, injectedFunctions, columnarRuleBuilders, and adaptiveRuleBuilders',
      'Ordered extension registration list applied on BaseSessionStateBuilder.build()',
    ],
    failureClues: [
      'ClassNotFoundException in spark.sql.extensions means the Scala/Java extension JAR was omitted from --jars / driver classpath',
      'Non-idempotent custom Catalyst Optimizer rule triggers maxIterations (100) warning and driver planning stalls',
      'Unregistered custom logical node reaching SparkPlanner fails with "No plan for ..." strategy error',
    ],
    sourceLinks: [
      {
        label: 'SparkSessionExtensions rule & strategy hooks',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/SparkSessionExtensions.scala#L55-L210`,
      },
    ],
    docFile: 'spark-session-extensions.md',
  },

  'scala-reflection-implicits': {
    id: 'scala-reflection-implicits',
    name: 'ScalaReflection & spark.implicits',
    shortName: 'ScalaReflection',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [360, 734],
    languages: ['scala'],
    port: 'in-JVM TypeTag',
    process: 'Driver JVM org.apache.spark.sql.catalyst.ScalaReflection + SQLImplicits (spark.implicits._)',
    role: 'Inspects Scala compile-time TypeTag / Product / Option / Seq / Map metadata to synthesize Spark SQL StructTypes and zero-reflection ExpressionEncoder[T] serializers/deserializers powering spark.implicits._ conversions (.toDS(), .toDF(), .as[Order], $"col").',
    keyDistinction:
      'ScalaReflection + spark.implicits._ is exclusively Scala: Java uses JavaTypeInference + Encoders.bean(Order.class), while PySpark uses untyped Row/DataFrame schemas without compile-time Product encoders.',
    startedBy: 'Triggered at compile-time/runtime whenever Scala code imports spark.implicits._ and creates a typed Dataset[T] or ScalaUDF',
    monitoredBy: 'Scala compile-time implicit resolution errors ("Unable to find encoder for type stored in a Dataset") and Catalyst schema inspection',
    stoppedBy: 'Compiled Encoder expressions are cached in Catalyst QueryExecution and garbage-collected with the plan',
    inputs: [
      'Scala case classes, tuples, Value Classes, Sealed Traits, and TypeTag[T] implicit evidence',
      'SQLImplicits conversions (localSeqToDatasetHolder, rddToDatasetHolder, StringToColumn $"col")',
    ],
    outputs: [
      'AgnosticEncoder[T] -> ExpressionEncoder[T] (Catalyst serializer & deserializer AST expressions)',
      'Exact nullability metadata (primitive Double/Int vs Option[T]) driving ScalaUDF null guards',
    ],
    internalState: [
      'ScalaReflection.encoderFor recursive TypeTag inspector and constructor/accessor binder',
      'SQLImplicits low-priority/high-priority encoder implicits on SparkSession',
      'Thread-safe Scala reflection mirror lock guarding universe symbol inspection',
    ],
    failureClues: [
      '"Unable to find encoder for type stored in a Dataset" means missing import spark.implicits._ or unsupported non-Product type (use Encoders.kryo)',
      'Case class defined inside a method body captures $outer reference; define case classes in a top-level package or Scala object',
      'Primitive Scala fields (Double/Int) infer nullable=false unless wrapped in Option[T]',
    ],
    sourceLinks: [
      {
        label: 'ScalaReflection TypeTag & Product encoder synthesis',
        url: `${SPARK_BLOB}/sql/api/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L60-L195`,
      },
    ],
    docFile: 'scala-reflection-implicits.md',
  },

  'dynamic-partition-pruning': {
    id: 'dynamic-partition-pruning',
    name: 'Dynamic Partition Pruning (DPP)',
    shortName: 'Dynamic Part Pruning',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [818, 606],
    languages: ['pyspark', 'scala', 'java'],
    port: 'in-JVM / Broadcast',
    process: 'Driver Catalyst PartitionPruning optimizer rule + PlanDynamicPruningFilters + SubqueryBroadcastExec',
    role: 'Detects star-schema joins where a filtered dimension table joins a partitioned fact table on its partition column, inserts a DynamicPruningExpression subquery that reuses the BroadcastExchangeExec hash build, and skips reading non-matching HDFS partition directories before launching scan tasks.',
    keyDistinction:
      'Static partition pruning uses literal filters known at compile time; DPP evaluates the dimension side at runtime via SubqueryBroadcastExec and prunes fact-table FileSourceScanExec / BatchScanExec partitions dynamically.',
    startedBy: 'Catalyst PartitionPruning rule (spark.sql.optimizer.dynamicPartitionPruning.enabled=true) and PlanDynamicPruningFilters physical strategy',
    monitoredBy: 'SQL UI "dynamic partition pruning" scan metric (number of partitions pruned / read) and SubqueryBroadcastExec nodes',
    stoppedBy: 'Broadcast subquery completes once the dimension BroadcastExchangeExec finishes, then fact scan tasks launch on surviving partitions only',
    inputs: [
      'Partitioned fact table scan joined to a selectively filtered dimension table on partition keys',
      'BroadcastExchangeExec HashedRelation reused by SubqueryBroadcastExec (zero duplicate shuffle)',
    ],
    outputs: [
      'Runtime DynamicPruningExpression filter keys applied inside FileSourceScanExec / BatchScanExec',
      'Drastically reduced HDFS NameNode file listings, scan tasks, and shuffle bytes read',
    ],
    internalState: [
      'PartitionPruning heuristic estimating pruning benefit ratio (reuseBroadcastOnly / fallbackFilterRatio)',
      'SubqueryBroadcastExec reusing BroadcastQueryStageExec under AQE',
      'Partition directory pruning filter evaluated before split creation',
    ],
    failureClues: [
      'Missing SubqueryBroadcastExec in df.explain() when joining partitioned fact tables means filter was not selective enough or join was not broadcastable',
      'Wrapping partition key in an opaque UDF blocks DPP because Catalyst cannot prove partition-column alignment',
      'Verify SQL UI "number of partitions read" vs total table partitions to confirm runtime pruning fired',
    ],
    sourceLinks: [
      {
        label: 'PartitionPruning Catalyst optimizer rule',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/dynamicpruning/PartitionPruning.scala#L42-L165`,
      },
      {
        label: 'SubqueryBroadcastExec runtime key broadcast',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/SubqueryBroadcastExec.scala#L35-L120`,
      },
    ],
    docFile: 'dynamic-partition-pruning.md',
  },

  'key-value-grouped-dataset': {
    id: 'key-value-grouped-dataset',
    name: 'KeyValueGroupedDataset (MapGroups)',
    shortName: 'KeyValueGroupedDS',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1818, 1114],
    languages: ['scala', 'java'],
    port: 'in-JVM groups',
    process: 'Driver KeyValueGroupedDataset[K, V] + Executor MapGroupsExec / FlatMapGroupsExec / CoGroupExec',
    role: 'Executes strongly-typed Scala and Java keyed group transformations (ds.groupByKey(k).mapGroups, flatMapGroups, reduceGroups, cogroup, agg) by shuffling on encoded key K, sorting partitions by K, and streaming typed (K, Iterator[V]) JVM objects directly into user closures on executor threads.',
    keyDistinction:
      'KeyValueGroupedDataset is exclusive to Scala and Java (PySpark uses untyped GroupedData / applyInPandas over Arrow sockets); unlike Typed Aggregator UDAFs, mapGroups shuffles all raw group values without map-side partial reduction.',
    startedBy: 'Planned when Scala/Java code calls ds.groupByKey(...).mapGroups / flatMapGroups / cogroup',
    monitoredBy: 'SQL UI MapGroups / CoGroup nodes, ShuffleExchange bytes written, and executor GC metrics',
    stoppedBy: 'Finishes once all sorted key groups on each reduce partition are iterated and re-encoded to UnsafeRow',
    inputs: [
      'Shuffle-partitioned and key-sorted InternalRow stream from ShuffleExchangeExec + SortExec',
      'Key Encoder[K], Value Encoder[V], Output Encoder[U], and cleaned Scala/Java group function',
    ],
    outputs: [
      'Typed output objects serialized back into Tungsten UnsafeRow via SerializeFromObjectExec',
      'Full stateful per-key streaming (or cogroup across two KeyValueGroupedDatasets) in pure JVM',
    ],
    internalState: [
      'GroupedIterator grouping consecutive rows sharing equal UnsafeRow key prefix without buffering entire groups unless user materializes',
      'DeserializeToObject key/value decoders + SerializeFromObject output encoder',
      'CoGroupExec lockstep dual-side key group iterator for ds1.cogroup(ds2)',
    ],
    failureClues: [
      'Prefer ds.groupByKey(...).agg(myAggregator.toColumn) or reduceGroups over mapGroups when partial map-side combining is possible (avoids full raw shuffle)',
      'Calling iterator.toList inside mapGroups on a skewed hot key causes executor JVM heap OOM',
      'EnsureRequirements inserts ShuffleExchange + SortExec automatically before MapGroupsExec',
    ],
    sourceLinks: [
      {
        label: 'KeyValueGroupedDataset Scala/Java typed group API',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/classic/KeyValueGroupedDataset.scala#L60-L210`,
      },
      {
        label: 'MapGroupsExec & CoGroupExec physical operators',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/objects.scala#L310-L430`,
      },
    ],
    docFile: 'key-value-grouped-dataset.md',
  },

  'window-sort-exec': {
    id: 'window-sort-exec',
    name: 'WindowExec & Tungsten SortExec',
    shortName: 'Window & SortExec',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [2024, 1114],
    languages: ['pyspark', 'scala', 'java'],
    port: 'in-JVM frame',
    process: 'Executor JVM SortExec (UnsafeExternalRowSorter + codegen) & WindowExec (WindowFunctionFrame)',
    role: 'Executes SQL ORDER BY / local partition sorts via Tungsten SortExec (participating in WholeStageCodegenExec via UnsafeExternalRowSorter) and analytic window functions (ROW_NUMBER, RANK, LAG, rolling SUM OVER PARTITION BY ... ORDER BY ...) via WindowExec frame processors backed by spillable ExternalAppendOnlyUnsafeRowArray.',
    keyDistinction:
      'SortExec fuses into WholeStageCodegenExec using Tungsten binary pointer sorting, while WindowExec consumes sorted partition streams and maintains sliding/unbounded/range frame buffers that spill to local disk when a single window partition grows large.',
    startedBy: 'SparkPlanner BasicOperators / EnsureRequirements inserts SortExec and WindowExec for ORDER BY, SortMergeJoin, and OVER() expressions',
    monitoredBy: 'SQL UI Sort & Window metrics (peakMemory, spillSize, sortTime) and WindowGroupLimitExec top-K optimization',
    stoppedBy: 'Releases UnsafeExternalRowSorter and ExternalAppendOnlyUnsafeRowArray spill files on task completion',
    inputs: [
      'Partitioned InternalRow / UnsafeRow stream (ShuffleExchangeExec by PARTITION BY keys)',
      'Window expressions (AggregateWindowFunction, OffsetWindowFunction, Sliding/Unbounded/Range WindowFunctionFrame)',
    ],
    outputs: [
      'Key-ordered UnsafeRow stream (SortExec) or input rows augmented with computed window columns (WindowExec)',
      'Spilled sorted runs / partition buffers on local disk when execution memory fills',
    ],
    internalState: [
      'UnsafeExternalRowSorter prefix-key pointer array with spillable merge sort',
      'WindowFunctionFrame implementations (UnboundedPreceding, Sliding, Offset, SegmentTree)',
      'ExternalAppendOnlyUnsafeRowArray buffering the current PARTITION BY window group',
    ],
    failureClues: [
      'Window function without PARTITION BY forces ShuffleExchangeExec(SinglePartition) — all data moves to 1 executor task!',
      'Huge spillSize on WindowExec points to a skewed PARTITION BY key with millions of rows in one window buffer',
      'Top-N window filters (WHERE row_number() <= K) trigger WindowGroupLimitExec partial map-side pruning',
    ],
    sourceLinks: [
      {
        label: 'WindowExec partition buffering & frame evaluation',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowExec.scala#L40-L155`,
      },
      {
        label: 'SortExec WholeStageCodegen & UnsafeExternalRowSorter',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/SortExec.scala#L38-L130`,
      },
    ],
    docFile: 'window-sort-exec.md',
  },

  'jvm-partitioner-pair-rdd': {
    id: 'jvm-partitioner-pair-rdd',
    name: 'JVM Partitioner & PairRDDFunctions',
    shortName: 'JVM Partitioner',
    subsystem: 'shuffle',
    hostZone: 'worker-node-a',
    clusterPos: [672, 1196],
    languages: ['scala', 'java'],
    port: 'in-JVM Partitioner',
    process: 'Driver/Executor JVM org.apache.spark.Partitioner (Hash / Range / Custom) + PairRDDFunctions',
    role: 'Controls exact RDD shuffle key-to-partition routing in Scala and Java via HashPartitioner, reservoir-sampled RangePartitioner, or user-defined Partitioner subclasses, enabling co-partitioned zero-shuffle joins, map-side combineByKeyWithClassTag, and single-pass repartitionAndSortWithinPartitions.',
    keyDistinction:
      'Custom JVM Partitioner subclasses and PairRDDFunctions (combineByKeyWithClassTag, repartitionAndSortWithinPartitions) execute natively inside the executor JVM without Python worker portable_hash serialization.',
    startedBy: 'Invoked by Scala PairRDDFunctions / JavaPairRDD transformations (partitionBy, reduceByKey, groupByKey, sortByKey, repartitionAndSortWithinPartitions)',
    monitoredBy: 'RDD partition skew distribution in Spark UI Stage task metrics and DAGScheduler ShuffleDependency reuse',
    stoppedBy: 'Partitioner metadata persists on ShuffledRDD so downstream co-partitioned joins skip redundant shuffles',
    inputs: [
      'JVM (K, V) pairs, numPartitions, optional Ordering[K], and custom getPartition(key: Any): Int logic',
      'Reservoir sample bounds computed on driver for RangePartitioner',
    ],
    outputs: [
      'Deterministic reduce partition index [0, numPartitions) fed straight into SortShuffleWriter / ExternalSorter',
      'Co-partitioned ShuffledRDD preserving known Partitioner to eliminate downstream shuffles',
    ],
    internalState: [
      'Partitioner.numPartitions, getPartition(key), and equals/hashCode (critical for Spark shuffle reuse!)',
      'RangePartitioner sorted rangeBounds array broadcast or shipped in closure',
      'Aggregator[K, V, C] map-side combiner wired into ExternalSorter',
    ],
    failureClues: [
      'Forgetting to override equals()/hashCode() on a custom Partitioner breaks co-partitioned join detection and forces extra shuffles',
      'RangePartitioner triggers a small sampling job on the driver before submitting the sort stage',
      'Using groupByKey() instead of reduceByKey() / aggregateByKey() disables map-side combining and explodes shuffle bytes',
    ],
    sourceLinks: [
      {
        label: 'Partitioner, HashPartitioner & RangePartitioner',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/Partitioner.scala#L30-L185`,
      },
    ],
    docFile: 'jvm-partitioner-pair-rdd.md',
  },

  'jvm-accumulator-v2': {
    id: 'jvm-accumulator-v2',
    name: 'AccumulatorV2 (Typed JVM Accumulators)',
    shortName: 'AccumulatorV2',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1476, 606],
    languages: ['scala', 'java'],
    port: 'in-JVM / Heartbeat',
    process: 'Driver AccumulatorContext + Executor typed AccumulatorV2[IN, OUT] (Long / Double / Collection / Custom)',
    role: 'Provides write-only, task-isolated distributed counters and custom typed aggregators (LongAccumulator, DoubleAccumulator, CollectionAccumulator, or custom AccumulatorV2[IN, OUT]) updated directly in executor JVM tasks and merged back on the driver via Heartbeat and TaskEnd CompletionEvent updates.',
    keyDistinction:
      'Scala and Java support arbitrary custom typed AccumulatorV2[IN, OUT] classes merged directly inside the Driver JVM with zero PythonAccumulatorV2 loopback socket server.',
    startedBy: 'Registered on driver via sc.longAccumulator / sc.collectionAccumulator / sc.register(customAcc) into AccumulatorContext',
    monitoredBy: 'Spark UI Stage Accumulators table, LiveListenerBus SparkListenerTaskEnd / ExecutorMetricsUpdate',
    stoppedBy: 'WeakReference tracked in AccumulatorContext and deregistered by ContextCleaner when GCed on driver',
    inputs: [
      'Executor task acc.add(v) mutations on task-cloned copyAndReset() AccumulatorV2 instances',
      'Serialized AccumulableInfo deltas piggybacked on Executor Heartbeats (live) and TaskEnd (authoritative)',
    ],
    outputs: [
      'Merged driver-side acc.value read after action completion (exactly-once per succeeded task in actions)',
      'Spark UI Accumulator metrics visible live during stage execution',
    ],
    internalState: [
      'AccumulatorContext global ConcurrentHashMap[Long, WeakReference[AccumulatorV2]] on driver',
      'Per-task cloned AccumulatorV2 instances merged via acc.merge(other) on DAGScheduler event loop',
      'countFailedValues flag controlling whether failed/speculative attempts merge into driver total',
    ],
    failureClues: [
      'Updating an AccumulatorV2 inside a lazy transformation (map/filter) can double-count if a stage is recomputed or tasks retry; rely on actions for exact counts',
      'Putting huge lists inside CollectionAccumulator bloats Heartbeat and TaskEnd RPC payloads and can OOM the driver',
      'Always call sc.register(customAcc) on the driver before shipping it inside a task closure',
    ],
    sourceLinks: [
      {
        label: 'AccumulatorV2 contract, copyAndReset & merge',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/util/AccumulatorV2.scala#L45-L185`,
      },
    ],
    docFile: 'jvm-accumulator-v2.md',
  },

  // =========================================================================
  // 4 PySpark-Specific Internals (NOT in Scala / Java)
  // =========================================================================
  'cloudpickle-serializer': {
    id: 'cloudpickle-serializer',
    name: 'CloudPickleSerializer & PythonFunction',
    shortName: 'CloudPickle',
    subsystem: 'python-output',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1276, 670],
    languages: ['pyspark'],
    port: 'Python pickle',
    process: 'Python Driver pyspark.serializers.CloudPickleSerializer -> Py4J -> PythonFunction bytes',
    role: 'Serializes Python lambdas, nested UDF closures, dynamically defined classes, and captured global references on the Python driver via bundled cloudpickle into command byte arrays embedded inside JVM PythonFunction / SimplePythonFunction objects for executor shipping.',
    keyDistinction:
      'CloudPickleSerializer is PySpark\'s closure-packaging counterpart to Scala/Java\'s ASM SparkClosureCleaner: it pickles Python bytecode & closure cells on the Python driver rather than cleaning JVM .class $outer fields.',
    startedBy: 'Invoked automatically on the Python driver whenever rdd.map/filter or @udf / @pandas_udf / @udtf is registered or executed',
    monitoredBy: 'Python PicklingError / TypeError on driver before stage launch and serialized PythonFunction byte size',
    stoppedBy: 'Synchronous per-closure serialization on the Python driver; pickled bytes live inside the JVM TaskDescription',
    inputs: [
      'Python callable (UDF, UDTF, mapPartitions lambda), captured closure variables, and module references',
      'Optional serializer pair (CPickleSerializer vs ArrowStreamPandasSerializer) for partition data',
    ],
    outputs: [
      'Pickled command byte array handed across Py4J into JVM SimplePythonFunction',
      'PicklingError if a closure captures an unpicklable object (e.g., SparkContext, socket, open file handle)',
    ],
    internalState: [
      'Bundled pyspark.cloudpickle bytecode & global-variable graph walker',
      'Python major.minor version header verified by pyspark.worker on executors',
      'Captured broadcast variable IDs replacing large closures with compact Broadcast handles',
    ],
    failureClues: [
      'PicklingError: "Could not serialize object: Exception: It appears that you are attempting to reference SparkContext" is PySpark\'s exact analog to Scala TaskNotSerializableException',
      'ModuleNotFoundError on worker during unpickle means a custom Python module was imported by reference instead of shipped via --py-files / venv archive',
      'Large pickled closures (> tens of MiB) bloat every LaunchTask RPC — wrap lookup tables in sc.broadcast()',
    ],
    sourceLinks: [
      {
        label: 'CloudPickleSerializer & BatchedSerializer',
        url: `${SPARK_BLOB}/python/pyspark/serializers.py#L430-L530`,
      },
      {
        label: 'SimplePythonFunction JVM command wrapper',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/api/python/PythonRDD.scala#L60-L125`,
      },
    ],
    docFile: 'cloudpickle-serializer.md',
  },

  'python-accumulator-v2': {
    id: 'python-accumulator-v2',
    name: 'PythonAccumulatorV2 & AccumulatorServer',
    shortName: 'PyAccumulatorV2',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1476, 670],
    languages: ['pyspark'],
    port: '127.0.0.1 TCP',
    process: 'Driver JVM PythonAccumulatorV2 <-> Python Driver TCP AccumulatorServer (pyspark.accumulators)',
    role: 'Bridges Python worker accumulator updates back to the Python driver: executor pyspark.worker pickles per-task accumulator deltas after END_OF_DATA_SECTION, the JVM ships those byte arrays to driver PythonAccumulatorV2, and PythonAccumulatorV2 opens an authenticated loopback socket to the Python driver AccumulatorServer to merge updates via AccumulatorParam.addInPlace.',
    keyDistinction:
      'Unlike Scala/Java AccumulatorV2 (which merges directly inside the Driver JVM with zero sockets), PythonAccumulatorV2 must relay pickled byte arrays across an authenticated driver-local TCP AccumulatorServer so Python driver code can unpickle and merge custom Python AccumulatorParam objects.',
    startedBy: 'PySpark SparkContext lazily starts AccumulatorServer on 127.0.0.1 and registers PythonAccumulatorV2 in JVM SparkContext on first sc.accumulator()',
    monitoredBy: 'AccumulatorServer socket auth logs and Spark UI Stage Accumulators metrics',
    stoppedBy: 'AccumulatorServer TCP listener shuts down during SparkContext.stop()',
    inputs: [
      'Task-local Python _accumulatorRegistry pickled updates emitted by pyspark.worker',
      'JVM AccumulatorV2.merge(other) forwarding byte arrays to Driver JVM PythonAccumulatorV2',
    ],
    outputs: [
      'Authenticated loopback TCP writes from Driver JVM PythonAccumulatorV2 to Python AccumulatorServer',
      'Merged Python driver Accumulator.value updated via AccumulatorParam.addInPlace',
    ],
    internalState: [
      'Driver JVM PythonAccumulatorV2(serverHost, serverPort, secretToken) byte-array buffer list',
      'Python driver AccumulatorServer (SocketServer.TCPServer on 127.0.0.1 with auth secret)',
      'Worker-side pyspark.accumulators._accumulatorRegistry flushed at task completion',
    ],
    failureClues: [
      'Socket failure between Driver JVM and Python AccumulatorServer points to loopback firewall or dead Python driver process',
      'Python accumulators are NOT displayed as typed values inside the JVM until unpickled on the Python driver',
      'Updating sc.accumulator inside lazy RDD transformations can double-count on task retry/recompute, identical to JVM AccumulatorV2',
    ],
    sourceLinks: [
      {
        label: 'PythonAccumulatorV2 JVM bridge to AccumulatorServer',
        url: `${SPARK_BLOB}/core/src/main/scala/org/apache/spark/api/python/PythonRDD.scala#L685-L760`,
      },
      {
        label: 'Python AccumulatorServer & AccumulatorParam',
        url: `${SPARK_BLOB}/python/pyspark/accumulators.py#L190-L295`,
      },
    ],
    docFile: 'python-accumulator-v2.md',
  },

  'arrow-pandas-udf-engine': {
    id: 'arrow-pandas-udf-engine',
    name: 'Arrow & Pandas UDF Vectorized Engine',
    shortName: 'Arrow/Pandas UDF',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1200, 1196],
    languages: ['pyspark'],
    port: 'Arrow IPC',
    process: 'Executor JVM ArrowEvalPythonExec / FlatMapGroupsInPandasExec / MapInPandasExec <-> pyarrow RecordBatch',
    role: 'Accelerates PySpark Pandas UDFs (@pandas_udf), mapInPandas, mapInArrow, and groupBy().applyInPandas() by converting Tungsten UnsafeRow chunks into columnar Apache Arrow RecordBatches in the Executor JVM, streaming them over the local worker socket into pyarrow/pandas/NumPy C vectorized kernels, and decoding returning Arrow batches back into UnsafeRow.',
    keyDistinction:
      'Eliminates row-at-a-time Python pickle overhead by transferring up to spark.sql.execution.arrow.maxRecordsPerBatch (default 10,000) columnar values per IPC batch directly into vectorized C/NumPy/Pandas operations.',
    startedBy: 'Catalyst ExtractPythonUDFs / SparkStrategies lowers Pandas UDFs and applyInPandas into ArrowEvalPythonExec, MapInPandasExec, or FlatMapGroupsInPandasExec',
    monitoredBy: 'SQL UI ArrowEvalPython / FlatMapGroupsInPandas metrics (arrow batches, Python worker RSS in YARN memoryOverhead)',
    stoppedBy: 'Closes ArrowStreamWriter / ArrowStreamReader and releases Netty off-heap Arrow buffers on task completion',
    inputs: [
      'Tungsten InternalRow stream encoded into Arrow RecordBatches via ArrowWriter (bounded by maxRecordsPerBatch)',
      'Serialized Pandas UDF / applyInPandas Python function and target Arrow schema',
    ],
    outputs: [
      'Columnar Arrow IPC stream over local socket to pyspark.worker -> pandas.Series / DataFrame / pyarrow.RecordBatch',
      'Returned Arrow ColumnarBatch decoded back into JVM InternalRow / UnsafeRow',
    ],
    internalState: [
      'ArrowRootAllocator off-heap Arrow buffers in Executor JVM',
      'ArrowStreamPandasUDFSerializer / ArrowStreamGroupUDFSerializer in Python worker',
      'spark.sql.execution.arrow.maxRecordsPerBatch & maxBytesPerBatch batching controls',
    ],
    failureClues: [
      'ArrowTypeError / schema mismatch occurs when the returned pandas.Series dtype diverges from the declared Spark returnType',
      'Large groupBy().applyInPandas() groups load the ENTIRE group into one pandas DataFrame in Python worker RAM — causing YARN PMEM kill (exit 143 / -104) if memoryOverhead is undersized',
      'Lower spark.sql.execution.arrow.maxRecordsPerBatch for very wide tables to cap Arrow IPC buffer RAM',
    ],
    sourceLinks: [
      {
        label: 'FlatMapGroupsInPandasExec grouped Arrow execution',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/python/FlatMapGroupsInPandasExec.scala#L35-L125`,
      },
      {
        label: 'ArrowStreamPandasUDFSerializer Python Arrow IPC',
        url: `${SPARK_BLOB}/python/pyspark/sql/pandas/serializers.py#L140-L265`,
      },
    ],
    docFile: 'arrow-pandas-udf-engine.md',
  },

  'python-udtf-exec': {
    id: 'python-udtf-exec',
    name: 'Python UDTF Operator (EvalPythonUDTF)',
    shortName: 'Python UDTF Exec',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1406, 1196],
    languages: ['pyspark'],
    port: 'UDTF socket IPC',
    process: 'Executor JVM EvalPythonUDTFExec / ArrowEvalPythonUDTFExec + Driver analyze() <-> pyspark.sql.udtf',
    role: 'Executes PySpark User-Defined Table Functions (@udtf) that yield 0..N output rows per input row or partitioned TABLE argument: runs optional polymorphic analyze() during Catalyst analysis on the driver, then streams input batches via EvalPythonUDTFExec / ArrowEvalPythonUDTFExec to Python worker eval() and terminate() generators.',
    keyDistinction:
      'Unlike scalar EvalPythonExec (strictly 1 output value per input row), Python UDTF operators yield arbitrary multi-column, multi-row tables, support TABLE(...) input partitioning/ordering, and can dynamically compute their output schema via analyze().',
    startedBy: 'Catalyst plans PythonUDTF / LATERAL udtf(...) into EvalPythonUDTFExec or ArrowEvalPythonUDTFExec',
    monitoredBy: 'SQL UI EvalPythonUDTF / ArrowEvalPythonUDTF metrics (rows yielded, worker timings, HybridRowQueue spill)',
    stoppedBy: 'Calls Python UDTF terminate() at end of partition/group, drains yielded rows, and releases worker on END_OF_STREAM',
    inputs: [
      'Scalar arguments or partitioned/ordered TABLE(...) input row streams',
      'CloudPickled Python UDTF class implementing eval(*args), optional terminate(), and optional static analyze()',
    ],
    outputs: [
      '0..N generated multi-column rows per input row (lateral-joined with pass-through columns when required)',
      'Polymorphic output StructType resolved at analysis time when analyze() is defined',
    ],
    internalState: [
      'EvalPythonUDTFExec (pickled) vs ArrowEvalPythonUDTFExec (Arrow-vectorized, enabled by default when pyarrow is available)',
      'Partitioned TABLE argument ShuffleExchangeExec + SortExec inserted by EnsureRequirements',
      'Python worker UDTF instance persisting state across eval() calls until terminate()',
    ],
    failureClues: [
      'analyze() runs inside a short-lived driver-side Python worker during Catalyst analysis — import or type errors fail before any YARN stage launches',
      'Returning a tuple/list whose column count differs from returnType / analyze() fails the task with UDTF_INVALID_OUTPUT_ROW_TYPE',
      'Infinite or exploding yield loops inside eval() amplify downstream row counts and can spill HybridRowQueue / shuffle disk',
    ],
    sourceLinks: [
      {
        label: 'EvalPythonUDTFExec physical table-function operator',
        url: `${SPARK_BLOB}/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonUDTFExec.scala#L45-L150`,
      },
      {
        label: 'PySpark @udtf decorator & AnalyzeResult contract',
        url: `${SPARK_BLOB}/python/pyspark/sql/udtf.py#L45-L160`,
      },
    ],
    docFile: 'python-udtf-exec.md',
  },
};
