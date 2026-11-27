import type { ComponentSpec } from './types';

export type CatalystJvmComponentId =
  // 11 Shared Core / Catalyst / Codegen / Tungsten / SQL components
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
  // 7 Scala & Java Native JVM components (NOT in PySpark)
  | 'closure-cleaner'
  | 'kryo-registrator'
  | 'typed-dataset-ops'
  | 'scala-java-udf'
  | 'typed-aggregator'
  | 'scala-repl-compiler'
  | 'java-spark-api';

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
};
