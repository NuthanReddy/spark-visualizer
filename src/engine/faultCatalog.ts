export type FaultCategoryId =
  | 'none'
  | 'sigkill-137'
  | 'yarn-am-submit'
  | 'driver-executor-jvm'
  | 'scheduling-dag'
  | 'rpc-result-storage'
  | 'shuffle-plane'
  | 'sql-broadcast-aqe'
  | 'python-commit-obs'
  | 'jvm-classpath-serde';

export interface FaultCategoryMeta {
  id: FaultCategoryId;
  label: string;
  short: string;
  description: string;
}

export const FAULT_CATEGORIES: FaultCategoryMeta[] = [
  {
    id: 'none',
    label: 'Happy Path (No Fault)',
    short: 'No Fault',
    description: 'Baseline end-to-end execution with no injected failures or limit breaches.',
  },
  {
    id: 'sigkill-137',
    label: 'SIGKILL (137), YARN Kills & Distinctions',
    short: 'SIGKILL / YARN Kills',
    description:
      'All 10 SIGKILL / exit-137 container termination paths plus key non-137 distinctions (JVM Heap OOM exit 52 and silent node crash).',
  },
  {
    id: 'yarn-am-submit',
    label: 'YARN Submission, AM Lifecycle & Node Health',
    short: 'YARN / AM Submit',
    description:
      'Client pre-submit guards, AM SparkContext init timeout, client-mode disconnects, executor failure ceilings, and AM node exclusion.',
  },
  {
    id: 'driver-executor-jvm',
    label: 'Driver & Executor JVM, Memory & Startup',
    short: 'JVM / Startup',
    description:
      'TaskMemoryManager spill OOM, local dir creation failure, executor heartbeat max failures, TaskReaper kills, closure serialization, SparkContext conflicts, and port exhaustion.',
  },
  {
    id: 'scheduling-dag',
    label: 'DAG Scheduling, Task Retries & Stage Limits',
    short: 'Scheduling / DAG',
    description:
      'Task maxFailures aborts, stage maxConsecutiveAttempts aborts, exclude-on-failure exhaustion, oversized LaunchTask RPCs, and indeterminate stage rollback.',
  },
  {
    id: 'rpc-result-storage',
    label: 'RPC Transport, Results & Block Storage',
    short: 'RPC / Results / Storage',
    description:
      'Driver maxResultSize aborts, lost TaskResultBlock fetches, Netty RPC ask timeouts, BlockManager re-registration rejection, and full local disks.',
  },
  {
    id: 'shuffle-plane',
    label: 'Shuffle Plane & External Shuffle Service',
    short: 'Shuffle / ESS',
    description:
      'FetchFailed recovery (ESS ON vs OFF), missing map output metadata, shuffle block checksum corruption diagnosis, and YarnShuffleService (:7337) outages.',
  },
  {
    id: 'sql-broadcast-aqe',
    label: 'SQL Broadcast, TorrentP2P & Codegen',
    short: 'SQL / Broadcast',
    description:
      '8 GiB / 512M-row broadcast guards, broadcast timeouts, driver HashedRelation OOM, TorrentBroadcast piece corruption, and WholeStageCodegen huge-method fallback.',
  },
  {
    id: 'python-commit-obs',
    label: 'PySpark Workers, Commits & Observability',
    short: 'Python / Commit / Obs',
    description:
      'Python worker crashes, daemon stdout corruption, version mismatch, idle worker kills, speculative commit races, V1 commitJob failures, listener bus drops, and dynamic allocation startup guards.',
  },
  {
    id: 'jvm-classpath-serde',
    label: 'JVM Classpath, Kryo & Security',
    short: 'JVM / Kryo / Sec',
    description:
      'Scala/Java entry-class & dependency ClassNotFoundException (101/13/15), Kryo registration & buffer overflow, Scala/Java UDF exceptions, Encoders.bean errors, and Kerberos NameNode delegation token expiry.',
  },
];

export const FAULT_CATEGORY_MAP: Record<FaultCategoryId, FaultCategoryMeta> =
  Object.fromEntries(FAULT_CATEGORIES.map((c) => [c.id, c])) as Record<
    FaultCategoryId,
    FaultCategoryMeta
  >;

export type SimulatedFault =
  // Category: none (1)
  | 'none'
  // Category: sigkill-137 (12)
  | 'oom-pmem'
  | 'oom-vmem'
  | 'kernel-node-oom'
  | 'yarn-app-kill'
  | 'yarn-preemption'
  | 'exec-intentional-remove'
  | 'heartbeat-timeout'
  | 'am-crash-retry'
  | 'sigterm-escalated-sigkill'
  | 'external-manual-kill'
  | 'jvm-heap-oom'
  | 'node-crash-power-loss'
  // Category: yarn-am-submit (6)
  | 'oversized-request'
  | 'am-sc-timeout'
  | 'client-disconnect'
  | 'max-executor-failures'
  | 'all-nodes-excluded'
  | 'am-invalid-java-opts'
  // Category: driver-executor-jvm (7)
  | 'task-spill-oom'
  | 'disk-dir-create-fail'
  | 'executor-heartbeat-max-fail'
  | 'task-reaper-kill'
  | 'closure-not-serializable'
  | 'multiple-spark-contexts'
  | 'port-bind-exhausted'
  // Category: scheduling-dag (5)
  | 'task-exception'
  | 'stage-max-attempts'
  | 'all-executors-excluded'
  | 'launch-task-too-large'
  | 'indeterminate-stage-rollback'
  // Category: rpc-result-storage (5)
  | 'max-result-size'
  | 'task-result-lost'
  | 'rpc-ask-timeout'
  | 'bm-reregister-rejected'
  | 'disk-store-full'
  // Category: shuffle-plane (4)
  | 'shuffle-fetch-failed'
  | 'metadata-fetch-failed'
  | 'shuffle-corrupt-checksum'
  | 'ess-unavailable'
  // Category: sql-broadcast-aqe (5)
  | 'broadcast-over-8gb'
  | 'broadcast-timeout'
  | 'broadcast-driver-oom'
  | 'torrent-piece-corrupt'
  | 'codegen-huge-method'
  // Category: python-commit-obs (8)
  | 'python-worker-crash'
  | 'python-daemon-stdout-corrupt'
  | 'python-version-mismatch'
  | 'python-idle-timeout'
  | 'speculation-race'
  | 'commit-job-failure'
  | 'listener-bus-overflow'
  | 'dynalloc-no-shuffle-service'
  // Category: jvm-classpath-serde (6)
  | 'class-not-found-jar'
  | 'kryo-unregistered-class'
  | 'kryo-buffer-overflow'
  | 'scala-java-udf-exception'
  | 'java-bean-encoder-error'
  | 'delegation-token-expired';

export interface FaultRequirements {
  shuffle?: boolean;
  fileCommit?: boolean;
  collect?: boolean;
  broadcast?: boolean;
  pythonUdf?: boolean;
  sqlCodegen?: boolean;
  clusterMode?: boolean;
  clientMode?: boolean;
  minExecutors?: number;
  essEnabled?: boolean;
  dynamicAllocation?: boolean;
  jvmApp?: boolean;
  pysparkOnly?: boolean;
}

export interface FaultMeta {
  id: SimulatedFault;
  category: FaultCategoryId;
  label: string;
  short: string;
  severity: 'ok' | 'degraded' | 'recoverable' | 'terminal';
  whoKillsOrDetects: string;
  exitCodeOrException: string;
  configOrLimit: string;
  evidence: string;
  triggerSummary: string;
  sourceRef: string;
  requires?: FaultRequirements;
}

export const SIMULATED_FAULTS: FaultMeta[] = [
  // ---------------------------------------------------------------------------
  // 1. Happy Path (1)
  // ---------------------------------------------------------------------------
  {
    id: 'none',
    category: 'none',
    label: 'None (Happy Path)',
    short: 'None',
    severity: 'ok',
    whoKillsOrDetects: 'None (normal completion)',
    exitCodeOrException: 'Exit 0 (FinalApplicationStatus.SUCCEEDED)',
    configOrLimit: 'All Spark & YARN budgets respected',
    evidence: 'AM unregisters SUCCEEDED, all stages and commits complete with zero failures.',
    triggerSummary: 'All stages, transfers, and cleanup succeed normally.',
    sourceRef: 'ApplicationMaster.scala (finish -> SUCCEEDED)',
  },

  // ---------------------------------------------------------------------------
  // 2. SIGKILL (137), YARN Kills & Distinctions (12)
  // ---------------------------------------------------------------------------
  {
    id: 'oom-pmem',
    category: 'sigkill-137',
    label: 'YARN Physical Memory Kill (PMEM / -104 / cgroup OOM)',
    short: 'YARN PMEM (-104)',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN NodeManager ContainersMonitor / Linux cgroup memory controller',
    exitCodeOrException: 'ContainerExitStatus.KILLED_EXCEEDED_PMEM (-104); lower-level process log may show 137 (SIGKILL)',
    configOrLimit:
      'spark.executor.memory + spark.executor.memoryOverhead (e.g. 400g heap + 20g overhead = 420g on a 425 GiB node leaves tight native/Python/off-heap headroom)',
    evidence:
      'YARN diagnostics: "Container killed by YARN for exceeding physical memory limits. X GB of Y GB physical memory used." YarnAllocator marks exitCausedByApp=true and counts toward task failures.',
    triggerSummary:
      'NM or container cgroup kills executor for exceeding container RSS (KILLED_EXCEEDED_PMEM=-104, exitCausedByApp=true); lower-level container log may report 137.',
    sourceRef: 'YarnAllocator.scala (ContainerExitStatus.KILLED_EXCEEDED_PMEM)',
  },
  {
    id: 'oom-vmem',
    category: 'sigkill-137',
    label: 'YARN Virtual Memory Kill (VMEM / -103)',
    short: 'YARN VMEM (-103)',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN NodeManager virtual memory monitor (ContainersMonitorImpl)',
    exitCodeOrException: 'ContainerExitStatus.KILLED_EXCEEDED_VMEM (-103); process terminated with SIGKILL (137)',
    configOrLimit:
      'yarn.nodemanager.vmem-check-enabled=true; yarn.nodemanager.vmem-pmem-ratio (default 2.1x)',
    evidence:
      'YARN diagnostics: "X GB of Y GB virtual memory used." High mmap/glibc-arena virtual address space != high physical RSS; commonly resolved by disabling vmem-check or raising vmem-pmem-ratio.',
    triggerSummary:
      'NM vmem monitor kills executor whose virtual address mapping exceeds vmem-pmem-ratio even when physical RSS is healthy (KILLED_EXCEEDED_VMEM=-103).',
    sourceRef: 'YarnAllocator.scala (ContainerExitStatus.KILLED_EXCEEDED_VMEM)',
  },
  {
    id: 'kernel-node-oom',
    category: 'sigkill-137',
    label: 'Linux Kernel Node-Wide OOM Killer (SIGKILL 137)',
    short: 'Kernel OOM (137)',
    severity: 'recoverable',
    whoKillsOrDetects: 'Linux kernel OOM killer (host-wide memory pressure across all processes)',
    exitCodeOrException: 'Exit 137 (128 + SIGKILL 9); ExecutorExitCode: "Unknown executor exit code (137) (died from signal 9?)"',
    configOrLimit:
      'Host RAM ceiling (e.g. 425 GiB node oversubscribed by sibling containers, OS page cache, Python/native libs, or yarn.nodemanager.resource.memory-mb)',
    evidence:
      'Node dmesg / syslog: "Out of memory: Killed process <pid> (java) total-vm:... anon-rss:...". Executor log cuts off mid-line with NO JVM shutdown hook and NO YARN PMEM message.',
    triggerSummary:
      'Host RAM exhaustion from sibling containers on a 425 GiB node causes Linux kernel OOM killer to send SIGKILL (137); executor log ends abruptly with no shutdown hook.',
    sourceRef: 'ExecutorExitCode.scala:explainExitCode (exitCode > 128 -> signal 9?)',
  },
  {
    id: 'yarn-app-kill',
    category: 'sigkill-137',
    label: 'YARN Application Kill (yarn application -kill)',
    short: 'YARN App Kill',
    severity: 'terminal',
    whoKillsOrDetects: 'Operator / external scheduler via YARN ResourceManager (AMLauncher & ResourceTracker)',
    exitCodeOrException:
      'YARN ContainerExitStatus: KILLED_BY_RESOURCEMANAGER (-106) for RM heartbeat-directed stop or KILLED_AFTER_APP_COMPLETION (-107) for post-finish NM cleanup; NM stop escalates SIGTERM -> SIGKILL (137); FinalApplicationStatus.KILLED; Spark exitCausedByApp=false',
    configOrLimit: 'External CLI/API kill (yarn application -kill <appId>)',
    evidence:
      'RM transitions application to KILLING/KILLED. In Hadoop 3.4.1 (AMLauncher.java:142-151), RM AMLauncher calls ContainerManagementProtocol.stopContainers(amContainerId) on the AM host NodeManager (am-nm) to terminate the AM container (SIGTERM -> SIGKILL 137), and RM instructs worker NMs via :8031 ResourceTracker heartbeats to stop executor containers: RM-directed stops report KILLED_BY_RESOURCEMANAGER (-106), while post-app-finish NM cleanup reports KILLED_AFTER_APP_COMPLETION (-107). Both are in NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS (exitCausedByApp=false).',
    triggerSummary:
      'yarn application -kill transitions app to KILLED: RM AMLauncher calls ContainerManagementProtocol.stopContainers on am-nm (SIGTERM -> SIGKILL 137) and RM instructs worker NMs via :8031 heartbeats (-106 / -107; Spark exitCausedByApp=false).',
    sourceRef: 'Hadoop 3.4.1 AMLauncher.java:142-151 (stopContainers); YarnAllocator.scala:1058-1064 (-106, -107)',
  },
  {
    id: 'yarn-preemption',
    category: 'sigkill-137',
    label: 'YARN Container Preemption (PREEMPTED / -102)',
    short: 'YARN Preemption',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN CapacityScheduler / FairScheduler preemption monitor',
    exitCodeOrException: 'ContainerExitStatus.PREEMPTED (-102); container killed by NM (SIGTERM -> SIGKILL 137)',
    configOrLimit: 'YARN queue capacity / yarn.resourcemanager.scheduler.monitor.enable',
    evidence:
      'YarnAllocator classifies PREEMPTED as exitCausedByApp=false ("Container preempted by scheduler"); tasks retry without incrementing task/executor failure budgets.',
    triggerSummary:
      'YARN reclaims a healthy executor container for a higher-priority queue (PREEMPTED=-102, exitCausedByApp=false; not charged to failure budget).',
    sourceRef: 'YarnAllocator.scala (ContainerExitStatus.PREEMPTED, SPARK-8167)',
  },
  {
    id: 'exec-intentional-remove',
    category: 'sigkill-137',
    label: 'Spark Intentional Executor Removal (ABORTED -100 / Released)',
    short: 'Executor Removed',
    severity: 'recoverable',
    whoKillsOrDetects: 'Spark ExecutorAllocationManager / explicit sc.killExecutors via YarnAllocator.internalReleaseContainer',
    exitCodeOrException:
      'YARN ContainerExitStatus: ABORTED (-100) via amClient.releaseAssignedContainer() (KILLED_BY_APPMASTER = -105 occurs only on direct NM stopContainers); NM OS kill: SIGTERM -> SIGKILL (137 if stalled); Spark exitCausedByApp=false (alreadyReleased)',
    configOrLimit:
      'spark.dynamicAllocation.executorIdleTimeout (default 60s) or explicit SparkContext.killExecutors',
    evidence:
      'Driver requests KillExecutors -> YarnAllocator records containerId in releasedContainers and calls amClient.releaseAssignedContainer(), so YARN scheduler yields ContainerExitStatus.ABORTED (-100) (KILLED_BY_APPMASTER=-105 is emitted only on NM direct stopContainers). Because alreadyReleased=true, Spark marks exitCausedByApp=false independently of exit status.',
    triggerSummary:
      'Spark DA scale-down or explicit killExecutors releases the container via amClient.releaseAssignedContainer() (YARN status ABORTED=-100; NM may escalate SIGTERM->SIGKILL 137); Spark marks alreadyReleased containers exitCausedByApp=false regardless of status.',
    sourceRef: 'YarnAllocator.scala:855,926-930,1005-1008 (releasedContainers, internalReleaseContainer, exitCausedByApp=false)',
    requires: { minExecutors: 2 },
  },
  {
    id: 'heartbeat-timeout',
    category: 'sigkill-137',
    label: 'Executor Heartbeat Timeout (120s -> Kill & Replace)',
    short: 'Heartbeat Timeout',
    severity: 'recoverable',
    whoKillsOrDetects: 'Driver HeartbeatReceiver.expireDeadHosts',
    exitCodeOrException:
      'ExecutorLostFailure ("Executor heartbeat timed out after ... ms"); timeout itself is NOT SIGKILL, but subsequent NM release may SIGKILL stale JVM',
    configOrLimit: 'spark.network.timeout (default 120s) / spark.executor.heartbeatInterval (default 10s)',
    evidence:
      'Driver logs "Removing executor <id> with no recent heartbeats: ... exceeds timeout 120000 ms", calls sc.killAndReplaceExecutor; AM releases container and NM may kill hung JVM.',
    triggerSummary:
      'Unresponsive executor misses heartbeats for 120s -> HeartbeatReceiver.expireDeadHosts triggers killAndReplaceExecutor; timeout itself is not SIGKILL, though NM cleanup may kill the old process.',
    sourceRef: 'HeartbeatReceiver.scala:210-224 (expireDeadHosts -> killAndReplaceExecutor)',
  },
  {
    id: 'am-crash-retry',
    category: 'sigkill-137',
    label: 'Cluster-Mode AM Crash -> Attempt 2 Container Cleanup',
    short: 'AM Crash Retry',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN ResourceManager (AM failure detection & app attempt retry)',
    exitCodeOrException:
      'AM Attempt 1 dies with SIGKILL (137, 0 JVM shutdown hooks); YARN cleans up Attempt 1 containers and launches Attempt 2 via am-nm startContainers',
    configOrLimit: 'spark.yarn.maxAppAttempts (default yarn.resourcemanager.am.max-attempts = 2)',
    evidence:
      'AM host NodeManager reports Attempt 1 exit 137 on :8031. Because the SIGKILLed Attempt 1 JVM never ran its ShutdownHookManager staging-cleanup hook (and RM never deletes Spark user-owned .sparkStaging/<appId>), staged archives remain intact on HDFS; RM cleans up Attempt 1 executors and launches AM Attempt 2 via ContainerManagementProtocol.startContainers on am-nm, rerunning user code from line 1.',
    triggerSummary:
      'AM Attempt 1 SIGKILLed (0 shutdown hooks, leaving .sparkStaging/<appId> intact on HDFS) -> YARN cleans up Attempt 1 executors -> RM launches AM Attempt 2 via am-nm with a fresh driver and Attempt 2 executors.',
    sourceRef: 'ApplicationMaster.scala (ShutdownHookManager); YARN AMLauncher / RMAppAttempt retry',
    requires: { clusterMode: true },
  },
  {
    id: 'sigterm-escalated-sigkill',
    category: 'sigkill-137',
    label: 'SIGTERM Escalated to SIGKILL (Single Exit 137)',
    short: 'SIGTERM -> SIGKILL',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN NodeManager container-executor signal escalation timer',
    exitCodeOrException:
      'OS signals: SIGTERM -> SIGKILL (9); single process/shell exit code: 137 (128 + 9; process never exited 143); Spark exitCausedByApp depends on whether YARN/Spark already requested container release (true only if unexpected/unreleased)',
    configOrLimit: 'yarn.nodemanager.sleep-delay-before-sigkill.ms (default 250ms)',
    evidence:
      'NM sends SIGTERM (signal 15) and waits yarn.nodemanager.sleep-delay-before-sigkill.ms (250ms). When the stalled JVM fails to exit in time, NM escalates to SIGKILL (signal 9) so the single process exit code is 137 (never 143). Spark attribution depends on release state: already-released or YARN-notified stops are exitCausedByApp=false, whereas an unexpected/unreleased forced kill (modeled here) is attributed as exitCausedByApp=true.',
    triggerSummary:
      'JVM fails to exit on SIGTERM within yarn.nodemanager.sleep-delay-before-sigkill.ms (250ms), so NM escalates to SIGKILL for a single exit code of 137 (never 143); Spark sets exitCausedByApp=true only for unexpected/unreleased kills (false if release was requested).',
    sourceRef: 'YarnAllocator.scala:855-930 (alreadyReleased, exitCausedByApp); Hadoop ContainerLaunch (sleep-delay-before-sigkill.ms)',
  },
  {
    id: 'external-manual-kill',
    category: 'sigkill-137',
    label: 'External Manual / Watchdog kill -9 (SIGKILL 137)',
    short: 'Manual kill -9',
    severity: 'recoverable',
    whoKillsOrDetects: 'Admin, host watchdog, or security/cleanup agent outside YARN',
    exitCodeOrException: 'Exit 137 (128 + 9); ExecutorExitCode: "Unknown executor exit code (137) (died from signal 9?)"',
    configOrLimit: 'External OS signal (SIGKILL / signal 9); bypasses JVM shutdown hooks and YARN memory monitor',
    evidence:
      'NM reports exit code 137 with no YARN PMEM/VMEM diagnostic and no kernel OOM line in dmesg; Spark explains exit 137 generically as "died from signal 9?".',
    triggerSummary:
      'An external admin or watchdog sends kill -9 directly to the executor PID; Spark reports "died from signal 9?" with no YARN memory explanation.',
    sourceRef: 'ExecutorExitCode.scala:62-66 (explainExitCode -> "died from signal 9?")',
  },
  {
    id: 'jvm-heap-oom',
    category: 'sigkill-137',
    label: 'JVM Heap OutOfMemoryError (Exit 52 != SIGKILL 137)',
    short: 'JVM Heap OOM (52)',
    severity: 'recoverable',
    whoKillsOrDetects: 'JVM GC / SparkUncaughtExceptionHandler (NOT YARN PMEM / NOT SIGKILL 137)',
    exitCodeOrException:
      'java.lang.OutOfMemoryError -> SparkExitCode.OOM = 52 (SparkUncaughtExceptionHandler calls System.exit(52), falling back to Runtime.halt(52) if the handler itself encounters OOM)',
    configOrLimit: 'spark.executor.memory (-Xmx) / spark.memory.fraction (default 0.6)',
    evidence:
      'Executor stderr prints java.lang.OutOfMemoryError: Java heap space; SparkUncaughtExceptionHandler walks cause chain (up to spark.executor.killOnFatalErrorDepth=5) and calls System.exit(52) (falling back to Runtime.halt(52) if the uncaught-exception handler itself encounters OOM), NOT 137.',
    triggerSummary:
      'Distinction: Java heap OutOfMemoryError is caught by SparkUncaughtExceptionHandler, which calls System.exit(52) (and falls back to Runtime.halt(52) if the uncaught-exception handler itself encounters OOM), NOT inherently SIGKILL 137.',
    sourceRef: 'SparkUncaughtExceptionHandler.scala:71-108; SparkExitCode.scala:41 (OOM = 52)',
  },
  {
    id: 'node-crash-power-loss',
    category: 'sigkill-137',
    label: 'Worker Node Crash / Power Loss (No Process Exit / YARN ABORTED -100)',
    short: 'Node Crash (No Code)',
    severity: 'recoverable',
    whoKillsOrDetects: 'YARN ResourceManager NMLivelinessMonitor (:8031) & Driver HeartbeatReceiver',
    exitCodeOrException:
      'OS process exit code: NONE (host powered off / kernel panic, no 137 or 52); YARN RM synthesized status: ContainerExitStatus.ABORTED (-100) on NM expiry; Spark exitCausedByApp=false (ABORTED in NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS)',
    configOrLimit: 'yarn.nm.liveness-monitor.expiry-interval-ms (default 600000ms) / spark.network.timeout (120s)',
    evidence:
      'Host power loss or kernel panic produces NO OS process exit code (no 137, -104, or 52). Driver loses RPC/heartbeat and shuffle fetches fail with FetchFailed; separately, when RM NMLivelinessMonitor expires the lost node (:8031), YARN scheduler synthesizes ContainerExitStatus.ABORTED (-100), which Spark classifies as exitCausedByApp=false.',
    triggerSummary:
      'Distinction: Host crash or power loss leaves NO OS process exit code (not 137); RM NMLivelinessMonitor (:8031) expires the lost node and synthesizes ContainerExitStatus.ABORTED (-100, Spark exitCausedByApp=false) while driver sees heartbeat loss and FetchFailed.',
    sourceRef: 'HeartbeatReceiver.scala; YarnAllocator.scala:1062 (ABORTED=-100); YARN NMLivelinessMonitor (:8031)',
  },

  // ---------------------------------------------------------------------------
  // 3. YARN Submission, AM Lifecycle & Node Health (6)
  // ---------------------------------------------------------------------------
  {
    id: 'oversized-request',
    category: 'yarn-am-submit',
    label: 'Pre-Submit Container Memory Limit Exceeded (verifyClusterResources)',
    short: 'Oversized Request',
    severity: 'terminal',
    whoKillsOrDetects: 'YARN Client.verifyClusterResources before submitApplication()',
    exitCodeOrException: 'IllegalArgumentException (required executor or AM memory exceeds RM max memory capability)',
    configOrLimit:
      'yarn.scheduler.maximum-allocation-mb (and yarn.nodemanager.resource.memory-mb) vs executor (heap + off-heap + overhead + PySpark memory) or AM (memory + overhead)',
    evidence:
      'After getNewApplication, Client.verifyClusterResources checks executor memory (heap + off-heap + overhead + PySpark memory) and AM memory (memory + overhead) against yarn.scheduler.maximum-allocation-mb (not vcores) and throws IllegalArgumentException before submitApplication().',
    triggerSummary:
      'Client.verifyClusterResources rejects oversized executor or AM container memory (heap + off-heap + overhead + PySpark memory > yarn.scheduler.maximum-allocation-mb) with IllegalArgumentException before submitApplication().',
    sourceRef: 'Client.scala:225,405-433 (verifyClusterResources)',
  },
  {
    id: 'am-sc-timeout',
    category: 'yarn-am-submit',
    label: 'AM SparkContext Init Timeout (100s -> EXIT 13)',
    short: 'AM SC Timeout',
    severity: 'terminal',
    whoKillsOrDetects: 'ApplicationMaster.runDriver waitForSparkContextInitialized',
    exitCodeOrException: 'ApplicationMaster.EXIT_SC_NOT_INITED = 13 (exits before registerAM / no unregisterAM)',
    configOrLimit: 'spark.yarn.am.waitTime (default 100s)',
    evidence:
      'Cluster-mode user script stalls before SparkContext initializes; AM times out after 100s, logs exitCode 13 (EXIT_SC_NOT_INITED), and exits without calling unregisterAM.',
    triggerSummary:
      'Cluster-mode driver fails to initialize SparkContext within spark.yarn.am.waitTime (100s); AM exits locally with EXIT_SC_NOT_INITED (13) without unregisterAM.',
    sourceRef: 'ApplicationMaster.scala:386,503,535,910 (EXIT_SC_NOT_INITED = 13)',
    requires: { clusterMode: true },
  },
  {
    id: 'client-disconnect',
    category: 'yarn-am-submit',
    label: 'Client-Mode External Driver Disconnect',
    short: 'Client Disconnect',
    severity: 'terminal',
    whoKillsOrDetects: 'ApplicationMaster AMEndpoint.onDisconnected',
    exitCodeOrException:
      'Default: FinalApplicationStatus.SUCCEEDED (EXIT_SUCCESS=0); with flag true: FinalApplicationStatus.FAILED (EXIT_DISCONNECTED=17)',
    configOrLimit: 'spark.yarn.am.clientModeTreatDisconnectAsFailed (default false)',
    evidence:
      'External driver on edge-01 disconnects abruptly; AMEndpoint.onDisconnected finishes SUCCEEDED by default, or FAILED with EXIT_DISCONNECTED (17) when clientModeTreatDisconnectAsFailed=true.',
    triggerSummary:
      'External driver disconnects in client mode; AMEndpoint.onDisconnected finishes SUCCEEDED by default or FAILED (EXIT_DISCONNECTED=17) if clientModeTreatDisconnectAsFailed=true.',
    sourceRef: 'ApplicationMaster.scala:878-890,913 (AMEndpoint.onDisconnected, EXIT_DISCONNECTED = 17)',
    requires: { clientMode: true },
  },
  {
    id: 'max-executor-failures',
    category: 'yarn-am-submit',
    label: 'AM Max Executor Failures Reached (EXIT 11)',
    short: 'Max Exec Failures',
    severity: 'terminal',
    whoKillsOrDetects: 'ApplicationMaster reporter thread checking YarnAllocator / ExecutorFailureTracker count',
    exitCodeOrException: 'ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES = SparkExitCode.EXCEED_MAX_EXECUTOR_FAILURES = 11',
    configOrLimit:
      'spark.executor.maxNumFailures (legacy alias spark.yarn.max.executor.failures); defaults to max(2 * numExecutors, 3) -- under Dynamic Allocation uses spark.dynamicAllocation.maxExecutors, NOT initialExecutors',
    evidence:
      'Unexpected counted executor exits (e.g. exit 1 or generic bad-node failure) enter YarnAllocator default branch and invoke handleResourceAllocationFailure -> ExecutorFailureTracker (explicitly NOT -104 PMEM / -103 VMEM, which set exitCausedByApp=true for task accounting without calling handleResourceAllocationFailure). Once failed count >= maxNumExecutorFailures, AM unregisters FAILED with exit 11: "Max number of executor failures (<N>) reached".',
    triggerSummary:
      'Repeated unexpected counted executor exits (e.g. exit 1 invoking handleResourceAllocationFailure; NOT -104 PMEM) hit spark.executor.maxNumFailures (default max(2 * numExecutors, 3), or 2 * maxExecutors under DA), failing the AM with EXIT_MAX_EXECUTOR_FAILURES (11).',
    sourceRef: 'ApplicationMaster.scala:100,593-596; YarnAllocator.scala:900-922; ExecutorFailureTracker.scala:82-101',
  },
  {
    id: 'all-nodes-excluded',
    category: 'yarn-am-submit',
    label: 'YARN Allocator All Nodes Excluded (EXIT 11)',
    short: 'All Nodes Excluded',
    severity: 'terminal',
    whoKillsOrDetects: 'ApplicationMaster reporter thread via YarnAllocatorNodeHealthTracker.isAllNodeExcluded',
    exitCodeOrException: 'ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES = 11 (FinalApplicationStatus.FAILED)',
    configOrLimit:
      'spark.yarn.executor.launch.excludeOnFailure.enabled=true (default false); YARN queue default-node-label-expression=workers ([worker-a, worker-b], with spark.yarn.am.nodeLabelExpression=am-nodes placing AM on driver-1); spark.excludeOnFailure.application.maxFailedExecutorsPerNode=2 (strict > check: 3rd failure excludes a host); spark.executor.maxNumFailures > 6 (e.g. 8)',
    evidence:
      'Per YarnAllocatorNodeHealthTracker.scala:91, a host is excluded only when failuresOnHost > maxFailuresPerHost (strict >, NOT >=): with maxFailedExecutorsPerNode=2, each worker host is excluded on its 3rd failure (3 > 2, 6 total). With YARN queue default-node-label-expression=workers covering [worker-a, worker-b] and spark.yarn.am.nodeLabelExpression=am-nodes placing the AM on driver-1, Hadoop DefaultAMSProcessor.java:351-365 reads the queue default node label and reports amClient.getClusterNodeCount()=2, so excluding worker-a and worker-b reaches 2/2 nodes (numExcludedNodes >= numClusterNodes) before spark.executor.maxNumFailures=8, calling finish(FAILED, 11, "Due to executor failures all available nodes are excluded").',
    triggerSummary:
      'With spark.yarn.executor.launch.excludeOnFailure.enabled=true and YARN queue default-node-label-expression=workers ([worker-a, worker-b], AM on driver-1 via spark.yarn.am.nodeLabelExpression=am-nodes), strict > check (3 > 2) excludes all 2/2 queue-partition nodes (6 total < spark.executor.maxNumFailures=8), aborting AM with EXIT_MAX_EXECUTOR_FAILURES (11).',
    sourceRef: 'ApplicationMaster.scala:597-600; YarnAllocatorNodeHealthTracker.scala:91-112; Hadoop DefaultAMSProcessor.java:351-365',
  },
  {
    id: 'am-invalid-java-opts',
    category: 'yarn-am-submit',
    label: 'Invalid spark.yarn.am.extraJavaOptions (-Dspark / -Xmx)',
    short: 'Invalid AM Java Opts',
    severity: 'terminal',
    whoKillsOrDetects: 'YARN Client.createContainerLaunchContext (client mode, after getNewApplication and before submitApplication)',
    exitCodeOrException: 'SparkException ("spark.yarn.am.extraJavaOptions is not allowed to set Spark options / -Xmx")',
    configOrLimit: 'spark.yarn.am.extraJavaOptions (use spark.yarn.am.memory instead of -Xmx; ignored in cluster mode)',
    evidence:
      'Per Client.scala:207-234, the client already contacts YARN RM (createApplication / getNewApplication) to obtain an appId and runs verifyClusterResources, then fails while building the AM launch context in createContainerLaunchContext (Client.scala:1094-1105) when spark.yarn.am.extraJavaOptions sets -Dspark or -Xmx before submitApplication() is called.',
    triggerSummary:
      'After contacting YARN RM (getNewApplication) for an appId, Client.createContainerLaunchContext rejects -Dspark or -Xmx in spark.yarn.am.extraJavaOptions in client mode with SparkException before submitApplication().',
    sourceRef: 'Client.scala:207-234,1094-1105 (getNewApplication -> createContainerLaunchContext AM_JAVA_OPTIONS validation)',
    requires: { clientMode: true },
  },

  // ---------------------------------------------------------------------------
  // 4. Driver & Executor JVM, Memory & Startup (7)
  // ---------------------------------------------------------------------------
  {
    id: 'task-spill-oom',
    category: 'driver-executor-jvm',
    label: 'TaskMemoryManager Spill OOM (Non-Fatal to Executor)',
    short: 'Task Spill OOM',
    severity: 'recoverable',
    whoKillsOrDetects: 'TaskMemoryManager.acquireExecutionMemory during MemoryConsumer.spill()',
    exitCodeOrException: 'SparkOutOfMemoryError (errorClass = "SPILL_OUT_OF_MEMORY")',
    configOrLimit: 'spark.memory.fraction / spark.memory.storageFraction / local spill disk health',
    evidence:
      'IOException during spilling throws SparkOutOfMemoryError("SPILL_OUT_OF_MEMORY") on the task thread; unlike raw JVM OOM, the executor JVM stays alive and fails only the task.',
    triggerSummary:
      'TaskMemoryManager hits an IOException while spilling execution memory, throwing SparkOutOfMemoryError (SPILL_OUT_OF_MEMORY) that fails the task without killing the executor JVM.',
    sourceRef: 'TaskMemoryManager.java (acquireExecutionMemory -> SPILL_OUT_OF_MEMORY)',
  },
  {
    id: 'disk-dir-create-fail',
    category: 'driver-executor-jvm',
    label: 'DiskBlockManager Local Dir Creation Failure (EXIT 53)',
    short: 'Local Dir Fail (53)',
    severity: 'recoverable',
    whoKillsOrDetects: 'DiskBlockManager initialization on executor startup',
    exitCodeOrException: 'ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR = 53 (System.exit(53))',
    configOrLimit: 'spark.local.dir / YARN yarn.nodemanager.local-dirs (permissions, read-only mount, or full disk)',
    evidence:
      'Executor logs "Failed to create any local dir." and calls System.exit(53); Spark explains exit 53 as "Failed to create local directory (bad spark.local.dir?)".',
    triggerSummary:
      'DiskBlockManager fails to create any local directory on executor startup and exits immediately with ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR (53).',
    sourceRef: 'DiskBlockManager.scala:63-66; ExecutorExitCode.scala:32 (DISK_STORE_FAILED_TO_CREATE_DIR = 53)',
  },
  {
    id: 'executor-heartbeat-max-fail',
    category: 'driver-executor-jvm',
    label: 'Executor Heartbeat Max Failures Self-Exit (EXIT 56)',
    short: 'HB Max Fail (56)',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor.reportHeartBeat consecutive failure counter on the executor',
    exitCodeOrException: 'ExecutorExitCode.HEARTBEAT_FAILURE = 56 (System.exit(56))',
    configOrLimit: 'spark.executor.heartbeat.maxFailures (default 60) / spark.executor.heartbeatInterval (default 10s)',
    evidence:
      'Executor fails 60 consecutive Heartbeat RPCs to HeartbeatReceiver and self-terminates with System.exit(56): "Unable to send heartbeats to driver."',
    triggerSummary:
      'Executor.reportHeartBeat hits spark.executor.heartbeat.maxFailures (60) consecutive failures and self-exits with ExecutorExitCode.HEARTBEAT_FAILURE (56).',
    sourceRef: 'Executor.scala:1587; ExecutorExitCode.scala:38 (HEARTBEAT_FAILURE = 56)',
  },
  {
    id: 'task-reaper-kill',
    category: 'driver-executor-jvm',
    label: 'TaskReaper Unkillable Task Timeout (EXIT 57)',
    short: 'TaskReaper Kill (57)',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor TaskReaper monitoring a killed task ignoring interrupts',
    exitCodeOrException: 'KilledByTaskReaperException -> ExecutorExitCode.KILLED_BY_TASK_REAPER = 57',
    configOrLimit: 'spark.task.reaper.enabled=true; spark.task.reaper.killTimeout (default -1 / disabled; positive ms enables kill)',
    evidence:
      'Killed task refuses Thread.interrupt past spark.task.reaper.killTimeout; TaskReaper throws KilledByTaskReaperException and SparkUncaughtExceptionHandler exits JVM with code 57.',
    triggerSummary:
      'Uninterruptible killed task outlives spark.task.reaper.killTimeout; TaskReaper throws KilledByTaskReaperException and exits the executor JVM with code 57.',
    sourceRef: 'Executor.scala:1331,1668; SparkUncaughtExceptionHandler.scala:78; ExecutorExitCode.scala:42 (57)',
  },
  {
    id: 'closure-not-serializable',
    category: 'driver-executor-jvm',
    label: 'Closure Not Serializable (Pre-Schedule Driver Abort)',
    short: 'Not Serializable',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver SparkClosureCleaner.clean / SparkContext.clean before task submission',
    exitCodeOrException: 'SparkException: Task not serializable (cause: java.io.NotSerializableException)',
    configOrLimit: 'Hardcoded JavaSerializer in SparkEnv.closureSerializer (used regardless of spark.serializer)',
    evidence:
      'Driver catches unserializable captured field (e.g. non-serializable connection or enclosing class reference) inside SparkClosureCleaner.clean using JavaSerializer directly regardless of spark.serializer, failing immediately with 0 tasks scheduled.',
    triggerSummary:
      'SparkClosureCleaner.clean serializes the task closure with JavaSerializer (regardless of spark.serializer) and aborts before scheduling with SparkException: Task not serializable.',
    sourceRef: 'SparkClosureCleaner.scala:42-48; SparkEnv.scala:734 (new JavaSerializer(conf))',
  },
  {
    id: 'multiple-spark-contexts',
    category: 'driver-executor-jvm',
    label: 'Multiple SparkContexts in One JVM (SPARK-2243)',
    short: 'Multi SparkContext',
    severity: 'terminal',
    whoKillsOrDetects: 'PySpark SparkContext._ensure_initialized (Python side) / JVM SparkContext.assertNoOtherContextIsRunning',
    exitCodeOrException:
      'PySpark: ValueError ("Cannot run multiple SparkContexts at once..."); JVM: SparkException ("Only one SparkContext should be running in this JVM (see SPARK-2243)")',
    configOrLimit: 'Unconditional single-active-SparkContext guard (no override flag; stop active context or reuse SparkSession.builder.getOrCreate())',
    evidence:
      'In PySpark, SparkContext._ensure_initialized rejects a second active SparkContext on the Python side with ValueError before invoking the JVM constructor. On the JVM side, SparkContext.assertNoOtherContextIsRunning unconditionally checks activeContext and throws SparkException (SPARK-2243).',
    triggerSummary:
      'Constructing a second active SparkContext is rejected unconditionally: PySpark SparkContext._ensure_initialized raises ValueError before JVM init, and JVM SparkContext.assertNoOtherContextIsRunning throws SparkException (SPARK-2243).',
    sourceRef: 'python/pyspark/core/context.py:451-475 (_ensure_initialized); SparkContext.scala:3380-3386 (assertNoOtherContextIsRunning)',
  },
  {
    id: 'port-bind-exhausted',
    category: 'driver-executor-jvm',
    label: 'Service Port Bind Retries Exhausted (maxRetries=16)',
    short: 'Port Bind Exhausted',
    severity: 'terminal',
    whoKillsOrDetects: 'Utils.startServiceOnPort (SparkUI / BlockManager / NettyRpcEnv bind loop)',
    exitCodeOrException: 'java.net.BindException (SparkUI / driver startup fails after 16 retries; process exits 1)',
    configOrLimit: 'spark.port.maxRetries (default 16) / spark.ui.port (default 4040)',
    evidence:
      'All 17 candidate ports (base..base+16) are occupied on the host; Utils.startServiceOnPort throws BindException advising to raise spark.port.maxRetries or pick a free port.',
    triggerSummary:
      'Utils.startServiceOnPort exhausts all spark.port.maxRetries (16) bind attempts on occupied ports and aborts service startup with BindException.',
    sourceRef: 'Utils.scala:2283-2358 (startServiceOnPort, spark.port.maxRetries)',
  },

  // ---------------------------------------------------------------------------
  // 5. DAG Scheduling, Task Retries & Stage Limits (5)
  // ---------------------------------------------------------------------------
  {
    id: 'task-exception',
    category: 'scheduling-dag',
    label: 'Deterministic Task Exception (maxFailures=4)',
    short: 'Task Exception',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver TaskSetManager.handleFailedTask',
    exitCodeOrException: 'SparkException: Job aborted due to stage failure: Task X failed 4 times',
    configOrLimit: 'spark.task.maxFailures (default 4)',
    evidence:
      'Same partition hits a deterministic user/data exception across 4 counted attempts; TaskSetManager aborts the TaskSet and fails the active job.',
    triggerSummary:
      'Same partition fails 4 counted attempts on TaskSetManager (spark.task.maxFailures=4), aborting the TaskSet and failing the job.',
    sourceRef: 'TaskSetManager.scala (handleFailedTask, maxTaskFailures)',
  },
  {
    id: 'stage-max-attempts',
    category: 'scheduling-dag',
    label: 'Stage Max Consecutive Attempts Exhausted (4)',
    short: 'Stage Max Attempts',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver DAGScheduler.abortStage after repeated FetchFailed',
    exitCodeOrException: 'SparkException: Stage X has failed the maximum allowable number of times: 4',
    configOrLimit: 'spark.stage.maxConsecutiveAttempts (default 4)',
    evidence:
      'Consecutive stage attempts fail with FetchFailed (e.g. bad shuffle host or unrecoverable fetch loop) until fetchFailedAttemptIds.size reaches 4 and DAGScheduler aborts the stage.',
    triggerSummary:
      'Repeated FetchFailed failures exhaust spark.stage.maxConsecutiveAttempts (4), causing DAGScheduler to abort the stage and fail the job.',
    sourceRef: 'DAGScheduler.scala (maxConsecutiveStageAttempts, spark.stage.maxConsecutiveAttempts)',
    requires: { shuffle: true, minExecutors: 2 },
  },
  {
    id: 'all-executors-excluded',
    category: 'scheduling-dag',
    label: 'Exclude-on-Failure Exhausts All Schedulable Executors',
    short: 'All Execs Excluded',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver HealthTracker / TaskSetExcludeList + TaskSchedulerImpl + TaskSetManager.abortSinceCompletelyExcludedOnFailure',
    exitCodeOrException: 'SparkException: Aborting TaskSet because task cannot run anywhere due to node and executor excludeOnFailure',
    configOrLimit:
      'spark.excludeOnFailure.enabled=true; spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor/Node; spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout (default 120s, used when DA is on or idle excluded executor is killed)',
    evidence:
      'HealthTracker excludes every surviving executor/node. When Dynamic Allocation is OFF and no idle excluded executor exists, TaskSchedulerImpl aborts the TaskSet immediately; when Dynamic Allocation is ON, TaskSchedulerImpl requests new capacity via ExecutorAllocationManager and waits spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout (120s) before aborting.',
    triggerSummary:
      'HealthTracker excludes all executors: with DA off (and no idle excluded executor) TaskSchedulerImpl aborts immediately; with DA on it requests capacity and waits spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout (120s) before aborting.',
    sourceRef: 'HealthTracker.scala; TaskSchedulerImpl.scala:700-725,835-855 (unschedulableTaskSetTimeout); TaskSetManager.scala:790-794',
  },
  {
    id: 'launch-task-too-large',
    category: 'scheduling-dag',
    label: 'Serialized LaunchTask Exceeds RPC Max Message Size',
    short: 'LaunchTask > 128 MiB',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks',
    exitCodeOrException: 'TaskSetManager abort ("Serialized task X was Y bytes, which exceeds max allowed: spark.rpc.message.maxSize")',
    configOrLimit: 'spark.rpc.message.maxSize (default 128 MiB)',
    evidence:
      'Serialized TaskDescription (e.g. giant driver-side closure/collection not broadcast) is >= maxRpcMessageSize; CoarseGrainedSchedulerBackend refuses to send LaunchTask and aborts the TaskSet.',
    triggerSummary:
      'Serialized task payload hits spark.rpc.message.maxSize (128 MiB) in CoarseGrainedSchedulerBackend.launchTasks, aborting the TaskSet before dispatch.',
    sourceRef: 'CoarseGrainedSchedulerBackend.scala (launchTasks -> serializedTask.limit() >= maxRpcMessageSize)',
  },
  {
    id: 'indeterminate-stage-rollback',
    category: 'scheduling-dag',
    label: 'Indeterminate Stage FetchFailed Rollback',
    short: 'Indeterminate Rollback',
    severity: 'recoverable',
    whoKillsOrDetects: 'Driver DAGScheduler.handleTaskCompletion (FetchFailed on indeterminate map stage)',
    exitCodeOrException: 'FetchFailedException -> DAGScheduler rollbackSucceedingStages(mapStage, rollbackCurrentStage = true)',
    configOrLimit:
      'RDD DeterministicLevel.INDETERMINATE; spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch (default true, sets checksumMismatchFullRetryEnabled in ShuffleExchangeExec.scala:607-615; disabling spark.shuffle.checksum.enabled alone does NOT unset it)',
    evidence:
      'Per DAGScheduler.scala:4289-4303,4875-4907 and ShuffleExchangeExec.scala:607-615, immediate full rollback (rollbackSucceedingStages(mapStage, rollbackCurrentStage = true)) occurs for a statically INDETERMINATE map stage when spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false (!checksumMismatchFullRetryEnabled). Disabling spark.shuffle.checksum.enabled alone does NOT unset checksumMismatchFullRetryEnabled; when true (default), full rollback waits until recomputed map output triggers a checksum mismatch.',
    triggerSummary:
      'FetchFailed on a statically INDETERMINATE map stage triggers immediate full rollback (rollbackCurrentStage=true) when spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false (!checksumMismatchFullRetryEnabled); when true (default), rollback triggers on recompute checksum mismatch.',
    sourceRef: 'DAGScheduler.scala:4124-4135,4289-4303,4875-4907; ShuffleExchangeExec.scala:607-615 (spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch)',
    requires: { shuffle: true, minExecutors: 2 },
  },

  // ---------------------------------------------------------------------------
  // 6. RPC Transport, Results & Block Storage (5)
  // ---------------------------------------------------------------------------
  {
    id: 'max-result-size',
    category: 'rpc-result-storage',
    label: 'collect() Exceeds spark.driver.maxResultSize',
    short: 'maxResultSize Abort',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver TaskSetManager.canFetchMoreResults',
    exitCodeOrException: 'SparkException: Total size of serialized results of N tasks is bigger than spark.driver.maxResultSize',
    configOrLimit: 'spark.driver.maxResultSize (simulated 512 MiB; Spark default 1 GiB)',
    evidence:
      'Individual IndirectTaskResult blocks fit in memory, but cumulative result bytes exceed spark.driver.maxResultSize; TaskSetManager.canFetchMoreResults aborts the job.',
    triggerSummary:
      'Cumulative serialized partition results from collect() exceed spark.driver.maxResultSize (512 MiB), causing TaskSetManager.canFetchMoreResults to abort.',
    sourceRef: 'TaskSetManager.scala:813 (canFetchMoreResults)',
    requires: { collect: true },
  },
  {
    id: 'task-result-lost',
    category: 'rpc-result-storage',
    label: 'TaskResultLost (IndirectTaskResult Block Vanished)',
    short: 'TaskResultLost',
    severity: 'recoverable',
    whoKillsOrDetects: 'Driver TaskResultGetter.enqueueSuccessfulTask (getRemoteBytes failure)',
    exitCodeOrException: 'TaskSchedulerImpl.handleFailedTask(tsm, tid, TaskState.FINISHED, TaskResultLost) -> task retried',
    configOrLimit: 'spark.task.maxDirectResultSize (1 MiB); IndirectTaskResult stored with StorageLevel.MEMORY_AND_DISK_SER',
    evidence:
      'Task finished and stored TaskResultBlockId with StorageLevel.MEMORY_AND_DISK_SER, then sent IndirectTaskResult; before driver fetched it via getRemoteBytes, the executor/disk became unavailable or the block was explicitly dropped/corrupted (NOT mere memory eviction). Per TaskResultGetter.scala:89-96, TaskResultGetter invokes scheduler.handleFailedTask(taskSetManager, tid, TaskState.FINISHED, TaskResultLost) (TaskState.FINISHED, not TaskState.FAILED) and reschedules the task before the action completes.',
    triggerSummary:
      'Driver TaskResultGetter fails to fetch an IndirectTaskResult (MEMORY_AND_DISK_SER) block because the executor/disk became unavailable or the block was dropped/corrupted, calling handleFailedTask(tsm, tid, TaskState.FINISHED, TaskResultLost) and retrying before the action completes.',
    sourceRef: 'Executor.scala:1030-1036 (MEMORY_AND_DISK_SER); TaskResultGetter.scala:89-96 (TaskState.FINISHED, TaskResultLost); DAGScheduler.scala',
  },
  {
    id: 'rpc-ask-timeout',
    category: 'rpc-result-storage',
    label: 'NettyRpcEnv Ask Timeout (RpcTimeoutException)',
    short: 'RPC Ask Timeout',
    severity: 'terminal',
    whoKillsOrDetects: 'NettyRpcEnv / RpcEndpointRef.askSync timeout future',
    exitCodeOrException: 'org.apache.spark.rpc.RpcTimeoutException (cause: java.util.concurrent.TimeoutException)',
    configOrLimit: 'spark.rpc.askTimeout / spark.network.timeout (default 120s)',
    evidence:
      'Synchronous RPC ask (e.g. BlockManagerMaster / MapOutputTracker / CoarseGrainedScheduler) receives no reply within 120s due to driver GC pause or network stall, raising RpcTimeoutException.',
    triggerSummary:
      'NettyRpcEnv synchronous ask times out after spark.rpc.askTimeout / spark.network.timeout (120s), throwing RpcTimeoutException.',
    sourceRef: 'RpcTimeout.scala:31 (RpcTimeoutException); NettyRpcEnv.scala',
  },
  {
    id: 'bm-reregister-rejected',
    category: 'rpc-result-storage',
    label: 'BlockManager Re-Registration Rejected (EXIT 58)',
    short: 'BM Reregister (58)',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor BlockManager.reregister after BlockManagerMaster returns INVALID_EXECUTOR_ID',
    exitCodeOrException: 'ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED = 58 (System.exit(58))',
    configOrLimit: 'BlockManagerMasterEndpoint registration state (driver already evicted executor ID)',
    evidence:
      'Executor logs "Exiting executor due to block manager re-registration failure" and calls System.exit(58) because driver rejected the stale executor with INVALID_EXECUTOR_ID.',
    triggerSummary:
      'BlockManager.reregister is rejected by BlockManagerMaster (INVALID_EXECUTOR_ID), causing the executor to exit with ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED (58).',
    sourceRef: 'BlockManager.scala:842-847; ExecutorExitCode.scala:45 (BLOCK_MANAGER_REREGISTRATION_FAILED = 58)',
  },
  {
    id: 'disk-store-full',
    category: 'rpc-result-storage',
    label: 'DiskStore Full (No Space Left on Local Dir)',
    short: 'DiskStore Full',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor DiskStore.put writing spill / RDD / shuffle block to local dir',
    exitCodeOrException: 'java.io.IOException: No space left on device',
    configOrLimit: 'YARN local directory disk capacity (yarn.nodemanager.local-dirs / spark.local.dir)',
    evidence:
      'DiskStore.put catches write failure, removes the partial block file via remove(blockId) to avoid corrupt reads, and rethrows IOException so the task fails and retries elsewhere.',
    triggerSummary:
      'DiskStore.put hits IOException: No space left on device on YARN local dirs, cleans up the partial block, and fails the task for retry.',
    sourceRef: 'DiskStore.scala:64-100 (put -> cleanup via remove(blockId))',
  },

  // ---------------------------------------------------------------------------
  // 7. Shuffle Plane & External Shuffle Service (4)
  // ---------------------------------------------------------------------------
  {
    id: 'shuffle-fetch-failed',
    category: 'shuffle-plane',
    label: 'Shuffle FetchFailed (Lost Producer / Block)',
    short: 'FetchFailed',
    severity: 'recoverable',
    whoKillsOrDetects: 'Reducer ShuffleBlockFetcherIterator -> DAGScheduler.handleTaskCompletion',
    exitCodeOrException: 'org.apache.spark.shuffle.FetchFailedException',
    configOrLimit: 'spark.shuffle.service.enabled (ESS :7337 ON vs OFF) / spark.stage.maxConsecutiveAttempts (4)',
    evidence:
      'With ESS OFF, lost producer invalidates MapOutputTracker locations -> DAGScheduler reruns missing mapId and retries reduce stage. With ESS ON, intact local files still serve via :7337.',
    triggerSummary:
      'Tests ESS ON (producer exit survives via :7337) vs ESS OFF (FetchFailed -> invalidate map output -> rerun mapId -> retry stage).',
    sourceRef: 'ShuffleBlockFetcherIterator.scala; DAGScheduler.scala (case FetchFailed)',
    requires: { shuffle: true, minExecutors: 2 },
  },
  {
    id: 'metadata-fetch-failed',
    category: 'shuffle-plane',
    label: 'MapOutputTracker Missing Shuffle Metadata',
    short: 'MetadataFetchFailed',
    severity: 'recoverable',
    whoKillsOrDetects: 'MapOutputTracker.validateStatus / MapOutputTrackerWorker.getStatuses',
    exitCodeOrException: 'org.apache.spark.shuffle.MetadataFetchFailedException ("Missing an output location for shuffle ...")',
    configOrLimit: 'MapOutputTrackerMaster shuffleStatuses / spark.shuffle.mapOutput.minSizeForBroadcast',
    evidence:
      'MapStatus array has a null entry for a map partition after unregistered/lost output; reducer throws MetadataFetchFailedException and DAGScheduler resubmits missing map tasks.',
    triggerSummary:
      'MapOutputTracker detects a null/missing MapStatus entry for a shuffle partition, raising MetadataFetchFailedException to trigger map recomputation.',
    sourceRef: 'MapOutputTracker.scala (MetadataFetchFailedException)',
    requires: { shuffle: true },
  },
  {
    id: 'shuffle-corrupt-checksum',
    category: 'shuffle-plane',
    label: 'Corrupt Shuffle Block & Checksum Diagnosis',
    short: 'Shuffle Checksum Fail',
    severity: 'recoverable',
    whoKillsOrDetects: 'ShuffleBlockFetcherIterator + ShuffleChecksumHelper.diagnoseCorruption',
    exitCodeOrException: 'FetchFailedException (after repeated corrupt block retries + checksum cause diagnosis)',
    configOrLimit: 'spark.shuffle.checksum.enabled (default true); spark.shuffle.checksum.algorithm (ADLER32)',
    evidence:
      'ShuffleBlockFetcherIterator retries corrupt stream; on repeat corruption it queries the .checksum file on the server to classify DISK_ISSUE vs NETWORK_ISSUE before raising FetchFailed.',
    triggerSummary:
      'Repeated corrupt shuffle block triggers spark.shuffle.checksum.enabled diagnosis (disk vs network) and escalates to FetchFailedException for map recomputation.',
    sourceRef: 'ShuffleBlockFetcherIterator.scala (diagnoseCorruption); ShuffleChecksumHelper.java',
    requires: { shuffle: true, minExecutors: 2 },
  },
  {
    id: 'ess-unavailable',
    category: 'shuffle-plane',
    label: 'External Shuffle Service (:7337) Unavailable / Unregistered',
    short: 'ESS (:7337) Down',
    severity: 'recoverable',
    whoKillsOrDetects: 'Reducer ExternalBlockStoreClient connecting to YARN NM YarnShuffleService (:7337)',
    exitCodeOrException: 'FetchFailedException (connection refused or executor not registered with YarnShuffleService)',
    configOrLimit:
      'spark.shuffle.service.enabled=true; spark.shuffle.service.port (default 7337); spark.files.fetchFailure.unRegisterOutputOnHost (default false -- required true for host-wide output unregistration)',
    evidence:
      'NM YarnShuffleService (:7337) is unreachable or lacks executor metadata; ESS failure does NOT kill executor JVMs (executor liveness is separate from shuffle-output availability). Reducer raises FetchFailedException; DAGScheduler unregisters only that executor by default, and unregisters outputs across the whole host only when spark.files.fetchFailure.unRegisterOutputOnHost=true (default false).',
    triggerSummary:
      'YARN YarnShuffleService (:7337) fails without killing executor JVMs; reducer raises FetchFailedException, unregistering that executor (or whole host only if spark.files.fetchFailure.unRegisterOutputOnHost=true) and recomputing map outputs.',
    sourceRef: 'YarnShuffleService.java:107-108; ExternalBlockStoreClient.java; DAGScheduler.scala:4875-4907 (unRegisterOutputOnHost)',
    requires: { shuffle: true, essEnabled: true, minExecutors: 2 },
  },

  // ---------------------------------------------------------------------------
  // 8. SQL Broadcast, TorrentP2P & Codegen (5)
  // ---------------------------------------------------------------------------
  {
    id: 'broadcast-over-8gb',
    category: 'sql-broadcast-aqe',
    label: 'Broadcast Table Exceeds 8 GiB / Row Limit Guard',
    short: 'Broadcast > 8 GiB',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver BroadcastExchangeExec.relationFuture row-count & built-relation byte guards',
    exitCodeOrException:
      'SparkException (_LEGACY_ERROR_TEMP_2248 for maxBroadcastRows; _LEGACY_ERROR_TEMP_2249 for 8 GiB built-HashedRelation byte guard)',
    configOrLimit:
      'spark.sql.maxBroadcastTableSize (default 8 GiB measured on built HashedRelation, not raw serialized rows); maxBroadcastRows: 512,000,000 for single LongType key (LongHashedRelation), (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong = ((1 << 29) / 1.5).toLong = 357,913,941 (~357.9M) for other hashed keys (UnsafeHashedRelation)',
    evidence:
      'BroadcastExchangeExec enforces numRows >= maxBroadcastRows (512,000,000 for single LongType LongHashedRelation; (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong = (536,870,912 / 1.5).toLong = 357,913,941 (~357.9M) for UnsafeHashedRelation -> _LEGACY_ERROR_TEMP_2248) and then checks built HashedRelation estimatedSize >= 8 GiB (maxBroadcastTableSizeInBytes -> _LEGACY_ERROR_TEMP_2249, measured on the built HashedRelation rather than raw serialized rows).',
    triggerSummary:
      'BroadcastExchangeExec aborts with SparkException when rows hit maxBroadcastRows (512,000,000 for LongHashedRelation; 357,913,941 (~357.9M) for UnsafeHashedRelation; _LEGACY_ERROR_TEMP_2248) or built HashedRelation reaches 8 GiB (_LEGACY_ERROR_TEMP_2249).',
    sourceRef: 'BroadcastExchangeExec.scala:163-216; BytesToBytesMap.java:132 (MAX_CAPACITY = 1 << 29); HashedRelation.scala; QueryExecutionErrors.scala:2344-2362 (_LEGACY_ERROR_TEMP_2248/_2249)',
    requires: { broadcast: true },
  },
  {
    id: 'broadcast-timeout',
    category: 'sql-broadcast-aqe',
    label: 'Broadcast Exchange Timeout (300s)',
    short: 'Broadcast Timeout',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver BroadcastExchangeExec.doExecuteBroadcast (relationFuture.get(timeout, SECONDS))',
    exitCodeOrException: 'SparkException ("Could not execute broadcast in 300 secs", cause: TimeoutException)',
    configOrLimit: 'spark.sql.broadcastTimeout (default 300s)',
    evidence:
      'Build-side broadcast job exceeds spark.sql.broadcastTimeout (300s); BroadcastExchangeExec cancels the tagged Spark broadcast job group (cancelJobsWithTag / cancelJobGroup) AND cancels relationFuture (relationFuture.cancel(true)), then fails the query.',
    triggerSummary:
      'BroadcastExchangeExec exceeds spark.sql.broadcastTimeout (300s), cancels the tagged broadcast job group (cancelJobsWithTag / cancelJobGroup) and cancels relationFuture, then aborts the query.',
    sourceRef: 'BroadcastExchangeExec.scala:179,259-269 (cancelJobsWithTag, relationFuture.cancel(true)); SQLConf.scala:2555',
    requires: { broadcast: true },
  },
  {
    id: 'broadcast-driver-oom',
    category: 'sql-broadcast-aqe',
    label: 'Driver OOM Building Forced HashedRelation',
    short: 'Broadcast Driver OOM',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver BroadcastExchangeExec.relationFuture catch OutOfMemoryError',
    exitCodeOrException:
      'SparkException ("Not enough memory to build and broadcast the table to all worker nodes", _LEGACY_ERROR_TEMP_2250; wrapped via SparkFatalException from OutOfMemoryError)',
    configOrLimit: 'spark.driver.memory / spark.sql.autoBroadcastJoinThreshold (or explicit broadcast() hint)',
    evidence:
      'Forced broadcast collects too many rows into driver heap while building HashedRelation; BroadcastExchangeExec catches OutOfMemoryError and wraps/rethrows SparkException ("Not enough memory to build and broadcast the table to all worker nodes", _LEGACY_ERROR_TEMP_2250) via SparkFatalException to fail the query.',
    triggerSummary:
      'Driver hits OutOfMemoryError while building HashedRelation; BroadcastExchangeExec catches it and wraps/rethrows SparkException ("Not enough memory to build and broadcast the table to all worker nodes").',
    sourceRef: 'BroadcastExchangeExec.scala:230-235; QueryExecutionErrors.scala:2364-2377 (_LEGACY_ERROR_TEMP_2250)',
    requires: { broadcast: true },
  },
  {
    id: 'torrent-piece-corrupt',
    category: 'sql-broadcast-aqe',
    label: 'TorrentBroadcast Piece Checksum Mismatch -> Task Retry',
    short: 'Torrent Piece Corrupt',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor TorrentBroadcast.readBlocks (calcChecksum) -> readBroadcastBlock (tryOrIOException) -> TaskSetManager',
    exitCodeOrException: 'IOException (wrapping SparkException.internalError "corrupt remote block broadcast_X_pieceY") -> fails task attempt',
    configOrLimit: 'spark.broadcast.checksum (default true); spark.broadcast.blockSize (default 4 MiB); spark.task.maxFailures (default 4)',
    evidence:
      'Per TorrentBroadcast.scala:205-215, on checksum mismatch TorrentBroadcast.readBlocks throws a corrupt-block error converted to IOException by readBroadcastBlock (Utils.tryOrIOException), failing the current task attempt (NOT a silent in-place driver fallback inside the same read). TaskSetManager then retries the failed task attempt, which fetches a clean copy.',
    triggerSummary:
      'On piece checksum mismatch (spark.broadcast.checksum=true), TorrentBroadcast.readBlocks throws IOException (via tryOrIOException), failing the current task attempt; TaskSetManager retries the task attempt, which fetches a clean copy.',
    sourceRef: 'TorrentBroadcast.scala:205-215,255-285 (readBlocks, readBroadcastBlock, Utils.tryOrIOException)',
    requires: { broadcast: true, minExecutors: 2 },
  },
  {
    id: 'codegen-huge-method',
    category: 'sql-broadcast-aqe',
    label: 'WholeStageCodegen Huge Method Fallback (INFO > hugeMethodLimit / WARN > 64 KiB)',
    short: 'Codegen Fallback',
    severity: 'degraded',
    whoKillsOrDetects: 'Driver WholeStageCodegenExec.doExecute (CodeGenerator.compile & maxMethodCodeSize > conf.hugeMethodLimit)',
    exitCodeOrException:
      'Non-fatal INFO fallback (compiled 9,420 B > lowered hugeMethodLimit 8,000 B -> child.execute()); sibling WARN fallback if bytecode exceeds JVM 64 KiB (65,535 B) Janino compile limit',
    configOrLimit:
      'spark.sql.codegen.hugeMethodLimit (default 65535 B = JVM 64 KiB max valid method; lowered e.g. 8,000 B for HotSpot JIT); spark.sql.codegen.fallback (default true)',
    evidence:
      'A valid JVM method cannot exceed 64 KiB (65,535 B). With spark.sql.codegen.hugeMethodLimit lowered to 8,000 B (HotSpot JIT limit), a valid 9,420 B compiled method triggers the WholeStageCodegenExec.doExecute INFO log ("the bytecode size (9420) is above the limit 8000") and falls back to interpreted child.execute(). Exceeding the JVM 64 KiB limit instead hits the Janino compilation-failure WARN fallback sibling branch.',
    triggerSummary:
      'Compiled codegen method (9,420 B) exceeds lowered spark.sql.codegen.hugeMethodLimit (8,000 B), logging INFO and falling back to interpreted child.execute() (sibling branch: exceeding the JVM 64 KiB limit hits Janino compile-failure WARN fallback).',
    sourceRef: 'WholeStageCodegenExec.scala:1024-1044 (compile catch WARN; maxMethodCodeSize > hugeMethodLimit INFO); SQLConf.scala:3360',
    requires: { sqlCodegen: true },
  },

  // ---------------------------------------------------------------------------
  // 9. PySpark Workers, Commits & Observability (8)
  // ---------------------------------------------------------------------------
  {
    id: 'python-worker-crash',
    category: 'python-commit-obs',
    label: 'Python Worker Exited Unexpectedly (Crashed)',
    short: 'Python Worker Crash',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor PythonRunner ReaderIterator (socket EOFException / IO error)',
    exitCodeOrException: 'SparkException: Python worker exited unexpectedly (crashed) (cause: EOFException)',
    configOrLimit: 'pyspark.daemon / Python native C-extension crash, segfault, or worker OOM',
    evidence:
      'Executor socket read from pyspark.worker hits premature EOF before END_OF_STREAM; PythonRunner throws "Python worker exited unexpectedly (crashed)" and task retries on a fresh worker.',
    triggerSummary:
      'Python worker dies mid-batch (segfault/crash); PythonRunner hits EOFException ("Python worker exited unexpectedly (crashed)") and retries the task.',
    sourceRef: 'PythonRunner.scala ("Python worker exited unexpectedly (crashed)")',
    requires: { pythonUdf: true, pysparkOnly: true },
  },
  {
    id: 'python-daemon-stdout-corrupt',
    category: 'python-commit-obs',
    label: 'Python Daemon Stdout Corrupted (Bad Handshake)',
    short: 'Daemon Stdout Bad',
    severity: 'terminal',
    whoKillsOrDetects: 'Executor PythonWorkerFactory.startDaemon handshake reader',
    exitCodeOrException: 'SparkException: Bad data in pyspark.daemon\'s standard output. Invalid port number/socket path',
    configOrLimit: 'spark.python.use.daemon (default true); Python startup scripts (sitecustomize.py / usercustomize.py)',
    evidence:
      'Unexpected stdout bytes emitted during pyspark.daemon startup (e.g. print() in sitecustomize.py or conda activation banner) corrupt the binary port/socket handshake in PythonWorkerFactory.startDaemon BEFORE any Python worker process is spawned.',
    triggerSummary:
      'Stray stdout from pyspark.daemon startup corrupts the daemon handshake BEFORE any worker process is spawned; PythonWorkerFactory.startDaemon throws "Bad data in ... standard output".',
    sourceRef: 'PythonWorkerFactory.scala:338-405 (startDaemon -> "Bad data in $daemonModule\'s standard output")',
    requires: { pythonUdf: true, pysparkOnly: true },
  },
  {
    id: 'python-version-mismatch',
    category: 'python-commit-obs',
    label: 'PySpark Driver vs Worker Python Version Mismatch',
    short: 'Python Ver Mismatch',
    severity: 'terminal',
    whoKillsOrDetects: 'pyspark.worker.invoke_udf -> pyspark.worker_util.check_python_version during length-framed init handshake',
    exitCodeOrException: 'PythonException / PySparkRuntimeError [PYTHON_VERSION_MISMATCH]',
    configOrLimit: 'PYSPARK_DRIVER_PYTHON vs PYSPARK_PYTHON (must share exact major.minor version)',
    evidence:
      'Inside the length-framed initialization handshake (WorkerInitInfo.from_stream), the worker reads the partition index (split_index) first and then checks the driver python_version string against sys.version_info[:2], returning PYTHON_VERSION_MISMATCH via the init response before deserializing UDFs or rows.',
    triggerSummary:
      'Worker reads partition index and then checks driver Python version inside the length-framed init handshake, returning PYTHON_VERSION_MISMATCH via the init response before processing rows.',
    sourceRef: 'python/pyspark/worker.py:100-146 (report_init_response, invoke_udf); worker_message.py:230-235; worker_util.py (PYTHON_VERSION_MISMATCH)',
    requires: { pythonUdf: true, pysparkOnly: true },
  },
  {
    id: 'python-idle-timeout',
    category: 'python-commit-obs',
    label: 'Python Worker Idle Timeout Terminate',
    short: 'Python Idle Timeout',
    severity: 'recoverable',
    whoKillsOrDetects: 'Executor PythonRunner idle-timeout monitor via PythonWorkerFactory / LocalPythonWorkerHandle',
    exitCodeOrException: 'PythonWorkerException (worker idle > spark.python.worker.idleTimeoutSeconds; graceful ProcessHandle.destroy())',
    configOrLimit: 'spark.python.worker.idleTimeoutSeconds (> 0) + spark.python.worker.killOnIdleTimeout=true',
    evidence:
      'Python UDF blocks without output past idleTimeoutSeconds; with killOnIdleTimeout=true, PythonRunner/PythonWorkerFactory invokes ProcessHandle.destroy() (graceful terminate, NOT destroyForcibly() / guaranteed SIGKILL) and throws PythonWorkerException so the task retries.',
    triggerSummary:
      'Stuck Python worker exceeds spark.python.worker.idleTimeoutSeconds with killOnIdleTimeout=true; PythonRunner/PythonWorkerFactory invokes ProcessHandle.destroy() (graceful terminate, not destroyForcibly()/SIGKILL) and retries the task.',
    sourceRef: 'PythonRunner.scala:885-905,1465-1475; PythonWorkerHandle.scala:62 (ProcessHandle.destroy()); PythonWorkerFactory.scala:501-532',
    requires: { pythonUdf: true, pysparkOnly: true },
  },
  {
    id: 'speculation-race',
    category: 'python-commit-obs',
    label: 'Speculative Copy + OutputCommitCoordinator Race',
    short: 'Speculation Race',
    severity: 'recoverable',
    whoKillsOrDetects: 'Driver OutputCommitCoordinator.canCommit',
    exitCodeOrException: 'TaskCommitDenied (loser attempt denied commit; NOT counted toward task failures)',
    configOrLimit: 'spark.speculation=true; OutputCommitCoordinator single-committer authorization per partition',
    evidence:
      'Speculative Attempt 1 asks canCommit first and wins authorization; slower Attempt 0 is rejected with TaskCommitDenied and cleanly discarded without penalizing maxTaskFailures.',
    triggerSummary:
      'Speculative Attempt 1 wins OutputCommitCoordinator.canCommit; Attempt 0 receives TaskCommitDenied (not charged to task failure budget).',
    sourceRef: 'OutputCommitCoordinator.scala:95,150 (canCommit, TaskCommitDenied)',
    requires: { fileCommit: true, minExecutors: 2 },
  },
  {
    id: 'commit-job-failure',
    category: 'python-commit-obs',
    label: 'V1 commitJob Failure After All Tasks Succeed',
    short: 'commitJob Fail',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver FileFormatWriter.write -> committer.commitJob(job, commitMsgs)',
    exitCodeOrException: 'IOException / SparkException during driver commitJob -> best-effort committer.abortJob(job)',
    configOrLimit: 'Hadoop MapReduce FileOutputCommitter algorithm v1 (driver-side final rename/commit)',
    evidence:
      'All output tasks commit cleanly to staging, then driver commitJob() throws (e.g. HDFS NameNode rename/quota error); FileFormatWriter invokes best-effort abortJob() and fails the job.',
    triggerSummary:
      'All output tasks succeed, then driver V1 commitJob() throws; FileFormatWriter runs best-effort abortJob() and fails the write.',
    sourceRef: 'FileFormatWriter.scala (committer.commitJob -> committer.abortJob)',
    requires: { fileCommit: true },
  },
  {
    id: 'listener-bus-overflow',
    category: 'python-commit-obs',
    label: 'ListenerBus AsyncEventQueue Overflow (Silent Event Drop)',
    short: 'ListenerBus Drop',
    severity: 'degraded',
    whoKillsOrDetects: 'Driver LiveListenerBus / per-queue AsyncEventQueue.onDropEvent',
    exitCodeOrException:
      'Non-fatal ERROR/WARN log ("Dropping event from queue <name>") -- job continues with stale Spark UI or incomplete event log depending on which queue overflowed',
    configOrLimit:
      'spark.scheduler.listenerbus.eventqueue.capacity (default 10000) or per-queue spark.scheduler.listenerbus.eventqueue.<name>.capacity (separate appStatus and eventLog queues)',
    evidence:
      'LiveListenerBus maintains separate AsyncEventQueue instances, consumers, and queue.<name>.numDroppedEvents counters for appStatus and eventLog. Saturating appStatus drops Spark UI updates without affecting eventLog, unless eventLog overflows its own queue independently.',
    triggerSummary:
      'An AsyncEventQueue (e.g. appStatus) fills at capacity (default 10000) and drops events (queue.<name>.numDroppedEvents++); because appStatus and eventLog have separate queues and consumers, appStatus drops affect Spark UI without affecting eventLog unless eventLog also overflows.',
    sourceRef: 'LiveListenerBus.scala:59-105,256-260 (appStatus, eventLog); AsyncEventQueue.scala:54-85,179-205 (numDroppedEvents)',
  },
  {
    id: 'dynalloc-no-shuffle-service',
    category: 'python-commit-obs',
    label: 'Dynamic Allocation Startup Without Shuffle Preservation',
    short: 'DynAlloc No ESS',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver SparkContext -> ExecutorAllocationManager.validateSettings (after _taskScheduler.start())',
    exitCodeOrException:
      'SparkException ("Dynamic allocation of executors requires one of the following conditions: ..."); in client mode, SparkContext.scala:789 catch calls stop() (exitCode=0) -> YARN AM SUCCEEDED while orders.py exits 1',
    configOrLimit:
      'spark.dynamicAllocation.enabled=true fails only when ALL 4 are off: spark.shuffle.service.enabled=false AND spark.dynamicAllocation.shuffleTracking.enabled=false AND shuffle-block decommission fallback off (spark.decommission.enabled + spark.storage.decommission.shuffleBlocks.enabled) AND no reliable ShuffleDataIO storage (spark.shuffle.sort.io.plugin.class / supportsReliableStorage()=false)',
    evidence:
      'ESS off alone is valid when shuffle tracking is on. ExecutorAllocationManager.validateSettings fails only when all 4 prerequisites are absent (ESS, shuffle tracking, shuffle-block decommission fallback, and reliable ShuffleDataIO storage). In client mode, SparkContext runs this check AFTER YarnClientSchedulerBackend submits the AM and reaches RUNNING; SparkContext.scala:789 catch calls zero-arg stop() (stop(0)), sending Shutdown(0) so the client AM unregisters SUCCEEDED on YARN even though orders.py exits 1.',
    triggerSummary:
      'With dynamic allocation on and all 4 shuffle-preservation options off, ExecutorAllocationManager.validateSettings throws SparkException (in client mode after AM reaches RUNNING, sc.stop() sends Shutdown(0) -> YARN AM SUCCEEDED while orders.py exits 1).',
    sourceRef: 'SparkContext.scala:678,722-736,785-795,2607; ApplicationMaster.scala:834-886; ExecutorAllocationManager.scala:234-255',
    requires: { dynamicAllocation: true, essEnabled: false },
  },

  // ---------------------------------------------------------------------------
  // 10. JVM Classpath, Kryo & Security (6)
  // ---------------------------------------------------------------------------
  {
    id: 'class-not-found-jar',
    category: 'jvm-classpath-serde',
    label: 'Main Class / Dependency ClassNotFoundException (101 / 13 / 15)',
    short: 'ClassNotFound (101/13/15)',
    severity: 'terminal',
    whoKillsOrDetects:
      'Client SparkSubmit lookup (exit 101) vs Cluster AM loadClass/getMethod pre-init (exit 13) vs post-init main() (exit 15)',
    exitCodeOrException:
      'Client: SparkUserAppException(SparkExitCode.CLASS_NOT_FOUND=101); Cluster AM pre-reg loadClass: EXIT_SC_NOT_INITED=13 (0 unregister); Cluster post-reg dependency in main(): EXIT_EXCEPTION_USER_CLASS=15',
    configOrLimit:
      '--class, --jar / --jars, spark.driver.userClassPathFirst (AM ChildFirstURLClassLoader; changes lookup order, cannot supply missing class)',
    evidence:
      'Per SparkSubmit.scala:992-1025 client lookup throws SparkUserAppException(101) before any AM exists. Per ApplicationMaster.scala:741-793 & 377-392, cluster AM loadClass().getMethod() runs BEFORE the user thread handler so a missing main class hits outer AM catch -> unregistered EXIT_SC_NOT_INITED=13 (no unregisterAM). A missing dependency first loaded inside invoked main() AFTER AM registration exits EXIT_EXCEPTION_USER_CLASS=15.',
    triggerSummary:
      'Missing --class fails in client SparkSubmit with exit 101 (before AM) or in cluster AM loadClass() with unregistered EXIT_SC_NOT_INITED=13; a missing dependency first loaded inside main() after AM registration exits EXIT_EXCEPTION_USER_CLASS=15.',
    sourceRef: 'SparkSubmit.scala:992-1025; ApplicationMaster.scala:84-95,377-392,741-793; SparkExitCode.scala:44',
    requires: { jvmApp: true },
  },
  {
    id: 'kryo-unregistered-class',
    category: 'jvm-classpath-serde',
    label: 'Kryo Unregistered Data Class (registrationRequired=true)',
    short: 'Kryo Unregistered',
    severity: 'terminal',
    whoKillsOrDetects:
      'Executor SerializerManager -> KryoSerializer (Kryo 4.0.3) on JVM Pair-RDD shuffle (rdd.reduceByKey / ShuffledRDD[K, OrderEvent]; SQL ShuffleExchangeExec uses UnsafeRowSerializer; closures use JavaSerializer)',
    exitCodeOrException:
      'IllegalArgumentException: Class is not registered (Kryo 4.0.3; wrapped in SparkException on task/serializer boundary)',
    configOrLimit:
      'spark.serializer=org.apache.spark.serializer.KryoSerializer; spark.kryo.registrationRequired=true (default false); spark.kryo.classesToRegister',
    evidence:
      'With spark.serializer=KryoSerializer and spark.kryo.registrationRequired=true, serializing an unregistered JVM DTO (com.acme.OrderEvent) during a JVM Pair-RDD shuffle (rdd.reduceByKey / ShuffledRDD[K, OrderEvent]) causes Kryo 4.0.3 to throw IllegalArgumentException("Class is not registered"). Explicitly distinct from SQL ShuffleExchangeExec (ShuffleExchangeExec.scala:219-265), which always uses UnsafeRowSerializer regardless of spark.serializer, and from task closures, which always use SparkEnv.closureSerializer (JavaSerializer). In PySpark mode, this executes on an auxiliary JVM-side Pair-RDD helper path.',
    triggerSummary:
      'JVM Pair-RDD shuffle (rdd.reduceByKey / ShuffledRDD[K, OrderEvent]) with spark.kryo.registrationRequired=true throws Kryo 4.0.3 IllegalArgumentException("Class is not registered") (SQL ShuffleExchangeExec uses UnsafeRowSerializer; closures use JavaSerializer).',
    sourceRef: 'KryoSerializer.scala:90,143; Kryo.scala:24-27; SerializerManager.scala:85-107; ShuffleExchangeExec.scala:219-265',
  },
  {
    id: 'kryo-buffer-overflow',
    category: 'jvm-classpath-serde',
    label: 'Kryo Buffer Overflow (> spark.kryoserializer.buffer.max)',
    short: 'Kryo Buffer Overflow',
    severity: 'terminal',
    whoKillsOrDetects:
      'Driver JVM PairRDDFunctions.aggregateByKey -> bounded KryoSerializerInstance.serialize() (in PySpark via JVM helper; Python RDD.aggregateByKey uses copy.deepcopy)',
    exitCodeOrException:
      'SparkException (errorClass="KRYO_BUFFER_OVERFLOW", wrapping KryoException: Buffer overflow)',
    configOrLimit:
      'spark.kryoserializer.buffer.max (default 64m, max < 2048m); applies to bounded KryoSerializerInstance.serialize(), NOT streamed shuffle partitions or SQL ShuffleExchangeExec (UnsafeRowSerializer)',
    evidence:
      'Per KryoSerializer.scala:448-461, bounded KryoSerializerInstance.serialize() catches KryoException("Buffer overflow") and wraps it into SparkException(KRYO_BUFFER_OVERFLOW) -- e.g. JVM PairRDDFunctions.aggregateByKey (PairRDDFunctions.scala:155-165) serializing an oversized zeroValue (> 64m) on the driver. In PySpark mode, this invokes a JVM helper because Python RDD.aggregateByKey (python/pyspark/core/rdd.py:3909-3919) clones zeroValue via copy.deepcopy rather than KryoSerializerInstance.serialize().',
    triggerSummary:
      'Driver JVM PairRDDFunctions.aggregateByKey exceeds spark.kryoserializer.buffer.max (64 MiB default) in bounded KryoSerializerInstance.serialize() -> SparkException(KRYO_BUFFER_OVERFLOW) (in PySpark via JVM helper; Python RDD.aggregateByKey uses copy.deepcopy).',
    sourceRef: 'KryoSerializer.scala:448-461; PairRDDFunctions.scala:155-165; python/pyspark/core/rdd.py:3909-3919; Kryo.scala:61-64',
  },
  {
    id: 'scala-java-udf-exception',
    category: 'jvm-classpath-serde',
    label: 'Scala / Java UDF Exception (FAILED_EXECUTE_UDF)',
    short: 'Scala/Java UDF Fail',
    severity: 'terminal',
    whoKillsOrDetects:
      'Executor JVM ScalaUDF / Java UDF1<Double, Double> (also PySpark registerJavaFunction)',
    exitCodeOrException:
      'SparkException (errorClass="FAILED_EXECUTE_UDF") -> Stage aborted (4 counted failures of same task); cluster AM exits 15 (Scala/Java) or 1 (PySpark); client launcher exits 1 while SC stop() sends Shutdown(0)',
    configOrLimit:
      'spark.task.maxFailures=4 (4 counted failures of same task); deterministic user exception inside ScalaUDF / Java UDF1.call(Double)',
    evidence:
      'Both generated and interpreted ScalaUDF paths (ScalaUDF.scala:1171) wrap ordinary exceptions from user Scala/Java UDF code (including PySpark spark.udf.registerJavaFunction) as SparkException(FAILED_EXECUTE_UDF). After 4 counted failures of the same task, TaskSetManager aborts the stage; uncaught exit in cluster mode selects AM exit 15 (Scala/Java) or 1 (PySpark), while client mode unwinds the launcher with exit 1 and YarnClientSchedulerBackend.stop() sends Shutdown(0) before super.stop().',
    triggerSummary:
      'Deterministic user exception inside ScalaUDF / Java UDF1 (including PySpark registerJavaFunction) wraps as SparkException(FAILED_EXECUTE_UDF), exhausts 4 task attempts, and aborts the stage.',
    sourceRef: 'ScalaUDF.scala:1125-1175; ApplicationMaster.scala:775-785,911; YarnClientSchedulerBackend.scala:174-190',
    requires: { sqlCodegen: true },
  },
  {
    id: 'java-bean-encoder-error',
    category: 'jvm-classpath-serde',
    label: 'Encoders.bean() Derivation Error (Circular / Interface / Unsupported)',
    short: 'JavaBean Encoder Err',
    severity: 'terminal',
    whoKillsOrDetects: 'Driver Encoders.bean() -> JavaTypeInference.encoderFor (before SparkSession.createDataset)',
    exitCodeOrException:
      'SparkUnsupportedOperationException (errorClass="CIRCULAR_CLASS_REFERENCE"; siblings: ENCODER_NOT_FOUND, BEAN_ENCODER_INTERFACE_NOT_SUPPORTED)',
    configOrLimit:
      'Encoders.bean(OrderNode.class) requires acyclic public JavaBean with no-arg constructor and getter/setter pairs',
    evidence:
      'Per Encoders.scala:211, JavaTypeInference.scala:153-186, and ExecutionErrors.scala:181-192, static Encoders.bean() fails directly on the driver during JavaBeanEncoder derivation (before spark.createDataset() is ever entered) when a readable bean property introduces a circular reference (CIRCULAR_CLASS_REFERENCE), an interface type (BEAN_ENCODER_INTERFACE_NOT_SUPPORTED), or an unsupported field type (ENCODER_NOT_FOUND) -- before any stage launches.',
    triggerSummary:
      'Driver Encoders.bean() -> JavaTypeInference.encoderFor rejects circular references (CIRCULAR_CLASS_REFERENCE), interface fields, or unsupported types before spark.createDataset() or any stage runs.',
    sourceRef: 'Encoders.scala:211; JavaTypeInference.scala:153-190; ExecutionErrors.scala:181-192',
    requires: { jvmApp: true },
  },
  {
    id: 'delegation-token-expired',
    category: 'jvm-classpath-serde',
    label: 'HDFS NameNode Delegation Token Expired (InvalidToken)',
    short: 'Delegation Token Exp',
    severity: 'terminal',
    whoKillsOrDetects:
      'HDFS NameNode (:8020) SASL IPC connection setup (AbstractDelegationTokenSecretManager.retrievePassword) rejecting expired HDFS delegation token (NOT DataNode block access token)',
    exitCodeOrException:
      'RemoteException(org.apache.hadoop.security.token.SecretManager$InvalidToken: token can not be found in cache / is expired)',
    configOrLimit:
      'Hadoop 3.4.1 defaults: 24h renewal interval, 7-day max lifetime (hdfs-default.xml:1458-1471); Spark HadoopDelegationTokenManager renews at 0.75 * (earliestProviderRenewalTime - now) when keytab/ticket-cache renewal is enabled; retry via spark.security.credentials.retryWait',
    evidence:
      'Per Hadoop Client.java:764-800 and AbstractDelegationTokenSecretManager.java:528-551, Hadoop authenticates HDFS delegation tokens during SASL IPC connection setup (an existing authenticated connection is reused without re-checking the token on every call). When a long-running job exceeds delegation token lifetime without Spark HadoopDelegationTokenManager acquiring fresh tokens, a fresh NameNode (:8020) IPC connection / reconnect fails during SASL token auth (retrievePassword -> SecretManager$InvalidToken) BEFORE the pending getBlockLocations/create metadata RPC executes (distinct from DataNode :9866 per-block access tokens and OIDC UserCredentialManager).',
    triggerSummary:
      'Expired HDFS delegation token (past 24h renewal / 7d max lifetime without HadoopDelegationTokenManager 0.75x refresh) causes a fresh NameNode (:8020) IPC connection / reconnect to fail during SASL token auth (AbstractDelegationTokenSecretManager.retrievePassword -> SecretManager$InvalidToken) before the pending metadata RPC executes.',
    sourceRef:
      'HadoopDelegationTokenManager.scala:274-313; SupportsDelegationToken.scala:75-93; Hadoop Client.java:764-800; AbstractDelegationTokenSecretManager.java:528-551',
  },
];

export const FAULT_MAP: Record<SimulatedFault, FaultMeta> = Object.fromEntries(
  SIMULATED_FAULTS.map((f) => [f.id, f]),
) as Record<SimulatedFault, FaultMeta>;

export const FAULTS_BY_CATEGORY: Record<FaultCategoryId, FaultMeta[]> =
  Object.fromEntries(
    FAULT_CATEGORIES.map((c) => [
      c.id,
      SIMULATED_FAULTS.filter((f) => f.category === c.id),
    ]),
  ) as Record<FaultCategoryId, FaultMeta[]>;
