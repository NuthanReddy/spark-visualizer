import type { ComponentId, ComponentSpec } from './types';

export const COMPONENTS: Record<ComponentId, ComponentSpec> = {
  // 1. Submission & Allocation (4)
  'spark-submit': {
    id: 'spark-submit',
    name: 'spark-submit Launcher',
    shortName: 'spark-submit',
    subsystem: 'submission',
    hostZone: 'submit-host',
    clusterPos: [144, 106],
    process: 'Submit JVM (org.apache.spark.deploy.SparkSubmit) on submit host',
    role: 'Parses CLI arguments and properties, resolves deploy mode and primary resources, and invokes YarnClusterApplication (cluster) or deploy.PythonRunner (client) inside the submit JVM.',
    keyDistinction:
      'In cluster mode the submit JVM runs the YARN submission client instead; the ApplicationMaster starts Python later.',
    startedBy: 'User shell or orchestrator invoking bin/spark-submit -> spark-class',
    monitoredBy: 'Submitting user/orchestrator via process exit code and --verbose output',
    stoppedBy: 'Exits after YarnClusterApplication finishes monitoring (cluster) or PythonRunner exits (client)',
    inputs: [
      'CLI options (--master yarn, --deploy-mode, --num-executors, --py-files, --conf)',
      'Primary .py script path/URI and post-script application arguments',
      'spark-defaults.conf, HADOOP_CONF_DIR / YARN_CONF_DIR, and process environment',
    ],
    outputs: [
      '(childArgs, childClasspath, sparkConf, childMainClass) from prepareSubmitEnvironment',
      'In-JVM dispatch to YarnClusterApplication (cluster) or deploy.PythonRunner (client)',
      'Mapped YARN configs (spark.executor.instances, spark.yarn.dist.pyFiles, spark.yarn.isPython=true)',
    ],
    internalState: [
      'SparkSubmitArguments (primaryResource, isPython, childArgs, sparkProperties)',
      'SparkSubmit.prepareSubmitEnvironment resolved SparkConf and classpath',
      'MutableURLClassLoader installed before runMain invokes SparkApplication.start',
    ],
    failureClues: [
      'Missing HADOOP_CONF_DIR/YARN_CONF_DIR or unloadable YARN classes fail in the submit process before any container exists',
      'Options placed after orders.py are passed as script arguments rather than Spark options; verify with --verbose',
      'With spark.yarn.submit.waitAppCompletion=false in cluster mode, shell exit 0 does not prove job success',
    ],
    sourceLinks: [
      {
        label: 'SparkSubmit.doSubmit & dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L70-L104',
      },
      {
        label: 'SparkSubmitArguments parsing & defaults',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L93-L218',
      },
      {
        label: 'YARN cluster Python dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L813-L843',
      },
    ],
    docFile: 'spark-submit.md',
  },

  'yarn-client': {
    id: 'yarn-client',
    name: 'Spark YARN Submission Client',
    shortName: 'YARN Client',
    subsystem: 'submission',
    hostZone: 'submit-host',
    clusterPos: [324, 106],
    process: 'Submit JVM (cluster) or client-mode Driver JVM (org.apache.spark.deploy.yarn.Client)',
    role: 'Obtains a YARN ApplicationId, validates per-container memory caps, stages local resources and __spark_conf__ to HDFS, submits the AM ApplicationSubmissionContext, and polls ApplicationReport.',
    keyDistinction:
      'Staging makes a file available to the cluster; localization makes it visible in one container working directory.',
    startedBy: 'YarnClusterApplication.start (cluster mode) or YarnClientSchedulerBackend.start (client mode)',
    monitoredBy: 'Caller polling monitorApplication reports and LauncherBackend state updates',
    stoppedBy: 'Stops after app completes or waitAppCompletion=false (cluster), or on YarnClientSchedulerBackend.stop (client)',
    inputs: [
      'ClientArguments (userClass, primaryPyFile, userArgs) and SparkConf / YarnConfiguration',
      'Local script, --py-files / --files / --archives, and PySpark runtime libraries',
      'RM getNewApplication() response (ApplicationId and max container capability)',
    ],
    outputs: [
      'Uploaded files and __spark_conf__ archive under .sparkStaging/<applicationId>',
      'ApplicationSubmissionContext + AM ContainerLaunchContext submitted to RM ClientRMService',
      'Polled YarnApplicationState and FinalApplicationStatus reports',
    ],
    internalState: [
      'Hadoop YarnClient handle and assigned appId: ApplicationId',
      'stagingDirPath (.sparkStaging/<applicationId>) for upload and failure cleanup',
      'ClientDistributedCacheManager and cachedResourcesConf (AM vs executor LocalResource manifests)',
    ],
    failureClues: [
      'ApplicationId printed followed by "above the max threshold" means verifyClusterResources rejected container memory before submitting',
      'File present on submit host but missing in AM points to copyFileToRemote rules or NM localization diagnostics',
      'YARN state RUNNING or fire-and-forget return does not prove orders.py finished or committed output',
    ],
    sourceLinks: [
      {
        label: 'Client.submitApplication & staging order',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/Client.scala#L194-L238',
      },
      {
        label: 'AM ContainerLaunchContext & command',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/Client.scala#L1119-L1195',
      },
      {
        label: 'ClientDistributedCacheManager manifest writer',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ClientDistributedCacheManager.scala#L62-L100',
      },
    ],
    docFile: 'yarn-client.md',
  },

  'application-master': {
    id: 'application-master',
    name: 'YARN ApplicationMaster',
    shortName: 'ApplicationMaster',
    subsystem: 'submission',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [148, 290],
    process: 'YARN AM Container JVM (ApplicationMaster in cluster mode / ExecutorLauncher in client mode)',
    role: 'Represents one YARN application attempt: starts the user Driver thread (cluster) or connects to the external driver (client), registers the attempt with the RM, hosts YarnAllocator and the Reporter loop, and unregisters final attempt status.',
    keyDistinction:
      'In client mode the ApplicationMaster connects to an existing external driver and never starts the Python script.',
    startedBy: 'Selected NodeManager launching ContainerLaunchContext submitted via RM AMLauncher',
    monitoredBy: 'ResourceManager via AM allocate heartbeats and container exit status',
    stoppedBy: 'User thread exit (cluster), driver Shutdown/disconnect (client), reporter failure threshold, or JVM shutdown hook',
    inputs: [
      'Localized __spark_conf__, CONTAINER_ID (ApplicationAttemptId), and ApplicationMasterArguments',
      'SparkContext rendezvous via sparkContextPromise (cluster) or external driver host:port (client)',
      'Driver YarnScheduler RPC messages: RequestExecutors, KillExecutors, GetExecutorLossReason, Shutdown',
    ],
    outputs: [
      'RM registerApplicationMaster(host, port, trackingUrl) and finishApplicationMaster(finalStatus)',
      'RegisterClusterManager(self) sent to driver YarnScheduler endpoint after allocator creation',
      'Reporter thread driving YarnAllocator.allocateResources() heartbeats',
    ],
    internalState: [
      'sparkContextPromise, allocator: YarnAllocator, and reporterThread',
      'Attempt outcome guards: registered, finished, unregistered, exitCode, finalStatus, finalMsg',
      'AMEndpoint (YarnAM) registered after allocator setup to handle driver capacity/loss RPCs',
    ],
    failureClues: [
      '"Waiting for spark context initialization" timeout means cluster AM never reached first allocator poll; inspect AM/Python stdout first',
      'Normal script return before AM registration is recorded as FAILED / EXIT_SC_NOT_INITED (13)',
      'Non-last attempt failure skips unregister and staging cleanup so RM can launch a fresh retry attempt',
    ],
    sourceLinks: [
      {
        label: 'ApplicationMaster fields & attempt state',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ApplicationMaster.scala#L71-L135',
      },
      {
        label: 'Cluster runDriver & SparkContext rendezvous',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ApplicationMaster.scala#L496-L540',
      },
      {
        label: 'AMEndpoint RPC handlers (YarnAM)',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ApplicationMaster.scala#L818-L876',
      },
    ],
    docFile: 'application-master.md',
  },

  'yarn-allocator': {
    id: 'yarn-allocator',
    name: 'YarnAllocator',
    shortName: 'YarnAllocator',
    subsystem: 'submission',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [148, 362],
    process: 'AM JVM (org.apache.spark.deploy.yarn.YarnAllocator)',
    role: 'Reconciles desired executor targets per ResourceProfile with YARN AMRMClient container requests, matches RM grants by host/rack/any, launches executor JVMs on NodeManagers via ExecutorRunnable, and classifies completed container losses.',
    keyDistinction:
      'The allocator can count an executor as running before that executor JVM actually registers with the Spark driver.',
    startedBy: 'YarnRMClient.createAllocator inside ApplicationMaster after RM registration succeeds',
    monitoredBy: 'AM Reporter thread polling allocateResources() and checking executor failure limits',
    stoppedBy: 'Reporter finally block calling allocator.stop() -> launcherPool.shutdownNow()',
    inputs: [
      'Desired absolute executor totals per ResourceProfile, locality hints, and excluded nodes from YarnAM',
      'Explicit killExecutor(executorId) and GetExecutorLossReason(executorId) requests',
      'RM AllocateResponse grants, updated node statuses, and completed ContainerStatus records',
    ],
    outputs: [
      'AMRMClient ContainerRequest adds/removals keyed by ResourceProfile ID as YARN priority',
      'NMClient.startContainer via ContainerLauncher pool (ExecutorRunnable) with --driver-url and --executor-id',
      'ExecutorExited loss reasons and unsolicited RemoveExecutor notifications to the Spark driver',
    ],
    internalState: [
      'amClient: AMRMClient[ContainerRequest] tracking pending YARN requests',
      'rpIdToTargetNumExecutors, numExecutorsStartingPerResourceProfileId, and running executor ID sets',
      'executorIdToContainer, containerIdToExecutorId, releasedContainers, and launcherPool thread pool',
    ],
    failureClues: [
      'Pending asks without grants in AM logs point to RM queue capacity, node fit, or placement exclusions',
      'AM launch log with no executor in Spark UI points to NM localization or executor-to-driver URL reachability',
      'Container exit diagnostics distinguish preemption (non-app failure) from PMEM kills (app-attributed failure)',
    ],
    sourceLinks: [
      {
        label: 'YarnAllocator state & maps',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala#L67-L148',
      },
      {
        label: 'allocateResources heartbeat & deficit arithmetic',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala#L439-L473',
      },
      {
        label: 'runAllocatedContainers & NM ExecutorRunnable',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/YarnAllocator.scala#L755-L845',
      },
    ],
    docFile: 'yarn-allocator.md',
  },

  // 2. Cluster & HDFS Services (5)
  'resource-manager': {
    id: 'resource-manager',
    name: 'YARN ResourceManager',
    shortName: 'ResourceManager',
    subsystem: 'cluster-hdfs',
    hostZone: 'yarn-control',
    clusterPos: [658, 106],
    port: ':8032 / :8030 / :8031',
    process: 'Long-lived Hadoop RM daemon JVM (shared cluster service, HA active/standby)',
    role: 'Arbitrates cluster queue capacity, tracks live NodeManagers and application attempts, directs AMLauncher to start the AM container, and grants executor containers in response to AM allocate heartbeats.',
    keyDistinction:
      'YARN application state RUNNING means the AM registered; Spark may still have zero executors.',
    startedBy: 'Cluster operator or service manager (outside Spark application lifecycle)',
    monitoredBy: 'Cluster monitoring / HA ZooKeeper failover controller; NMs heartbeat to ResourceTrackerService',
    stoppedBy: 'Cluster operator or HA failover; Spark application completion releases resources, not the RM',
    inputs: [
      'ClientRMService (:8032): getNewApplication, submitApplication, getApplicationReport',
      'ApplicationMasterService (:8030): registerApplicationMaster, allocate (asks/releases), finishApplicationMaster',
      'ResourceTrackerService (:8031): registerNodeManager and nodeHeartbeat health/container statuses',
    ],
    outputs: [
      'AMLauncher startContainers request to the chosen NodeManager for the AM container',
      'AllocateResponse carrying Container grants, NM tokens, headroom, and completed ContainerStatus records',
      'Node heartbeat cleanup instructions and application-level final reports',
    ],
    internalState: [
      'RMApp and RMAppAttempt state machines plus scheduler queue accounting',
      'Registered live NodeId capacity map and allocated container ledger',
      'Cached last AllocateResponse per responseId for idempotent AM heartbeat retry',
    ],
    failureClues: [
      'Long-lived ACCEPTED with no AM container ID points to queue limits, node fit, or placement rules',
      'RUNNING with zero Spark executors requires comparing AM asks vs grants in RM scheduler view before checking NM logs',
      'RM unavailability stalls new allocations; surviving containers continue until recovery or process exit',
    ],
    sourceLinks: [
      {
        label: 'Hadoop 3.4.1 YARN architecture docs',
        url: 'https://hadoop.apache.org/docs/r3.4.1/hadoop-yarn/hadoop-yarn-site/YARN.html',
      },
      {
        label: 'Hadoop AMLauncher.java',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/amlauncher/AMLauncher.java',
      },
      {
        label: 'Hadoop ApplicationMasterService.java',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/ApplicationMasterService.java',
      },
    ],
    docFile: 'resource-manager.md',
  },

  'node-manager': {
    id: 'node-manager',
    name: 'YARN NodeManager',
    shortName: 'NodeManager',
    subsystem: 'cluster-hdfs',
    hostZone: 'worker-node-a',
    clusterPos: [156, 758],
    port: 'dynamic (:0)',
    process: 'Long-lived Hadoop NM daemon JVM (one per YARN worker node)',
    role: 'Validates container grants, localizes staged files into container working directories, spawns and monitors AM/executor OS process trees, enforces memory/disk health, hosts optional YarnShuffleService, and reports container exits to the RM.',
    keyDistinction:
      'If localization fails, the executor JVM never starts; check the NodeManager localizer log rather than executor stderr.',
    startedBy: 'Cluster operator or node service manager (independent of Spark jobs)',
    monitoredBy: 'ResourceManager via ResourceTracker nodeHeartbeat and local disk health checks',
    stoppedBy: 'Cluster operator or node outage (optional NM recovery can reattach to surviving containers)',
    inputs: [
      'ContainerManagementProtocol startContainers / stopContainers / getContainerStatuses from RM (AM) or AM (executors)',
      'Container token + ContainerLaunchContext (LocalResources, env, Java command, credentials)',
      'RM nodeHeartbeat responses carrying container/application cleanup commands',
    ],
    outputs: [
      'Localized files under yarn.nodemanager.local-dirs and spawned AM/executor JVM process trees',
      'nodeHeartbeat reports carrying node health and terminal container exit status/diagnostics',
      'Container stdout/stderr logs and optional log aggregation to shared storage',
    ],
    internalState: [
      'ContainerManagerImpl authorization state and active ContainerImpl state machines',
      'Localizer resource cache across yarn.nodemanager.local-dirs and log-dirs',
      'ContainerExecutor process monitor and auxiliary services map (e.g., spark_shuffle)',
    ],
    failureClues: [
      'Container granted with no executor stderr points to NM localizer errors, missing staged URI, or bad local-dirs',
      'JVM starts but no Spark tasks arrive points to container stderr driver registration errors rather than NM launch failure',
      'yarn node -status distinguishes unhealthy disk exclusion from a lost NM heartbeat',
    ],
    sourceLinks: [
      {
        label: 'Hadoop 3.4.1 NodeManager docs',
        url: 'https://hadoop.apache.org/docs/r3.4.1/hadoop-yarn/hadoop-yarn-site/NodeManager.html',
      },
      {
        label: 'Hadoop ContainerManagerImpl start validation',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/ContainerManagerImpl.java#L927-L1051',
      },
      {
        label: 'Spark ExecutorRunnable NM launch call',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ExecutorRunnable.scala#L92-L123',
      },
    ],
    docFile: 'node-manager.md',
  },

  'yarn-container': {
    id: 'yarn-container',
    name: 'YARN Container Grant',
    shortName: 'YARN Container',
    subsystem: 'cluster-hdfs',
    hostZone: 'worker-node-a',
    clusterPos: [408, 758],
    process: 'YARN resource allocation & OS process-tree budget on a granted worker node (not a daemon)',
    role: 'Represents a resource grant (ContainerId, NodeId, memory/vcores capability, and container token) that bounds one AM or executor JVM plus any child Python worker processes on that node.',
    keyDistinction:
      'YARN can grant both vcores before the executor process even exists; a container ID is neither a Spark executor ID nor an OS PID.',
    startedBy: 'Granted by RM scheduler in AllocateResponse; materialized on disk/OS when NM runs StartContainerRequest',
    monitoredBy: 'NodeManager ContainerImpl process-tree monitor and RM allocation ledger',
    stoppedBy: 'Normal child exit, AM stop/release, RM preemption (-102), PMEM kill (-104), or app cleanup',
    inputs: [
      'AM ResourceRequest (memory = heap + overhead + off-heap + pyspark memory, vcores, priority, locality)',
      'ContainerLaunchContext + container token presented to the granted NM',
    ],
    outputs: [
      'Isolated working directory and process-tree resource envelope hosting one AM or executor JVM (and Python children)',
      'ContainerStatus (state, exitStatus, diagnostics) reported from NM to RM to AM',
    ],
    internalState: [
      'ContainerId (appAttemptId + container sequence), NodeId, Resource vector, and container token',
      'Node-side ContainerImpl internal state (LOCALIZING / SCHEDULED / RUNNING / COMPLETE)',
      'Exit status codes: 0 normal, -102 PREEMPTED, -104 KILLED_EXCEEDED_PMEM',
    ],
    failureClues: [
      'Public state RUNNING can still mean internal LOCALIZING or SCHEDULED before the JVM starts; check container substate',
      'COMPLETE with exitStatus -104 (KILLED_EXCEEDED_PMEM) signals JVM + Python child RSS exceeded container budget',
      'COMPLETE with -102 (PREEMPTED) is cluster scheduler reclamation rather than a Spark task bug',
    ],
    sourceLinks: [
      {
        label: 'Hadoop ContainerImpl state mapping',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/container/ContainerImpl.java#L751-L805',
      },
      {
        label: 'Hadoop ContainerExitStatus constants',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/api/records/ContainerExitStatus.java',
      },
      {
        label: 'Spark ResourceProfile executor memory sizing',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/resource/ResourceProfile.scala#L561-L613',
      },
    ],
    docFile: 'yarn-container.md',
  },

  'hdfs-namenode': {
    id: 'hdfs-namenode',
    name: 'HDFS NameNode',
    shortName: 'HDFS NameNode',
    subsystem: 'cluster-hdfs',
    hostZone: 'storage-history',
    clusterPos: [1032, 106],
    port: ':8020',
    process: 'Long-lived Hadoop NameNode daemon JVM (HA active/standby with JournalNodes)',
    role: 'Owns the HDFS directory tree, file inodes, writer leases, block ID allocations, and live block-to-DataNode location map without ever proxying Parquet or staging file bytes.',
    keyDistinction:
      'The NameNode supplies block locations and write pipeline targets; the chosen DataNode supplies the actual bytes.',
    startedBy: 'Cluster operations / service manager (independent of YARN and Spark)',
    monitoredBy: 'Cluster HA failover controller, JournalNodes, and DataNode heartbeats',
    stoppedBy: 'Cluster operations or HA failover',
    inputs: [
      'ClientProtocol RPC (:8020): getListing, getBlockLocations, create, addBlock, complete, rename',
      'DatanodeProtocol RPC: sendHeartbeat, full blockReport, incremental blockReceivedAndDeleted',
    ],
    outputs: [
      'Directory listings and LocatedBlocks (block IDs, file offsets, DataNode replica endpoints, block access tokens)',
      'Under-construction block allocations with chosen pipeline targets and edit-log synced metadata',
      'Heartbeat commands telling live DataNodes to replicate or invalidate blocks',
    ],
    internalState: [
      'In-memory namespace directory tree, file inodes, and ordered block ID lists (checkpointed via fsimage + edits)',
      'Live block-to-DataNode replica map rebuilt from DataNode block reports',
      'Active files-under-construction table and writer lease manager',
    ],
    failureClues: [
      'AccessControlException or StandbyException during listing fails before any Spark task reads data',
      'SafeModeException on create/write while reads succeed means NameNode is still awaiting DataNode block reports',
      'hdfs fsck <file> -files -blocks -locations separates missing replicas from unreachable DataNode addresses',
    ],
    sourceLinks: [
      {
        label: 'Hadoop 3.4.1 HDFS architecture & safe mode',
        url: 'https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html',
      },
      {
        label: 'Hadoop FSNamesystem.getAdditionalBlock',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/FSNamesystem.java#L3047-L3090',
      },
      {
        label: 'Hadoop ClientProtocol.java',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java',
      },
    ],
    docFile: 'hdfs-namenode.md',
  },

  'hdfs-datanode': {
    id: 'hdfs-datanode',
    name: 'HDFS DataNode',
    shortName: 'HDFS DataNode',
    subsystem: 'cluster-hdfs',
    hostZone: 'storage-history',
    clusterPos: [1324, 106],
    port: ':9866',
    process: 'Long-lived Hadoop DataNode storage daemon JVM on storage/worker nodes',
    role: 'Stores and serves replicated HDFS block files and checksum metadata on disk, streams read packets to Spark tasks, forwards write packets down replication pipelines, and reports block inventory to the NameNode.',
    keyDistinction:
      'A successful write pipeline packet ACK neither closes the HDFS file nor commits the Spark job.',
    startedBy: 'Cluster operations / service manager (replicas outlive Spark applications)',
    monitoredBy: 'NameNode via DatanodeProtocol heartbeats and operators monitoring failed volumes',
    stoppedBy: 'Cluster operations or host failure (surviving replicas keep files readable)',
    inputs: [
      'DataTransferProtocol readBlock (extended block ID, token, offset, length) from executor HDFS clients',
      'DataTransferProtocol writeBlock packets from writer DataStreamer or upstream pipeline DataNode',
      'NameNode heartbeat responses commanding background block replication or invalidation',
    ],
    outputs: [
      'Streamed data packets + checksums via BlockSender to reading Spark tasks',
      'Forwarded pipeline packets and upstream PacketResponder sequence ACKs',
      'DatanodeProtocol heartbeats, full blockReport, and incremental received/deleted block reports',
    ],
    internalState: [
      'Local block pool files (blk_<id>) and checksum sidecars (blk_<id>_<genStamp>.meta)',
      'Replica lifecycle state (RBW under construction vs finalized) across configured volumes',
      'DataXceiver / BlockReceiver / PacketResponder active transfer handlers',
    ],
    failureClues: [
      'Input listing works but executor read times out points to unreachable DataNode transfer address (:9866)',
      'ChecksumException on one replica triggers client failover to another replica; check hdfs fsck if all replicas are corrupt',
      'Slow or failed write pipeline points to downstream DataNode volume errors or transfer latency, not just writer CPU',
    ],
    sourceLinks: [
      {
        label: 'Hadoop DataXceiver read/write handlers',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java#L580-L930',
      },
      {
        label: 'Hadoop BlockReceiver packet pipeline',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java',
      },
      {
        label: 'Hadoop DataStreamer send & ACK queues',
        url: 'https://github.com/apache/hadoop/blob/4d7825309348956336b8f06a08322b78422849b1/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DataStreamer.java#L100-L117',
      },
    ],
    docFile: 'hdfs-datanode.md',
  },

  // 3. Driver Runtime (6)
  'spark-driver': {
    id: 'spark-driver',
    name: 'Spark Driver JVM',
    shortName: 'Spark Driver JVM',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [344, 290],
    port: ':41000',
    process: 'AM JVM (cluster mode) or Submit-host JVM (client mode)',
    role: 'Process role hosting SparkContext, SparkSession, SparkEnv, SQL planner, DAG/task schedulers, executor registry, storage/shuffle masters, and SparkUI for one application lifetime.',
    keyDistinction:
      'YARN grants the container; the Spark driver JVM decides which tasks each registered executor runs.',
    startedBy: 'ApplicationMaster Driver thread (cluster mode) or SparkSubmit invoking PythonRunner (client mode)',
    monitoredBy: 'ApplicationMaster (cluster) or submitting user / YarnClientSchedulerBackend monitor (client)',
    stoppedBy: 'spark.stop() -> SparkContext.stop(), Python child exit, or fatal JVM/container crash',
    inputs: [
      'Local Py4J calls from the Python driver script (SQL queries, RDD actions, stop)',
      'Executor Spark RPC messages: RegisterExecutor, LaunchedExecutor, StatusUpdate, Heartbeat',
      'AM control handoffs (SparkContext rendezvous in cluster mode or YarnScheduler RPC in client mode)',
    ],
    outputs: [
      'Encoded LaunchTask and KillTask commands over Spark RPC to registered executors',
      'Executor capacity requests to the AM via YarnSchedulerBackend',
      'Action results returned to Python driver (scalar over Py4J or batches over local data socket)',
    ],
    internalState: [
      'Live SparkContext, driver SparkEnv, and listening NettyRpcEnv (spark.driver.port)',
      'Registered executor ledger, DAG/stage graph, and SQL QueryExecution plans',
      'Master metadata directories (BlockManagerMaster, MapOutputTrackerMaster, AppStatusStore)',
    ],
    failureClues: [
      '"Timed out waiting for SparkContext" in cluster AM means pre-context Python code blocked past spark.yarn.am.waitTime',
      'Driver JVM crash loses all in-memory scheduler state; YARN retry reruns orders.py from the start (cluster) or cannot resurrect it (client)',
      'spark.stop() ends the JVM context without automatically terminating remaining Python script code',
    ],
    sourceLinks: [
      {
        label: 'SparkContext scheduler & env initialization',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkContext.scala#L633-L682',
      },
      {
        label: 'AM wait for SparkContext initialization',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/deploy/yarn/ApplicationMaster.scala#L496-L539',
      },
      {
        label: 'Driver LaunchTask dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L439-L485',
      },
    ],
    docFile: 'spark-driver.md',
  },

  'python-driver': {
    id: 'python-driver',
    name: 'Python Driver Process',
    shortName: 'Python Driver',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [148, 506],
    process: 'Python interpreter child process spawned by org.apache.spark.deploy.PythonRunner',
    role: 'Executes the submitted top-level .py script once per driver attempt, constructs DataFrames/RDDs and serialized Python closures, invokes driver JVM APIs via Py4J, and receives action results or exceptions.',
    keyDistinction:
      'The main Python script runs once per driver attempt, not once per partition.',
    startedBy: 'org.apache.spark.deploy.PythonRunner.main via ProcessBuilder([pythonExec, script, ...appArgs])',
    monitoredBy: 'deploy.PythonRunner waiting for child process exit and forwarding merged stdout/stderr',
    stoppedBy: 'Normal script return (exit 0) or uncaught Python exception (SparkUserAppException(exitCode))',
    inputs: [
      'sys.argv (script path + user arguments), PYTHONPATH, PYSPARK_GATEWAY_PORT, PYSPARK_GATEWAY_SECRET',
      'Py4J replies (scalar values or JVM object proxies) and collected row batches from local data socket',
    ],
    outputs: [
      'Py4J method calls (_jsparkSession.sql, _jdf.count, _jsc.runJob, spark.stop)',
      'Cloudpickled SimplePythonFunction + serializers for distributed Python RDDs/UDFs',
      'Process exit code propagated to AM user thread (cluster) or spark-submit shell (client)',
    ],
    internalState: [
      'Python SparkSession / SparkContext wrappers holding _jsparkSession and _jsc proxies',
      'Cached Py4J JavaGateway / ClientServer connection and jvm view',
      'Selected executable precedence: spark.pyspark.driver.python -> spark.pyspark.python -> PYSPARK_DRIVER_PYTHON -> PYSPARK_PYTHON -> python3',
    ],
    failureClues: [
      'ModuleNotFoundError before session creation appears in AM stdout (cluster) or submit output (client); fixing executor deps alone cannot help',
      'Nonzero Python exit becomes SparkUserAppException(exitCode), unless AM failed before registration (EXIT_SC_NOT_INITED)',
      'Do not confuse org.apache.spark.deploy.PythonRunner (driver launcher) with org.apache.spark.api.python.PythonRunner (executor runner)',
    ],
    sourceLinks: [
      {
        label: 'deploy.PythonRunner executable selection & launch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala#L60-L86',
      },
      {
        label: 'Child env & gateway handoff',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala#L97-L119',
      },
      {
        label: 'Python RDD closure serialization',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/core/rdd.py#L5106-L5136',
      },
    ],
    docFile: 'python-driver.md',
  },

  'py4j-gateway': {
    id: 'py4j-gateway',
    name: 'Py4J Gateway',
    shortName: 'Py4J Gateway',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [148, 578],
    port: '127.0.0.1:ephemeral',
    process: 'Py4JServer (ClientServer/GatewayServer) listening on driver-local loopback',
    role: 'Provides the authenticated loopback IPC bridge allowing the Python driver to invoke methods on live driver JVM objects, while large collectToPython row batches stream over a separate SocketAuthServer socket.',
    keyDistinction:
      'A successful Py4J call tests only the local driver bridge, not the executors route to the driver.',
    startedBy: 'deploy.PythonRunner creating Py4JServer(SparkConf) on port 0 before spawning Python (or launch_gateway fallback in shell)',
    monitoredBy: 'deploy.PythonRunner (join on init thread) and Python SparkContext._ensure_initialized',
    stoppedBy: 'deploy.PythonRunner finally block shutting down Py4JServer after the Python child exits',
    inputs: [
      'Authenticated Py4J method invocations on JVM proxies (_jsc, _jsparkSession, _jdf) over loopback',
      'Local data-socket connections authenticated with per-transfer SocketAuthServer secret for collectToPython',
    ],
    outputs: [
      'Serialized scalar return values or live JVM proxy handles back to Python',
      'Array[Any] socket connection metadata plus streamed pickled row batches for DataFrame.collect()',
      'Optional callback listener for JVM-to-Python callbacks',
    ],
    internalState: [
      'Py4JServer wrapping pinned-thread ClientServer (PYSPARK_PIN_THREAD=true default) or GatewayServer',
      'Ephemeral bound loopback port (PYSPARK_GATEWAY_PORT) and auth secret (PYSPARK_GATEWAY_SECRET)',
      'SocketAuthServer ephemeral loopback/UDS server for bulk collect/broadcast transfers',
    ],
    failureClues: [
      'JAVA_GATEWAY_EXITED occurs in interactive shell fallback when spawned JVM fails to publish connection-info file',
      'Connection refused on PYSPARK_GATEWAY_PORT points to local driver JVM/listener failure, not worker networking',
      'Stopping SparkContext does not close the Py4J listener; the gateway closes only after the Python script exits',
    ],
    sourceLinks: [
      {
        label: 'Py4JServer loopback listener & auth',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/Py4JServer.scala#L28-L69',
      },
      {
        label: 'PySpark java_gateway attach branch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/java_gateway.py#L53-L64',
      },
      {
        label: 'SocketAuthServer collect data channel',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/security/SocketAuthServer.scala#L53-L95',
      },
    ],
    docFile: 'py4j-gateway.md',
  },

  'spark-session': {
    id: 'spark-session',
    name: 'SparkSession',
    shortName: 'SparkSession',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [344, 362],
    process: 'Driver JVM (org.apache.spark.sql.classic.SparkSession) + Python wrapper',
    role: 'Provides the SQL and DataFrame entry point, isolating per-session SQLConf, local temp views, and registered UDFs in SessionState while sharing CacheManager, ExternalCatalog, and SparkContext via SharedState.',
    keyDistinction:
      'Stopping any sibling SparkSession stops their shared SparkContext, even though their local temporary views are separate.',
    startedBy: 'SparkSession.builder.getOrCreate() or spark.newSession() on the driver',
    monitoredBy: 'Active/default JVM session references and SQLAppStatusListener',
    stoppedBy: 'spark.stop() stopping the underlying shared SparkContext and clearing active/default references',
    inputs: [
      'Builder configuration options and modifiable runtime SQLConf overrides',
      'SQL strings, named parameters, DataFrame API calls, and catalog/temp-view operations',
    ],
    outputs: [
      'Dataset[Row] bound to QueryExecution (lazy for SELECT, eager for DDL/DML commands)',
      'Analyzed/optimized/planned SparkPlans delegated to SparkContext for distributed execution',
    ],
    internalState: [
      'SessionState (per-session SQLConf, local temp views, FunctionRegistry, parser/analyzer/optimizer/planner)',
      'SharedState (shared CacheManager, ExternalCatalog, GlobalTempViewManager, SQLAppStatusStore)',
      'Thread-local activeSession vs process-wide defaultSession references',
    ],
    failureClues: [
      'Table or view not found after spark.newSession() means a local temp view was registered only in the sibling SessionState',
      'Calling getOrCreate() on an already-live session applies only modifiable configs and will not change master or driver memory',
      'Calling other.stop() on a sibling session terminates the shared SparkContext for all sessions',
    ],
    sourceLinks: [
      {
        label: 'SparkSession state & newSession isolation',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala#L175-L214',
      },
      {
        label: 'SessionState per-session services',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/internal/SessionState.scala#L71-L103',
      },
      {
        label: 'SharedState catalog & cache sharing',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/internal/SharedState.scala#L148-L191',
      },
    ],
    docFile: 'spark-session.md',
  },

  'spark-context': {
    id: 'spark-context',
    name: 'SparkContext',
    shortName: 'SparkContext',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [344, 434],
    process: 'Driver JVM singleton (org.apache.spark.SparkContext / JavaSparkContext) + Python wrapper',
    role: 'Owns the Spark application lifecycle, initializes driver SparkEnv, LiveListenerBus, HeartbeatReceiver, schedulers, and optional dynamic allocation/event log, and serves as the runJob boundary into DAGScheduler.',
    keyDistinction:
      'SparkContext exists only on the driver; read properties like sc.defaultParallelism on the driver rather than capturing sc in a task closure.',
    startedBy: 'SparkSession.builder.getOrCreate() or SparkContext.getOrCreate(conf) via Py4J JavaSparkContext',
    monitoredBy: 'JVM activeContext singleton guard, LiveListenerBus application events, and JVM shutdown hook',
    stoppedBy: 'sc.stop() / spark.stop() (atomic stopped guard) or JVM shutdown hook cleanup',
    inputs: [
      'Validated SparkConf (spark.master=yarn, spark.app.name, driver host/port)',
      'RDD actions calling runJob(rdd, func, partitions, resultHandler) and broadcast/unpersist requests',
    ],
    outputs: [
      'Initialized driver SparkEnv, SparkUI, HeartbeatReceiver, DAGScheduler, TaskScheduler, and SchedulerBackend',
      'Cleaned task closures and local properties handed to DAGScheduler.runJob',
      'SparkListenerApplicationStart / End events and orderly teardown of all driver subsystems',
    ],
    internalState: [
      'Atomic activeContext / stopped guards enforcing one live SparkContext per driver JVM',
      'References to _env, _listenerBus, _schedulerBackend, _taskScheduler, _dagScheduler, _ui, _eventLogger',
      'Thread-local properties (job group, pool, SQL execution ID) and persistentRdds registry',
    ],
    failureClues: [
      'CONTEXT_ONLY_VALID_ON_DRIVER during serialization means a Python task closure captured sc instead of a plain value',
      'Constructing a second SparkContext in the same driver JVM is rejected by the activeContext guard',
      'Initialization failures trigger partial cleanup in post-init catch blocks before activeContext is published',
    ],
    sourceLinks: [
      {
        label: 'SparkContext scheduler & block manager startup',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkContext.scala#L676-L704',
      },
      {
        label: 'SparkContext.runJob handoff to DAGScheduler',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkContext.scala#L2784-L2831',
      },
      {
        label: 'Python SparkContext pickling guard',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/core/context.py#L490-L495',
      },
    ],
    docFile: 'spark-context.md',
  },

  'spark-env': {
    id: 'spark-env',
    name: 'SparkEnv',
    shortName: 'SparkEnv',
    subsystem: 'driver-runtime',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [344, 506],
    process: 'One SparkEnv per Spark JVM (driver JVM and each executor JVM; never in Python processes)',
    role: 'Assembles and exposes per-JVM runtime services (RpcEnv, serializers, SecurityManager, MemoryManager, ShuffleManager, BlockManager, BroadcastManager, MapOutputTracker, OutputCommitCoordinator, PythonWorkerFactory cache).',
    keyDistinction:
      'Driver SparkEnv registers master RPC endpoints; executor SparkEnv looks up references to those driver endpoints using spark.driver.host and bound spark.driver.port.',
    startedBy: 'SparkContext.createSparkEnv (driver) or CoarseGrainedExecutorBackend after driverPropsFetcher (executor)',
    monitoredBy: 'Owning SparkContext (driver) or Executor (worker)',
    stoppedBy: 'SparkContext.stop() or Executor.stop() via synchronized SparkEnv.stop()',
    inputs: [
      'SparkConf, executorId ("driver" or numeric ID), bindAddress, advertised host, port, cores, optional I/O encryption key',
      'LiveListenerBus (driver only) and SparkEnv.get() lookups from local JVM code',
    ],
    outputs: [
      'Bound driver RpcEnv (writes actual spark.driver.port back to conf) or client-mode executor RpcEnv',
      'Registered master endpoints on driver vs looked-up RpcEndpointRefs on executors',
      'Per-JVM serializers, MemoryManager, BlockManager, ShuffleManager, and cached PythonWorkerFactories',
    ],
    internalState: [
      'Volatile SparkEnv.env singleton accessor per JVM and synchronized isStopped flag',
      'rpcEnv, serializer, closureSerializer, serializerManager, memoryManager, blockManager, mapOutputTracker',
      'pythonWorkers: HashMap[(exec, workerModule, daemonModule, env), PythonWorkerFactory]',
    ],
    failureClues: [
      'Setting spark.driver.host=localhost lets local Py4J succeed while remote AM/executors fail trying to reach their own loopback',
      'Compare spark.driver.bindAddress (local interface) vs spark.driver.host (advertised peer address) and bound RPC port',
      'Executors initialize BlockManager in Executor construction and load ShuffleManager with user classpath later',
    ],
    sourceLinks: [
      {
        label: 'SparkEnv fields & service registry',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkEnv.scala#L60-L87',
      },
      {
        label: 'SparkEnv.create common factory',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkEnv.scala#L699-L745',
      },
      {
        label: 'SparkEnv synchronized stop sequence',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/SparkEnv.scala#L283-L324',
      },
    ],
    docFile: 'spark-env.md',
  },

  // 4. Scheduling (5)
  'dag-scheduler': {
    id: 'dag-scheduler',
    name: 'DAGScheduler',
    shortName: 'DAGScheduler',
    subsystem: 'scheduling',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [780, 306],
    process: 'Driver JVM single-threaded dag-scheduler-event-loop',
    role: 'Splits RDD job lineages at shuffle boundaries into ShuffleMapStages and ResultStages, submits missing partitions as TaskSets once parent shuffle outputs are available, and handles FetchFailed stage resubmission.',
    keyDistinction:
      'A stage can have all its inputs ready and submitted as a TaskSet while still waiting for an executor slot.',
    startedBy: 'SparkContext initialization after TaskScheduler creation, starting dag-scheduler-event-loop',
    monitoredBy: 'LiveListenerBus stage/job/task events and SparkContext active job waiters',
    stoppedBy: 'SparkContext.stop() -> DAGScheduler.stop() failing outstanding JobWaiters then stopping TaskScheduler',
    inputs: [
      'SparkContext.runJob -> JobSubmitted events with target RDD, partitions, and resultHandler',
      'TaskSetManager callbacks -> BeginEvent, CompletionEvent (MapStatus or result value), TaskSetFailed',
      'JobCancelled, ExecutorLost, and coalesced ResubmitFailedStages (200 ms delay) events',
    ],
    outputs: [
      'TaskSet batches of ShuffleMapTasks or ResultTasks submitted to TaskScheduler.submitTasks',
      'Broadcast common task binary (RDD + dependency/function) and MapOutputTrackerMaster registrations',
      'SparkListenerJobStart/End, StageSubmitted/Completed, and TaskStart/End events on LiveListenerBus',
    ],
    internalState: [
      'jobIdToActiveJob, jobIdToStageIds, stageIdToStage, and shuffleIdToMapStage',
      'waitingStages, runningStages, and failedStages queues serialized on dag-scheduler-event-loop',
      'Stage attempt counters (makeNewStageAttempt) and per-stage pendingPartitions sets',
    ],
    failureClues: [
      'Driver log "Resubmitting ... due to fetch failure" shows FetchFailed triggered map-output invalidation and producer stage retry',
      'Slow event handler inside dag-scheduler-event-loop delays stage transitions for all concurrent jobs',
      'Job cancellation cancels only stages not shared with another active job (check stage.jobIds)',
    ],
    sourceLinks: [
      {
        label: 'DAGScheduler state & indexes',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L124-L170',
      },
      {
        label: 'submitMissingTasks & TaskSet emission',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L3441-L3490',
      },
      {
        label: 'FetchFailed map-output invalidation',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L4190-L4249',
      },
    ],
    docFile: 'dag-scheduler.md',
  },

  'task-scheduler': {
    id: 'task-scheduler',
    name: 'TaskSchedulerImpl',
    shortName: 'TaskSchedulerImpl',
    subsystem: 'scheduling',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [780, 390],
    process: 'Driver JVM (YarnScheduler / YarnClusterScheduler extending TaskSchedulerImpl)',
    role: 'Orders active TaskSetManagers through FIFO or FAIR scheduling pools, matches DriverEndpoint WorkerOffers to eligible task sets across locality levels and resource profiles, allocates unique TIDs, and routes StatusUpdates to TaskResultGetter.',
    keyDistinction:
      'A WorkerOffer describes free capacity on an already-registered executor, not a new container grant from YARN.',
    startedBy: 'YarnClusterManager creates scheduler/backend; SparkContext calls start() and postStartHook()',
    monitoredBy: 'DAGScheduler callbacks, DriverEndpoint offers, and optional speculation timer',
    stoppedBy: 'DAGScheduler.stop() -> taskScheduler.stop(exitCode) stopping backend, TaskResultGetter, and timers',
    inputs: [
      'DAGScheduler.submitTasks(TaskSet) and killAllTaskAttempts(stageId, ...)',
      'DriverEndpoint.resourceOffers(WorkerOffers) and statusUpdate(tid, state, serializedData)',
      'TaskResultGetter callbacks (handleSuccessfulTask / handleFailedTask) and executorLost notifications',
    ],
    outputs: [
      'Seq[Seq[TaskDescription]] grouped by offered executor returned to DriverEndpoint for LaunchTask',
      'reviveOffers and killTask calls to SchedulerBackend',
      'Enqueued terminal payloads to TaskResultGetter and two-phase executor loss notifications',
    ],
    internalState: [
      'rootPool (FIFO or FAIR Schedulable tree) and taskSetsByStageIdAndAttempt',
      'taskIdToTaskSetManager, taskIdToExecutorId, executorIdToRunningTaskIds, and nextTaskId counter',
      'Per-round WorkerOffer availableCpus (exact BigDecimal) and availableResources snapshots',
    ],
    failureClues: [
      'Skipped barrier scheduling round logs compare required vs simultaneously available compatible slots',
      'LossReasonPending cleans TID routing while keeping executor-to-host mapping until AM reports final reason',
      'StatusUpdate for an unknown TID is logged and ignored as a late/duplicate report',
    ],
    sourceLinks: [
      {
        label: 'TaskSchedulerImpl indexes & locking contract',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L52-L148',
      },
      {
        label: 'resourceOffers pass & pool ordering',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L573-L631',
      },
      {
        label: 'statusUpdate & TaskResultGetter handoff',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala#L865-L894',
      },
    ],
    docFile: 'task-scheduler.md',
  },

  'task-set-manager': {
    id: 'task-set-manager',
    name: 'TaskSetManager',
    shortName: 'TaskSetManager',
    subsystem: 'scheduling',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [780, 474],
    process: 'Driver JVM Schedulable leaf (one per submitted stage attempt, guarded by TaskSchedulerImpl lock)',
    role: 'Schedules individual partition attempts for one stage attempt using delay-scheduling locality queues, serializes Tasks into TaskDescriptions, manages retries up to spark.task.maxFailures, arbitrates speculation winners, and checks driver result size quotas.',
    keyDistinction:
      'On retry or speculation the task attempt and TID change, while the partition ID and stage attempt stay the same.',
    startedBy: 'TaskSchedulerImpl.submitTasks constructs one TaskSetManager per (stageId, stageAttemptId)',
    monitoredBy: 'Parent Pool and TaskSchedulerImpl (no dedicated thread)',
    stoppedBy: 'Marked isZombie on completion, abort, suspension, or superseding attempt; removed when runningTasks == 0',
    inputs: [
      'TaskSet (tasks array of missing partitions, stageId, stageAttemptId, priority, properties)',
      'TaskSchedulerImpl.resourceOffer(execId, host, maxLocality, taskCpus, assignments)',
      'handleSuccessfulTask / handleFailedTask callbacks from TaskResultGetter via TaskSchedulerImpl',
    ],
    outputs: [
      'Option[TaskDescription] with allocated TID, attemptNumber, serialized Task, and resource assignments',
      'dagScheduler.taskStarted / taskEnded / taskSetFailed notifications',
      'KillTask requests for losing speculative siblings once a winner succeeds',
    ],
    internalState: [
      'Per-index arrays: successful(index), copiesRunning(index), numFailures(index), taskAttempts(index)',
      'Locality pending queues (executor, host, no-pref, rack, any) for regular and speculative tasks',
      'taskInfos (TID -> TaskInfo), executorIdToTaskIds, isZombie flag, and totalResultSize counter',
    ],
    failureClues: [
      '"Task ... failed N times; aborting job" counts failures of one task index against spark.task.maxFailures (not total across indices)',
      'FetchFailed marks the index locally successful and turns the TaskSetManager into a zombie so DAGScheduler handles stage recovery',
      'Preempted/non-app executor loss and TaskKilled/TaskCommitDenied do not consume the per-partition failure budget',
    ],
    sourceLinks: [
      {
        label: 'TaskSetManager indexes & state',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L131-L245',
      },
      {
        label: 'TaskSetManager.resourceOffer contract',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L466-L535',
      },
      {
        label: 'handleFailedTask & failure budget',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L1001-L1042',
      },
    ],
    docFile: 'task-set-manager.md',
  },

  'driver-scheduler-backend': {
    id: 'driver-scheduler-backend',
    name: 'CoarseGrainedSchedulerBackend',
    shortName: 'DriverSchedBackend',
    subsystem: 'scheduling',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [780, 558],
    port: 'CoarseGrainedScheduler',
    process: 'Driver JVM IsolatedThreadSafeRpcEndpoint (DriverEndpoint / YarnDriverEndpoint)',
    role: 'Maintains the driver ledger of registered executors and their free cores/custom resources, generates WorkerOffers for TaskSchedulerImpl, encodes and sends LaunchTask/KillTask RPCs, and credits resources back on terminal StatusUpdates.',
    keyDistinction:
      'A finished task frees a Spark scheduling slot on that executor, not its YARN container.',
    startedBy: 'Registered in driver RpcEnv during backend construction; start() enables credentials and periodic ReviveOffers',
    monitoredBy: 'TaskSchedulerImpl and NettyRpcEnv disconnect callbacks',
    stoppedBy: 'TaskSchedulerImpl.stop() -> StopExecutors (fan-out StopExecutor) then StopDriver',
    inputs: [
      'Executor RPCs: RegisterExecutor (ask), LaunchedExecutor, StatusUpdate',
      'Scheduler/timer triggers: ReviveOffers, killTask, disableExecutor, RemoveExecutor',
    ],
    outputs: [
      'TaskSchedulerImpl.resourceOffers(WorkerOffers) and statusUpdate(tid, state, data)',
      'Serialized LaunchTask, KillTask, DecommissionExecutor, and StopExecutor RPCs to executor refs',
      'SparkListenerExecutorAdded / Removed events posted to LiveListenerBus',
    ],
    internalState: [
      'executorDataMap: HashMap[String, ExecutorData] (RPC ref, host, totalCores, freeCores, resources, profileId)',
      'addressToExecutorId, totalCoreCount, and totalRegisteredExecutors atomic counters',
      'executorsPendingToRemove, executorsPendingLossReason, and executorsPendingDecommission sets',
    ],
    failureClues: [
      'RegisterExecutor starts with freeCores=0; cores are offered only after LaunchedExecutor arrives',
      'Encoded TaskDescription >= spark.rpc.message.maxSize aborts the TaskSet before sending LaunchTask',
      'Joint locking always acquires TaskSchedulerImpl monitor before backend monitor (withLock) during offers/removals',
    ],
    sourceLinks: [
      {
        label: 'ExecutorData ledger & counters',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L56-L108',
      },
      {
        label: 'RegisterExecutor admission & reply',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L310-L348',
      },
      {
        label: 'launchTasks size guard & dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala#L457-L487',
      },
    ],
    docFile: 'driver-scheduler-backend.md',
  },

  'yarn-scheduler-backend': {
    id: 'yarn-scheduler-backend',
    name: 'YarnSchedulerBackend',
    shortName: 'YarnSchedBackend',
    subsystem: 'scheduling',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [148, 434],
    process: 'Driver JVM YarnSchedulerEndpoint (RPC name: YarnScheduler) + YarnDriverEndpoint override',
    role: 'Adapts cluster-neutral Spark executor allocation and loss queries to YARN by proxying absolute per-profile RequestExecutors, KillExecutors, GetExecutorLossReason, and Shutdown RPCs between the driver and the registered ApplicationMaster.',
    keyDistinction:
      'An executor disconnect stops new task offers immediately via LossReasonPending; it does not tell you why the executor disappeared until the AM reports the container exit.',
    startedBy: 'YarnClusterManager creates YarnClusterSchedulerBackend or YarnClientSchedulerBackend; started by TaskSchedulerImpl',
    monitoredBy: 'AM RegisterClusterManager handshake and (client mode) ClientMonitorThread polling YARN state',
    stoppedBy: 'TaskSchedulerImpl.stop() sends empty targets and Shutdown(exitCode) to AM, then stops base backend',
    inputs: [
      'ExecutorAllocationManager requestTotalExecutors(profileIdToTotal, localityCounts, hostCounts) and doKillExecutors',
      'AM RegisterClusterManager(amRef) and unsolicited RemoveExecutor notifications',
      'YarnDriverEndpoint.onDisconnected triggering asynchronous GetExecutorLossReason',
    ],
    outputs: [
      'RequestExecutors, KillExecutors, GetExecutorLossReason, and Shutdown RPCs to AM YarnAM endpoint',
      'RemoveExecutor(id, ExecutorLossReason) posted to YarnDriverEndpoint once AM classifies container exit',
      'Client-mode YARN application submission via deploy.yarn.Client (YarnClientSchedulerBackend)',
    ],
    internalState: [
      'appId, optional cluster attemptId, and amEndpoint: Option[RpcEndpointRef]',
      'totalExpectedExecutors and readiness threshold (minRegisteredRatio default 0.8 or max wait expiry)',
      'ThreadSafeRpcEndpoint YarnSchedulerEndpoint proxying async asks to AM',
    ],
    failureClues: [
      '"Attempted to request executors before the AM has registered!" means driver has no amEndpoint route yet, not a YARN queue rejection',
      'A true reply to RequestExecutors confirms AM received the target, not that RM granted containers',
      'Missing AM or timed-out GetExecutorLossReason falls back to ExecutorProcessLost instead of preemption classification',
    ],
    sourceLinks: [
      {
        label: 'YarnSchedulerBackend RequestExecutors prep',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala#L146-L175',
      },
      {
        label: 'YarnDriverEndpoint disconnect & GetExecutorLossReason',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala#L274-L328',
      },
      {
        label: 'YarnSchedulerEndpoint RPC forwarding',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/scheduler/cluster/YarnSchedulerBackend.scala#L366-L409',
      },
    ],
    docFile: 'yarn-scheduler-backend.md',
  },

  // 5. Executor & RPC (5)
  'executor-backend': {
    id: 'executor-backend',
    name: 'CoarseGrainedExecutorBackend',
    shortName: 'ExecutorBackend',
    subsystem: 'executor-rpc',
    hostZone: 'worker-node-a',
    clusterPos: [660, 758],
    port: 'client-mode RPC',
    process: 'Executor JVM IsolatedThreadSafeRpcEndpoint (YarnCoarseGrainedExecutorBackend, RPC name: Executor)',
    role: 'Bootstraps an executor JVM by fetching SparkAppConfig from the driver URL, creates the executor SparkEnv, registers with the driver, constructs the Executor runtime, signals LaunchedExecutor, and relays LaunchTask/KillTask/StatusUpdate RPCs.',
    keyDistinction:
      'SparkListenerExecutorAdded confirms RegisterExecutor; the driver offers the executor cores only after receiving LaunchedExecutor.',
    startedBy: 'NodeManager executing AM ExecutorRunnable launch command with --driver-url and --executor-id',
    monitoredBy: 'Driver CoarseGrainedSchedulerBackend RPC channel and HeartbeatReceiver',
    stoppedBy: 'StopExecutor RPC, DecommissionExecutor drain completion, driver disconnect, or fatal exit',
    inputs: [
      'CLI args (--driver-url, --executor-id, --cores, --app-id, --resourceProfileId) and SparkAppConfig reply',
      'Driver RPCs: RegisteredExecutor (self-posted), LaunchTask, KillTask, DecommissionExecutor, StopExecutor',
      'TaskRunner statusUpdate(taskId, state, data) calls from executor task threads',
    ],
    outputs: [
      'RetrieveSparkAppConfig (temp driverPropsFetcher), RegisterExecutor, LaunchedExecutor, and StatusUpdate RPCs',
      'In-JVM calls to executor.launchTask(backend, TaskDescription) and executor.killTask',
    ],
    internalState: [
      'Volatile driver: Option[RpcEndpointRef] and executor: Executor (null until RegisteredExecutor)',
      'Atomic stopping flag and decommissioned drain/migration state',
      'Dedicated single-thread IsolatedThreadSafeRpcEndpoint inbox (stop runs executor.stop() off-dispatcher to avoid deadlock)',
    ],
    failureClues: [
      'Failed driverPropsFetcher lookup precedes registration; "Cannot register with driver" fails in onStart',
      '"Successfully registered with driver" followed by crash narrows failure to Executor/BlockManager construction gap',
      'Unexpected driver disconnect triggers executor self-shutdown without notifying the vanished driver',
    ],
    sourceLinks: [
      {
        label: 'YarnCoarseGrainedExecutorBackend entry point',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/resource-managers/yarn/src/main/scala/org/apache/spark/executor/YarnCoarseGrainedExecutorBackend.scala#L36-L86',
      },
      {
        label: 'Bootstrap driverPropsFetcher & SparkEnv setup',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala#L439-L538',
      },
      {
        label: 'LaunchTask, KillTask, and Shutdown handlers',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala#L167-L234',
      },
    ],
    docFile: 'executor-backend.md',
  },

  executor: {
    id: 'executor',
    name: 'Executor Runtime',
    shortName: 'Executor',
    subsystem: 'executor-rpc',
    hostZone: 'worker-node-a',
    clusterPos: [156, 854],
    process: 'Long-lived Scala object inside the executor JVM (org.apache.spark.executor.Executor)',
    role: 'Initializes the executor BlockManager, user class loader, plugins, and heartbeater; runs assigned TaskRunners on its daemon UninterruptibleThread pool; tracks kill marks and optional task reapers; and reports periodic heartbeats to the driver.',
    keyDistinction:
      'A kill mark belongs to one task attempt ID (TID), not to every attempt for that partition.',
    startedBy: 'CoarseGrainedExecutorBackend upon receiving local RegisteredExecutor message',
    monitoredBy: 'Driver HeartbeatReceiver via executor-heartbeater and executor metrics poller',
    stoppedBy: 'ExecutorBackend shutdown thread calling Executor.stop() (atomic guard) or JVM shutdown hook',
    inputs: [
      'launchTask(backend, TaskDescription) and killTask(taskId, interruptThread, reason) from ExecutorBackend',
      'HeartbeatResponse(reregisterBlockManager) from driver HeartbeatReceiver',
      'Versioned files, jars, archives, and credentials attached to TaskDescriptions',
    ],
    outputs: [
      'TaskRunner submissions onto cached UninterruptibleThread worker pool',
      'Periodic Heartbeat(executorId, accumUpdates, blockManagerId, executorUpdates) RPCs to driver',
      'TaskRunner StatusUpdates relayed back through ExecutorBackend',
    ],
    internalState: [
      'runningTasks: ConcurrentHashMap[Long, TaskRunner] keyed by attempt TID',
      'threadPool (cached UninterruptibleThread pool), killMarks map with sweep timer, and optional taskReaperPool',
      'currentFiles / currentJars / currentArchives dependency lock, urlClassLoader, and heartbeater',
    ],
    failureClues: [
      '"Killed task ... is still running" comes from TaskReaper; positive spark.task.reaper.killTimeout can kill the JVM',
      '"Issue communicating with driver in heartbeater" comes from the heartbeat thread and exits the JVM after max failures',
      'One stalled TID while heartbeats and other tasks progress points to that task thread rather than container loss',
    ],
    sourceLinks: [
      {
        label: 'Executor initialization order (BlockManager, heartbeater, plugins)',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L478-L539',
      },
      {
        label: 'launchTask, killMarks, and killTask',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L541-L639',
      },
      {
        label: 'Executor heartbeater report loop',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1543-L1591',
      },
    ],
    docFile: 'executor.md',
  },

  'task-attempt': {
    id: 'task-attempt',
    name: 'Task Attempt (TaskRunner)',
    shortName: 'Task Attempt',
    subsystem: 'executor-rpc',
    hostZone: 'worker-node-a',
    clusterPos: [408, 854],
    process: 'One UninterruptibleThread in Executor pool (Executor.TaskRunner + TaskContextImpl)',
    role: 'Executes one scheduled attempt of a ShuffleMapTask or ResultTask: fetches dependencies, deserializes the Task, installs thread-local TaskContext and TaskMemoryManager, runs partition computation, cleans locks/memory, and sends terminal StatusUpdate.',
    keyDistinction:
      'Competing attempts for one partition have distinct TIDs; a late result from a losing speculative copy is handled as killed, not a second success.',
    startedBy: 'Executor.launchTask submitting TaskRunner(backend, TaskDescription) to the worker pool',
    monitoredBy: 'Driver TaskInfo (via StatusUpdate + Heartbeat accumulators) and local TaskReaper if killed',
    stoppedBy: 'Normal return (FINISHED), caught exception/FetchFailed (FAILED), kill/commit-denied (KILLED), or JVM exit',
    inputs: [
      'Decoded TaskDescription (TID, attemptNumber, index, partitionId, artifacts, properties, CPUs, serialized Task)',
      'Broadcast task binary (RDD + ShuffleDependency or result function) and current tracker epoch',
      'Optional kill(interruptThread, reason) cancellation signal',
    ],
    outputs: [
      'StatusUpdate(RUNNING) sent before dependency download/deserialization',
      'Terminal StatusUpdate carrying DirectTaskResult, IndirectTaskResult(TaskResultBlockId), or TaskFailedReason',
      'MapStatus metadata (ShuffleMapTask) or partition result value (ResultTask)',
    ],
    internalState: [
      'Per-attempt TaskMemoryManager and thread-local TaskContextImpl (stageId, stageAttemptId, partitionId, TID)',
      'Task completion/failure listeners and BlockManager per-task block lock registrations',
      'Interrupt/kill reason state guarded across UninterruptibleThread critical sections',
    ],
    failureClues: [
      'Same partitionId with different TIDs in logs indicates task retry or speculation; different stageAttemptId indicates stage retry',
      'A recorded shuffle FetchFailed is reported as FetchFailed even if user code caught and wrapped the exception',
      'StatusUpdate(FINISHED) means the executor prepared the result, not that TaskResultGetter accepted it',
    ],
    sourceLinks: [
      {
        label: 'TaskRunner startup, deps, and RUNNING report',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L815-L895',
      },
      {
        label: 'Task.run & TaskContextImpl lifecycle',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/Task.scala#L79-L168',
      },
      {
        label: 'Failure reason mapping & FetchFailed reporting',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1055-L1147',
      },
    ],
    docFile: 'task-attempt.md',
  },

  'task-result-getter': {
    id: 'task-result-getter',
    name: 'TaskResultGetter',
    shortName: 'TaskResultGetter',
    subsystem: 'executor-rpc',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [584, 558],
    process: 'Driver JVM worker pool (4 daemon task-result-getter threads owned by TaskSchedulerImpl)',
    role: 'Deserializes direct task results or fetches IndirectTaskResult blocks via BlockManager outside the scheduler lock, enforces spark.driver.maxResultSize via TaskSetManager, decodes failure reasons, and invokes scheduler completion callbacks.',
    keyDistinction:
      'For collect, the driver counts the sizes of the indirect results against maxResultSize, not the tiny sizes of their block references.',
    startedBy: 'Constructed by TaskSchedulerImpl (spark.resultGetter.threads, default 4)',
    monitoredBy: 'TaskSchedulerImpl completion callbacks and task Getting Result timing metrics',
    stoppedBy: 'TaskSchedulerImpl.stop() calling taskResultGetter.stop() -> shutdownNow()',
    inputs: [
      'enqueueSuccessfulTask(taskSetManager, tid, serializedData) for FINISHED attempts',
      'enqueueFailedTask(taskSetManager, tid, taskState, serializedData) for FAILED/KILLED attempts',
      'Remote TaskResultBlockId(tid) bytes fetched via sparkEnv.blockManager.getRemoteBytes',
    ],
    outputs: [
      'TaskSetManager.canFetchMoreResults(size) budget check and blockManagerMaster.removeBlock cleanup',
      'scheduler.handleSuccessfulTask(tsm, tid, DirectTaskResult) or handleFailedTask(tsm, tid, state, reason)',
    ],
    internalState: [
      'getTaskResultExecutor FixedThreadPool (default 4 daemon threads)',
      'Thread-local serializer and closureSerializer instances for lock-free decoding',
      'Branching rules for DirectTaskResult vs IndirectTaskResult vs dropped oversized result (S > M)',
    ],
    failureClues: [
      '"Total size of serialized results ... is bigger than spark.driver.maxResultSize" aborts non-shuffle-map TaskSets without fetching remaining blocks',
      'TaskResultLost occurs when an executor vanishes between sending FINISHED and getRemoteBytes fetching TaskResultBlockId(tid)',
      'Increasing spark.rpc.message.maxSize alone will not fix a spark.driver.maxResultSize abort',
    ],
    sourceLinks: [
      {
        label: 'TaskResultGetter direct & indirect result fetch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskResultGetter.scala#L58-L106',
      },
      {
        label: 'Executor direct/indirect/drop size thresholds',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L1005-L1052',
      },
      {
        label: 'TaskSetManager.canFetchMoreResults budget check',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L801-L828',
      },
    ],
    docFile: 'task-result-getter.md',
  },

  'rpc-env': {
    id: 'rpc-env',
    name: 'NettyRpcEnv',
    shortName: 'NettyRpcEnv',
    subsystem: 'executor-rpc',
    hostZone: 'driver-jvm',
    subZone: 'AM BRIDGE & DRIVER CORE',
    clusterPos: [344, 578],
    port: ':41000',
    process: 'Control-plane Netty transport router in driver (server mode), executors & client AM (clientMode=true)',
    role: 'Multiplexes named Spark RpcEndpoints over one listening port on the driver and delivers reverse LaunchTask/KillTask commands to client-mode executors over the existing TCP connection each executor opened.',
    keyDistinction:
      'Task launches need no Spark RPC listener on the executor; fetching a shuffle or indirect result block uses the separate NettyBlockTransferService listener.',
    startedBy: 'SparkEnv.create (driver binds port; executors use clientMode=true) and client-mode ApplicationMaster',
    monitoredBy: 'Netty channel lifecycle events (onConnected / onDisconnected) and ask timeout scheduler',
    stoppedBy: 'SparkEnv.stop() closing Outboxes, Dispatcher, timeout scheduler, and Netty TransportServer',
    inputs: [
      'RpcEndpointRef.send (one-way), ask (Future), and askSync (blocking) calls',
      'Endpoint URI lookups verified via RpcEndpointVerifier.CheckExistence(name)',
      'Incoming Netty TransportClient frames routed by named RpcEndpoint',
    ],
    outputs: [
      'Local in-JVM Inbox delivery (same address), existing TransportClient delivery, or per-address Outbox send',
      'Endpoint lifecycle callbacks: onStart, receive, receiveAndReply, onDisconnected, onStop',
    ],
    internalState: [
      'Dispatcher endpoint-name map and per-endpoint Inbox queues (normal, ThreadSafe, IsolatedThreadSafe)',
      'Per-remote-address Outbox connection queues and NettyRpcEndpointRef attached TransportClient handles',
      'Driver TransportServer (bound spark.driver.port) vs executor clientMode=true (address == null)',
    ],
    failureClues: [
      'RpcEndpointNotFoundException means lookup reached the host/port but no endpoint with that name is registered',
      'RpcTimeoutException on ask means no reply arrived before deadline; ask does not auto-retry and peer may have executed it',
      'Working driver-executor RPC does not prove block-transfer ports (:42001 / :7337) are reachable',
    ],
    sourceLinks: [
      {
        label: 'NettyRpcEnv ask routing & timeout',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L232-L285',
      },
      {
        label: 'Client-mode ref attachment & inbound dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/rpc/netty/NettyRpcEnv.scala#L701-L733',
      },
      {
        label: 'Dispatcher & Inbox endpoint threading',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/rpc/netty/Dispatcher.scala#L39-L88',
      },
    ],
    docFile: 'rpc-env.md',
  },

  // 6. Capacity & Observability (7)
  'executor-allocation-manager': {
    id: 'executor-allocation-manager',
    name: 'ExecutorAllocationManager',
    shortName: 'ExecutorAllocMgr',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1020, 306],
    process: 'Driver JVM policy controller (100 ms fixed-delay daemon thread, active only with dynamic allocation)',
    role: 'Computes per-ResourceProfile executor demand from pending/running tasks, ramps target executor counts exponentially on sustained backlog, lowers target when demand falls, and removes eligible idle executors supplied by ExecutorMonitor.',
    keyDistinction:
      'Lowering the target from six to one does not kill five executors; idle executor removal is a separate decision.',
    startedBy: 'SparkContext creates and starts it when spark.dynamicAllocation.enabled=true and shuffle safety passes',
    monitoredBy: 'Driver metrics gauges (numberMaxNeededExecutors, numberTargetExecutors, numberAllExecutors)',
    stoppedBy: 'SparkContext.stop() shutting down the allocation daemon thread',
    inputs: [
      'executorManagement LiveListenerBus events via ExecutorAllocationListener (stage/task backlog and localities)',
      'ExecutorMonitor timedOutExecutors() idle candidates and per-profile live counts',
      'Dynamic allocation config bounds (min, initial, max, backlog timeouts, allocation ratio)',
    ],
    outputs: [
      'requestTotalExecutors(profileIdToTotal, localityCounts, hostCounts) to YarnSchedulerBackend',
      'killExecutors / decommissionExecutorsIfIdle (with adjustTargetNumExecutors=false) for timed-out IDs',
    ],
    internalState: [
      'numExecutorsTargetPerResourceProfileId and numExecutorsToAddPerResourceProfileId (1, 2, 4, ...)',
      'addTime backlog timer per profile and ExecutorAllocationListener stage/task counters',
      'Deferred target-push retry/backoff state across hold/resume or AM reset',
    ],
    failureClues: [
      'Startup fails shuffle safety check unless ESS, shuffle tracking, decommissioning, or reliable shuffle storage is enabled',
      'High numberTargetExecutors alongside low numberAllExecutors means AM/RM has not granted or started containers yet',
      'Dropped demand lowers target immediately while existing executors remain until ExecutorMonitor deadlines expire',
    ],
    sourceLinks: [
      {
        label: 'ExecutorAllocationManager start & listeners',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L269-L297',
      },
      {
        label: 'Allocation tick & target downward adjustment',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L460-L528',
      },
      {
        label: 'Idle candidate removal path',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/ExecutorAllocationManager.scala#L675-L749',
      },
    ],
    docFile: 'executor-allocation-manager.md',
  },

  'executor-monitor': {
    id: 'executor-monitor',
    name: 'ExecutorMonitor',
    shortName: 'ExecutorMonitor',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1020, 390],
    process: 'Driver JVM SparkListener on executorManagement queue (owned by ExecutorAllocationManager)',
    role: 'Tracks each executor running-task count, cached RDD blocks, and tracked shuffle outputs to compute idle expiration deadlines and return removal-eligible (executorId, resourceProfileId) candidates.',
    keyDistinction:
      'An executor can appear on the timed-out candidate list and still stay if removing it would put its profile below target or if it becomes busy.',
    startedBy: 'Constructed by ExecutorAllocationManager and registered on executorManagement queue + optional ContextCleaner',
    monitoredBy: 'ExecutorAllocationManager polling timedOutExecutors() on allocation ticks',
    stoppedBy: 'Ends when SparkContext stops ContextCleaner, ExecutorAllocationManager, and LiveListenerBus',
    inputs: [
      'Executor added/removed, Task start/end, Block updated, UnpersistRDD, and Job/Stage lifecycle events',
      'ContextCleaner private ShuffleCleanedEvent posted back through the listener bus',
      'Allocation manager marks for accepted pendingRemoval / decommissioning IDs',
    ],
    outputs: [
      'Sorted Seq[(executorId, resourceProfileId)] of expired idle candidates from timedOutExecutors()',
      'Per-resource-profile live and pending-removal executor counts',
    ],
    internalState: [
      'Per-executor Tracker map (runningTasks, idleStart, cachedBlocks bitsets, held shuffles, flags)',
      'Job-to-shuffle and active-shuffle tracking maps (when shuffle tracking is enabled without ESS)',
      'Atomic nextTimeout watermark and cached candidate list avoiding redundant full scans',
    ],
    failureClues: [
      'Idle executor stays alive indefinitely if it holds shuffle data for any active job under shuffle tracking without ESS',
      'Cache or shuffle deadlines are measured from original idleStart, not from when the last job finished',
      'Even a successful speculative map copy marks that executor as holding shuffle data until cleaned',
    ],
    sourceLinks: [
      {
        label: 'ExecutorMonitor timedOutExecutors scan',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/dynalloc/ExecutorMonitor.scala#L45-L138',
      },
      {
        label: 'Task start/end & shuffle tracking handlers',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/dynalloc/ExecutorMonitor.scala#L309-L380',
      },
      {
        label: 'Idle deadline calculation rules',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/dynalloc/ExecutorMonitor.scala#L537-L617',
      },
    ],
    docFile: 'executor-monitor.md',
  },

  'heartbeat-receiver': {
    id: 'heartbeat-receiver',
    name: 'HeartbeatReceiver',
    shortName: 'HeartbeatReceiver',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1020, 474],
    port: 'HeartbeatReceiver',
    process: 'Driver JVM ThreadSafeRpcEndpoint (RPC name: HeartbeatReceiver) + SparkListener',
    role: 'Tracks driver receipt timestamps for executor heartbeats, forwards in-flight task accumulator and metric updates to TaskScheduler/DAGScheduler and BlockManagerMasterHeartbeat, and expires silent executors past network timeout.',
    keyDistinction:
      'The driver measures the gap between heartbeats it receives; a heartbeat timeout is not proof that YARN killed the container.',
    startedBy: 'SparkContext registers HeartbeatReceiver before TaskScheduler creation and sends TaskSchedulerIsSet',
    monitoredBy: 'Periodic ExpireDeadHosts timer (spark.network.timeoutInterval)',
    stoppedBy: 'SparkContext.stop() stops the endpoint, cancelling timers and shutting down event-loop/kill threads',
    inputs: [
      'Executor Heartbeat(executorId, accumUpdates, blockManagerId, executorUpdates) RPCs',
      'executorManagement listener events (ExecutorAdded / ExecutorRemoved) updating executorLastSeen',
      'Periodic ExpireDeadHosts timer ticks',
    ],
    outputs: [
      'HeartbeatResponse(reregisterBlockManager) telling executor whether to re-register storage',
      'TaskSchedulerImpl.executorHeartbeatReceived -> SparkListenerExecutorMetricsUpdate + BlockManagerHeartbeat',
      'killAndReplaceExecutor + RemoveExecutor(HeartbeatTimeout) on expired silent executors',
    ],
    internalState: [
      'executorLastSeen: HashMap[String, Long] of driver receipt timestamps',
      'executorTimeoutMs (blockManagerHeartbeatTimeoutMs / spark.network.timeout) and checkTimeoutIntervalMs',
      'Dedicated eventLoopThread and killExecutorThread executors',
    ],
    failureClues: [
      'Expiry check is strictly greater than timeout (now - lastSeen > timeout), so exact boundary check keeps the executor',
      'Heartbeat timeout can reflect long GC pause, overloaded driver, or network partition rather than dead YARN node',
      'AM-to-RM allocate heartbeats and NM-to-RM node health are separate YARN channels and do not refresh executorLastSeen',
    ],
    sourceLinks: [
      {
        label: 'HeartbeatReceiver timeout thresholds',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L75-L118',
      },
      {
        label: 'Heartbeat receipt & forwarding',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L120-L168',
      },
      {
        label: 'ExpireDeadHosts & replacement trigger',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala#L210-L257',
      },
    ],
    docFile: 'heartbeat-receiver.md',
  },

  'listener-bus': {
    id: 'listener-bus',
    name: 'LiveListenerBus',
    shortName: 'LiveListenerBus',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'OBSERVABILITY & LIVE UI',
    clusterPos: [1554, 334],
    process: 'Driver JVM asynchronous event dispatcher (named AsyncEventQueue daemon threads)',
    role: 'Decouples scheduler threads from observers by offering posted SparkListenerEvents into isolated, bounded per-queue FIFOs (appStatus, eventLog, executorManagement, shared) for serial per-queue delivery.',
    keyDistinction:
      'A full eventLog queue can drop a task-end event even while the live UI (appStatus queue) shows the task as finished.',
    startedBy: 'Created early by SparkContext (buffering startup events); started once initial internal/extra listeners register',
    monitoredBy: 'LiveListenerBusMetrics (numEventsPosted, queue.<name>.numDroppedEvents, size, listenerProcessingTime)',
    stoppedBy: 'SparkContext.stop() posts ApplicationEnd then stops bus, draining queues up to join timeout',
    inputs: [
      'post(SparkListenerEvent) calls from DAGScheduler, CoarseGrainedSchedulerBackend, BlockManagerMaster, SQL',
      'Listener registrations routed to named queues (appStatus, eventLog, executorManagement, shared)',
    ],
    outputs: [
      'Typed SparkListener callbacks (onTaskEnd, onStageCompleted, onExecutorAdded, onOtherEvent) per queue thread',
      'Driver warning logs and dropped-event counters on non-blocking queue overflow',
    ],
    internalState: [
      'queues: CopyOnWriteArrayList[AsyncEventQueue] (one spark-listener-group-<name> daemon thread per queue)',
      'Startup queuedEvents buffer drained once start() runs',
      'Per-queue LinkedBlockingQueue bounded by spark.scheduler.listenerbus.eventqueue.capacity',
    ],
    failureClues: [
      'Discrepancies between live UI and History Server point to queue.<name>.numDroppedEvents overflow warnings',
      'Non-blocking offer drops excess events without back-pressuring DAGScheduler; increasing capacity only buys time',
      'Driver crash skips orderly drain so buffered events in eventLog queue are lost',
    ],
    sourceLinks: [
      {
        label: 'LiveListenerBus.post to active queues',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/LiveListenerBus.scala#L126-L183',
      },
      {
        label: 'AsyncEventQueue non-blocking offer & overflow',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/AsyncEventQueue.scala#L168-L205',
      },
      {
        label: 'SparkListenerBus typed callback dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/SparkListenerBus.scala#L25-L104',
      },
    ],
    docFile: 'listener-bus.md',
  },

  'event-log': {
    id: 'event-log',
    name: 'Spark Event Log',
    shortName: 'Spark Event Log',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'OBSERVABILITY & LIVE UI',
    clusterPos: [1554, 434],
    process: 'Driver JVM EventLoggingListener (eventLog queue) writing JSONL files to shared storage',
    role: 'Serializes selected SparkListenerEvents via JsonProtocol as UTF-8 JSON lines (optionally compressed and rolled into eventlog_v2 segments) to shared HDFS/filesystem storage for offline History Server replay.',
    keyDistinction:
      'A failed task event record names its executor and failure reason; inspect YARN container logs for raw stdout/stderr.',
    startedBy: 'SparkContext creates and starts EventLoggingListener when spark.eventLog.enabled=true after appId is known',
    monitoredBy: 'LiveListenerBus eventLog queue metrics and writer I/O error logs',
    stoppedBy: 'SparkContext.stop() drains listener bus, then calls writer.stop() to flush and rename .inprogress marker',
    inputs: [
      'Typed SparkListenerEvents delivered on LiveListenerBus eventLog queue',
      'spark.eventLog.dir URI, compression codec, rolling settings, and event filter/redaction configs',
    ],
    outputs: [
      'SparkListenerLogStart header followed by one JSON line per persisted event',
      'Single-file log or rolling eventlog_v2_<key>/events_<idx>_<key> segments + appstatus_<key>.inprogress marker',
      'Flushed/hflushed streams at stage completion, job/executor changes, and application end',
    ],
    internalState: [
      'EventLogFileWriter (SingleEventLogFileWriter or RollingEventLogFilesWriter; rolling enabled by default)',
      'Per-(stageId, stageAttemptId, executorId) executor peak metrics map emitted at stage completion',
      'Pre-compression byte counter driving rolling segment rollover',
    ],
    failureClues: [
      'Missing trailing attempts or .inprogress marker remaining indicates driver crash, queue drops, or writer I/O error',
      'Using a local file: URI means driver and History Server on different hosts read different filesystems',
      'Event log does not capture print()/println() stdout or executor stderr (use yarn logs -applicationId)',
    ],
    sourceLinks: [
      {
        label: 'EventLoggingListener startup & header',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/EventLoggingListener.scala#L63-L104',
      },
      {
        label: 'RollingEventLogFilesWriter layout & marker',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/history/EventLogFileWriters.scala#L300-L399',
      },
      {
        label: 'JsonProtocol task-end JSON encoding',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/util/JsonProtocol.scala#L196-L216',
      },
    ],
    docFile: 'event-log.md',
  },

  'spark-ui': {
    id: 'spark-ui',
    name: 'Live Spark UI',
    shortName: 'Live Spark UI',
    subsystem: 'observability',
    hostZone: 'driver-jvm',
    subZone: 'OBSERVABILITY & LIVE UI',
    clusterPos: [1554, 534],
    port: ':4040',
    process: 'Embedded Jetty HTTP server + AppStatusStore inside the driver JVM',
    role: 'Serves live web tabs (Jobs, Stages, Storage, Environment, Executors, SQL) and /api/v1 REST endpoints directly from the driver in-memory/local KV AppStatusStore updated by AppStatusListener and SQLAppStatusListener.',
    keyDistinction:
      'Two task IDs for one partition in the Stages tab can mean a failed first attempt and a successful retry.',
    startedBy: 'SparkContext binds Jetty early (default :4040) and attaches full handlers once appId is assigned',
    monitoredBy: 'User browsers, YARN RM tracking URL proxy, and /api/v1 REST clients',
    stoppedBy: 'SparkContext.stop() stops Jetty server and closes AppStatusStore (unavailable once driver exits)',
    inputs: [
      'appStatus queue events feeding AppStatusListener and SQLAppStatusListener (plus heartbeat metric deltas)',
      'HTTP GET requests for HTML tabs or /api/v1/applications/<appId>/stages/<id>/<attempt>',
      'Authorized live kill/cancel job requests routed to SparkContext.cancelJob',
    ],
    outputs: [
      'Live stage/task/executor/storage/SQL execution HTML pages and structured JSON REST responses',
      'Tracking URL registered by AM with YARN ResourceManager',
    ],
    internalState: [
      'AppStatusStore + SQLAppStatusStore backed by ElementTrackingStore (InMemoryStore or spark.ui.store.path)',
      'AppStatusListener live entity maps keyed by jobId, (stageId, attemptId), TID, executorId, rddId',
      'Update throttling state (coalesced live updates vs direct final writes) and retention eviction limits',
    ],
    failureClues: [
      'Blank or stale rows can result from appStatus queue lag/drops, update throttling, or UI retention eviction',
      'SQL execution ID, Spark job ID, and stage attempt ID are distinct keys; always check the stage attempt number',
      'Live UI disappears when the driver exits; post-mortem inspection requires event logging + History Server',
    ],
    sourceLinks: [
      {
        label: 'SparkUI Jetty handlers & tabs',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/ui/SparkUI.scala#L66-L128',
      },
      {
        label: 'AppStatusListener live entity maps',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/status/AppStatusListener.scala#L48-L110',
      },
      {
        label: 'StagesResource REST API',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/status/api/v1/StagesResource.scala#L83-L107',
      },
    ],
    docFile: 'spark-ui.md',
  },

  'history-server': {
    id: 'history-server',
    name: 'Spark History Server',
    shortName: 'History Server',
    subsystem: 'observability',
    hostZone: 'storage-history',
    clusterPos: [1616, 106],
    port: ':18080',
    process: 'Optional standalone JVM daemon (org.apache.spark.deploy.history.HistoryServer)',
    role: 'Scans shared spark.history.fs.logDirectory event logs across many applications, indexes application attempts, and synchronously replays JSONL event logs through ReplayListenerBus to serve reconstructed read-only SparkUI and REST views.',
    keyDistinction:
      'An attempt can still appear in progress on the History Server if the driver died before writing the application-end event.',
    startedBy: 'Cluster operator via start-history-server.sh / service manager (independent of spark-submit)',
    monitoredBy: 'Cluster operations via HTTP :18080 and FsHistoryProvider scan/cache logs',
    stoppedBy: 'stop-history-server.sh or JVM shutdown hook closing web server and provider stores',
    inputs: [
      'Shared filesystem event logs (single-file or rolling eventlog_v2 directories) under spark.history.fs.logDirectory',
      'Browser/REST requests under /history/<appId>/<attemptId>/',
    ],
    outputs: [
      'Application/attempt listing index and cached read-only SparkUI + SQL tab views (with SparkContext = None)',
      'Invalidated/rebuilt stores when an in-progress event log advances or finalizes',
    ],
    internalState: [
      'FsHistoryProvider listing KV store tracking log file sizes, rolling segment indices, and completion flags',
      'ApplicationCache LRU cache of attached per-attempt SparkUI handlers',
      'ReplayListenerBus + non-live AppStatusListener(live=false) + SQLHistoryServerPlugin',
    ],
    failureClues: [
      'Empty listing points to mismatched spark.eventLog.dir vs spark.history.fs.logDirectory, permissions, or disabled event logging',
      'Slow first detail page load indicates cold event-log replay on an ApplicationCache miss',
      'Incomplete listing cannot prove driver liveness or Parquet job commit; check YARN attempt status and output commit evidence',
    ],
    sourceLinks: [
      {
        label: 'HistoryServer main & handler attachment',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/history/HistoryServer.scala#L290-L326',
      },
      {
        label: 'FsHistoryProvider.rebuildAppStore replay',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/deploy/history/FsHistoryProvider.scala#L1428-L1477',
      },
      {
        label: 'ReplayListenerBus JSON parsing & error handling',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/ReplayListenerBus.scala#L207-L262',
      },
    ],
    docFile: 'history-server.md',
  },

  // 7. Block Storage (5)
  'block-manager': {
    id: 'block-manager',
    name: 'BlockManager',
    shortName: 'BlockManager',
    subsystem: 'block-storage',
    hostZone: 'worker-node-a',
    clusterPos: [660, 854],
    port: ':42001',
    process: 'Per-JVM storage subsystem (one in driver SparkEnv, one in each executor SparkEnv)',
    role: 'Stores and retrieves RDD partitions, broadcast pieces, and task results in local MemoryStore or DiskStore under per-block locks, replicates blocks to peer managers, reports statuses to BlockManagerMaster, and falls back to peer fetches on local misses.',
    keyDistinction:
      'MEMORY_AND_DISK allows disk fallback on memory pressure; it does not require storing a copy in both tiers.',
    startedBy: 'Constructed in SparkEnv; initialize(appId) starts transfer server, registers with optional ESS, and registers with master',
    monitoredBy: 'Driver BlockManagerMaster via RegisterBlockManager, UpdateBlockInfo, and Heartbeat checks',
    stoppedBy: 'SparkEnv.stop() -> BlockManager.stop() closing transfer service, RPC endpoint, memory/locks, and conditional disk cleanup',
    inputs: [
      'Local task put/get/getOrCompute calls keyed by BlockId and StorageLevel',
      'Storage RPC commands from master (RemoveRdd, RemoveBroadcast, ReplicateBlock, GetBlockStatus)',
      'BlockTransferService local data requests (getLocalBlockData / putBlockData)',
    ],
    outputs: [
      'BlockResult iterators or ManagedBuffers from MemoryStore, DiskStore, same-host disk, or remote peers',
      'RegisterBlockManager and UpdateBlockInfo metadata reports to BlockManagerMaster',
      'Outbound peer replication uploads (replication=1) via NettyBlockTransferService',
    ],
    internalState: [
      'BlockInfoManager (per-block read/write locks, StorageLevel, size, writer task)',
      'MemoryStore (deserialized/serialized entries) and DiskStore / DiskBlockManager (hashed YARN local-dirs)',
      'BlockManagerId (host + bound transfer port), shuffleServerId (ESS port if enabled), and BlockManagerStorageEndpoint',
    ],
    failureClues: [
      'Distinguish never admitted vs dropped-from-memory-to-disk vs last replica gone in executor placement/drop logs',
      'Serving executor asked for a vanished non-shuffle block reports empty status and fails that fetch',
      'Re-registration after HeartbeatResponse refreshes metadata for live local blocks; it cannot restore bytes from a dead JVM',
    ],
    sourceLinks: [
      {
        label: 'BlockManager initialization & ESS/master registration',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L689-L748',
      },
      {
        label: 'Block placement, disk fallback, and status reporting',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1864-L1964',
      },
      {
        label: 'BlockInfoManager locking invariants',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L36-L117',
      },
    ],
    docFile: 'block-manager.md',
  },

  'block-manager-master': {
    id: 'block-manager-master',
    name: 'BlockManagerMaster',
    shortName: 'BlockManagerMaster',
    subsystem: 'block-storage',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1216, 306],
    port: 'BlockManagerMaster',
    process: 'Driver JVM RPC endpoints (BlockManagerMaster & BlockManagerMasterHeartbeat) + per-JVM client facade',
    role: 'Indexes which BlockManagers have reported each RDD, broadcast, or task-result block along with storage tiers, sizes, and memory accounting; answers location and peer lookups; and fans out block removal commands without ever storing or proxying bytes.',
    keyDistinction:
      'The directory returns reported holder addresses, not the block bytes or proof that the block is still readable.',
    startedBy: 'Driver SparkEnv registers master and heartbeat endpoints; executors look up RpcEndpointRefs',
    monitoredBy: 'Heartbeat forwarding from DAGScheduler/HeartbeatReceiver to BlockManagerMasterHeartbeatEndpoint',
    stoppedBy: 'Driver SparkEnv.stop() -> BlockManagerMaster.stop() stopping both driver endpoints',
    inputs: [
      'RegisterBlockManager, UpdateBlockInfo, GetLocations / GetLocationsAndStatus, GetPeers RPCs',
      'BlockManagerHeartbeat(blockManagerId) forwarded on executor heartbeats',
      'RemoveRdd, RemoveBroadcast, RemoveBlock, and RemoveExecutor notifications',
    ],
    outputs: [
      'Candidate BlockManagerId location lists, BlockStatus, and same-host local-dir hints',
      'Boolean UpdateBlockInfo / heartbeat replies triggering re-registration if manager is unknown',
      'Async fan-out removal and optional proactive replication commands to executor storage endpoints',
    ],
    internalState: [
      'blockLocations: JHashMap[BlockId, mutable.HashSet[BlockManagerId]]',
      'blockManagerInfo: mutable.HashMap[BlockManagerId, BlockManagerInfo] (endpoint, per-block statuses, memory caps)',
      'blockManagerIdByExecutor index and bounded executor local-directory cache for same-host reads',
    ],
    failureClues: [
      'Empty GetLocations result points to tellMaster=false, eviction, failed admission, or removed executor',
      'Location returned followed by fetch failure requires inspecting the serving executor host:port or ESS disk logs',
      'Non-blocking RemoveRdd / RemoveBroadcast clears metadata before remote executors finish deleting local files',
    ],
    sourceLinks: [
      {
        label: 'BlockManagerMasterEndpoint directory state',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L49-L104',
      },
      {
        label: 'BlockManagerMasterEndpoint UpdateBlockInfo mutations',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L828-L922',
      },
      {
        label: 'BlockManagerMasterHeartbeatEndpoint',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterHeartbeatEndpoint.scala#L26-L57',
      },
    ],
    docFile: 'block-manager-master.md',
  },

  'block-transfer-service': {
    id: 'block-transfer-service',
    name: 'NettyBlockTransferService',
    shortName: 'BlockTransferSvc',
    subsystem: 'block-storage',
    hostZone: 'worker-node-a',
    clusterPos: [912, 854],
    port: ':42001',
    process: 'Per-JVM Netty data-plane server & client (NettyBlockTransferService + NettyBlockRpcServer)',
    role: 'Moves raw block bytes (RDD partitions, broadcast pieces, indirect task results, non-ESS shuffle blocks, and replicated uploads) between Spark JVMs on a dedicated Netty port separate from scheduler RPC.',
    keyDistinction:
      'The OpenBlocks reply returns only a StreamHandle; subsequent FetchChunk responses carry the actual block bytes.',
    startedBy: 'Constructed in SparkEnv; bound and initialized inside BlockManager.initialize(appId) on spark.blockManager.port',
    monitoredBy: 'RetryingBlockTransferor I/O retries and Netty channel/stream lifecycle callbacks',
    stoppedBy: 'BlockManager.stop() closing TransportServer, TransportClientFactory, and TransportContext',
    inputs: [
      'fetchBlocks / fetchBlockSync(host, port, execId, blockIds) and uploadBlock / uploadBlockSync calls',
      'Inbound Netty RPCs: OpenBlocks, FetchShuffleBlocks, FetchChunk, UploadBlock, UploadBlockStream',
    ],
    outputs: [
      'StreamHandle(streamId, numChunks) via OneForOneStreamManager followed by streamed chunk bytes',
      'ManagedBuffer callbacks (in-memory or temp-file-backed above maxRemoteBlockSizeFetchToMem = 200m)',
      'Local BlockManager getLocalBlockData / putBlockData / putBlockDataAsStream invocations',
    ],
    internalState: [
      'Bound Netty TransportServer (advertised port in BlockManagerId) and TransportClientFactory',
      'NettyBlockRpcServer + OneForOneStreamManager (connection-scoped chunk streams)',
      'OneForOneBlockFetcher and RetryingBlockTransferor retry state',
    ],
    failureClues: [
      'Connection error before StreamHandle vs "block not found" on live server vs local download disk error are distinct failures',
      'RetryingBlockTransferor retries the same target; failing over to another replica is handled by BlockManager',
      'Large remote blocks (>200m default) stream to DownloadFileManager temp files instead of heap memory',
    ],
    sourceLinks: [
      {
        label: 'NettyBlockTransferService server setup & init',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/network/netty/NettyBlockTransferService.scala#L49-L104',
      },
      {
        label: 'NettyBlockRpcServer OpenBlocks handler',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/network/netty/NettyBlockRpcServer.scala#L78-L122',
      },
      {
        label: 'OneForOneBlockFetcher protocol & chunk streaming',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/OneForOneBlockFetcher.java#L282-L332',
      },
    ],
    docFile: 'block-transfer-service.md',
  },

  'rdd-cache': {
    id: 'rdd-cache',
    name: 'Persisted RDD Partition Path',
    shortName: 'RDD Cache',
    subsystem: 'block-storage',
    hostZone: 'worker-node-a',
    clusterPos: [156, 950],
    process: 'Executor JVM RDD.iterator / getOrCompute collaborating with local BlockManager',
    role: 'Implements the per-partition cache lookup, write-lock admission, memory-or-disk storage, and lineage recomputation fallback for RDDBlockId(rddId, partitionIndex) whenever a persisted RDD is evaluated.',
    keyDistinction:
      'A task can succeed and return all records on a cache miss even if the partition did not fit in memory and was left uncached.',
    startedBy: 'Driver records persistence policy on rdd.cache()/persist(); executor tasks trigger getOrCompute lazily on actions',
    monitoredBy: 'Spark UI Storage tab (fraction cached, memory/disk sizes) and BlockManagerMaster location reports',
    stoppedBy: 'rdd.unpersist(blocking) clearing policy and requesting cluster-wide block removal, or LRU memory eviction',
    inputs: [
      'RDD.iterator(split, context) with RDDBlockId(rddId, partitionIndex), StorageLevel, and lazy compute closure',
      'Local MemoryStore / DiskStore entries or remote peer copies via BlockManager',
    ],
    outputs: [
      'InterruptibleIterator of partition records from local hit, remote hit, freshly stored block, or uncached fallback',
      'Reported BlockStatus updates to BlockManagerMaster when admitted and optional replication',
    ],
    internalState: [
      'RDD.storageLevel on driver (PySpark default cache() is serialized MEMORY_ONLY, replication 1)',
      'BlockManager.getOrElseUpdate write-lock coordination preventing duplicate local computation',
      'Optional spark.rdd.cache.visibilityTracking.enabled gate requiring producing task completion',
    ],
    failureClues: [
      'rdd.is_cached only means persist() was called; check Spark UI Storage tab for actual cached partition count and size',
      'Remote cache hit returns records without automatically installing a new local replica',
      'Lost last replica of a finalized local checkpoint throws rather than recomputing from lineage',
    ],
    sourceLinks: [
      {
        label: 'RDD.iterator & getOrCompute entry point',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L430-L459',
      },
      {
        label: 'BlockManager.getOrElseUpdate cache contract',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1585-L1641',
      },
      {
        label: 'MemoryStore LRU victim selection',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala#L462-L525',
      },
    ],
    docFile: 'rdd-cache.md',
  },

  'broadcast-manager': {
    id: 'broadcast-manager',
    name: 'BroadcastManager',
    shortName: 'BroadcastManager',
    subsystem: 'block-storage',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1216, 474],
    process: 'Per-JVM BroadcastManager + TorrentBroadcastFactory (seeded on driver, resolved lazily on executors)',
    role: 'Seeds a read-only broadcast value on the driver as numbered 4 MiB torrent pieces in BlockManager, hands tasks a compact Broadcast handle, and lazily fetches missing pieces from driver or peers to reassemble and cache the value per JVM.',
    keyDistinction:
      'Tasks carry a small Broadcast handle rather than the value, and executors can fetch missing pieces from peers as well as the driver.',
    startedBy: 'Constructed inside each JVM SparkEnv; driver creates handles via SparkContext.broadcast / broadcastInternal',
    monitoredBy: 'Driver Created broadcast logs, executor Started reading broadcast logs, and ContextCleaner',
    stoppedBy: 'bc.unpersist() (removes executor copies), bc.destroy() (invalidates handle + driver), ContextCleaner, or SparkEnv.stop()',
    inputs: [
      'Driver value (JVM object, HashedRelation, or PythonBroadcast pickle file) and serializedOnly flag',
      'Executor TorrentBroadcast.getValue calls triggered lazily by tasks',
    ],
    outputs: [
      'Serializable TorrentBroadcast handle (broadcast ID, numBlocks, optional Adler-32 piece checksums)',
      'Reported broadcast_<id>_pieceN blocks (MEMORY_AND_DISK_SER) and local unreported broadcast_<id> whole value',
      'Local DISK_ONLY broadcast_<id>_python file or decrypted socket stream for Python workers',
    ],
    internalState: [
      'nextBroadcastId atomic Long counter and TorrentBroadcastFactory',
      'Per-JVM cachedValues map (strong ID keys, weak values) plus per-ID synchronization lock',
      'Transient soft/weak value reference on TorrentBroadcast handle',
    ],
    failureClues: [
      'Missing remote piece or Adler-32 checksum mismatch fails piece fetch without rerunning RDD lineage',
      'PythonBroadcast stores pieces in JVM while each Python worker unpickles its own copy (separate worker memory cost)',
      'unpersist() keeps the driver seed valid for re-fetching, whereas destroy() permanently invalidates the handle',
    ],
    sourceLinks: [
      {
        label: 'BroadcastManager & weak-value cache',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/broadcast/BroadcastManager.scala#L32-L81',
      },
      {
        label: 'TorrentBroadcast driver piece seeding',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L139-L186',
      },
      {
        label: 'TorrentBroadcast keyed lookup & reassembly',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/broadcast/TorrentBroadcast.scala#L249-L312',
      },
    ],
    docFile: 'broadcast-manager.md',
  },

  // 8. Shuffle (6)
  'map-output-tracker-master': {
    id: 'map-output-tracker-master',
    name: 'MapOutputTrackerMaster',
    shortName: 'MapOutputMaster',
    subsystem: 'shuffle',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1216, 390],
    port: 'MapOutputTracker',
    process: 'Driver JVM RPC endpoint (MapOutputTracker) + dedicated serialization dispatcher pool',
    role: 'Maintains the driver directory of accepted MapStatus entries per shuffle (indexed by logical mapIndex), serves compressed or broadcast status snapshots to executors, computes per-reducer statistics for AQE, and advances epoch on map-output invalidation.',
    keyDistinction:
      'A retry can put a new attempt into a map slot; the tracker tells reducers where to fetch the new output without ever forwarding shuffle bytes.',
    startedBy: 'Driver SparkEnv creates MapOutputTrackerMaster and registers the MapOutputTracker RPC endpoint',
    monitoredBy: 'DAGScheduler shuffle availability checks and AQE getStatistics queries',
    stoppedBy: 'ContextCleaner unregisters individual shuffles; SparkEnv.stop() stops dispatch pool and endpoint',
    inputs: [
      'DAGScheduler registerShuffle, registerMapOutput(shuffleId, mapIndex, MapStatus), and unregisterMapOutput',
      'Executor GetMapOutputStatuses(shuffleId) RPC asks on worker cache misses',
    ],
    outputs: [
      'Serialized compressed MapStatus array or Broadcast reference (>= minSizeForBroadcast) to executors',
      'MapOutputStatistics (estimated bytesByPartitionId) for AQE replanning',
      'Incremented epoch stamped on subsequent TaskSets to invalidate stale worker caches',
    ],
    internalState: [
      'shuffleStatuses: ConcurrentHashMap[Int, ShuffleStatus] with per-shuffle read/write lock',
      'ShuffleStatus.mapStatuses(mapIndex) holding (mapId, BlockManagerId location, estimated reduce sizes)',
      'Cached serialized/broadcast snapshot per shuffle and monotonic tracker epoch counter',
    ],
    failureClues: [
      'removeMapOutput clears a slot only if current location matches the reported failed address, protecting newer replacements',
      '"Missing an output location" in a reducer means a slot is null (lost/unregistered map output), not a zero-byte block',
      'status.mapId (typically taskAttemptId) names the physical shuffle file, distinct from logical mapIndex slot',
    ],
    sourceLinks: [
      {
        label: 'ShuffleStatus slot layout & addMapOutput',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L63-L108',
      },
      {
        label: 'MapOutputTrackerMasterEndpoint & dispatcher',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L844-L892',
      },
      {
        label: 'Status serialization & broadcast threshold',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L1739-L1785',
      },
    ],
    docFile: 'map-output-tracker-master.md',
  },

  'map-output-tracker-worker': {
    id: 'map-output-tracker-worker',
    name: 'MapOutputTrackerWorker',
    shortName: 'MapOutputWorker',
    subsystem: 'shuffle',
    hostZone: 'worker-node-a',
    clusterPos: [912, 950],
    process: 'Executor JVM shared shuffle metadata cache (one instance per executor SparkEnv)',
    role: 'Caches full shuffle MapStatus arrays per executor, fetches missing snapshots once per shuffle via per-shuffle KeyLock, filters out zero-byte blocks, groups nonempty ShuffleBlockIds by serving BlockManagerId, and invalidates its cache on higher task epochs.',
    keyDistinction:
      'A zero estimated size means an empty block that is skipped; a missing (null) map status means the location is unknown and throws MetadataFetchFailedException.',
    startedBy: 'Executor SparkEnv creates MapOutputTrackerWorker and resolves the driver MapOutputTracker RPC ref',
    monitoredBy: 'Executor updateEpoch(task.epoch) before every task run and SortShuffleManager.getReader calls',
    stoppedBy: 'State ends with executor JVM shutdown (base stop() is a no-op; does not stop driver endpoint)',
    inputs: [
      'SortShuffleManager.getMapSizesByExecutorId(shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)',
      'Driver GetMapOutputStatuses reply (inline compressed array or broadcast reference)',
      'Task epoch updates (Executor.TaskRunner) and RemoveShuffle storage endpoint messages',
    ],
    outputs: [
      'GetMapOutputStatuses(shuffleId) RPC ask to driver MapOutputTrackerMaster on cache miss',
      'Iterator[(BlockManagerId, Seq[(ShuffleBlockId, estimatedBytes, mapIndex)])] for BlockStoreShuffleReader',
    ],
    internalState: [
      'mapStatuses: ConcurrentHashMap[Int, Array[MapStatus]] per-executor cache',
      'Per-shuffle KeyLock deduplicating concurrent cold misses across tasks on the same executor',
      'Local tracker epoch cleared across all shuffles whenever task.epoch > current epoch',
    ],
    failureClues: [
      'MetadataFetchFailedException reports null BlockManagerId and mapId=-1 because the driver had a null map slot',
      '"Error communicating with MapOutputTracker" points to driver RPC reachability rather than producer shuffle files',
      '"Don\'t have map outputs for shuffle ... Got the map output locations" is a normal cold cache lookup',
    ],
    sourceLinks: [
      {
        label: 'MapOutputTrackerWorker cache-miss KeyLock',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L1667-L1698',
      },
      {
        label: 'Status range conversion & zero-size filter',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/MapOutputTracker.scala#L1951-L1966',
      },
      {
        label: 'Executor task epoch application',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/executor/Executor.scala#L883-L890',
      },
    ],
    docFile: 'map-output-tracker-worker.md',
  },

  'shuffle-writer': {
    id: 'shuffle-writer',
    name: 'Shuffle Writer',
    shortName: 'Shuffle Writer',
    subsystem: 'shuffle',
    hostZone: 'worker-node-a',
    clusterPos: [408, 950],
    process: 'Task-local writer in map executor JVM (BypassMergeSort / Unsafe / SortShuffleWriter)',
    role: 'Consumes one map partition iterator, routes each record to its destination reduce partition (with optional combine/spill), commits contiguous partition segments via IndexShuffleBlockResolver, and returns MapStatus metadata to the driver.',
    keyDistinction:
      'Sort-shuffle groups records contiguously by reduce partition on disk; it does not require sorting records by key unless an ordering is requested.',
    startedBy: 'ShuffleMapTask -> ShuffleWriteProcessor -> SortShuffleManager.getWriter(handle, mapId, context, metrics)',
    monitoredBy: 'Task ShuffleWriteMetrics (bytes/records written, write time) and ExternalSorter spill metrics',
    stoppedBy: 'ShuffleWriteProcessor calls writer.stop(success=true) returning MapStatus, or stop(false) cleaning scratch files',
    inputs: [
      'Parent RDD partition iterator of (key, value) pairs and ShuffleHandle selected on driver',
      'mapId (taskAttemptId), logical mapIndex (partitionId), TaskContext, and ShuffleWriteMetricsReporter',
    ],
    outputs: [
      'Committed local shuffle_<shuffleId>_<mapId>_0.data and .index files (plus optional checksum sidecar)',
      'MapStatus(shuffleServerId, partitionLengths, mapId) returned as the ShuffleMapTask result',
    ],
    internalState: [
      'BypassMergeSortShuffleWriter (<=200 reducers, no map combine): per-partition temp writers merged in order',
      'UnsafeShuffleWriter (relocatable serializer, no combine): serialized record pointer sorter and spill files',
      'SortShuffleWriter: ExternalSorter object buffer with optional map-side Aggregator and spill merge',
    ],
    failureClues: [
      'High memory/disk bytes spilled with modest final shuffle write size points to ExternalSorter or Unsafe spill merges',
      'Write failure calls stop(false) and deletes scratch files; stopping a successful writer keeps committed files for reducers',
      'Commit I/O exceptions fail the map attempt on local disk; downstream FetchFailed is a separate consumer event',
    ],
    sourceLinks: [
      {
        label: 'SortShuffleManager writer selection order',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/sort/SortShuffleManager.scala#L91-L111',
      },
      {
        label: 'ShuffleWriteProcessor write & stop lifecycle',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/ShuffleWriteProcessor.scala#L30-L61',
      },
      {
        label: 'UnsafeShuffleWriter serialized pages & spill',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java#L233-L310',
      },
    ],
    docFile: 'shuffle-writer.md',
  },

  'shuffle-reader': {
    id: 'shuffle-reader',
    name: 'Shuffle Reader',
    shortName: 'Shuffle Reader',
    subsystem: 'shuffle',
    hostZone: 'worker-node-b',
    clusterPos: [1204, 758],
    process: 'Task-local BlockStoreShuffleReader + ShuffleBlockFetcherIterator in reducing executor JVM',
    role: 'Reads a reducer task partition range across map outputs via local disk, host-local direct reads, or asynchronous remote Netty fetches (bounded by in-flight limits), decompresses/deserializes streams, and applies optional reduce-side aggregation or sorting.',
    keyDistinction:
      'Reducer 2 reads only its slice (e.g., shuffle_7_41_2) from each map output, never the entire map data file.',
    startedBy: 'ShuffledRDD.compute / ShuffledRowRDD.compute -> SortShuffleManager.getReader(...).read()',
    monitoredBy: 'ShuffleReadMetrics (local/remote blocks & bytes, fetch wait time, records read)',
    stoppedBy: 'Iterator exhaustion or TaskContext completion listener releasing buffers and deleting temp download files',
    inputs: [
      'Grouped (BlockManagerId, Seq[(ShuffleBlockId, estimatedBytes, mapIndex)]) from MapOutputTrackerWorker',
      'Local BlockManager segments, host-local shuffle files, or remote ManagedBuffers from BlockStoreClient',
    ],
    outputs: [
      'InterruptibleIterator of decoded (K, V) pairs, merged combiners, or ExternalSorter-sorted records',
      'FetchFailedException(bmAddress, shuffleId, mapId, mapIndex, reduceId) if retries exhaust',
    ],
    internalState: [
      'ShuffleBlockFetcherIterator queues (local, host-local, deferred remote FetchRequests)',
      'In-flight byte, request-count, and blocks-per-address throttling budgets + results LinkedBlockingQueue',
      'Optional reduce-side ExternalAppendOnlyMap (aggregator) or ExternalSorter (key ordering)',
    ],
    failureClues: [
      '"Getting ... non-empty blocks" log breaks down local vs host-local vs remote fetches before reading bytes',
      'High fetch wait time with high remote bytes points to producer host/network; high spill after fetch points to reduce-side sort/agg',
      'Fetch failure naming port 7337 points to NodeManager External Shuffle Service rather than driver RPC',
    ],
    sourceLinks: [
      {
        label: 'BlockStoreShuffleReader read, combine & sort',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/BlockStoreShuffleReader.scala#L72-L157',
      },
      {
        label: 'ShuffleBlockFetcherIterator local/host-local/remote split',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L711-L744',
      },
      {
        label: 'Fetch throttling limits & request deferral',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala#L1192-L1248',
      },
    ],
    docFile: 'shuffle-reader.md',
  },

  'shuffle-block-resolver': {
    id: 'shuffle-block-resolver',
    name: 'IndexShuffleBlockResolver',
    shortName: 'ShuffleResolver',
    subsystem: 'shuffle',
    hostZone: 'worker-node-a',
    clusterPos: [660, 950],
    process: 'Executor JVM file-format resolver (IndexShuffleBlockResolver + DiskBlockManager)',
    role: 'Commits each map output into permanent shuffle_<shuffleId>_<mapId>_0.data and .index files (8*(R+1) big-endian 64-bit offsets), and translates logical ShuffleBlockId / ShuffleBlockBatchId reads into exact FileSegmentManagedBuffer byte ranges.',
    keyDistinction:
      'MapOutputTracker reports an estimated size; the on-disk .index file supplies the exact start and end byte offsets.',
    startedBy: 'Instantiated by SortShuffleManager (serving) and LocalDiskShuffleExecutorComponents (writing)',
    monitoredBy: 'Local disk I/O exceptions propagated through writer commit or BlockManager.getLocalBlockData',
    stoppedBy: 'SortShuffleManager.unregisterShuffle calls removeDataByMap; resolver.stop() itself is a no-op',
    inputs: [
      'writeMetadataFileAndCommit(shuffleId, mapId, exact partitionLengths, checksums, tempDataFile)',
      'getBlockData(ShuffleBlockId or ShuffleBlockBatchId) and optional host-local executor local-dirs',
    ],
    outputs: [
      'Committed shuffle_<id>_<mapId>_0.data, .index, and optional checksum files in hashed local-dirs',
      'FileSegmentManagedBuffer(dataFile, startOffset, endOffset - startOffset) without copying bytes',
    ],
    internalState: [
      'Synchronized commit guard checking for already-committed matching index/data pairs',
      'DiskBlockManager hash-based local-directory mapping (index and data may reside in different subdirs)',
      'On-disk 64-bit prefix-sum offset table (placeholder _0 suffix in permanent filenames)',
    ],
    failureClues: [
      'Search producer local-dirs for shuffle_<id>_<mapId>_0.{data,index}, never _<reduceId>.data or mapIndex',
      'Missing .index prevents lookup even when .data still exists; equal adjacent offsets signify an empty partition',
      'Index length must equal 8 * (numReducers + 1) bytes and its final offset must match the .data file length',
    ],
    sourceLinks: [
      {
        label: 'IndexShuffleBlockResolver host-local dirs',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L126-L155',
      },
      {
        label: 'writeMetadataFileAndCommit index/data commit',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L391-L461',
      },
      {
        label: 'getBlockData offset seek & FileSegmentManagedBuffer',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/shuffle/IndexShuffleBlockResolver.scala#L615-L658',
      },
    ],
    docFile: 'shuffle-block-resolver.md',
  },

  'external-shuffle-service': {
    id: 'external-shuffle-service',
    name: 'External Shuffle Service',
    shortName: 'Ext Shuffle Service',
    subsystem: 'shuffle',
    hostZone: 'worker-node-a',
    clusterPos: [912, 758],
    port: ':7337',
    process: 'Optional NodeManager auxiliary service inside NM JVM (YarnShuffleService)',
    role: 'Runs a Netty shuffle server inside each NodeManager so reducers can fetch local shuffle file slices (and optionally disk RDD blocks) even after the producing executor JVM exits, decoupling shuffle availability from executor lifetime.',
    keyDistinction:
      'Registration tells the service where the executor files live without copying them; it outlives the executor JVM, not a lost node or disk.',
    startedBy: 'NodeManager serviceInit when spark_shuffle auxiliary service is installed and configured on workers',
    monitoredBy: 'NodeManager lifecycle, optional registration LevelDB/RocksDB recovery state, and fetch retry metrics',
    stoppedBy: 'stopApplication clears app registrations (YARN cleans dirs); NM serviceStop closes Netty server and DB',
    inputs: [
      'Executor BlockManager.initialize synchronous RegisterExecutor(appId, execId, ExecutorShuffleInfo)',
      'NM initializeApplication service data from ExecutorRunnable container launch context',
      'Reducer FetchShuffleBlocks(appId, execId, shuffleId, mapIds, reduceIds) or legacy OpenBlocks',
    ],
    outputs: [
      'Registration acknowledgement to producing executors',
      'StreamHandle and local disk shuffle file segments served via ExternalShuffleBlockResolver to reducers',
    ],
    internalState: [
      'ExternalShuffleBlockResolver registry keyed by (appId, execId) -> localDirs + subDirsPerLocalDir',
      'Bounded shuffle index offset cache (caches offsets, never shuffle payload bytes)',
      'ExternalBlockHandler + Netty TransportServer bound to spark.shuffle.service.port (default 7337)',
    ],
    failureClues: [
      '"Failed to connect to external shuffle server" at executor startup points to missing NM aux service or mismatched port 7337',
      '"Executor is not registered (appId=..., execId=...)" on reducer points to NM restart without state DB recovery or app cleanup',
      'Setting spark.shuffle.service.enabled=true in Spark conf does not start the NM auxiliary service automatically',
    ],
    sourceLinks: [
      {
        label: 'YarnShuffleService serviceInit & app lifecycle',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/common/network-yarn/src/main/java/org/apache/spark/network/yarn/YarnShuffleService.java#L268-L338',
      },
      {
        label: 'ExternalShuffleBlockResolver registry & segment lookup',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalShuffleBlockResolver.java#L127-L178',
      },
      {
        label: 'ExternalBlockHandler FetchShuffleBlocks stream',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java#L188-L236',
      },
    ],
    docFile: 'external-shuffle-service.md',
  },

  // 9. SQL Execution (5)
  'sql-planner': {
    id: 'sql-planner',
    name: 'Spark SQL Physical Planner',
    shortName: 'SQL Planner',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [584, 306],
    process: 'Driver JVM Catalyst planning pipeline (SessionState.planner + QueryExecution)',
    role: 'Transforms an analyzed and optimized LogicalPlan through ordered SparkStrategies (such as JoinSelection) into a physical SparkPlan tree, then runs preparation rules like EnsureRequirements to insert exchanges and local sorts.',
    keyDistinction:
      'The SQL planner chooses physical operators; core DAGScheduler and TaskScheduler later form stages and launch tasks.',
    startedBy: 'QueryExecution lazily evaluates sparkPlan and executedPlan on explain() or DataFrame actions',
    monitoredBy: 'df.explain("extended" | "cost" | "formatted") and SQL UI plan visualization',
    stoppedBy: 'Per-query planning completes when executedPlan is constructed; QueryExecution is GCed after query finishes',
    inputs: [
      'Cloned optimized LogicalPlan and table/column size estimates from Catalyst Analyzer & Optimizer',
      'Ordered SparkPlanner strategies (JoinSelection, Aggregation, PythonEvals, etc.) and SQLConf settings',
    ],
    outputs: [
      'QueryExecution.sparkPlan (first physical candidate returned by planner.plan(ReturnAnswer(plan)))',
      'QueryExecution.executedPlan after prepareForExecution (EnsureRequirements, AQE wrapper, codegen)',
      'RDD[InternalRow] produced when SparkPlan.execute() runs on an action',
    ],
    internalState: [
      'QueryExecution staged plan fields: analyzed, optimizedPlan, sparkPlan, executedPlan',
      'Physical-to-logical origin links attached by SparkStrategies.plan (required by AQE)',
      'JoinSelection build-side eligibility and autoBroadcastJoinThreshold (default 10 MiB) checks',
    ],
    failureClues: [
      'Unresolved column/table fails during analysis before any Spark stage or executor task runs',
      'Stale size estimates can pick a broadcast join that later times out or OOMs the driver during build',
      'executedPlan means prepared for execution, not already executed; compare estimates in explain("cost") vs SQL UI runtime size',
    ],
    sourceLinks: [
      {
        label: 'QueryExecution plan pipeline & stages',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L343-L405',
      },
      {
        label: 'SparkStrategies JoinSelection & fallback order',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L244-L359',
      },
      {
        label: 'EnsureRequirements exchange & sort insertion',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/EnsureRequirements.scala#L179-L195',
      },
    ],
    docFile: 'sql-planner.md',
  },

  'adaptive-query-execution': {
    id: 'adaptive-query-execution',
    name: 'Adaptive Query Execution (AQE)',
    shortName: 'Adaptive Query Exec',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [584, 390],
    process: 'Driver JVM AdaptiveSparkPlanExec execution loop',
    role: 'Materializes leaf ShuffleQueryStageExec and BroadcastQueryStageExec stages, reads actual runtime MapOutputStatistics, and replans unread portions of the query to coalesce partitions, switch joins to broadcast, or split skewed join reads.',
    keyDistinction:
      'AQE changes how later tasks read already-written shuffle blocks; it never splits or migrates a currently running task.',
    startedBy: 'InsertAdaptiveSparkPlan wraps eligible plans when spark.sql.adaptive.enabled=true',
    monitoredBy: 'SQL UI initial vs final plan (isFinalPlan=true) and AQEShuffleRead skew/coalesce metrics',
    stoppedBy: 'Marks isFinalPlan=true when result stage finishes, or cancels outstanding exchanges on stage materialization failure',
    inputs: [
      'Initial physical plan with logical links and child exchanges',
      'Materialized ShuffleQueryStageExec.mapStats (bytesByPartitionId) and exchange runtime size/row stats',
    ],
    outputs: [
      'Materialized ShuffleQueryStageExec / BroadcastQueryStageExec futures (with canonical stage reuse)',
      'Re-optimized physical plans substituting LogicalQueryStage, BroadcastHashJoin, or AQEShuffleReadExec (skewed/coalesced)',
    ],
    internalState: [
      'AdaptiveSparkPlanExec currentPhysicalPlan, stageCache, stage-ID counter, and isFinalPlan flag',
      'AdaptiveExecutionContext scoped to one QueryExecution',
      'Cost comparator gate adopting cheaper or different equal-cost replanned candidates',
    ],
    failureClues: [
      'Initial explain() before running an action does not show final AQE decisions; inspect SQL UI with isFinalPlan=true',
      'Skew split requires partition bytes strictly > max(factor * median, advisoryThreshold) and permissible join replication side',
      'A required stage materialization failure cancels in-flight exchanges and propagates the error',
    ],
    sourceLinks: [
      {
        label: 'InsertAdaptiveSparkPlan admission & logical links',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/InsertAdaptiveSparkPlan.scala#L49-L135',
      },
      {
        label: 'AdaptiveSparkPlanExec event loop & cost gate',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L359-L497',
      },
      {
        label: 'OptimizeSkewedJoin cutoffs & split pairings',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala#L59-L193',
      },
    ],
    docFile: 'adaptive-query-execution.md',
  },

  'broadcast-exchange': {
    id: 'broadcast-exchange',
    name: 'BroadcastExchangeExec',
    shortName: 'BroadcastExchange',
    subsystem: 'sql-execution',
    hostZone: 'driver-jvm',
    subZone: 'SQL PLANNING & SCHEDULERS',
    clusterPos: [584, 474],
    process: 'Driver JVM async build job on broadcast-exchange thread pool',
    role: 'Collects all rows of the build-side child onto the driver, constructs an indexed HashedRelation (LongHashedRelation or UnsafeHashedRelation), checks row and 8 GiB table-size guards, and publishes it via SparkContext.broadcastInternal(serializedOnly=true).',
    keyDistinction:
      'BroadcastExchangeExec builds and publishes the lookup relation on the driver; join tasks probe it later after resolving the Broadcast handle.',
    startedBy: 'EnsureRequirements inserts it for BroadcastDistribution; doPrepare or AQE BroadcastQueryStageExec starts relationFuture',
    monitoredBy: 'SQL UI BroadcastExchange metrics (time to collect, time to build, time to broadcast, data size, output rows)',
    stoppedBy: 'Completes promise with Broadcast[HashedRelation], fails on guard/OOM, or cancels tagged job on spark.sql.broadcastTimeout',
    inputs: [
      'HashedRelationBroadcastMode (bound build keys, null-aware flag) and child SparkPlan',
      'Collected partition row counts and compressed row buffers from child.executeCollectIterator()',
    ],
    outputs: [
      'Built LongHashedRelation or UnsafeHashedRelation published to BroadcastManager',
      'Broadcast[HashedRelation] handle returned to BroadcastHashJoinExec via executeBroadcast()',
      'Runtime statistics (dataSize memory estimate and numOutputRows) exposed to AQE',
    ],
    internalState: [
      'runId / Spark job tag for targeted cancellation, lazy relationFuture, and completionPromise',
      'SQL metrics (dataSize, numOutputRows, collectTime, buildTime, broadcastTime)',
      'Post-build maxBroadcastRows and spark.sql.maxBroadcastTableSize (8 GiB default) guards',
    ],
    failureClues: [
      'Exceeding spark.sql.broadcastTimeout (300s default) logs a timeout, cancels the tagged build job, and throws',
      'Driver OOM during build fails the future; raising broadcastTimeout cannot fix an oversized build side',
      'dataSize measures built HashedRelation memory, not planner estimate or wire bytes; broadcast hints do not bypass 8 GiB / row caps',
    ],
    sourceLinks: [
      {
        label: 'BroadcastExchangeExec submission & cancellation',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L46-L111',
      },
      {
        label: 'Build relation, guards, and broadcastInternal',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/BroadcastExchangeExec.scala#L161-L229',
      },
      {
        label: 'HashedRelation Long vs Unsafe selection',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashedRelation.scala#L136-L165',
      },
    ],
    docFile: 'broadcast-exchange.md',
  },

  'broadcast-hash-join': {
    id: 'broadcast-hash-join',
    name: 'BroadcastHashJoinExec',
    shortName: 'BroadcastHashJoin',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1428, 758],
    process: 'Executor JVM task operator (codegen or row path) + Driver plan node',
    role: 'Resolves the Broadcast[HashedRelation] handle via asReadOnlyCopy() inside each streamed-side partition task, probes the in-memory hash table for every streamed row, evaluates residual non-equi predicates, and emits joined rows without shuffling the streamed side.',
    keyDistinction:
      'A hash table hit alone is not a SQL match: candidate rows must also satisfy any residual join condition (and left-outer misses emit one null-padded row).',
    startedBy: 'Planned by JoinSelection (or AQE replan); executes inside each streamed-side executor task',
    monitoredBy: 'SQL UI BroadcastHashJoin output rows and task peak execution memory metrics',
    stoppedBy: 'Finishes when streamed partition iterator ends (broadcast handle remains reusable across tasks/retries)',
    inputs: [
      'Broadcast[HashedRelation] handle from BroadcastExchangeExec',
      'Streamed child partition RDD[InternalRow] (UnspecifiedDistribution; no join shuffle required)',
      'joinType, buildSide (BuildLeft/BuildRight), bound keys, optional residual condition, isNullAwareAntiJoin',
    ],
    outputs: [
      'Joined RDD[InternalRow] preserving streamed child partitioning (inner, outer, semi, anti, or null-aware anti)',
      'Updated numOutputRows and per-task peak execution memory metric',
    ],
    internalState: [
      'Shared backing storage from broadcast.value.asReadOnlyCopy() (separate per-task lookup cursors, not duplicate maps)',
      'Bound key projections, reusable joined output row, and whole-stage codegen probe loop',
      'Distinct single-key unique lookup vs multi-match iterator and null-aware anti-join branches',
    ],
    failureClues: [
      'Summing peak execution memory across tasks over-counts executor RAM because tasks share one read-only backing map',
      'Large output row count relative to streamed input indicates key multiplicity / join explosion even with a small build side',
      'Build timeout fails before probe tasks start; task failure during probe retries only that streamed partition',
    ],
    sourceLinks: [
      {
        label: 'BroadcastHashJoinExec contract & guards',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BroadcastHashJoinExec.scala#L40-L89',
      },
      {
        label: 'Row & codegen probe paths',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BroadcastHashJoinExec.scala#L133-L219',
      },
      {
        label: 'HashJoin outer probe & residual predicates',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala#L184-L273',
      },
    ],
    docFile: 'broadcast-hash-join.md',
  },

  'python-udf-operator': {
    id: 'python-udf-operator',
    name: 'Python UDF Physical Operator',
    shortName: 'Python UDF Exec',
    subsystem: 'sql-execution',
    hostZone: 'worker-node-b',
    clusterPos: [1652, 758],
    process: 'Executor JVM EvalPythonExec (BatchEvalPythonExec / ArrowEvalPythonExec)',
    role: 'Extracts non-JVM scalar PythonUDFs into a physical operator that projects and deduplicates only UDF arguments to a local Python worker via pickle or Arrow batches, buffers pass-through columns in a FIFO HybridRowQueue, and joins returning results one-for-one.',
    keyDistinction:
      'A scalar Python UDF operator returns one value per input row; the downstream JVM Filter operator decides which rows to keep.',
    startedBy: 'ExtractPythonUDFs rewrites logical plan; PythonEvals maps it to BatchEvalPythonExec or ArrowEvalPythonExec',
    monitoredBy: 'PythonSQLMetrics (data sent/returned, number of output rows, worker timings)',
    stoppedBy: 'Task completion closes the task-local HybridRowQueue and releases/closes the runner socket',
    inputs: [
      'Child partition InternalRow (or supported columnar Arrow) stream',
      'Projected and deduplicated PythonUDF argument expressions and eval type (SQL_BATCHED_UDF / SQL_ARROW_BATCHED_UDF)',
    ],
    outputs: [
      'Pickled or Arrow record batches streamed to BasePythonRunner -> local Python worker',
      'JoinedRow / UnsafeProjection combining oldest pass-through row with returned UDF column value',
    ],
    internalState: [
      'EvalPythonPartitionEvaluator deduplicated argument offsets and named-argument metadata',
      'Task-local FIFO HybridRowQueue (spills to local disk if JVM producer outpaces Python worker)',
      'BatchEvalPythonExec pickle converter vs ArrowEvalPythonExec Arrow batch type validator',
    ],
    failureClues: [
      'Check explain("formatted") for BatchEvalPython or ArrowEvalPython to confirm data is actually crossing to Python',
      'A Python exception in the UDF fails the task attempt immediately (never silently substitutes NULL)',
      'Incompatible Arrow return schema fails on the JVM evaluator side; wide inputs can cause HybridRowQueue disk spill',
    ],
    sourceLinks: [
      {
        label: 'ExtractPythonUDFs logical rewrite',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala#L304-L382',
      },
      {
        label: 'EvalPythonEvaluatorFactory HybridRowQueue alignment',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/python/EvalPythonEvaluatorFactory.scala#L78-L174',
      },
      {
        label: 'ArrowEvalPythonExec batch evaluation & type check',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ArrowEvalPythonExec.scala#L238-L275',
      },
    ],
    docFile: 'python-udf-operator.md',
  },

  // 10. Python Workers & Output Commits (6)
  'python-worker-factory': {
    id: 'python-worker-factory',
    name: 'PythonWorkerFactory',
    shortName: 'PythonWorkerFactory',
    subsystem: 'python-output',
    hostZone: 'worker-node-b',
    clusterPos: [1204, 854],
    port: 'local socket',
    process: 'Executor JVM factory cached in SparkEnv (manages local pyspark.daemon & forked worker sockets)',
    role: 'Lazily starts a local pyspark.daemon (Unix) or direct worker process, hands executor runners a connected SocketChannel + PythonWorkerHandle from an idle pool or newly forked child, and evicts or destroys dead/excess idle workers.',
    keyDistinction:
      'The factory supplies the local process and connected socket; BasePythonRunner sends the function and pyspark.worker executes it.',
    startedBy: 'Lazily created and cached in executor SparkEnv on first BasePythonRunner.compute() for a Python key',
    monitoredBy: 'Idle worker monitor thread draining inactive pools and daemon connect retry logic',
    stoppedBy: 'SparkEnv.stop() shuts down all cached factories, daemons, and idle worker processes',
    inputs: [
      'Cache key (pythonExec, workerModule, daemonModule, effective env) and daemon-mode flag',
      'Runner create() acquire calls and releaseWorker() / destroyWorker() returns',
    ],
    outputs: [
      'PythonWorker (connected non-blocking SocketChannel + selector) and PythonWorkerHandle (PID/liveness)',
      'Spawned pyspark.daemon (Unix) or direct pythonExec -m workerModule subprocesses inside the executor container',
    ],
    internalState: [
      'daemon process handle + advertised loopback port or Unix-domain socket path',
      'FIFO idleWorkers queue (capped by spark.python.factory.idleWorkerMaxPoolSize) and worker-to-handle map',
      'simpleWorkers map for non-daemon / Windows direct-launched processes',
    ],
    failureClues: [
      '"Bad data in ... standard output" during daemon startup means stray stdout (e.g., sitecustomize.py print) corrupted port handshake',
      '"Python worker failed to connect back" occurs on direct-launch path; inspect executor Python path and child stderr',
      'spark.python.use.daemon and spark.python.worker.reuse are separate flags; idleWorkerMaxPoolSize does not cap concurrent tasks',
    ],
    sourceLinks: [
      {
        label: 'PythonWorkerFactory socket wrapper & state',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala#L40-L150',
      },
      {
        label: 'Daemon startup & socket handshake',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala#L338-L414',
      },
      {
        label: 'pyspark.daemon fork & child reuse loop',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/daemon.py#L211-L278',
      },
    ],
    docFile: 'python-worker-factory.md',
  },

  'python-worker': {
    id: 'python-worker',
    name: 'Python Worker Process',
    shortName: 'Python Worker',
    subsystem: 'python-output',
    hostZone: 'worker-node-b',
    clusterPos: [1652, 854],
    port: 'local socket',
    process: 'Forked/spawned Python subprocess inside executor container (pyspark.worker)',
    role: 'Decodes one task invocation init frame, checks Python major.minor compatibility, installs Python TaskContext and incremental broadcasts, executes the deserialized partition function or SQL UDF over streamed inputs, and returns results, metrics, and END_OF_STREAM.',
    keyDistinction:
      'Worker reuse keeps the Python interpreter and broadcast registry alive, not the previous task context or input iterator.',
    startedBy: 'Forked by pyspark.daemon on accepted socket (Unix) or launched directly by PythonWorkerFactory',
    monitoredBy: 'Executor BasePythonRunner socket protocol + kill monitor and container PMEM enforcement',
    stoppedBy: 'Stream misalignment (exits instead of reusing), reuse=false, idle eviction, killTimeout, or executor exit',
    inputs: [
      'Length-delimited init frame (splitIndex, Python version, TaskContext, SparkFiles, broadcast diffs, evalType, command)',
      'Streamed partition input batches (pickle or Arrow) terminated by END_OF_DATA_SECTION / END_OF_STREAM',
    ],
    outputs: [
      'START_OF_INIT_RESPONSE (empty on success or error payload) to local executor JVM',
      'Serialized output frames, metrics, END_OF_DATA_SECTION, accumulator updates, and END_OF_STREAM reuse confirmation',
    ],
    internalState: [
      'Process-scoped interpreter, imported modules, and incremental broadcast registry',
      'Per-invocation SparkSocketMessageReceiver, WorkerInitInfo, TaskContext, profiler, and serializers',
      'Optional RLIMIT_AS memory cap when spark.python.worker.memory is set on supported OS',
    ],
    failureClues: [
      'Init response error (e.g., PYTHON_VERSION_MISMATCH or missing module) aborts before any partition rows run',
      'PYTHON_EXCEPTION_THROWN after init carries the user traceback back to the executor task attempt',
      'Unexpected EOF without a Python traceback points to OOM/SIGKILL or native crash (enable faulthandler / check YARN PMEM)',
    ],
    sourceLinks: [
      {
        label: 'pyspark.worker init & evalType dispatch',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/worker.py#L164-L239',
      },
      {
        label: 'Worker cleanup & END_OF_STREAM reply',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/worker.py#L290-L341',
      },
      {
        label: 'Worker version check & broadcast registry',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/python/pyspark/worker_util.py#L196-L288',
      },
    ],
    docFile: 'python-worker.md',
  },

  'python-runner': {
    id: 'python-runner',
    name: 'Executor-Side BasePythonRunner',
    shortName: 'BasePythonRunner',
    subsystem: 'python-output',
    hostZone: 'worker-node-b',
    clusterPos: [1428, 854],
    port: 'local socket',
    process: 'Executor JVM task-scoped runner (org.apache.spark.api.python.BasePythonRunner)',
    role: 'Acquires a local Python worker socket from SparkEnv, sends the init header and incremental broadcast diffs, multiplexes input feeding and result reading via a non-blocking selector (or pipelined UDF writer), and releases the worker only on clean END_OF_STREAM.',
    keyDistinction:
      'If the consumer stops reading early, task cleanup closes the worker socket instead of returning it to the reusable pool.',
    startedBy: 'PythonRDD.compute (NON_UDF) or SQL Python operators (e.g., BatchEvalPythonExec / ArrowEvalPythonExec)',
    monitoredBy: 'TaskContext completion listener, cancellation MonitorThread (killTimeout), and boot/init/finish timing logs',
    stoppedBy: 'Clean END_OF_STREAM releases worker once (atomic releasedOrClosed CAS); early stop/error closes or destroys it',
    inputs: [
      'Parent JVM partition iterator, splitIndex, TaskContext, and serialized PythonFunction / SQL eval metadata',
      'Connected PythonWorker socket from PythonWorkerFactory and worker result/control frames',
    ],
    outputs: [
      'START_OF_INIT_MESSAGE header + serialized partition inputs + END_OF_DATA_SECTION / END_OF_STREAM to worker',
      'InterruptibleIterator[OUT] yielded back to the JVM task thread plus Python accumulator updates',
    ],
    internalState: [
      'Writer (header + buffered input framing) and ReaderIterator (init response + data/control frame decoder)',
      'Non-blocking NIO selector loop on task thread (or separate blocking writer thread for pipelined UDFs)',
      'AtomicBoolean releasedOrClosed and cancellation MonitorThread (spark.python.task.killTimeout)',
    ],
    failureClues: [
      'Executor log "Times: total ... boot ... init ... finish" isolates worker startup/init cost from compute time',
      'Socket I/O exception with a recorded writerException indicates an upstream JVM iterator failure, not a Python bug',
      'Do not confuse api.python.BasePythonRunner (executor partition streaming) with deploy.PythonRunner (driver launcher)',
    ],
    sourceLinks: [
      {
        label: 'BasePythonRunner.compute worker acquisition & iterator',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/PythonRunner.scala#L690-L806',
      },
      {
        label: 'Non-blocking selector loop & buffering',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/PythonRunner.scala#L1440-L1490',
      },
      {
        label: 'Error selection & cancellation monitor',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/api/python/PythonRunner.scala#L1291-L1368',
      },
    ],
    docFile: 'python-runner.md',
  },

  'file-format-writer': {
    id: 'file-format-writer',
    name: 'FileFormatWriter',
    shortName: 'FileFormatWriter',
    subsystem: 'python-output',
    hostZone: 'worker-node-b',
    clusterPos: [1204, 950],
    process: 'Driver write coordinator + executor FileFormatDataWriter tasks (Spark SQL DataSource V1)',
    role: 'Validates output schema and calls FileFormat.prepareWrite on the driver, runs WriteFilesExec/executeTask per partition to encode rows into Parquet/file writers (with dynamic directory/bucket rollover), collects WriteTaskResults, and invokes FileCommitProtocol.commitJob.',
    keyDistinction:
      'Each write task returns a compact TaskCommitMessage and ExecutedWriteSummary to the driver, not the written Parquet files.',
    startedBy: 'V1 write commands such as InsertIntoHadoopFsRelationCommand on df.write.parquet(...)',
    monitoredBy: 'Driver WriteJobStatsTracker metrics and "Start to commit write Job" / "committed" logs',
    stoppedBy: 'Returns updated partition path set after commitJob, or runs abortTask / abortJob on exceptions',
    inputs: [
      'SparkPlan, FileFormat, FileCommitProtocol, OutputSpec, partition/bucket spec, Hadoop conf, options',
      'Executor partition InternalRow iterators ordered by partition/bucket columns when required',
    ],
    outputs: [
      'Encoded part files written via OutputWriter to paths allocated by FileCommitProtocol',
      'WriteTaskResult(TaskCommitMessage, ExecutedWriteSummary) sent from each task to driver',
      'Set[String] of updated partition paths returned to caller for catalog refresh after commitJob',
    ],
    internalState: [
      'Serializable WriteJobDescription (writerFactory, dataColumns, partitionColumns, maxRecordsPerFile)',
      'Per-task SingleDirectoryDataWriter or DynamicPartitionDataWriter (active partition, file counter, record counter)',
      'Driver-collected array of WriteTaskResults and job-level WriteJobStatsTrackers',
    ],
    failureClues: [
      'Executor log "Task attempt ... aborted" pinpoints a write, close, or task-commit failure on that partition',
      'Driver log "Start to commit write Job" without matching "committed" points to driver-side job finalization failure',
      'Static overwrite deletes existing target files before tasks run; a later job abort cannot roll back deleted data',
    ],
    sourceLinks: [
      {
        label: 'FileFormatWriter WriteJobDescription prep',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatWriter.scala#L88-L146',
      },
      {
        label: 'Driver task result reduction & commitJob',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatWriter.scala#L292-L345',
      },
      {
        label: 'FileFormatDataWriter path selection & encoding',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatDataWriter.scala#L264-L319',
      },
    ],
    docFile: 'file-format-writer.md',
  },

  'file-commit-protocol': {
    id: 'file-commit-protocol',
    name: 'FileCommitProtocol',
    shortName: 'FileCommitProtocol',
    subsystem: 'python-output',
    hostZone: 'worker-node-b',
    clusterPos: [1428, 950],
    process: 'Serializable driver/executor protocol (default: SQLHadoopMapReduceCommitProtocol)',
    role: 'Separates file encoding from publication by allocating attempt staging paths, gating Hadoop OutputCommitter task commits via OutputCommitCoordinator, finalizing Hadoop commitJob on the driver, and replacing touched directories during dynamic partition overwrite.',
    keyDistinction:
      'In dynamic partition overwrite, untouched directories keep their old data, and touched directories are replaced separately rather than in one atomic swap.',
    startedBy: 'Instantiated per V1 write on driver (setupJob) and serialized to executor tasks (setupTask)',
    monitoredBy: 'FileFormatWriter driver/executor hooks and "Using output committer class ..." logs',
    stoppedBy: 'Driver commitJob deletes staging directory; abortTask / abortJob perform best-effort staging cleanup',
    inputs: [
      'Write UUID, target path, dynamicPartitionOverwrite flag, JobContext (driver), TaskAttemptContext (executor)',
      'newTaskTempFile(dir, FileNameSpec) path requests from FileFormatDataWriter',
      'Collected Seq[TaskCommitMessage] (addedAbsPathFiles + touched partitionPaths) at driver commitJob',
    ],
    outputs: [
      'Attempt work paths (e.g., under .spark-staging-<uuid>) where format writers emit part files',
      'TaskCommitMessage(addedAbsPathFiles, partitionPaths) returned to driver',
      'Hadoop commitJob, absolute-path renames, dynamic partition directory replacements, and staging cleanup',
    ],
    internalState: [
      'Transient per-JVM Hadoop mapreduce.OutputCommitter (FileOutputCommitter algorithm 1 or 2, or custom)',
      'Per-attempt addedAbsPathFiles (temp -> final absolute path) and partitionPaths (touched default dirs)',
      'spark.sql.sources.commitProtocolClass (Spark adapter) vs spark.sql.sources.outputCommitterClass (Hadoop committer)',
    ],
    failureClues: [
      'Visible part-... files or task success do not prove atomic job commit (depends on Hadoop committer algorithm 1 vs 2)',
      'Driver "Failed to rename" after all tasks succeeded points to destination conflict or permissions during commitJob',
      'Dynamic overwrite deletes/renames partitions one by one; failure midway leaves prior replacements visible and staging dirs behind',
    ],
    sourceLinks: [
      {
        label: 'SQLHadoopMapReduceCommitProtocol selection',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SQLHadoopMapReduceCommitProtocol.scala#L33-L72',
      },
      {
        label: 'HadoopMapReduceCommitProtocol per-attempt state',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L71-L121',
      },
      {
        label: 'Driver commitJob dynamic moves & cleanup',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala#L212-L253',
      },
    ],
    docFile: 'file-commit-protocol.md',
  },

  'output-commit-coordinator': {
    id: 'output-commit-coordinator',
    name: 'OutputCommitCoordinator',
    shortName: 'OutputCommitCoord',
    subsystem: 'python-output',
    hostZone: 'driver-jvm',
    subZone: 'CAPACITY, LIVENESS & MASTERS',
    clusterPos: [1020, 558],
    process: 'Driver JVM RPC endpoint (OutputCommitCoordinator) + executor client in SparkEnv',
    role: 'Arbitrates competing attempts of the same Spark partition (such as speculative copies or retries) by granting commit permission (Boolean) to the first eligible (stageAttempt, taskAttempt) per (stageId, partitionId) slot.',
    keyDistinction:
      'A true grant only authorizes that attempt to call Hadoop task commit; the coordinator neither moves files nor commits the job.',
    startedBy: 'Registered in driver SparkEnv; executors resolve its RpcEndpointRef; DAGScheduler calls stageStart/stageEnd',
    monitoredBy: 'DAGScheduler taskCompleted callbacks and driver/executor TaskCommitDenied logs',
    stoppedBy: 'DAGScheduler.stageEnd clears per-stage slot table; driver SparkEnv.stop() clears all in-memory state',
    inputs: [
      'DAGScheduler stageStart(stageId, maxPartitionId), taskCompleted(..., reason), and stageEnd(stageId)',
      'Executor AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) RPC asks',
    ],
    outputs: [
      'Boolean permission grant (first eligible caller wins; competing or known-failed callers get false)',
      'Denied attempts abort local output and report KILLED with TaskCommitDenied',
    ],
    internalState: [
      'stageStates: Map[StageId, StageState] with authorizedCommitters array sized maxPartitionId + 1',
      'Per-partition owner recorded as (stageAttemptNumber, taskAttemptNumber)',
      'Per-partition failures set preventing previously failed attempts from winning on late requests',
    ],
    failureClues: [
      'TaskCommitDenied alongside a succeeding sibling attempt is expected arbitration and does not count against maxFailures',
      'RPC ask timeout on canCommit means caller got no reply, not necessarily that no grant was recorded on the driver',
      'Only failure of the current slot owner clears the slot; TaskCommitDenied from a losing attempt never clears the winner',
    ],
    sourceLinks: [
      {
        label: 'OutputCommitCoordinator synchronized decision',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/OutputCommitCoordinator.scala#L176-L213',
      },
      {
        label: 'SparkHadoopMapRedUtil.commitTask permission gate',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/mapred/SparkHadoopMapRedUtil.scala#L41-L102',
      },
      {
        label: 'DAGScheduler stage teardown & completion callback',
        url: 'https://github.com/apache/spark/blob/dada2a6db5d73b152d6ba16e4fe21406f86abfe3/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L5113-L5122',
      },
    ],
    docFile: 'output-commit-coordinator.md',
  },
};

export const COMPONENT_LIST: ComponentSpec[] = Object.values(COMPONENTS);

export function getComponent(id: ComponentId): ComponentSpec {
  return COMPONENTS[id];
}
