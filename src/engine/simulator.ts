import { COMPONENTS } from '../data/components.ts';
import type {
  ExecutionRun,
  RunEvent,
  RunStep,
  RunTelemetry,
  RuntimeInstance,
  StepCategory,
} from '../data/runModel.ts';
import type { ChannelKind, ComponentId, HostZoneId, TraceField } from '../data/types.ts';
import { isLocalPort } from './adaptTrace.ts';
import {
  CLIENT_HOST_DRIVER_COMPONENTS as CLIENT_HOST_COMPONENTS,
  validateEventLocality,
} from './hostPlacement.ts';
import {
  emitTerminalAppOutcome,
  injectDelegationTokenExpiredStep,
  injectExecutorStartupFault,
  injectKryoUnregisteredClassStep,
  injectPostWorkloadMidRunFault,
  injectScalaJavaUdfExceptionStep,
  injectTaskResultLostStep,
  injectTaskSpillOomStep,
  injectTorrentPieceCorruptStep,
  tryInjectBroadcastFault,
  tryInjectClientDynAllocNoEssFault,
  tryInjectPreDriverStartupFault,
  tryInjectPreSubmitFault,
  tryInjectPreWorkloadFault,
  tryInjectPythonUdfFault,
  tryInjectShuffleReduceFault,
} from './faultInjectors.ts';
import {
  FAULT_MAP,
  JOB_TYPE_MAP,
  validateSimulationConfig,
  type SimulationConfig,
  type SparkLanguage,
} from './simulatorTypes.ts';

const PYTHON_ONLY_COMPONENTS: ReadonlySet<ComponentId> = new Set<ComponentId>([
  'python-driver',
  'py4j-gateway',
  'cloudpickle-serializer',
  'python-accumulator-v2',
  'python-udf-operator',
  'arrow-pandas-udf-engine',
  'python-udtf-exec',
  'python-worker-factory',
  'python-worker',
  'python-runner',
]);

export interface SimRunMeta {
  runId: string;
  runName: string;
  runSubtitle: string;
  runSummary: string;
  telemetry: RunTelemetry;
}

export interface SimRuntimeState {
  cfg: SimulationConfig;
  totalSlots: number;
  idealWavesPerStage: number;
  targetPart: number;
  initialMapId: number;
  recomputedMapId: number;
  activeAm: string;
  activeAttempt: number;
  amRegistered: boolean;
  amNm: string | null;
  liveExecs: string[];
  liveBms: string[];
  liveExecNums: number[];
  nextExecNum: number;
  abortedByFault: boolean;
  submit: string;
  rm: string;
  alloc: string;
  schedBackend: string;
  bmm: string;
  hbRx: string;
}

function clampExecutors(n: number): number {
  if (!Number.isFinite(n)) return 2;
  return Math.max(1, Math.min(6, Math.round(n)));
}

export function resolveValidSimulationConfig(input: SimulationConfig): SimulationConfig {
  let cfg: SimulationConfig = { ...input, numExecutors: clampExecutors(input.numExecutors) };
  for (let i = 0; i < 12; i++) {
    const check = validateSimulationConfig(cfg);
    if (check.valid) return cfg;
    const patch = check.blockers[0]?.suggestedPatch;
    if (!patch) break;
    cfg = { ...cfg, ...patch, numExecutors: clampExecutors(patch.numExecutors ?? cfg.numExecutors) };
  }
  return cfg;
}

interface DynamicAllocRampRound {
  round: number;
  prevTarget: number;
  nextTarget: number;
  requestedDelta: number;
  nominalDelta: number;
  capped: boolean;
  trigger: 'schedulerBacklogTimeout' | 'sustainedSchedulerBacklogTimeout';
}

function computeDynamicAllocationRamp(
  initial: number,
  targetScale: number,
): DynamicAllocRampRound[] {
  const rounds: DynamicAllocRampRound[] = [];
  let current = initial;
  let stepDelta = 1;
  let round = 0;
  while (current < targetScale) {
    round += 1;
    const next = Math.min(targetScale, current + stepDelta);
    const actualDelta = next - current;
    rounds.push({
      round,
      prevTarget: current,
      nextTarget: next,
      requestedDelta: actualDelta,
      nominalDelta: stepDelta,
      capped: actualDelta < stepDelta,
      trigger: round === 1 ? 'schedulerBacklogTimeout' : 'sustainedSchedulerBacklogTimeout',
    });
    current = next;
    stepDelta *= 2;
  }
  return rounds;
}

export class SimRunBuilder {
  readonly cfg: SimulationConfig;
  private readonly instances: Record<string, RuntimeInstance> = {};
  private readonly steps: RunStep[] = [];
  private readonly events: RunEvent[] = [];
  private readonly stepIds = new Set<string>();
  private eventSeq = 0;
  private readonly registeredAmIds = new Set<string>();
  private activeDriverAttempt = 1;
  private readonly nextExecNumByAttempt = new Map<number, number>();
  private readonly nextContainerSeqByAttempt = new Map<number, number>();
  private readonly execContainerByKey = new Map<string, string>();
  readonly isClient: boolean;
  readonly language: SparkLanguage;
  readonly isPySpark: boolean;
  readonly isScala: boolean;
  readonly isJava: boolean;
  readonly isJvmApp: boolean;
  readonly langDisplay: string;
  readonly appArtifact: string;
  readonly mainClass: string;
  readonly appMainClass: string;
  readonly appEntryLabel: string;
  readonly driverHost: string;
  readonly driverRpc: string;
  readonly driverBmPort: string;
  readonly driverUiUrl: string;

  constructor(cfg: SimulationConfig) {
    this.cfg = cfg;
    this.isClient = cfg.deployMode === 'client';
    this.language = cfg.language;
    this.isPySpark = cfg.language === 'pyspark';
    this.isScala = cfg.language === 'scala';
    this.isJava = cfg.language === 'java';
    this.isJvmApp = cfg.language === 'scala' || cfg.language === 'java';
    this.langDisplay =
      cfg.language === 'pyspark' ? 'PySpark' : cfg.language === 'scala' ? 'Scala' : 'Java';
    this.appArtifact =
      cfg.language === 'pyspark'
        ? 'orders.py'
        : cfg.language === 'scala'
          ? 'orders-assembly.jar'
          : 'orders-java.jar';
    this.mainClass =
      cfg.language === 'pyspark'
        ? 'org.apache.spark.deploy.PythonRunner'
        : cfg.fault === 'class-not-found-jar'
          ? cfg.language === 'java'
            ? 'com.acme.MissingOrdersJavaJob'
            : 'com.acme.MissingOrdersJob'
          : cfg.language === 'scala'
            ? 'com.acme.OrdersJob'
            : 'com.acme.OrdersJavaJob';
    this.appMainClass = this.mainClass;
    this.appEntryLabel =
      cfg.language === 'pyspark'
        ? 'orders.py'
        : cfg.fault === 'class-not-found-jar'
          ? cfg.language === 'java'
            ? 'MissingOrdersJavaJob.main'
            : 'MissingOrdersJob.main'
          : cfg.language === 'scala'
            ? 'OrdersJob.main'
            : 'OrdersJavaJob.main';
    this.driverHost = this.isClient ? 'edge-01' : 'driver-1';
    this.driverRpc = `${this.driverHost}:41000`;
    this.driverBmPort = `${this.driverHost}:41001`;
    this.driverUiUrl = `http://${this.driverHost}:4040`;
  }

  submitCommand(extraFlags = ''): string {
    const base = `spark-submit --master yarn --deploy-mode ${this.cfg.deployMode}${extraFlags}`;
    if (this.isPySpark) {
      return `${base} --num-executors ${this.cfg.numExecutors} --executor-cores ${this.cfg.executorCores} orders.py`;
    }
    return `${base} --class ${this.appMainClass} --num-executors ${this.cfg.numExecutors} --executor-cores ${this.cfg.executorCores} ${this.appArtifact}`;
  }

  userAppInstance(): string {
    return this.isPySpark ? this.std('python-driver') : this.std('jvm-user-app');
  }

  isAmRegistered(amId?: string): boolean {
    return amId ? this.registeredAmIds.has(amId) : this.registeredAmIds.size > 0;
  }

  resetForAttempt(attempt: number): void {
    if (!this.isClient) {
      this.activeDriverAttempt = attempt;
    }
    this.nextExecNumByAttempt.set(attempt, 1);
    this.nextContainerSeqByAttempt.set(attempt, 2);
  }

  allocateExec(attempt = 1): { execNum: number; containerId: string } {
    const execNum = this.nextExecNumByAttempt.get(attempt) ?? 1;
    const containerSeq = this.nextContainerSeqByAttempt.get(attempt) ?? 2;
    this.nextExecNumByAttempt.set(attempt, execNum + 1);
    this.nextContainerSeqByAttempt.set(attempt, containerSeq + 1);
    const containerId = `container_e0${attempt}_${String(containerSeq).padStart(6, '0')}`;
    this.execContainerByKey.set(`${attempt}:${execNum}`, containerId);
    return { execNum, containerId };
  }

  peekNextExecNum(attempt = 1): number {
    return this.nextExecNumByAttempt.get(attempt) ?? 1;
  }

  instanceExecutorNumber(id: string): number {
    return this.instances[id]?.executorNumber ?? 1;
  }

  ensureInstance(id: string, spec: Omit<RuntimeInstance, 'id'>): string {
    if (this.isJvmApp && PYTHON_ONLY_COMPONENTS.has(spec.componentId)) {
      throw new Error(
        `Python-only component "${spec.componentId}" is forbidden in ${this.cfg.language} runs (instance "${id}")`,
      );
    }
    if (!this.instances[id]) {
      this.instances[id] = { id, ...spec };
    }
    return id;
  }

  std(compId: ComponentId): string {
    const meta = COMPONENTS[compId];
    const isLaterClusterAttempt =
      !this.isClient && this.activeDriverAttempt > 1 && meta.hostZone === 'driver-jvm';
    const id = isLaterClusterAttempt ? `${compId}-a${this.activeDriverAttempt}` : compId;
    if (this.instances[id]) return id;
    if (isLaterClusterAttempt) {
      const attempt = this.activeDriverAttempt;
      return this.ensureInstance(id, {
        componentId: compId,
        label: `${meta.name} (Attempt ${attempt})`,
        shortLabel: `${meta.shortName} A${attempt}`,
        hostZone: 'driver-jvm',
        hostLabel: `driver-1 (Attempt ${attempt})`,
        processId:
          compId === 'python-driver'
            ? `Python Driver Attempt ${attempt}`
            : `AM JVM Attempt ${attempt}`,
        containerId: `container_e0${attempt}_000001`,
        attemptNumber: attempt,
      });
    }
    const hostZone: HostZoneId =
      this.isClient && CLIENT_HOST_COMPONENTS.has(compId) ? 'submit-host' : meta.hostZone;
    const hostLabel =
      this.isClient && hostZone === 'submit-host'
        ? 'edge-01 (client driver)'
        : hostZone === 'submit-host'
          ? 'edge-01'
          : hostZone === 'yarn-control'
            ? 'rm01'
            : hostZone === 'storage-history'
              ? meta.port ? `hdfs ${meta.port}` : 'hdfs / history'
              : hostZone === 'driver-jvm'
                ? 'driver-1 (AM)'
                : hostZone === 'worker-node-a'
                  ? 'worker-a'
                  : 'worker-b';
    return this.ensureInstance(id, {
      componentId: compId,
      label: meta.name,
      shortLabel: meta.shortName,
      hostZone,
      hostLabel,
      processId: meta.process,
      ...(compId === 'application-master'
        ? { containerId: 'container_e01_000001', attemptNumber: 1 }
        : {}),
    });
  }

  workerStd(compId: ComponentId, hostZone: 'worker-node-a' | 'worker-node-b'): string {
    const attempt = this.activeDriverAttempt;
    const baseId = attempt > 1 ? `${compId}-a${attempt}` : compId;
    const existing = this.instances[baseId];
    if (existing && existing.hostZone === hostZone) return baseId;
    const id = existing
      ? `${baseId}-${hostZone === 'worker-node-a' ? 'a' : 'b'}`
      : baseId;
    if (this.instances[id]) return id;
    const meta = COMPONENTS[compId];
    const hostLabel = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    return this.ensureInstance(id, {
      componentId: compId,
      label: attempt > 1 ? `${meta.name} (Attempt ${attempt})` : meta.name,
      shortLabel: attempt > 1 ? `${meta.shortName} A${attempt}` : meta.shortName,
      hostZone,
      hostLabel,
      processId: meta.process,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  nmInstance(hostZone: 'worker-node-a' | 'worker-node-b'): string {
    const id = hostZone === 'worker-node-a' ? 'nm-a' : 'nm-b';
    const host = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    return this.ensureInstance(id, {
      componentId: 'node-manager',
      label: `NodeManager (${host})`,
      shortLabel: hostZone === 'worker-node-a' ? 'NodeMgr A' : 'NodeMgr B',
      hostZone,
      hostLabel: `${host}:dynamic`,
      processId: 'NodeManager JVM',
    });
  }

  instanceWorkerHost(id: string): { hostZone: 'worker-node-a' | 'worker-node-b'; hostName: 'worker-a' | 'worker-b' } {
    const inst = this.instances[id];
    const hostZone: 'worker-node-a' | 'worker-node-b' =
      inst?.hostZone === 'worker-node-b' ? 'worker-node-b' : 'worker-node-a';
    return {
      hostZone,
      hostName: hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b',
    };
  }

  instanceHostLabel(id: string): string {
    return this.instances[id]?.hostLabel ?? 'worker-a:42001';
  }

  instanceContainerId(id: string): string {
    return this.instances[id]?.containerId ?? this.execContainerId(1);
  }

  driverToAmLink(): { kind: 'local' | 'wire'; protocol: string; port: string } {
    return this.isClient
      ? {
          kind: 'wire',
          protocol: 'Spark Netty RPC (AM outbound)',
          port: `${this.driverRpc} (AM outbound RPC)`,
        }
      : { kind: 'local', protocol: 'In-JVM AMEndpoint', port: 'driver-1 in-JVM' };
  }

  amToDriverLink(): { kind: 'local' | 'wire'; protocol: string; port: string } {
    return this.isClient
      ? { kind: 'wire', protocol: 'Spark Netty RPC', port: this.driverRpc }
      : { kind: 'local', protocol: 'In-JVM DriverRef', port: 'driver-1 in-JVM' };
  }

  execHostZone(execNum: number): 'worker-node-a' | 'worker-node-b' {
    return execNum % 2 === 1 ? 'worker-node-a' : 'worker-node-b';
  }

  execHostName(execNum: number): string {
    return execNum % 2 === 1 ? 'worker-a' : 'worker-b';
  }

  execDataPort(execNum: number): number {
    return 42000 + execNum;
  }

  execContainerId(execNum: number, attempt = 1): string {
    return (
      this.execContainerByKey.get(`${attempt}:${execNum}`) ??
      `container_e0${attempt}_${String(execNum + 1).padStart(6, '0')}`
    );
  }

  execInstance(execNum: number, attempt = 1, containerId?: string): string {
    const id = attempt === 1 ? `exec-${execNum}` : `exec-a${attempt}-${execNum}`;
    const host = this.execHostName(execNum);
    const cid = containerId ?? this.execContainerId(execNum, attempt);
    return this.ensureInstance(id, {
      componentId: 'executor-backend',
      label: attempt === 1 ? `Executor ${execNum} Backend` : `Attempt ${attempt} Exec-${execNum}`,
      shortLabel: attempt === 1 ? `Exec-${execNum}` : `A${attempt} Exec-${execNum}`,
      hostZone: this.execHostZone(execNum),
      hostLabel: `${host} (${this.cfg.executorCores}c)`,
      processId: attempt === 1 ? `Executor ${execNum} JVM` : `Attempt ${attempt} Exec-${execNum} JVM`,
      containerId: cid,
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execBmInstance(execNum: number, attempt = 1, containerId?: string): string {
    const id = attempt === 1 ? `exec-${execNum}/bm` : `exec-a${attempt}-${execNum}/bm`;
    const host = this.execHostName(execNum);
    const port = this.execDataPort(execNum);
    const cid = containerId ?? this.execContainerId(execNum, attempt);
    return this.ensureInstance(id, {
      componentId: 'block-manager',
      label: attempt === 1 ? `Executor ${execNum} BlockManager` : `Attempt ${attempt} Exec-${execNum} BM`,
      shortLabel: attempt === 1 ? `Exec-${execNum} BM` : `A${attempt} E${execNum} BM`,
      hostZone: this.execHostZone(execNum),
      hostLabel: `${host}:${port}`,
      processId: attempt === 1 ? `Executor ${execNum} JVM` : `Attempt ${attempt} Exec-${execNum} JVM`,
      containerId: cid,
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execReplInstance(
    execNum: number,
    hostZone: 'worker-node-a' | 'worker-node-b',
    attempt = 1,
    containerId?: string,
  ): string {
    const baseId = attempt === 1 ? 'exec-repl' : `exec-a${attempt}-repl`;
    const id = this.instances[baseId] && this.instances[baseId].executorNumber !== execNum
      ? `${baseId}-${execNum}`
      : baseId;
    const host = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const cid = containerId ?? this.execContainerId(execNum, attempt);
    return this.ensureInstance(id, {
      componentId: 'executor-backend',
      label: `Replacement Exec-${execNum} Backend`,
      shortLabel: attempt === 1 ? 'Exec-repl' : `A${attempt} Exec-repl`,
      hostZone,
      hostLabel: `${host} (${this.cfg.executorCores}c)`,
      processId: `Executor ${execNum} JVM`,
      containerId: cid,
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execReplBmInstance(
    execNum: number,
    hostZone: 'worker-node-a' | 'worker-node-b',
    attempt = 1,
    containerId?: string,
  ): string {
    const baseId = attempt === 1 ? 'exec-repl/bm' : `exec-a${attempt}-repl/bm`;
    const id = this.instances[baseId] && this.instances[baseId].executorNumber !== execNum
      ? `exec-repl-${execNum}/bm`
      : baseId;
    const host = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const port = this.execDataPort(execNum);
    const cid = containerId ?? this.execContainerId(execNum, attempt);
    return this.ensureInstance(id, {
      componentId: 'block-manager',
      label: `Replacement Exec-${execNum} BM`,
      shortLabel: attempt === 1 ? 'Exec-repl BM' : `A${attempt} E-repl BM`,
      hostZone,
      hostLabel: `${host}:${port}`,
      processId: `Executor ${execNum} JVM`,
      containerId: cid,
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  driverBmInstance(): string {
    const attempt = !this.isClient && this.activeDriverAttempt > 1 ? this.activeDriverAttempt : 1;
    const id = attempt > 1 ? `driver-bm-a${attempt}` : 'driver-bm';
    return this.ensureInstance(id, {
      componentId: 'block-manager',
      label: attempt > 1 ? `Driver BlockManager (Attempt ${attempt})` : 'Driver BlockManager',
      shortLabel: attempt > 1 ? `Driver BM A${attempt}` : 'Driver BM',
      hostZone: this.isClient ? 'submit-host' : 'driver-jvm',
      hostLabel: this.driverBmPort,
      processId: this.isClient
        ? 'Client Driver JVM'
        : attempt > 1
          ? `AM JVM Attempt ${attempt}`
          : 'AM / Driver JVM',
      ...(attempt > 1
        ? { containerId: `container_e0${attempt}_000001`, attemptNumber: attempt }
        : {}),
    });
  }

  driverCommitInstance(): string {
    const attempt = !this.isClient && this.activeDriverAttempt > 1 ? this.activeDriverAttempt : 1;
    const id = attempt > 1 ? `driver-commit-a${attempt}` : 'driver-commit';
    return this.ensureInstance(id, {
      componentId: 'file-commit-protocol',
      label: attempt > 1 ? `Driver FileCommitProtocol (Attempt ${attempt})` : 'Driver FileCommitProtocol',
      shortLabel: attempt > 1 ? `Driver Commit A${attempt}` : 'Driver Commit',
      hostZone: this.isClient ? 'submit-host' : 'driver-jvm',
      hostLabel: this.driverHost,
      processId: this.isClient
        ? 'Client Driver JVM'
        : attempt > 1
          ? `AM JVM Attempt ${attempt}`
          : 'AM / Driver JVM',
      ...(attempt > 1
        ? { containerId: `container_e0${attempt}_000001`, attemptNumber: attempt }
        : {}),
    });
  }

  driverEncoderInstance(): string {
    const attempt = !this.isClient && this.activeDriverAttempt > 1 ? this.activeDriverAttempt : 1;
    const id = attempt > 1 ? `driver-encoder-a${attempt}` : 'driver-encoder';
    const baseLabel = this.isJava
      ? 'JavaTypeInference & UDF1 (Driver)'
      : this.isScala
        ? 'ExpressionEncoder & ScalaUDF (Driver)'
        : 'Java UDF Registration (Driver)';
    return this.ensureInstance(id, {
      componentId: 'dataset-encoder-udf',
      label: attempt > 1 ? `${baseLabel} A${attempt}` : baseLabel,
      shortLabel: attempt > 1 ? `Driver Enc A${attempt}` : 'Driver Encoder',
      hostZone: this.isClient ? 'submit-host' : 'driver-jvm',
      hostLabel: this.driverHost,
      processId: this.isClient
        ? 'Client Driver JVM'
        : attempt > 1
          ? `AM JVM Attempt ${attempt}`
          : 'AM / Driver JVM',
      ...(attempt > 1
        ? { containerId: `container_e0${attempt}_000001`, attemptNumber: attempt }
        : {}),
    });
  }

  execEncoderInstance(execId: string): string {
    const execInst = this.instances[execId];
    const execNum = execInst?.executorNumber ?? 1;
    const attempt = execInst?.attemptNumber ?? 1;
    const hostZone: 'worker-node-a' | 'worker-node-b' =
      execInst?.hostZone === 'worker-node-b' ? 'worker-node-b' : 'worker-node-a';
    const hostName = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const id = `${execId}/dataset-encoder-udf`;
    const roleLabel = this.isJava
      ? 'BeanEncoder & UDF1'
      : this.isScala
        ? 'ExpressionEncoder & ScalaUDF'
        : 'Java UDF1 (registerJavaFunction)';
    return this.ensureInstance(id, {
      componentId: 'dataset-encoder-udf',
      label: `${roleLabel} (Exec-${execNum})`,
      shortLabel: `Exec-${execNum} UDF`,
      hostZone,
      hostLabel: hostName,
      processId: execInst?.processId ?? `Executor ${execNum} JVM`,
      ...(execInst?.containerId ? { containerId: execInst.containerId } : {}),
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execSerializerInstance(execId: string): string {
    const execInst = this.instances[execId];
    const execNum = execInst?.executorNumber ?? 1;
    const attempt = execInst?.attemptNumber ?? 1;
    const hostZone: 'worker-node-a' | 'worker-node-b' =
      execInst?.hostZone === 'worker-node-b' ? 'worker-node-b' : 'worker-node-a';
    const hostName = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const id = `${execId}/serializer-manager`;
    return this.ensureInstance(id, {
      componentId: 'serializer-manager',
      label: `SerializerManager (Exec-${execNum})`,
      shortLabel: `Exec-${execNum} SerMgr`,
      hostZone,
      hostLabel: hostName,
      processId: execInst?.processId ?? `Executor ${execNum} JVM`,
      ...(execInst?.containerId ? { containerId: execInst.containerId } : {}),
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execTaskAttemptInstance(execId: string): string {
    const execInst = this.instances[execId];
    const execNum = execInst?.executorNumber ?? 1;
    const attempt = execInst?.attemptNumber ?? 1;
    const hostZone: 'worker-node-a' | 'worker-node-b' =
      execInst?.hostZone === 'worker-node-b' ? 'worker-node-b' : 'worker-node-a';
    const hostName = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const id = `${execId}/task-attempt`;
    return this.ensureInstance(id, {
      componentId: 'task-attempt',
      label: `TaskAttempt (Exec-${execNum})`,
      shortLabel: `Exec-${execNum} Task`,
      hostZone,
      hostLabel: hostName,
      processId: execInst?.processId ?? `Executor ${execNum} JVM`,
      ...(execInst?.containerId ? { containerId: execInst.containerId } : {}),
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  execLocalComponentInstance(
    execId: string,
    componentId: ComponentId,
    shortSuffix?: string,
  ): string {
    const execInst = this.instances[execId];
    const execNum = execInst?.executorNumber ?? 1;
    const attempt = execInst?.attemptNumber ?? 1;
    const hostZone: 'worker-node-a' | 'worker-node-b' =
      execInst?.hostZone === 'worker-node-b' ? 'worker-node-b' : 'worker-node-a';
    const hostName = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const spec = COMPONENTS[componentId];
    const id = `${execId}/${componentId}`;
    return this.ensureInstance(id, {
      componentId,
      label: `${spec.name} (Exec-${execNum})`,
      shortLabel: shortSuffix ? `E${execNum} ${shortSuffix}` : `E${execNum} ${spec.shortName}`,
      hostZone,
      hostLabel: hostName,
      processId: execInst?.processId ?? `Executor ${execNum} JVM`,
      ...(execInst?.containerId ? { containerId: execInst.containerId } : {}),
      executorNumber: execNum,
      ...(attempt > 1 ? { attemptNumber: attempt } : {}),
    });
  }

  emitSqlCatalystCodegenPlan(
    stepId: string,
    opts: {
      hasShuffle: boolean;
      isSkewJoin?: boolean;
      dagLabel: string;
      dagInfo: string;
      dagCommand: string;
      dagFields: TraceField[];
      dagSenderState: string;
      dagReceiverState: string;
    },
  ): void {
    const analyzer = this.std('catalyst-analyzer');
    const optimizer = this.std('catalyst-optimizer');
    const sqlPlanner = this.std('sql-planner');
    const wscg = this.std('whole-stage-codegen');
    const dag = this.std('dag-scheduler');

    this.addEvent({
      stepId,
      from: analyzer,
      to: optimizer,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'Catalyst Analyzer -> RBO',
      port: `${this.driverHost} in-JVM`,
      label: 'Analyze & optimize AST',
      info: 'Catalyst Analyzer resolves catalog schemas & ExprIds, then Catalyst Optimizer applies predicate pushdown and column pruning.',
      size: 'in-memory',
      command: 'Analyzer.executeAndCheck(plan) -> SparkOptimizer.execute(analyzedPlan)',
      fields: [
        { name: 'phase', value: 'Analyzed -> Optimized LogicalPlan' },
        { name: 'rules', value: 'PushDownPredicates, ColumnPruning, ConstantFolding' },
      ],
      senderState: 'Resolved catalog attributes & types',
      receiverState: 'Optimized logical plan ready for SparkPlanner',
    });
    this.addEvent({
      stepId,
      from: optimizer,
      to: sqlPlanner,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'SparkPlanner Strategies',
      port: `${this.driverHost} in-JVM`,
      label: opts.isSkewJoin ? 'Plan SMJ operators' : opts.hasShuffle ? 'Plan HashAgg + Exchange' : 'Plan Scan + Filter',
      info: opts.isSkewJoin
        ? 'SQL Planner selects SortMergeJoinExec with co-partitioned SortExec inputs.'
        : opts.hasShuffle
          ? 'SQL Planner selects two-phase Partial/Final HashAggregateExec around a hash exchange.'
          : 'SQL Planner lowers narrow Scan -> Filter -> Project with zero shuffle boundaries.',
      size: 'in-memory',
      command: 'SparkPlanner.plan(optimizedPlan) -> EnsureRequirements',
      fields: [{ name: 'aqeEnabled', value: String(this.cfg.aqeEnabled) }],
      senderState: 'Handed optimized LogicalPlan to SparkPlanner',
      receiverState: opts.hasShuffle ? 'Inserting ShuffleExchangeExec' : 'Preparing WholeStageCodegenExec',
    });
    if (opts.hasShuffle) {
      const exch = this.std('shuffle-exchange');
      this.addEvent({
        stepId,
        from: sqlPlanner,
        to: exch,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'EnsureRequirements',
        port: `${this.driverHost} in-JVM`,
        label: opts.isSkewJoin ? 'Insert 2x Exchange' : 'Insert ShuffleExchange',
        info: 'EnsureRequirements inserts ShuffleExchangeExec with UnsafeRowSerializer to satisfy required ClusteredDistribution.',
        size: 'in-memory',
        command: `EnsureRequirements -> ShuffleExchangeExec(hashpartitioning(${opts.isSkewJoin ? 'cust_id' : 'region'}, ${this.cfg.numPartitions}))`,
        fields: [
          { name: 'partitions', value: String(this.cfg.numPartitions) },
          { name: 'serializer', value: 'UnsafeRowSerializer (Tungsten binary)' },
        ],
        senderState: 'Satisfied child distribution requirements',
        receiverState: 'Created ShuffleDependency; wrapping stages in WholeStageCodegen',
      });
      this.addEvent({
        stepId,
        from: exch,
        to: wscg,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'CollapseCodegenStages',
        port: `${this.driverHost} in-JVM`,
        label: 'Fuse WholeStageCodegen',
        info: 'CollapseCodegenStages wraps codegen-eligible operators in WholeStageCodegenExec.doCodeGen() to emit fused BufferedRowIterator Java source.',
        size: 'in-memory',
        command: 'WholeStageCodegenExec.doCodeGen() // *(1) map & *(2) reduce Java loops',
        fields: [
          { name: 'compiler', value: 'Janino SimpleCompiler (default on executors)' },
          { name: 'format', value: 'Tungsten UnsafeRow (zero object boxing)' },
        ],
        senderState: 'Partitioned physical plan into codegen stages',
        receiverState: 'Emitted WholeStageCodegen Java source for executors',
      });
    } else {
      this.addEvent({
        stepId,
        from: sqlPlanner,
        to: wscg,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'CollapseCodegenStages',
        port: `${this.driverHost} in-JVM`,
        label: 'Fuse WholeStageCodegen',
        info: 'CollapseCodegenStages fuses Scan -> Filter -> Project into one WholeStageCodegenExec *(1) BufferedRowIterator Java loop.',
        size: 'in-memory',
        command: 'WholeStageCodegenExec.doCodeGen() // single fused *(1) stage',
        fields: [
          { name: 'compiler', value: 'Janino SimpleCompiler (default on executors)' },
          { name: 'format', value: 'Tungsten UnsafeRow (zero object boxing)' },
        ],
        senderState: 'Handed narrow physical plan to codegen',
        receiverState: 'Emitted fused *(1) WholeStageCodegen Java source',
      });
    }
    this.addEvent({
      stepId,
      from: wscg,
      to: dag,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'In-JVM Codegen -> DAG',
      port: `${this.driverHost} in-JVM`,
      label: opts.dagLabel,
      info: opts.dagInfo,
      size: 'in-memory',
      command: opts.dagCommand,
      fields: opts.dagFields,
      senderState: opts.dagSenderState,
      receiverState: opts.dagReceiverState,
    });
  }

  amNmInstance(): string {
    return this.ensureInstance('am-nm', {
      componentId: 'node-manager',
      label: 'AM NodeManager',
      shortLabel: 'AM NodeMgr',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1:dynamic',
      processId: 'NodeManager JVM (driver-1)',
    });
  }

  amContainerInstance(attempt = 1): string {
    const id = attempt === 1 ? 'am-container' : `am-container-${attempt}`;
    return this.ensureInstance(id, {
      componentId: 'yarn-container',
      label: `AM Container (Attempt ${attempt})`,
      shortLabel: attempt === 1 ? 'AM Container' : `AM Cont #${attempt}`,
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 OS',
      processId: 'LinuxContainerExecutor',
      containerId: `container_e0${attempt}_000001`,
      attemptNumber: attempt,
    });
  }

  essInstance(hostZone: 'worker-node-a' | 'worker-node-b'): string {
    const id = hostZone === 'worker-node-a' ? 'ess-a' : 'ess-b';
    const host = hostZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    return this.ensureInstance(id, {
      componentId: 'external-shuffle-service',
      label: `External Shuffle Service (${host})`,
      shortLabel: `ESS (${host})`,
      hostZone,
      hostLabel: `${host}:7337`,
      processId: 'NodeManager Aux Service',
    });
  }

  addStep(step: RunStep): string {
    if (this.stepIds.has(step.id)) {
      throw new Error(`Duplicate simulated step id: ${step.id}`);
    }
    this.stepIds.add(step.id);
    this.steps.push(step);
    return step.id;
  }

  addEvent(opts: {
    stepId: string;
    from: string;
    to: string;
    channel: ChannelKind;
    protocol: string;
    port: string;
    label: string;
    info: string;
    size: string;
    command: string;
    fields: TraceField[];
    senderState: string;
    receiverState: string;
    kind?: 'wire' | 'local';
    categoryOverride?: StepCategory;
    isError?: boolean;
    peerTag?: string;
  }): string {
    const fromInst = this.instances[opts.from];
    const toInst = this.instances[opts.to];
    if (!fromInst) throw new Error(`Missing fromInstance ${opts.from}`);
    if (!toInst) throw new Error(`Missing toInstance ${opts.to}`);
    const id = `sim-ev-${++this.eventSeq}`;
    const derivedKind: 'wire' | 'local' =
      opts.kind ??
      (isLocalPort(opts.port) || (opts.from === opts.to && opts.channel === 'internal')
        ? 'local'
        : 'wire');
    const localityViolation = validateEventLocality(
      {
        id,
        kind: derivedKind,
        channel: opts.channel,
        port: opts.port,
        protocol: opts.protocol,
        fromInstance: opts.from,
        toInstance: opts.to,
        command: opts.command,
      },
      this.instances,
    );
    if (localityViolation) {
      throw new Error(localityViolation);
    }
    const isRegisterAmEvent =
      fromInst.componentId === 'application-master' &&
      toInst.componentId === 'resource-manager' &&
      opts.port === 'rm01:8030' &&
      !opts.isError &&
      /register/i.test(`${opts.label} ${opts.command}`) &&
      !/unregister/i.test(`${opts.label} ${opts.command}`);
    if (isRegisterAmEvent) {
      this.registeredAmIds.add(opts.from);
    }
    if (
      fromInst.componentId === 'node-manager' &&
      toInst.componentId === 'resource-manager' &&
      opts.port === 'rm01:8031'
    ) {
      const hbText = `${opts.command}; ${opts.info}`;
      const completedContainers = new Set<string>();
      for (const m of hbText.matchAll(
        /\b(container_e\d+_\d+)\b(?:(?![);\n]|\bcontainer_e\d+_\d+\b|\bRUNNING\b)[\s\S])*?\bCOMPLETE\b/gi,
      )) {
        completedContainers.add(m[1]);
      }
      if (completedContainers.size > 0) {
        for (const inst of Object.values(this.instances)) {
          if (
            inst.componentId === 'application-master' &&
            inst.containerId &&
            completedContainers.has(inst.containerId)
          ) {
            this.registeredAmIds.delete(inst.id);
          }
        }
      }
    }
    const isUnregisterEvent =
      /(?:^|\b)(?:amclient\.)?unregister(?:am|applicationmaster)?\s*\(/i.test(opts.label) ||
      /(?:^|\b)(?:amclient\.)?unregister(?:am|applicationmaster)?\s*\(/i.test(opts.command) ||
      (fromInst.componentId === 'application-master' &&
        toInst.componentId === 'resource-manager' &&
        /unregister/i.test(`${opts.label} ${opts.command}`));
    if (isUnregisterEvent && !this.registeredAmIds.has(opts.from)) {
      throw new Error(
        `AM unregister forbidden before registerApplicationMaster for ${opts.from}: ${opts.from} -> ${opts.to} (${opts.label})`,
      );
    }
    const safeLabel = opts.label.length > 26 ? `${opts.label.slice(0, 25)}.` : opts.label;
    this.events.push({
      id,
      stepId: opts.stepId,
      kind: derivedKind,
      categoryOverride: opts.categoryOverride,
      fromInstance: opts.from,
      toInstance: opts.to,
      fromComponent: fromInst.componentId,
      toComponent: toInst.componentId,
      channel: opts.channel,
      protocol: opts.protocol,
      port: opts.port,
      label: safeLabel,
      info: opts.info,
      size: opts.size,
      command: opts.command,
      fields: opts.fields,
      senderState: opts.senderState,
      receiverState: opts.receiverState,
      isError: opts.isError,
      peerTag: opts.peerTag,
    });
    return id;
  }

  finalize(id: string, name: string, subtitle: string, summary: string, layerTag: string, telemetry: RunTelemetry): ExecutionRun {
    // Collapsed macro events define the clean initial focusInstances set;
    // deep-only instances live in `instances` and appear dynamically on Go Deeper.
    const topStepIds = new Set(this.steps.filter(s => !s.parentId).map(s => s.id));
    const focusSet = new Set<string>();
    for (const ev of this.events) {
      if (topStepIds.has(ev.stepId)) {
        focusSet.add(ev.fromInstance);
        focusSet.add(ev.toInstance);
      }
    }
    return {
      id,
      sourceKind: 'simulation',
      traceKind: 'lifecycle',
      name,
      subtitle,
      summary,
      deployMode: this.cfg.deployMode,
      layerTag,
      instances: this.instances,
      focusInstances: [...focusSet],
      steps: this.steps,
      events: this.events,
      telemetry,
    };
  }
}

export function buildSimulatedRun(rawConfig: SimulationConfig): ExecutionRun {
  const cfg: SimulationConfig = {
    ...rawConfig,
    numExecutors: clampExecutors(rawConfig.numExecutors),
  };
  const validation = validateSimulationConfig(cfg);
  if (!validation.valid) {
    throw new Error(`Invalid simulation config: ${validation.blockers.map(b => b.reason).join(' ')}`);
  }

  const job = JOB_TYPE_MAP[cfg.jobType];
  const fault = FAULT_MAP[cfg.fault];
  const totalSlots = cfg.numExecutors * cfg.executorCores;
  const idealWavesPerStage = Math.ceil(cfg.numPartitions / totalSlots);
  const containers = cfg.numExecutors + 1; // 1 AM container + N executor containers

  const telemetry: RunTelemetry = {
    totalSlots,
    idealWavesPerStage,
    containers,
    numExecutors: cfg.numExecutors,
    executorCores: cfg.executorCores,
    numPartitions: cfg.numPartitions,
    externalShuffleService: cfg.externalShuffleService,
    dynamicAllocation: cfg.dynamicAllocation,
    aqeEnabled: cfg.aqeEnabled,
    faultLabel: fault.label,
    actionSummary: job.actionSink,
  };

  const b = new SimRunBuilder(cfg);

  const runId = [
    'sim',
    cfg.language,
    cfg.jobType,
    cfg.dataProfile,
    cfg.deployMode,
    `e${cfg.numExecutors}c${cfg.executorCores}p${cfg.numPartitions}`,
    cfg.externalShuffleService ? 'ess1' : 'ess0',
    cfg.dynamicAllocation ? 'da1' : 'da0',
    cfg.aqeEnabled ? 'aqe1' : 'aqe0',
    cfg.fault,
  ].join('-');

  const runName = `Simulated (${b.langDisplay}): ${job.short} (${cfg.numExecutors}x${cfg.executorCores}c, ${cfg.deployMode})`;
  const runSubtitle =
    cfg.fault === 'none'
      ? `${b.langDisplay} | ${job.actionSink} | ${cfg.numPartitions} parts (~${idealWavesPerStage} ideal wave${idealWavesPerStage > 1 ? 's' : ''}/stage)`
      : `Fault (${b.langDisplay}): ${fault.short} on ${job.short} (${cfg.deployMode}, ${cfg.numExecutors} execs)`;
  const runSummary =
    `[${b.langDisplay}] ${job.summary} Configured with ${cfg.numExecutors} ${cfg.dynamicAllocation ? 'initial ' : ''}executor(s) x ${cfg.executorCores} core(s) (${totalSlots} slots; ideal ${idealWavesPerStage} wave(s)/stage for ${cfg.numPartitions} partitions under fixed capacity), ESS ${cfg.externalShuffleService ? 'ON (:7337)' : 'OFF'}, AQE ${cfg.aqeEnabled ? 'ON' : 'OFF'}.${cfg.deployMode === 'client' ? ' Driver runs outside YARN on edge-01.' : ''}`;

  const runMeta: SimRunMeta = {
    runId,
    runName,
    runSubtitle,
    runSummary,
    telemetry,
  };

  // =========================================================================
  // PHASE 1: Pre-submit & Client-mode local startup short-circuits
  // =========================================================================
  const preSubmitShortCircuit = tryInjectPreSubmitFault(b, cfg, runMeta);
  if (preSubmitShortCircuit) {
    return preSubmitShortCircuit;
  }

  const submit = b.std('spark-submit');
  const yClient = b.std('yarn-client');
  const rm = b.std('resource-manager');

  // =========================================================================
  // MACRO STEP 1: Submit & Stage (+ Expandable Staging Sub-Steps)
  // =========================================================================
  const am = b.std('application-master');
  let amNmInstanceId: string | null = null;

  if (cfg.deployMode === 'cluster') {
    const nn = b.std('hdfs-namenode');
    const amNm = b.amNmInstance();
    amNmInstanceId = amNm;
    const amCont = b.amContainerInstance(1);
    const submitCmd = b.submitCommand();

    const sSubmit = b.addStep({
      id: 'sim-step-submit',
      title: 'Submit application, stage files to HDFS & start AM container',
      explanation: b.isPySpark
        ? `spark-submit stages orders.py and __spark_conf__.zip to HDFS .sparkStaging/, submits ApplicationMaster to RM :8032, and RM launches AM Attempt 1 on driver-1. Expand for staging & localization sub-steps.`
        : `spark-submit stages ${b.appArtifact} (YARN localized symlink __app__.jar; local: URIs bypass HDFS upload) and __spark_conf__.zip to HDFS .sparkStaging/, submits ApplicationMaster (--jar ${b.appArtifact} --class ${b.appMainClass}) to RM :8032, and RM launches AM Attempt 1 on driver-1 with zero Python artifacts.`,
      category: 'yarn-lifecycle',
      keyState: 'AM container_e01_000001 booting on driver-1',
      command: submitCmd,
      detailsPassed: [
        { name: 'language', value: b.langDisplay },
        { name: 'deployMode', value: 'cluster' },
        ...(b.isJvmApp
          ? [
              { name: 'userClass', value: b.appMainClass },
              { name: 'userJarArg', value: `${b.appArtifact} (localized symlink __app__.jar; local: bypasses HDFS upload)` },
            ]
          : []),
        { name: 'numExecutors', value: String(cfg.numExecutors) },
        { name: 'executorCores', value: String(cfg.executorCores) },
      ],
      deepDiveComponentId: 'yarn-client',
      isMacro: true,
    });
    b.addEvent({
      stepId: sSubmit,
      from: submit,
      to: yClient,
      channel: 'internal',
      protocol: 'In-JVM SparkApplication',
      port: 'edge-01 in-JVM',
      label: 'YarnClusterApplication',
      info: b.isPySpark
        ? 'spark-submit hands parsed YARN configs and orders.py to yarn.Client.'
        : `spark-submit hands parsed YARN configs, --class ${b.appMainClass}, and ${b.appArtifact} (isPython=false) to yarn.Client.`,
      size: 'in-memory',
      command: 'SparkSubmit.runMain(childArgs, sparkConf, "YarnClusterApplication")',
      fields: [
        { name: 'master', value: 'yarn' },
        { name: 'deployMode', value: 'cluster' },
        { name: 'spark.yarn.isPython', value: String(b.isPySpark) },
      ],
      senderState: 'Parsed CLI flags',
      receiverState: 'Staging files & submitting AM to RM',
    });
    b.addEvent({
      stepId: sSubmit,
      from: yClient,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationClientProtocol)',
      port: 'rm01:8032',
      label: 'submitApplication(AM)',
      info: b.isPySpark
        ? 'yarn.Client stages archives to HDFS and submits AM ContainerLaunchContext to RM :8032.'
        : `yarn.Client stages ${b.appArtifact} to HDFS .sparkStaging/ and submits AM ContainerLaunchContext (--jar ${b.appArtifact} --class ${b.appMainClass}; localized symlink __app__.jar) to RM :8032.`,
      size: '14.4 KB',
      command: b.isPySpark
        ? 'yarnClient.submitApplication(appContext)'
        : `yarnClient.submitApplication(appContext[ApplicationMaster --jar ${b.appArtifact} --class ${b.appMainClass}])`,
      fields: [
        { name: 'amClass', value: 'ApplicationMaster' },
        { name: 'userClass', value: b.isPySpark ? 'deploy.PythonRunner' : b.appMainClass },
        ...(b.isJvmApp
          ? [{ name: 'userJarArg', value: `--jar ${b.appArtifact} (localized symlink __app__.jar)` }]
          : []),
      ],
      senderState: 'Monitoring RM app report',
      receiverState: 'App ACCEPTED; launching AM on driver-1',
    });
    b.addEvent({
      stepId: sSubmit,
      from: rm,
      to: amNm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: 'driver-1:dynamic',
      label: 'startContainers(AM)',
      info: 'RM AMLauncher asks driver-1 NodeManager to localize .sparkStaging/ and boot AM Attempt 1.',
      size: '16.0 KB',
      command: 'AMLauncher -> cmProxy.startContainers(container_e01_000001)',
      fields: [{ name: 'containerId', value: 'container_e01_000001' }],
      senderState: 'Awaiting AM registration',
      receiverState: 'AM JVM booting inside container_e01_000001',
    });

    // Sub-steps of Macro Step 1 (staging-localization + yarn-lifecycle)
    const s1a = b.addStep({
      id: 'sim-step-submit-a',
      parentId: sSubmit,
      title: b.isPySpark
        ? 'yarn.Client stages orders.py & __spark_conf__.zip to HDFS .sparkStaging/'
        : `yarn.Client stages ${b.appArtifact} (__app__.jar) & __spark_conf__.zip to HDFS .sparkStaging/`,
      explanation: b.isPySpark
        ? 'Client.prepareLocalResources() uploads orders.py, pyspark.zip, py4j.zip, and __spark_conf__.zip to HDFS so NMs can localize them.'
        : `Client.prepareLocalResources() registers ${b.appArtifact} (NameNode :8020 metadata, localized container symlink __app__.jar; local: URIs bypass HDFS upload) and __spark_conf__.zip in HDFS .sparkStaging/ with zero Python archives.`,
      category: 'staging-localization',
      keyState: 'HDFS .sparkStaging/ populated for localization',
      command: b.isPySpark
        ? 'Client.prepareLocalResources(stagingDirPath, pySparkArchives)'
        : `Client.prepareLocalResources(stagingDirPath, ${b.appArtifact}) // symlink __app__.jar`,
      detailsPassed: [
        { name: 'stagingDir', value: 'hdfs:///user/app/.sparkStaging/application_1740_0051' },
        ...(b.isJvmApp
          ? [{ name: 'localBypassNote', value: 'local:/path/' + b.appArtifact + ' skips HDFS upload' }]
          : []),
      ],
      deepDiveComponentId: 'yarn-client',
    });
    b.addEvent({
      stepId: s1a,
      from: yClient,
      to: nn,
      channel: 'data',
      protocol: 'Hadoop RPC (ClientProtocol)',
      port: 'nn01:8020',
      label: 'Stage .sparkStaging/*',
      info: b.isPySpark
        ? 'Uploads orders.py, pyspark.zip, and __spark_conf__.zip to HDFS staging directory.'
        : `Creates HDFS .sparkStaging/ metadata on NameNode :8020 for ${b.appArtifact} (YARN localized symlink __app__.jar; local: URIs bypass HDFS upload) and __spark_conf__.zip.`,
      size: b.isPySpark ? '18.2 MiB' : '24.6 MiB',
      command: b.isPySpark
        ? 'Client.prepareLocalResources(stagingDir, pySparkArchives)'
        : `Client.prepareLocalResources(stagingDir, ${b.appArtifact}) // symlink __app__.jar`,
      fields: [
        {
          name: 'files',
          value: b.isPySpark
            ? 'orders.py, pyspark.zip, __spark_conf__.zip'
            : `${b.appArtifact} (symlink __app__.jar), __spark_conf__.zip`,
        },
      ],
      senderState: 'LocalResource map built',
      receiverState: 'Staged files registered in HDFS namespace',
    });
    const s1b = b.addStep({
      id: 'sim-step-submit-b',
      parentId: sSubmit,
      title: 'RM submits AM container -> driver-1 NM localizes & boots AM JVM',
      explanation: b.isPySpark
        ? 'RM calls startContainers on driver-1 NM, NM downloads .sparkStaging/ files, sets cgroups, and starts ApplicationMaster.main().'
        : `RM calls startContainers on driver-1 NM, NM downloads ${b.appArtifact} & __spark_conf__.zip, launch_container.sh creates symlink __app__.jar, sets cgroups, and starts ApplicationMaster.main(--jar ${b.appArtifact} --class ${b.appMainClass}).`,
      category: 'yarn-lifecycle',
      keyState: 'AM Attempt 1 running in container_e01_000001',
      command: 'cmProxy.startContainers(amCtx) -> ApplicationMaster.main()',
      detailsPassed: [{ name: 'containerId', value: 'container_e01_000001' }],
      deepDiveComponentId: 'node-manager',
    });
    b.addEvent({
      stepId: s1b,
      from: yClient,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationClientProtocol)',
      port: 'rm01:8032',
      label: 'submitApplication(AM)',
      info: b.isPySpark
        ? 'Submits AM ApplicationSubmissionContext to RM :8032.'
        : `Submits AM ApplicationSubmissionContext (--jar ${b.appArtifact} --class ${b.appMainClass}) to RM :8032.`,
      size: '14.4 KB',
      command: b.isPySpark
        ? 'yarnClient.submitApplication(appContext)'
        : `yarnClient.submitApplication(appContext[--jar ${b.appArtifact} --class ${b.appMainClass}])`,
      fields: [{ name: 'queue', value: 'default' }],
      senderState: 'Polling ApplicationReport',
      receiverState: 'Allocated container_e01_000001',
    });
    b.addEvent({
      stepId: s1b,
      from: rm,
      to: amNm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: 'driver-1:dynamic',
      label: 'startContainers(AM)',
      info: 'RM AMLauncher dispatches AM launch context to driver-1 NodeManager.',
      size: '16.0 KB',
      command: 'cmProxy.startContainers(container_e01_000001)',
      fields: [{ name: 'attemptId', value: 'appattempt_1740_0051_000001' }],
      senderState: 'AM launch requested',
      receiverState: 'Localizing archives & starting cgroup',
    });
    b.addEvent({
      stepId: s1b,
      from: amNm,
      to: nn,
      categoryOverride: 'staging-localization',
      channel: 'data',
      protocol: 'HDFS Localization Fetch',
      port: 'nn01:8020',
      label: 'Localize AM archives',
      info: b.isPySpark
        ? 'driver-1 NM downloads staged archives into container_e01_000001 working directory.'
        : `driver-1 NM downloads ${b.appArtifact} & __spark_conf__.zip; container launch script symlinks __app__.jar in container_e01_000001 working directory.`,
      size: b.isPySpark ? '18.2 MiB' : '24.6 MiB',
      command: 'ResourceLocalizationService.localize(localResources)',
      fields: [{ name: 'state', value: 'LOCALIZING -> LOCALIZED' }],
      senderState: 'Working directory populated',
      receiverState: 'Served staged blocks',
    });
    b.addEvent({
      stepId: s1b,
      from: amNm,
      to: amCont,
      kind: 'local',
      channel: 'yarn',
      protocol: 'ContainerExecutor + cgroups',
      port: 'driver-1 OS',
      label: 'Boot AM JVM container',
      info: 'NM spawns container_e01_000001 cgroup and starts ApplicationMaster.main().',
      size: 'OS exec',
      command: 'ContainerExecutor.launchContainer(container_e01_000001)',
      fields: [{ name: 'pmemLimit', value: '1408 MiB' }],
      senderState: 'Monitoring AM container',
      receiverState: 'AM JVM entering runDriver()',
    });
    b.addEvent({
      stepId: s1b,
      from: amCont,
      to: am,
      kind: 'local',
      channel: 'internal',
      protocol: 'JVM main() entry',
      port: 'driver-1 in-JVM',
      label: 'ApplicationMaster.main',
      info: 'AM JVM starts runDriver() inside container_e01_000001.',
      size: 'in-memory',
      command: 'ApplicationMaster.main() -> runDriver()',
      fields: [{ name: 'mode', value: 'cluster' }],
      senderState: 'Hosting AM + Driver JVM',
      receiverState: 'Spawning user Driver thread',
    });

    // =========================================================================
    // PHASE 2: Cluster-Mode Pre-Driver Startup Short-Circuits
    // =========================================================================
    const preDriverShortCircuit = tryInjectPreDriverStartupFault(b, cfg, am, amNm, runMeta);
    if (preDriverShortCircuit) {
      return preDriverShortCircuit;
    }

    // =========================================================================
    // MACRO STEP 2 (Cluster): AM Launches User Driver & SparkContext
    // =========================================================================
    const sc = b.std('spark-context');
    const session = b.std('spark-session');
    const sparkEnv = b.std('spark-env');
    const listenerBus = b.std('listener-bus');
    const sparkUi = b.std('spark-ui');

    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      const py4j = b.std('py4j-gateway');
      const sDriver = b.addStep({
        id: 'sim-step-driver',
        title: 'AM launches Python driver, Py4J & SparkContext (:41000)',
        explanation:
          'ApplicationMaster spawns deploy.PythonRunner -> Py4JServer -> python3 orders.py; orders.py creates SparkContext first (binding :41000, :41001, SparkUI :4040), resolves sparkContextPromise, and wraps SparkSession. Expand for full internal handoffs.',
        category: 'driver-python-bridge',
        keyState: 'Driver live on driver-1:41000 | SparkUI :4040 | AM unblocked',
        command: 'ApplicationMaster.runDriver() -> PythonRunner -> SparkContext.getOrCreate()',
        detailsPassed: [
          { name: 'driverRpc', value: 'driver-1:41000' },
          { name: 'py4jPort', value: '43122 (127.0.0.1 loopback)' },
          { name: 'sparkUi', value: 'http://driver-1:4040' },
        ],
        deepDiveComponentId: 'application-master',
        isMacro: true,
      });
      b.addEvent({
        stepId: sDriver,
        from: am,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder + Py4J',
        port: 'driver-1 OS',
        label: 'Spawn orders.py',
        info: 'AM Driver thread starts Py4JServer on 127.0.0.1:43122 and forks python3 orders.py.',
        size: 'OS fork/exec',
        command: 'PythonRunner.main(Array("orders.py", ""))',
        fields: [{ name: 'PYSPARK_GATEWAY_PORT', value: '43122' }],
        senderState: 'AM waiting on sparkContextPromise',
        receiverState: 'orders.py calling SparkSession.builder.getOrCreate()',
      });
      b.addEvent({
        stepId: sDriver,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback -> JVM SC',
        port: '127.0.0.1:43122',
        label: 'Init SparkContext :41000',
        info: 'orders.py initializes SparkContext over Py4J, binding NettyRpcEnv (:41000) & SparkUI (:4040) and resolving sparkContextPromise.',
        size: '2.1 KB',
        command: 'SparkContext.getOrCreate(conf) -> ApplicationMaster.sparkContextInitialized(sc)',
        fields: [
          { name: 'spark.driver.port', value: '41000' },
          { name: 'spark.ui.port', value: '4040' },
        ],
        senderState: 'Received SparkSession proxy handle',
        receiverState: 'SparkContext & AM unblocked for RM registration',
      });

      const s2a = b.addStep({
        id: 'sim-step-driver-a',
        parentId: sDriver,
        title: 'PythonRunner starts Py4JServer (:43122) & spawns orders.py',
        explanation: 'AM Driver thread invokes deploy.PythonRunner, starts loopback Py4JServer with 256-bit secret, and spawns python3 orders.py.',
        category: 'driver-python-bridge',
        keyState: 'Py4JServer listening on 127.0.0.1:43122; orders.py running',
        command: 'new Py4JServer(conf).start() -> new ProcessBuilder("python3", "orders.py").start()',
        detailsPassed: [{ name: 'PYSPARK_GATEWAY_PORT', value: '43122' }],
        deepDiveComponentId: 'py4j-gateway',
      });
      b.addEvent({
        stepId: s2a,
        from: am,
        to: py4j,
        kind: 'local',
        channel: 'python',
        protocol: 'Py4JServer Loopback Bind',
        port: 'driver-1 in-JVM',
        label: 'Start Py4JServer :43122',
        info: 'PythonRunner binds loopback Py4JServer with random 256-bit PYSPARK_GATEWAY_SECRET.',
        size: 'socket bind',
        command: 'gatewayServer = Some(new Py4JServer(sparkConf))',
        fields: [{ name: 'port', value: '43122' }],
        senderState: 'Passing gateway env vars to ProcessBuilder',
        receiverState: 'Listening on 127.0.0.1:43122',
      });
      b.addEvent({
        stepId: s2a,
        from: py4j,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder + env',
        port: 'driver-1 OS',
        label: 'Fork python3 orders.py',
        info: 'Python subprocess launches with PYTHONPATH and gateway credentials.',
        size: 'OS fork/exec',
        command: 'builder.start() // python3 orders.py',
        fields: [{ name: 'PYTHONUNBUFFERED', value: 'YES' }],
        senderState: 'Waiting for auth handshake',
        receiverState: 'Executing orders.py top-level code',
      });

      const s2b = b.addStep({
        id: 'sim-step-driver-b',
        parentId: sDriver,
        title: 'SparkContext binds SparkEnv (:41000), ListenerBus & SparkUI (:4040), then signals AM',
        explanation: 'Classic PySpark constructs SparkContext BEFORE SparkSession: SparkEnv binds NettyRpcEnv :41000 & BM :41001, LiveListenerBus & SparkUI :4040 start, then YarnClusterScheduler.postStartHook fulfills sparkContextPromise before SparkSession wraps SparkContext.',
        category: 'heartbeats-metrics',
        keyState: 'SparkContext, SparkUI :4040, and SparkSession ready; AM unblocked',
        command: 'SparkContext.getOrCreate() -> SparkEnv -> SparkUI -> postStartHook() -> new SparkSession(sc)',
        detailsPassed: [
          { name: 'rpcPort', value: '41000' },
          { name: 'uiPort', value: '4040' },
        ],
        deepDiveComponentId: 'spark-context',
      });
      b.addEvent({
        stepId: s2b,
        from: pyDriver,
        to: sc,
        categoryOverride: 'driver-python-bridge',
        channel: 'python',
        protocol: 'Py4J Loopback TCP',
        port: '127.0.0.1:43122',
        label: 'SparkContext.getOrCreate',
        info: 'PySpark obtains/creates JVM SparkContext BEFORE wrapping it in SparkSession.',
        size: '2.1 KB',
        command: 'JavaSparkContext(jconf)',
        fields: [{ name: 'order', value: 'SparkContext first, then SparkSession' }],
        senderState: 'Awaiting JVM SparkContext',
        receiverState: 'Initializing SparkEnv and schedulers',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: sparkEnv,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM SparkEnv Init',
        port: 'driver-1 in-JVM',
        label: 'Bind RPC :41000 & BM',
        info: 'SparkEnv.createDriverEnv binds NettyRpcEnv on :41000 and driver BlockManager on :41001.',
        size: 'in-memory',
        command: 'SparkEnv.createDriverEnv(conf, false, listenerBus)',
        fields: [
          { name: 'rpc', value: 'driver-1:41000' },
          { name: 'bm', value: 'driver-1:41001' },
        ],
        senderState: 'Driver RPC & BlockManager bound',
        receiverState: 'All driver RpcEndpoints active',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: listenerBus,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Observability',
        port: 'driver-1 in-JVM',
        label: 'Start ListenerBus + UI',
        info: 'LiveListenerBus and Jetty SparkUI (:4040) bind before postStartHook signals AM.',
        size: 'in-memory',
        command: 'listenerBus.start(); SparkUI.create(Some(this), ...)',
        fields: [{ name: 'uiUrl', value: 'http://driver-1:4040' }],
        senderState: 'UI bound on :4040',
        receiverState: 'Event queues running',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: sparkUi,
        kind: 'local',
        channel: 'internal',
        protocol: 'Jetty Bind',
        port: 'driver-1 in-JVM',
        label: 'Bind SparkUI :4040',
        info: 'SparkUI binds :4040 so AM can advertise trackingUrl to RM.',
        size: 'in-memory',
        command: 'ui.bind() // http://driver-1:4040',
        fields: [{ name: 'webUrl', value: 'http://driver-1:4040' }],
        senderState: 'UI ready',
        receiverState: 'Serving live status store',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: am,
        kind: 'local',
        categoryOverride: 'yarn-lifecycle',
        channel: 'internal',
        protocol: 'YarnClusterScheduler.postStartHook',
        port: 'driver-1 in-JVM',
        label: 'Resolve scPromise',
        info: 'postStartHook signals ApplicationMaster.sparkContextInitialized(sc), unblocking registerAM().',
        size: 'in-memory',
        command: 'ApplicationMaster.sparkContextInitialized(sc)',
        fields: [{ name: 'driverPort', value: '41000' }],
        senderState: 'Returning active SparkContext to SparkSession',
        receiverState: 'AM unblocked for RM registration',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: session,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM SparkSession Wrap',
        port: 'driver-1 in-JVM',
        label: 'new SparkSession(sc)',
        info: 'SparkSession wraps the live SparkContext and returns proxy handle to Python.',
        size: 'in-memory',
        command: 'new SparkSession(sc)',
        fields: [{ name: 'sharedState', value: 'Bound to active SparkContext' }],
        senderState: 'SparkContext active',
        receiverState: 'SparkSession ready for SQL/DataFrame calls',
      });
    } else {
      // Pure-JVM Cluster Mode Startup (Scala / Java): ZERO Python, ZERO Py4J, ZERO ProcessBuilder!
      const jvmApp = b.std('jvm-user-app');
      const sDriver = b.addStep({
        id: 'sim-step-driver',
        title: `AM reflects ${b.appEntryLabel}() in-JVM & initializes SparkContext (:41000)`,
        explanation:
          `ApplicationMaster.startUserApplication() builds MutableURLClassLoader (or ChildFirstURLClassLoader when spark.driver.userClassPathFirst=true) over localized __app__.jar, loads ${b.appMainClass}, and invokes static main(args) directly in-JVM on driver-1 (ZERO ProcessBuilder, ZERO Py4J). ${b.appEntryLabel}() calls SparkSession.builder.getOrCreate() -> SparkContext (:41000, :4040) -> YarnClusterScheduler.postStartHook() fulfills sparkContextPromise and pauses the user thread until AM registers on :8030, creates YarnAllocator, and resumes the driver thread.`,
        category: 'driver-python-bridge',
        keyState: `Pure-JVM Driver live on driver-1:41000 | SparkUI :4040 | 0 Python processes`,
        command: `ApplicationMaster.startUserApplication() -> ${b.appMainClass}.main(args) -> SparkSession.builder.getOrCreate()`,
        detailsPassed: [
          { name: 'userClass', value: b.appMainClass },
          { name: 'classLoader', value: 'MutableURLClassLoader (ChildFirstURLClassLoader if spark.driver.userClassPathFirst=true)' },
          { name: 'driverRpc', value: 'driver-1:41000' },
          { name: 'sparkUi', value: 'http://driver-1:4040' },
        ],
        deepDiveComponentId: 'jvm-user-app',
        isMacro: true,
      });
      b.addEvent({
        stepId: sDriver,
        from: am,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Reflection main()',
        port: 'driver-1 in-JVM',
        label: b.isScala ? 'Reflect OrdersJob.main' : 'Reflect OrdersJavaJob',
        info: `AM creates MutableURLClassLoader (or ChildFirstURLClassLoader if spark.driver.userClassPathFirst=true) over __app__.jar and invokes ${b.appMainClass}.main(args) directly in-JVM on Driver thread (ZERO ProcessBuilder, ZERO py4j-gateway).`,
        size: 'in-memory',
        command: `userClassLoader.loadClass("${b.appMainClass}").getMethod("main", String[].class).invoke(null, args)`,
        fields: [
          { name: 'classLoader', value: 'MutableURLClassLoader (ChildFirst if spark.driver.userClassPathFirst=true)' },
          { name: 'pythonProcesses', value: '0 (pure JVM in-process launch)' },
        ],
        senderState: 'AM awaiting sparkContextPromise',
        receiverState: `${b.appEntryLabel}() calling SparkSession.builder.getOrCreate()`,
      });
      b.addEvent({
        stepId: sDriver,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM SparkSession -> SC',
        port: 'driver-1 in-JVM',
        label: 'Init SparkContext :41000',
        info: `${b.appEntryLabel}() calls SparkSession.builder.getOrCreate() -> SparkContext (:41000, SparkUI :4040); YarnClusterScheduler.postStartHook() fulfills sparkContextPromise & pauses user thread until AM registers on :8030 and creates YarnAllocator.`,
        size: 'in-memory',
        command: 'SparkSession.builder().getOrCreate() -> SparkContext -> postStartHook() (pause user thread)',
        fields: [
          { name: 'spark.driver.port', value: '41000' },
          { name: 'spark.ui.port', value: '4040' },
          { name: 'postStartHook', value: 'Fulfills sparkContextPromise & pauses user thread until AM resumeDriver()' },
        ],
        senderState: 'User Driver thread paused awaiting AM registration + resumeDriver()',
        receiverState: 'SparkContext live; AM unblocked for RM :8030 registration',
      });

      const s2a = b.addStep({
        id: 'sim-step-driver-a',
        parentId: sDriver,
        title: `AM builds MutableURLClassLoader over __app__.jar & invokes ${b.appEntryLabel}()`,
        explanation: `ApplicationMaster.startUserApplication() loads ${b.appMainClass} via MutableURLClassLoader (or ChildFirstURLClassLoader when spark.driver.userClassPathFirst=true) and invokes static main(args) directly on the AM JVM Driver thread with ZERO ProcessBuilder and ZERO Py4J.`,
        category: 'driver-python-bridge',
        keyState: `Driver thread executing ${b.appEntryLabel}() directly inside AM JVM`,
        command: `userClassLoader.loadClass("${b.appMainClass}").getMethod("main", String[].class).invoke(null, args)`,
        detailsPassed: [
          { name: 'classLoader', value: 'MutableURLClassLoader (or ChildFirstURLClassLoader)' },
          { name: 'pythonOverhead', value: '0 (pure JVM)' },
        ],
        deepDiveComponentId: 'jvm-user-app',
      });
      b.addEvent({
        stepId: s2a,
        from: am,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM ClassLoader Invoke',
        port: 'driver-1 in-JVM',
        label: b.isScala ? 'Invoke OrdersJob.main()' : 'Invoke OrdersJavaJob()',
        info: `AM reflects ${b.appMainClass}.main(args) directly in-JVM on driver-1 (no subprocess, no Py4J loopback socket).`,
        size: 'in-memory',
        command: `mainMethod.invoke(null, userArgs) // ${b.appMainClass}`,
        fields: [{ name: 'thread', value: 'Driver (non-daemon AM JVM thread)' }],
        senderState: 'Waiting on sparkContextPromise (100s)',
        receiverState: `Running ${b.appEntryLabel}() in-JVM`,
      });

      const s2b = b.addStep({
        id: 'sim-step-driver-b',
        parentId: sDriver,
        title: 'SparkContext binds SparkEnv (:41000), ListenerBus & SparkUI (:4040), postStartHook pauses user thread',
        explanation: `${b.appEntryLabel}() calls SparkSession.builder.getOrCreate() -> SparkContext.getOrCreate() (SparkSession.scala:1057-1069): SparkEnv binds NettyRpcEnv :41000 & BM :41001, LiveListenerBus & SparkUI :4040 start, then YarnClusterScheduler.postStartHook() calls ApplicationMaster.sparkContextInitialized(sc) (ApplicationMaster.scala:405-418) to fulfill sparkContextPromise and pause the user thread BEFORE new SparkSession(sc) runs.`,
        category: 'heartbeats-metrics',
        keyState: 'SparkContext & SparkUI :4040 bound; user thread paused before new SparkSession(sc) until AM resumeDriver()',
        command: 'SparkSession.builder.getOrCreate() -> SparkContext.getOrCreate() -> postStartHook() (pause)',
        detailsPassed: [
          { name: 'rpcPort', value: '41000' },
          { name: 'uiPort', value: '4040' },
          { name: 'sessionOrder', value: 'new SparkSession(sc) runs ONLY AFTER AM registerAM(:8030) + createAllocator() + resumeDriver()' },
        ],
        deepDiveComponentId: 'spark-context',
      });
      b.addEvent({
        stepId: s2b,
        from: jvmApp,
        to: sc,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM SparkSession -> SC',
        port: 'driver-1 in-JVM',
        label: 'SparkSession -> SC init',
        info: `${b.appEntryLabel}() invokes SparkSession.builder.getOrCreate() -> SparkContext.getOrCreate() (SparkSession.scala:1057-1069) directly in-JVM.`,
        size: 'in-memory',
        command: 'SparkSession.builder().appName("Orders").getOrCreate() -> SparkContext.getOrCreate()',
        fields: [{ name: 'api', value: `${b.langDisplay} native JVM` }],
        senderState: 'Constructing SparkContext inside SparkSession.builder.getOrCreate()',
        receiverState: 'Initializing SparkEnv and schedulers',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: sparkEnv,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM SparkEnv Init',
        port: 'driver-1 in-JVM',
        label: 'Bind RPC :41000 & BM',
        info: 'SparkEnv.createDriverEnv binds NettyRpcEnv on :41000 and driver BlockManager on :41001.',
        size: 'in-memory',
        command: 'SparkEnv.createDriverEnv(conf, false, listenerBus)',
        fields: [
          { name: 'rpc', value: 'driver-1:41000' },
          { name: 'bm', value: 'driver-1:41001' },
        ],
        senderState: 'Driver RPC & BlockManager bound',
        receiverState: 'All driver RpcEndpoints active',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: listenerBus,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Observability',
        port: 'driver-1 in-JVM',
        label: 'Start ListenerBus + UI',
        info: 'LiveListenerBus and Jetty SparkUI (:4040) bind before postStartHook signals AM.',
        size: 'in-memory',
        command: 'listenerBus.start(); SparkUI.create(Some(this), ...)',
        fields: [{ name: 'uiUrl', value: 'http://driver-1:4040' }],
        senderState: 'UI bound on :4040',
        receiverState: 'Event queues running',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: sparkUi,
        kind: 'local',
        channel: 'internal',
        protocol: 'Jetty Bind',
        port: 'driver-1 in-JVM',
        label: 'Bind SparkUI :4040',
        info: 'SparkUI binds :4040 so AM can advertise trackingUrl to RM.',
        size: 'in-memory',
        command: 'ui.bind() // http://driver-1:4040',
        fields: [{ name: 'webUrl', value: 'http://driver-1:4040' }],
        senderState: 'UI ready',
        receiverState: 'Serving live status store',
      });
      b.addEvent({
        stepId: s2b,
        from: sc,
        to: am,
        kind: 'local',
        categoryOverride: 'yarn-lifecycle',
        channel: 'internal',
        protocol: 'YarnClusterScheduler.postStartHook',
        port: 'driver-1 in-JVM',
        label: 'postStartHook (pause)',
        info: 'postStartHook calls ApplicationMaster.sparkContextInitialized(sc) (ApplicationMaster.scala:405-418), fulfilling sparkContextPromise AND pausing the user Driver thread inside SparkContext.getOrCreate() BEFORE new SparkSession(sc) is constructed.',
        size: 'in-memory',
        command: 'ApplicationMaster.sparkContextInitialized(sc) // sparkContextPromise.wait()',
        fields: [
          { name: 'driverPort', value: '41000' },
          { name: 'userThreadState', value: 'Paused until AM registerAM(:8030) + createAllocator() + resumeDriver()' },
        ],
        senderState: 'User Driver thread paused before new SparkSession(sc)',
        receiverState: 'AM unblocked for RM :8030 registration',
      });
    }
  } else {
    // CLIENT MODE STARTUP: Driver boots on edge-01 BEFORE YARN AM
    const sc = b.std('spark-context');
    const yarnBackend = b.std('yarn-scheduler-backend');
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      const sClientBoot = b.addStep({
        id: 'sim-step-client-boot',
        title: 'Client driver starts on edge-01:41000 BEFORE YARN submits AM',
        explanation:
          'In client mode, spark-submit runs PythonRunner and orders.py directly on edge-01. SparkContext binds edge-01:41000 first, then YarnClientSchedulerBackend submits ExecutorLauncher (--arg edge-01:41000) to RM :8032.',
        category: 'driver-python-bridge',
        keyState: 'Driver live on edge-01:41000 | Client-mode AM ExecutorLauncher launched',
        command: `spark-submit --master yarn --deploy-mode client --num-executors ${cfg.numExecutors} orders.py`,
        detailsPassed: [
          { name: 'driverHost', value: 'edge-01 (external submit host)' },
          { name: 'driverRpcPort', value: '41000' },
          { name: 'amMainClass', value: 'ExecutorLauncher --arg edge-01:41000' },
        ],
        deepDiveComponentId: 'yarn-scheduler-backend',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: submit,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder (edge-01)',
        port: 'edge-01 OS',
        label: 'Run orders.py locally',
        info: 'spark-submit starts local PythonRunner and orders.py on edge-01.',
        size: 'OS fork/exec',
        command: 'PythonRunner.main(Array("orders.py", ""))',
        fields: [{ name: 'PYSPARK_GATEWAY_PORT', value: '44108' }],
        senderState: 'Hosting driver JVM on edge-01',
        receiverState: 'Creating local SparkContext',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback TCP',
        port: '127.0.0.1:44108',
        label: 'Bind edge-01:41000',
        info: 'SparkContext binds NettyRpcEnv on edge-01:41000 before contacting YARN.',
        size: '2.0 KB',
        command: 'SparkContext.getOrCreate(conf) // spark.driver.host=edge-01',
        fields: [{ name: 'advertised', value: 'edge-01:41000' }],
        senderState: 'Awaiting YARN application RUNNING',
        receiverState: 'Starting YarnClientSchedulerBackend',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: yarnBackend,
        to: yClient,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM yarn.Client',
        port: 'edge-01 in-JVM',
        label: 'Submit ExecutorLauncher',
        info: 'YarnClientSchedulerBackend passes --arg edge-01:41000 to yarn.Client.',
        size: 'in-memory',
        command: 'new Client(Array("--arg", "edge-01:41000"), conf).submitApplication()',
        fields: [{ name: 'arg0', value: 'edge-01:41000' }],
        senderState: 'Waiting in waitForApplication()',
        receiverState: 'Submitting lightweight AM to RM :8032',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: yClient,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationClientProtocol)',
        port: 'rm01:8032',
        label: 'submitApplication(AM)',
        info: 'RM accepts ExecutorLauncher AM and boots it on a cluster container.',
        size: '13.2 KB',
        command: 'yarnClient.submitApplication(appContext[ExecutorLauncher])',
        fields: [{ name: 'amClass', value: 'ExecutorLauncher' }],
        senderState: 'Monitoring RM app state',
        receiverState: 'Launching AM container',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: rm,
        to: am,
        channel: 'yarn',
        protocol: 'Hadoop RPC (AM Launch)',
        port: 'driver-1:dynamic',
        label: 'Boot ExecutorLauncher',
        info: 'RM launches lightweight AM (ExecutorLauncher) without the user Python driver.',
        size: '14.0 KB',
        command: 'ApplicationMaster.runExecutorLauncher()',
        fields: [{ name: 'driverArg', value: 'edge-01:41000' }],
        senderState: 'AM container started',
        receiverState: 'AM registering port=-1, creating YarnAllocator, then dialing edge-01:41000',
      });
    } else {
      // Pure-JVM Client Mode Startup (Scala / Java): JavaMainApplication.start() on edge-01 in-JVM
      const jvmApp = b.std('jvm-user-app');
      const sClientBoot = b.addStep({
        id: 'sim-step-client-boot',
        title: `Client ${b.langDisplay} driver starts on edge-01:41000 BEFORE YARN submits AM`,
        explanation:
          `In client mode, spark-submit invokes JavaMainApplication.start() directly in-JVM on edge-01 (ZERO PythonRunner, ZERO Py4J), calling ${b.appMainClass}.main(). SparkSession/SparkContext binds edge-01:41000 first, then YarnClientSchedulerBackend submits lightweight ExecutorLauncher AM (--arg edge-01:41000) to RM :8032.`,
        category: 'driver-python-bridge',
        keyState: `Pure-JVM client driver live on edge-01:41000 | ExecutorLauncher AM launched`,
        command: b.submitCommand(),
        detailsPassed: [
          { name: 'language', value: b.langDisplay },
          { name: 'clientLauncher', value: `JavaMainApplication.start() -> ${b.appMainClass}.main()` },
          { name: 'driverHost', value: 'edge-01 (external submit host)' },
          { name: 'amMainClass', value: 'ExecutorLauncher --arg edge-01:41000' },
        ],
        deepDiveComponentId: 'yarn-scheduler-backend',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: submit,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM JavaMainApplication',
        port: 'edge-01 in-JVM',
        label: b.isScala ? 'JavaMainApp -> OrdersJob' : 'JavaMainApp -> JavaJob',
        info: `spark-submit invokes JavaMainApplication.start() directly in-JVM on edge-01 -> ${b.appMainClass}.main() (ZERO python-driver, ZERO py4j-gateway).`,
        size: 'in-memory',
        command: `JavaMainApplication.start(args, sparkConf) -> ${b.appMainClass}.main(args)`,
        fields: [
          { name: 'userClass', value: b.appMainClass },
          { name: 'pythonProcesses', value: '0' },
        ],
        senderState: 'Hosting pure-JVM client driver on edge-01',
        receiverState: 'Creating local SparkSession / SparkContext',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM SparkSession -> SC',
        port: 'edge-01 in-JVM',
        label: 'Bind edge-01:41000',
        info: `${b.appEntryLabel}() creates SparkSession/SparkContext in-JVM, binding NettyRpcEnv on edge-01:41000 before contacting YARN.`,
        size: 'in-memory',
        command: 'SparkSession.builder().getOrCreate() // spark.driver.host=edge-01',
        fields: [{ name: 'advertised', value: 'edge-01:41000' }],
        senderState: 'Awaiting YARN application RUNNING',
        receiverState: 'Starting YarnClientSchedulerBackend',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: yarnBackend,
        to: yClient,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM yarn.Client',
        port: 'edge-01 in-JVM',
        label: 'Submit ExecutorLauncher',
        info: 'YarnClientSchedulerBackend passes --arg edge-01:41000 to yarn.Client.',
        size: 'in-memory',
        command: 'new Client(Array("--arg", "edge-01:41000"), conf).submitApplication()',
        fields: [{ name: 'arg0', value: 'edge-01:41000' }],
        senderState: 'Waiting in waitForApplication()',
        receiverState: 'Submitting lightweight AM to RM :8032',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: yClient,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationClientProtocol)',
        port: 'rm01:8032',
        label: 'submitApplication(AM)',
        info: 'RM accepts ExecutorLauncher AM and boots it on a cluster container.',
        size: '13.2 KB',
        command: 'yarnClient.submitApplication(appContext[ExecutorLauncher])',
        fields: [{ name: 'amClass', value: 'ExecutorLauncher' }],
        senderState: 'Monitoring RM app state',
        receiverState: 'Launching AM container',
      });
      b.addEvent({
        stepId: sClientBoot,
        from: rm,
        to: am,
        channel: 'yarn',
        protocol: 'Hadoop RPC (AM Launch)',
        port: 'driver-1:dynamic',
        label: 'Boot ExecutorLauncher',
        info: `RM launches lightweight AM (ExecutorLauncher) without the user ${b.langDisplay} driver.`,
        size: '14.0 KB',
        command: 'ApplicationMaster.runExecutorLauncher()',
        fields: [{ name: 'driverArg', value: 'edge-01:41000' }],
        senderState: 'AM container started',
        receiverState: 'AM registering port=-1, creating YarnAllocator, then dialing edge-01:41000',
      });
    }
  }

  // =========================================================================
  // PHASE 3: Client-Mode Post-AM-Submit Startup Faults (dynalloc-no-shuffle-service)
  // =========================================================================
  if (cfg.deployMode === 'client') {
    const clientDynAllocShortCircuit = tryInjectClientDynAllocNoEssFault(b, cfg, am, runMeta);
    if (clientDynAllocShortCircuit) {
      return clientDynAllocShortCircuit;
    }
  }

  // =========================================================================
  // MACRO STEP 3: AM Registers with RM, Allocates & Launches N Executors
  // =========================================================================
  const alloc = b.std('yarn-allocator');
  const schedBackend = b.std('driver-scheduler-backend');
  const bmm = b.std('block-manager-master');
  const hbRx = b.std('heartbeat-receiver');
  b.resetForAttempt(1);
  const initialAllocations = Array.from({ length: cfg.numExecutors }, () => b.allocateExec(1));
  const execIds = initialAllocations.map(a => b.execInstance(a.execNum, 1, a.containerId));
  const execBms = initialAllocations.map(a => b.execBmInstance(a.execNum, 1, a.containerId));
  const isDiskDirFail = cfg.fault === 'disk-dir-create-fail';

  const sAlloc = b.addStep({
    id: 'sim-step-alloc',
    title: isDiskDirFail
      ? `AM registers with RM & launches ${cfg.numExecutors} executor container(s) (Exec-1 fails local dir init)`
      : `AM registers with RM & launches ${cfg.numExecutors} executor(s) (${totalSlots} slots)`,
    explanation: isDiskDirFail
      ? `ApplicationMaster registers with RM :8030 (${cfg.deployMode === 'client' ? 'driver-1:-1, then createAllocator before RegisterClusterManager' : 'driver RPC port 41000'}), YarnAllocator requests ${cfg.numExecutors} container(s), and NodeManagers launch Exec-1..Exec-${cfg.numExecutors}. Exec-1 crashes during DiskBlockManager.createLocalDirs() (exit 53) BEFORE registering its executor or BlockManager.`
      : `ApplicationMaster registers with RM :8030 (${cfg.deployMode === 'client' ? 'driver-1:-1, then createAllocator before RegisterClusterManager' : 'driver RPC port 41000'}), YarnAllocator requests ${cfg.numExecutors} container(s) (${cfg.executorCores} cores each = ${totalSlots} total slots), NodeManagers boot Exec-1..Exec-${cfg.numExecutors}, and executors register with the driver & BlockManagerMaster.`,
    category: 'yarn-lifecycle',
    keyState: isDiskDirFail
      ? `AM registered; Exec-1 container launched on worker-a (pending DiskBlockManager startup)`
      : `${cfg.numExecutors} executor(s) registered (${totalSlots} slots | ideal ${idealWavesPerStage} wave/stage)`,
    command: `amClient.register(...) -> amClient.allocate(${cfg.numExecutors}x ${cfg.executorCores}vcore) -> startContainers`,
    detailsPassed: [
      { name: cfg.dynamicAllocation ? 'initialExecutors' : 'fixedExecutors', value: String(cfg.numExecutors) },
      { name: 'totalSlots', value: `${totalSlots} (${cfg.numExecutors} x ${cfg.executorCores} cores)` },
      { name: 'idealWavesPerStage', value: `${idealWavesPerStage} (fixed-capacity ideal for ${cfg.numPartitions} parts)` },
      { name: 'containers', value: `${containers} YARN containers (1 AM + ${cfg.numExecutors} Exec)` },
    ],
    deepDiveComponentId: 'yarn-allocator',
    isMacro: true,
  });

  b.addEvent({
    stepId: sAlloc,
    from: am,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: cfg.deployMode === 'client' ? 'registerAM(port=-1)' : 'registerAM(driver:41000)',
    info:
      cfg.deployMode === 'client'
        ? 'Client-mode AM registers with RM passing host=driver-1, rpcPort=-1 (no listening AM RPC port; outbound channel to edge-01:41000) and edge-01 UI URL.'
        : 'Cluster-mode AM registers with RM passing driver-1:41000 and Spark UI trackingUrl.',
    size: '1.6 KB',
    command: `amClient.register("driver-1", ${cfg.deployMode === 'client' ? -1 : 41000}, conf, "${b.driverUiUrl}")`,
    fields: [
      { name: 'amRpcAddress', value: cfg.deployMode === 'client' ? 'driver-1:-1 (client-only RPC)' : 'driver-1:41000' },
      { name: 'trackingUrl', value: b.driverUiUrl },
    ],
    senderState: 'AM registered; creating YarnAllocator',
    receiverState: 'RM marks app RUNNING',
  });
  b.addEvent({
    stepId: sAlloc,
    from: alloc,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: `allocate(${cfg.numExecutors} exec${cfg.numExecutors > 1 ? 's' : ''})`,
    info: `YarnAllocator requests ${cfg.numExecutors} executor container(s) (${cfg.executorCores} vcores each) from RM :8030.`,
    size: '2.6 KB',
    command: `amClient.allocate(progress=0.1f) // ${cfg.numExecutors}x ResourceRequest(${cfg.executorCores} vcores)`,
    fields: [
      { name: 'requestedContainers', value: String(cfg.numExecutors) },
      { name: 'vcoresPerContainer', value: String(cfg.executorCores) },
    ],
    senderState: 'Polling RM for container grants',
    receiverState: `RM grants ${cfg.numExecutors} container(s) across worker nodes`,
  });
  b.addEvent({
    stepId: sAlloc,
    from: rm,
    to: alloc,
    channel: 'yarn',
    protocol: 'Hadoop RPC (AllocateResponse)',
    port: 'rm01:8030',
    label: `Grant ${cfg.numExecutors} container${cfg.numExecutors > 1 ? 's' : ''}`,
    info: `RM returns ${cfg.numExecutors} container grant(s) with NodeManager addresses and security tokens.`,
    size: '3.2 KB',
    command: `AllocateResponse(allocated = [${initialAllocations[0].containerId}..${initialAllocations[initialAllocations.length - 1].containerId}])`,
    fields: [
      { name: 'granted', value: `${cfg.numExecutors} containers` },
      { name: 'tokens', value: 'ContainerToken + NMToken' },
    ],
    senderState: 'Containers reserved on workers',
    receiverState: 'Launching ExecutorRunnable threads',
  });
  for (let i = 0; i < cfg.numExecutors; i++) {
    const { execNum, containerId } = initialAllocations[i];
    const nm = b.nmInstance(b.execHostZone(execNum));
    const host = b.execHostName(execNum);
    b.addEvent({
      stepId: sAlloc,
      from: alloc,
      to: nm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: `${host}:dynamic`,
      label: `startContainer(Exec-${execNum})`,
      info: `ExecutorRunnable starts ${containerId} on ${host} with --driver-url spark://CoarseGrainedScheduler@${b.driverRpc}.`,
      size: '9.4 KB',
      command: `nmClient.startContainer(${containerId}, ctx[exec=${execNum}])`,
      fields: [
        { name: 'executorId', value: `"${execNum}"` },
        { name: 'containerId', value: containerId },
        { name: 'driverUrl', value: `spark://CoarseGrainedScheduler@${b.driverRpc}` },
      ],
      senderState: `Dispatched Exec-${execNum} to ${host} NM`,
      receiverState: `Booting Exec-${execNum} JVM`,
    });
    if (!(isDiskDirFail && i === 0)) {
      b.addEvent({
        stepId: sAlloc,
        from: execIds[i],
        to: schedBackend,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `RegisterExecutor(id=${execNum})`,
        info: `Exec-${execNum} on ${host} registers ${cfg.executorCores} core(s) and its BlockManager (${host}:${b.execDataPort(execNum)}) with the driver.`,
        size: '1.8 KB',
        command: `ref.ask[Boolean](RegisterExecutor("${execNum}", self, "${host}", ${cfg.executorCores}))`,
        fields: [
          { name: 'executorId', value: `"${execNum}"` },
          { name: 'cores', value: String(cfg.executorCores) },
          { name: 'blockMgrPort', value: `${host}:${b.execDataPort(execNum)}` },
        ],
        senderState: `Exec-${execNum} ready for LaunchTask`,
        receiverState: `Registered Exec-${execNum} (${cfg.executorCores} slots)`,
      });
    }
  }

  // Expandable sub-steps of Macro Step 3 (including client-mode createAllocator-before-RegisterClusterManager & Heartbeat)
  const s3a = b.addStep({
    id: 'sim-step-alloc-a',
    parentId: sAlloc,
    title: cfg.deployMode === 'client'
      ? 'AM registers driver-1:-1, creates YarnAllocator, then sends RegisterClusterManager'
      : 'AM registers driver:41000, creates YarnAllocator & negotiates containers with RM',
    explanation:
      cfg.deployMode === 'client'
        ? 'Client-mode AM registers with RM (driver-1:-1; no listening AM RPC port), creates YarnAllocator FIRST, then AMEndpoint.onStart() dials outbound to edge-01:41000 with RegisterClusterManager before requesting containers.'
        : 'Cluster-mode AM registers driver-1:41000 with RM :8030, instantiates YarnAllocator, and sends ResourceRequests for all executors.',
    category: 'yarn-lifecycle',
    keyState: `YarnAllocator active and negotiating ${cfg.numExecutors} container(s)`,
    command: 'registerAM() -> createAllocator() -> amClient.allocate()',
    detailsPassed: [{ name: 'targetExecutors', value: String(cfg.numExecutors) }],
    deepDiveComponentId: 'yarn-allocator',
  });
  b.addEvent({
    stepId: s3a,
    from: am,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: cfg.deployMode === 'client' ? 'registerAM(port=-1)' : 'registerAM(driver:41000)',
    info: 'AM registers application attempt with RM :8030.',
    size: '1.6 KB',
    command: `amClient.register("driver-1", ${cfg.deployMode === 'client' ? -1 : 41000}, ...)`,
    fields: [{ name: 'trackingUrl', value: b.driverUiUrl }],
    senderState: 'AM registered with RM',
    receiverState: 'RM transitioned app to RUNNING',
  });
  b.addEvent({
    stepId: s3a,
    from: am,
    to: alloc,
    kind: 'local',
    channel: 'internal',
    protocol: 'In-JVM createAllocator',
    port: 'driver-1 in-JVM',
    label: 'createAllocator()',
    info: 'AM instantiates YarnAllocator before registering AMEndpoint so incoming requests never hit a null allocator.',
    size: 'in-memory',
    command: 'allocator = client.createAllocator(driverRef, sparkConf, rpcEnv)',
    fields: [{ name: 'targetExecutors', value: String(cfg.numExecutors) }],
    senderState: 'Allocator created',
    receiverState: 'Ready to send ResourceRequests',
  });
  if (cfg.deployMode === 'client') {
    const yarnBackend = b.std('yarn-scheduler-backend');
    b.addEvent({
      stepId: s3a,
      from: am,
      to: yarnBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: 'RegisterClusterManager',
      info: 'AMEndpoint.onStart() sends RegisterClusterManager(self) to edge-01:41000.',
      size: '1.4 KB',
      command: 'driver.send(RegisterClusterManager(self))',
      fields: [{ name: 'driver', value: 'edge-01:41000' }],
      senderState: 'Linked to external client driver',
      receiverState: 'Driver stored AM endpoint reference',
    });
    if (b.isJava) {
      const jvmApp = b.std('jvm-user-app');
      const sc = b.std('spark-context');
      b.addEvent({
        stepId: s3a,
        from: jvmApp,
        to: sc,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM JavaSparkContext Wrap',
        port: 'edge-01 in-JVM',
        label: 'JavaSparkContext.fromSC',
        info: 'AFTER AM registers on :8030 and waitForApplication() returns, OrdersJavaJob.main() optionally wraps the SAME single SparkContext via JavaSparkContext.fromSparkContext(spark.sparkContext()) (zero duplicate SparkContext or SparkEnv).',
        size: 'in-memory',
        command: 'JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())',
        fields: [{ name: 'singleContextRule', value: 'Wraps existing SparkContext AFTER AM registration' }],
        senderState: 'Java RDD adapter wrapping active SparkContext',
        receiverState: 'Single SparkContext shared across SparkSession & JavaSparkContext',
      });
    }
  } else if (b.isJvmApp) {
    const jvmApp = b.std('jvm-user-app');
    const sc = b.std('spark-context');
    const session = b.std('spark-session');
    b.addEvent({
      stepId: s3a,
      from: am,
      to: jvmApp,
      kind: 'local',
      categoryOverride: 'yarn-lifecycle',
      channel: 'internal',
      protocol: 'In-JVM AM.resumeDriver',
      port: 'driver-1 in-JVM',
      label: 'resumeDriver() unblocks',
      info: `After registering with RM :8030 and creating YarnAllocator, AM calls resumeDriver() (ApplicationMaster.scala:405-418, sparkContextPromise.notify()) to unblock the user thread inside SparkContext.getOrCreate() for ${b.appEntryLabel}().`,
      size: 'in-memory',
      command: 'ApplicationMaster.resumeDriver() // sparkContextPromise.notify()',
      fields: [{ name: 'order', value: 'registerAM(:8030) -> createAllocator() -> resumeDriver()' }],
      senderState: 'Resumed user Driver thread',
      receiverState: 'SparkContext.getOrCreate() returning to SparkSession.builder.getOrCreate()',
    });
    b.addEvent({
      stepId: s3a,
      from: sc,
      to: session,
      kind: 'local',
      categoryOverride: 'driver-python-bridge',
      channel: 'internal',
      protocol: 'In-JVM SparkSession Wrap',
      port: 'driver-1 in-JVM',
      label: 'new SparkSession(sc)',
      info: `ONLY AFTER AM.resumeDriver() unblocks the user thread does SparkSession.builder.getOrCreate() construct new SparkSession(sc) (SparkSession.scala:1057-1069) and return it to ${b.appEntryLabel}().`,
      size: 'in-memory',
      command: 'new SparkSession(sc) // SparkSession.scala:1057-1069 (after AM.resumeDriver())',
      fields: [
        { name: 'sharedState', value: 'Bound to active SparkContext after resumeDriver()' },
        { name: 'order', value: 'registerAM(:8030) -> createAllocator() -> resumeDriver() -> new SparkSession(sc)' },
      ],
      senderState: 'SparkContext active; user thread resumed',
      receiverState: `SparkSession returned to ${b.appEntryLabel}()`,
    });
    if (b.isJava) {
      b.addEvent({
        stepId: s3a,
        from: jvmApp,
        to: sc,
        kind: 'local',
        categoryOverride: 'driver-python-bridge',
        channel: 'internal',
        protocol: 'In-JVM JavaSparkContext Wrap',
        port: 'driver-1 in-JVM',
        label: 'JavaSparkContext.fromSC',
        info: 'AFTER AM registers on :8030, calls resumeDriver(), and new SparkSession(sc) returns, OrdersJavaJob.main() optionally wraps the SAME single SparkContext via JavaSparkContext.fromSparkContext(spark.sparkContext()) (NEVER before AM registration; zero duplicate SparkContext).',
        size: 'in-memory',
        command: 'JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())',
        fields: [{ name: 'singleContextRule', value: 'Wraps existing SparkContext AFTER AM registration, resumeDriver() & new SparkSession(sc)' }],
        senderState: 'Java RDD adapter wrapping active SparkContext',
        receiverState: 'Single SparkContext shared across SparkSession & JavaSparkContext',
      });
    }
  }
  b.addEvent({
    stepId: s3a,
    from: alloc,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: `allocate(${cfg.numExecutors} exec${cfg.numExecutors > 1 ? 's' : ''})`,
    info: `Requests ${cfg.numExecutors} container(s) (${cfg.executorCores} vcores) from RM :8030.`,
    size: '2.6 KB',
    command: `amClient.allocate(0.1f)`,
    fields: [{ name: 'numContainers', value: String(cfg.numExecutors) }],
    senderState: 'Awaiting grants',
    receiverState: `Allocated ${cfg.numExecutors} container(s)`,
  });
  b.addEvent({
    stepId: s3a,
    from: rm,
    to: alloc,
    channel: 'yarn',
    protocol: 'Hadoop RPC (AllocateResponse)',
    port: 'rm01:8030',
    label: `Grant ${cfg.numExecutors} container${cfg.numExecutors > 1 ? 's' : ''}`,
    info: 'RM returns container IDs, NM addresses, and tokens.',
    size: '3.2 KB',
    command: 'handleAllocatedContainers(response.getAllocatedContainers)',
    fields: [{ name: 'granted', value: String(cfg.numExecutors) }],
    senderState: 'Resources reserved',
    receiverState: 'Starting ExecutorRunnables',
  });

  const s3b = b.addStep({
    id: 'sim-step-alloc-b',
    parentId: sAlloc,
    title: isDiskDirFail
      ? `NMs launch ${cfg.numExecutors} container(s); Exec-1 aborts before BM registration`
      : `NMs launch ${cfg.numExecutors} executor(s), each registers BlockManager & starts 10s Heartbeats`,
    explanation: isDiskDirFail
      ? `ExecutorRunnable calls NM startContainer for each granted container; Exec-1 hits DiskBlockManager.createLocalDirs() failure (exit 53) BEFORE registering its Executor, BlockManager, or Heartbeats${cfg.numExecutors >= 2 ? `, while Exec-2..Exec-${cfg.numExecutors} register normally` : ''}.`
      : `ExecutorRunnable calls NM startContainer for each of the ${cfg.numExecutors} granted containers; each executor fetches SparkAppConfig, registers with CoarseGrainedSchedulerBackend, starts NettyBlockTransferService (:42001..:4200${cfg.numExecutors}), sends RegisterBlockManager, and starts 10s heartbeats to HeartbeatReceiver.`,
    category: 'heartbeats-metrics',
    keyState: isDiskDirFail
      ? `Exec-1 container launched on worker-a (exits 53 before BM reg)${cfg.numExecutors >= 2 ? `; ${cfg.numExecutors - 1} other exec(s) registered` : ''}`
      : `All ${cfg.numExecutors} executors & BlockManagers registered and heartbeating every 10s`,
    command: 'nmClient.startContainer() -> RetrieveSparkAppConfig -> RegisterExecutor -> RegisterBlockManager -> Heartbeat',
    detailsPassed: [
      { name: 'executorsLaunched', value: String(cfg.numExecutors) },
      { name: 'heartbeatInterval', value: '10s (spark.executor.heartbeatInterval)' },
      { name: 'networkTimeout', value: '120s (spark.network.timeout)' },
    ],
    deepDiveComponentId: 'heartbeat-receiver',
  });
  for (let i = 0; i < cfg.numExecutors; i++) {
    const { execNum, containerId } = initialAllocations[i];
    const nm = b.nmInstance(b.execHostZone(execNum));
    const host = b.execHostName(execNum);
    const dataPort = b.execDataPort(execNum);
    b.addEvent({
      stepId: s3b,
      from: alloc,
      to: nm,
      categoryOverride: 'yarn-lifecycle',
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: `${host}:dynamic`,
      label: `startContainer(Exec-${execNum})`,
      info: `ExecutorRunnable launches ${containerId} on ${host} with --driver-url spark://CoarseGrainedScheduler@${b.driverRpc}.`,
      size: '9.4 KB',
      command: `nmClient.startContainer(${containerId}, ctx[exec=${execNum}])`,
      fields: [
        { name: 'executorId', value: `"${execNum}"` },
        { name: 'containerId', value: containerId },
        { name: 'driverUrl', value: `spark://CoarseGrainedScheduler@${b.driverRpc}` },
      ],
      senderState: `Dispatched Exec-${execNum} to ${host} NM`,
      receiverState: `Booting Exec-${execNum} JVM`,
    });
    if (!(isDiskDirFail && i === 0)) {
      b.addEvent({
        stepId: s3b,
        from: execIds[i],
        to: schedBackend,
        categoryOverride: 'yarn-lifecycle',
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `RegisterExecutor(id=${execNum})`,
        info: `Exec-${execNum} fetches SparkAppConfig and registers ${cfg.executorCores} core(s) with ${b.driverRpc}.`,
        size: '1.8 KB',
        command: `RegisterExecutor("${execNum}", self, "${host}", ${cfg.executorCores})`,
        fields: [
          { name: 'executorId', value: `"${execNum}"` },
          { name: 'cores', value: String(cfg.executorCores) },
        ],
        senderState: `Exec-${execNum} received RegisteredExecutor`,
        receiverState: `Added Exec-${execNum} to executorDataMap`,
      });
      b.addEvent({
        stepId: s3b,
        from: execBms[i],
        to: bmm,
        categoryOverride: 'yarn-lifecycle',
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `RegisterBM(Exec-${execNum})`,
        info: `Exec-${execNum} BlockManager binds ${host}:${dataPort} and registers with driver BlockManagerMaster.`,
        size: '1.2 KB',
        command: `master.registerBlockManager(BlockManagerId("${execNum}", "${host}", ${dataPort}), ...)`,
        fields: [
          { name: 'dataPort', value: String(dataPort) },
          { name: 'externalShuffleService', value: cfg.externalShuffleService ? 'enabled (:7337)' : 'disabled' },
        ],
        senderState: `Listening on ${host}:${dataPort}`,
        receiverState: `BlockManager directory recorded Exec-${execNum}`,
      });
    }
  }
  const hbIdx = isDiskDirFail ? (cfg.numExecutors >= 2 ? 1 : -1) : 0;
  if (hbIdx >= 0) {
    const hbExecNum = initialAllocations[hbIdx].execNum;
    b.addEvent({
      stepId: s3b,
      from: execIds[hbIdx],
      to: hbRx,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (Heartbeat)',
      port: b.driverRpc,
      label: `Heartbeat(exec=${hbExecNum}, 10s)`,
      info: 'Executors send liveness + task accumulator metrics every 10s to driver HeartbeatReceiver.',
      size: '780 B',
      command: `heartbeatReceiverRef.askSync[HeartbeatResponse](Heartbeat("${hbExecNum}", accumUpdates, bmId))`,
      fields: [{ name: 'interval', value: '10s' }],
      senderState: 'Heartbeat timer active',
      receiverState: 'Updated executorLastSeen timestamps',
    });
  }

  // Track live AM, active endpoints, and live executors across faults and dynamic allocation
  let activeAm = am;
  let activeAlloc = alloc;
  let activeSchedBackend = schedBackend;
  let activeBmm = bmm;
  let activeHbRx = hbRx;
  let activeAttempt = 1;
  const liveExecs: string[] = [...execIds];
  const liveBms: string[] = [...execBms];
  const liveExecNums: number[] = initialAllocations.map(a => a.execNum);
  let nextExecNum = b.peekNextExecNum(1);

  // =========================================================================
  // FAULT: Cluster-Mode AM Crash -> Attempt 2 Restart (mid-run)
  // =========================================================================
  if (cfg.fault === 'am-crash-retry') {
    const amNm = b.amNmInstance();
    const am2 = b.ensureInstance('am-attempt-2', {
      componentId: 'application-master',
      label: 'ApplicationMaster (Attempt 2)',
      shortLabel: 'AM Attempt 2',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 (Attempt 2)',
      processId: 'AM JVM Attempt 2',
      containerId: 'container_e02_000001',
      attemptNumber: 2,
    });
    const alloc2 = b.ensureInstance('yarn-allocator-a2', {
      componentId: 'yarn-allocator',
      label: 'YarnAllocator (Attempt 2)',
      shortLabel: 'YarnAlloc A2',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 (Attempt 2)',
      processId: 'AM JVM Attempt 2',
      containerId: 'container_e02_000001',
      attemptNumber: 2,
    });
    const schedBackend2 = b.ensureInstance('driver-scheduler-backend-a2', {
      componentId: 'driver-scheduler-backend',
      label: 'SchedulerBackend (Attempt 2)',
      shortLabel: 'SchedBack A2',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 (Attempt 2)',
      processId: 'AM JVM Attempt 2',
      containerId: 'container_e02_000001',
      attemptNumber: 2,
    });
    const bmm2 = b.ensureInstance('block-manager-master-a2', {
      componentId: 'block-manager-master',
      label: 'BlockManagerMaster (Attempt 2)',
      shortLabel: 'BMMaster A2',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 (Attempt 2)',
      processId: 'AM JVM Attempt 2',
      containerId: 'container_e02_000001',
      attemptNumber: 2,
    });
    const hbRx2 = b.ensureInstance('heartbeat-receiver-a2', {
      componentId: 'heartbeat-receiver',
      label: 'HeartbeatReceiver (Attempt 2)',
      shortLabel: 'Heartbeat A2',
      hostZone: 'driver-jvm',
      hostLabel: 'driver-1 (Attempt 2)',
      processId: 'AM JVM Attempt 2',
      containerId: 'container_e02_000001',
      attemptNumber: 2,
    });
    const sCrash = b.addStep({
      id: 'sim-step-am-crash',
      title: `Cluster AM Attempt 1 SIGKILL (137); YARN spawns Attempt 2 rerunning ${b.appEntryLabel}`,
      explanation:
        `AM Attempt 1 container dies with SIGKILL (exit 137) before reporting SUCCEEDED. SIGKILL terminates the AM JVM immediately with ZERO JVM shutdown hooks; driver-1 NodeManager reports ContainerStatus(exitStatus=137) to RM on :8031. Because the SIGKILLed Attempt 1 AM JVM never ran its ShutdownHookManager cleanup hook (and YARN RM never deletes Spark user-owned .sparkStaging/<appId>), staged archives remain intact on HDFS while RM cleans up Attempt 1 executor containers and launches AM Attempt 2 (container_e02_000001) via driver-1 NodeManager (am-nm). The fresh driver reruns ${b.appEntryLabel} from ${b.isPySpark ? 'line 1' : 'its JVM main() entry point (zero Python processes)'}.`,
      category: 'yarn-lifecycle',
      keyState: 'Attempt 1 LOST (NM->RM :8031 exit 137) -> Attempt 2 launched via am-nm (fresh driver)',
      command: 'NM nodeHeartbeat(container_e01_000001, exitStatus=137) -> RM AMLauncher -> am-nm startContainers(container_e02_000001)',
      detailsPassed: [
        { name: 'failedAttempt', value: 'appattempt_1740_0051_000001 (SIGKILL 137; 0 JVM shutdown hooks)' },
        { name: 'newAttempt', value: 'appattempt_1740_0051_000002 (container_e02_000001 via am-nm)' },
        { name: 'reEntryPoint', value: b.appEntryLabel },
        {
          name: 'stagingDirIntact',
          value: '.sparkStaging/<appId> remains on HDFS because SIGKILLed Attempt 1 AM never ran its ShutdownHookManager cleanup hook',
        },
        { name: 'warning', value: 'Non-idempotent appends from Attempt 1 can duplicate output' },
      ],
      deepDiveComponentId: 'application-master',
    });
    b.addEvent({
      stepId: sCrash,
      from: amNm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker :8031)',
      port: 'rm01:8031',
      label: 'AM Cont #1 exit (137)',
      info: 'driver-1 NodeManager nodeHeartbeat reports container_e01_000001 COMPLETE (exitStatus=137, SIGKILL) on :8031. ZERO JVM shutdown hooks ran, so .sparkStaging/<appId> remains intact on HDFS (Attempt 1 AM never ran ShutdownHookManager cleanup, and RM retries Attempt 2 without deleting Spark staging dir) while RM cleans up Attempt 1 executor containers.',
      size: 'ContainerStatus',
      command: 'nodeHeartbeat -> ContainerStatus(container_e01_000001, COMPLETE, exitStatus=137)',
      fields: [
        { name: 'reporter', value: 'am-nm -> resource-manager (:8031)' },
        { name: 'shutdownHooksRan', value: 'false (SIGKILL bypasses ShutdownHookManager cleanup)' },
        { name: 'stagingDirState', value: '.sparkStaging/<appId> intact on HDFS (Attempt 1 AM cleanup hook never ran)' },
      ],
      senderState: 'Attempt 1 JVM killed abruptly; NM reaped container_e01_000001',
      receiverState: 'RM cleans Attempt 1 executors & schedules fresh Attempt 2 container',
      isError: true,
    });
    b.addEvent({
      stepId: sCrash,
      from: rm,
      to: amNm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: 'driver-1:dynamic',
      label: 'startContainers(AM A2)',
      info: 'RM AMLauncher sends ContainerManagementProtocol.startContainers(container_e02_000001) to driver-1 NodeManager (am-nm) for Attempt 2 under the SAME application ID.',
      size: '16.0 KB',
      command: 'AMLauncher.launch(appattempt_1740_0051_000002) -> cmProxy.startContainers(container_e02_000001)',
      fields: [
        { name: 'sameAppId', value: 'application_1740_0051' },
        { name: 'newAttemptId', value: 'appattempt_1740_0051_000002' },
        { name: 'containerId', value: 'container_e02_000001' },
      ],
      senderState: 'Dispatched Attempt 2 launch to driver-1 am-nm',
      receiverState: 'am-nm localizing intact .sparkStaging/ and launching container_e02_000001',
    });
    b.addEvent({
      stepId: sCrash,
      from: amNm,
      to: am2,
      kind: 'local',
      channel: 'yarn',
      protocol: 'ContainerExecutor + cgroups',
      port: 'driver-1 OS',
      label: 'Launch AM Attempt 2',
      info: `driver-1 NodeManager (am-nm) launches container_e02_000001 OS process on driver-1 and boots Attempt 2 ApplicationMaster JVM rerunning ${b.appEntryLabel} from ${b.isPySpark ? 'line 1' : 'entry point (zero Python processes)'}.`,
      size: 'OS exec',
      command: `ContainerExecutor.launchContainer(container_e02_000001) -> ApplicationMaster.main() -> ${b.appEntryLabel}`,
      fields: [
        { name: 'containerId', value: 'container_e02_000001' },
        { name: 'attemptNumber', value: '2' },
        { name: 'entryPoint', value: b.appEntryLabel },
      ],
      senderState: 'Monitoring AM Attempt 2 container_e02_000001',
      receiverState: `New AM Attempt 2 JVM starting ${b.appEntryLabel}`,
    });
    b.addEvent({
      stepId: sCrash,
      from: am2,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: 'Register AM Attempt 2',
      info: `Attempt 2 re-initializes SparkContext inside ${b.appEntryLabel}, registers with RM on :8030, instantiates Attempt-2 YarnAllocator, and acquires fresh executors.`,
      size: '1.6 KB',
      command: 'amClient.register("driver-1", 41000, conf, "http://driver-1:4040")',
      fields: [{ name: 'attempt', value: '2 (fresh SparkContext & YarnAllocator)' }],
      senderState: `Re-executing ${b.appEntryLabel} from entry point`,
      receiverState: 'RM marks Attempt 2 RUNNING',
    });
    activeAm = am2;
    activeAlloc = alloc2;
    activeSchedBackend = schedBackend2;
    activeBmm = bmm2;
    activeHbRx = hbRx2;
    activeAttempt = 2;
    b.resetForAttempt(2);

    // Allocate, launch, and register fresh Attempt-2 executors (container_e02_000002..)
    const a2Allocs = Array.from({ length: cfg.numExecutors }, () => b.allocateExec(2));
    const a2Execs = a2Allocs.map(a => b.execInstance(a.execNum, 2, a.containerId));
    const a2Bms = a2Allocs.map(a => b.execBmInstance(a.execNum, 2, a.containerId));
    liveExecs.splice(0, liveExecs.length, ...a2Execs);
    liveBms.splice(0, liveBms.length, ...a2Bms);
    liveExecNums.splice(0, liveExecNums.length, ...a2Allocs.map(a => a.execNum));
    nextExecNum = b.peekNextExecNum(2);

    const a2ContRange =
      cfg.numExecutors === 1
        ? a2Allocs[0].containerId
        : `${a2Allocs[0].containerId}..${a2Allocs[a2Allocs.length - 1].containerId}`;
    const sAm2Alloc = b.addStep({
      id: 'sim-step-am2-alloc',
      title: `AM Attempt 2 allocates & registers ${cfg.numExecutors} fresh executor(s) (${a2ContRange})`,
      explanation: `YARN retired all Attempt 1 executor containers. AM Attempt 2 YarnAllocator (${activeAlloc}) requests ${cfg.numExecutors} fresh Attempt-2 container(s) (${a2ContRange}) from RM :8030, NodeManagers boot ${a2Execs.join(', ')}, and each registers with Attempt 2 SchedulerBackend and BlockManagerMaster.`,
      category: 'yarn-lifecycle',
      keyState: `${cfg.numExecutors} Attempt-2 executor(s) registered (${a2Execs.join(', ')})`,
      command: `amClient.allocate(${cfg.numExecutors}x A2) -> nmClient.startContainer(${a2ContRange}) -> RegisterExecutor`,
      detailsPassed: [
        { name: 'attemptNumber', value: '2' },
        { name: 'containers', value: a2ContRange },
        { name: 'liveExecutors', value: a2Execs.join(', ') },
      ],
      deepDiveComponentId: 'yarn-allocator',
    });
    b.addEvent({
      stepId: sAm2Alloc,
      from: activeAlloc,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: `allocate(${cfg.numExecutors} A2 exec${cfg.numExecutors > 1 ? 's' : ''})`,
      info: `Attempt 2 YarnAllocator requests ${cfg.numExecutors} fresh container(s) (${a2ContRange}) from RM :8030.`,
      size: '2.6 KB',
      command: `amClient.allocate() // Attempt 2: ${a2ContRange}`,
      fields: [
        { name: 'attemptNumber', value: '2' },
        { name: 'containers', value: a2ContRange },
      ],
      senderState: 'Waiting for Attempt 2 container grants',
      receiverState: `RM grants ${cfg.numExecutors} Attempt 2 container(s)`,
    });
    for (let i = 0; i < cfg.numExecutors; i++) {
      const { execNum, containerId: cid } = a2Allocs[i];
      const nm = b.nmInstance(b.execHostZone(execNum));
      const host = b.execHostName(execNum);
      const dataPort = b.execDataPort(execNum);
      b.addEvent({
        stepId: sAm2Alloc,
        from: activeAlloc,
        to: nm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ContainerManagementProtocol)',
        port: `${host}:dynamic`,
        label: `startContainer(A2 E${execNum})`,
        info: `Attempt 2 ExecutorRunnable launches fresh ${cid} (${a2Execs[i]}) on ${host}.`,
        size: '9.4 KB',
        command: `nmClient.startContainer(${cid}, ctx[attempt=2, exec=${execNum}])`,
        fields: [
          { name: 'containerId', value: cid },
          { name: 'executorInstance', value: a2Execs[i] },
        ],
        senderState: `Dispatched ${a2Execs[i]} to ${host} NM`,
        receiverState: `Booting ${cid} JVM`,
      });
      b.addEvent({
        stepId: sAm2Alloc,
        from: a2Execs[i],
        to: activeSchedBackend,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `RegisterExec(a2-${execNum})`,
        info: `Fresh Attempt 2 executor ${a2Execs[i]} (${cid}) on ${host} registers ${cfg.executorCores} core(s) with Attempt 2 driver.`,
        size: '1.8 KB',
        command: `ref.ask[Boolean](RegisterExecutor("${execNum}", self, "${host}", ${cfg.executorCores})) // ${cid}`,
        fields: [
          { name: 'containerId', value: cid },
          { name: 'cores', value: String(cfg.executorCores) },
        ],
        senderState: `${a2Execs[i]} ready for Attempt 2 tasks`,
        receiverState: `Registered ${execNum}/${cfg.numExecutors} Attempt 2 executors`,
      });
      b.addEvent({
        stepId: sAm2Alloc,
        from: a2Bms[i],
        to: activeBmm,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `RegisterBM(a2-${execNum})`,
        info: `Attempt 2 BlockManager ${a2Bms[i]} (${cid}) binds ${host}:${dataPort} and registers with Attempt 2 BlockManagerMaster.`,
        size: '1.2 KB',
        command: `master.registerBlockManager(BlockManagerId("${execNum}", "${host}", ${dataPort}), ...) // ${cid}`,
        fields: [
          { name: 'containerId', value: cid },
          { name: 'dataPort', value: String(dataPort) },
        ],
        senderState: `Listening on ${host}:${dataPort}`,
        receiverState: `Recorded Attempt 2 BM ${a2Bms[i]}`,
      });
    }
  }

  // =========================================================================
  // FAULT: Client-Mode External Driver Disconnect (terminal)
  // =========================================================================
  if (cfg.fault === 'client-disconnect') {
    const yarnBackend = b.std('yarn-scheduler-backend');
    const sDisc = b.addStep({
      id: 'sim-step-client-disc',
      title: 'External driver on edge-01 dies; AM detects onDisconnected',
      explanation:
        `The submit-host driver disappears without sending Shutdown(code). Client-mode AMEndpoint.onDisconnected() fires on the client-outbound RPC channel: with default spark.yarn.am.clientModeTreatDisconnectAsFailed=false it reports SUCCEEDED (EXIT_SUCCESS=0); with =true it reports FAILED (EXIT_DISCONNECTED=17). YARN cannot recreate the external ${b.langDisplay} driver.`,
      category: 'yarn-lifecycle',
      keyState: 'Client driver lost | EXIT_DISCONNECTED=17 (if strict) or SUCCEEDED/0 (default)',
      command: 'AMEndpoint.onDisconnected(edge-01:41000) -> finish(FAILED, EXIT_DISCONNECTED=17)',
      detailsPassed: [
        { name: 'defaultConfig', value: 'clientModeTreatDisconnectAsFailed=false -> reports SUCCEEDED (EXIT_SUCCESS=0)' },
        { name: 'strictConfig', value: 'clientModeTreatDisconnectAsFailed=true -> reports FAILED (EXIT_DISCONNECTED=17)' },
        { name: 'yarnRecovery', value: 'None -- YARN cannot relaunch edge-01 driver' },
      ],
      deepDiveComponentId: 'application-master',
    });
    b.addEvent({
      stepId: sDisc,
      from: yarnBackend,
      to: activeAm,
      channel: 'error',
      protocol: 'Netty TCP Disconnect (AM outbound)',
      port: 'edge-01:41000 (AM outbound RPC)',
      label: 'Driver TCP disconnect',
      info: 'Submit host edge-01 dies abruptly without sending Shutdown(0) RPC to AMEndpoint over the AM-outbound RPC channel.',
      size: 'TCP RST/EOF',
      command: 'AMEndpoint.onDisconnected(RpcAddress("edge-01", 41000))',
      fields: [
        { name: 'shutdownReceived', value: 'false' },
        { name: 'driverAlive', value: 'false' },
      ],
      senderState: 'External driver process terminated on edge-01',
      receiverState: 'AM evaluating clientModeTreatDisconnectAsFailed',
      isError: true,
    });
    b.addEvent({
      stepId: sDisc,
      from: activeAm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: 'unregisterAM(DISCONN)',
      info: 'Registered client AM unregisters with RM (FAILED with EXIT_DISCONNECTED=17 when clientModeTreatDisconnectAsFailed=true; SUCCEEDED/0 under default false!) and YARN tears down executors.',
      size: '980 B',
      command: 'finish(FinalApplicationStatus.FAILED, EXIT_DISCONNECTED=17); amClient.unregister(...)',
      fields: [
        { name: 'ifTreatDisconnectAsFailed', value: 'FAILED (EXIT_DISCONNECTED=17)' },
        { name: 'ifDefaultFalse', value: 'SUCCEEDED (EXIT_SUCCESS=0; verify sink commit logs!)' },
      ],
      senderState: 'AM shutting down',
      receiverState: 'RM cleans up remaining executor containers',
      isError: true,
    });
    return b.finalize(runId, runName, runSubtitle, runSummary, 'CLIENT DISCONNECT', telemetry);
  }

  // =========================================================================
  // RUNTIME STATE + PHASE 4 (Executor Startup) & PHASE 5 (Pre-Workload Faults)
  // =========================================================================
  const targetPart = Math.min(cfg.numPartitions - 1, Math.floor(cfg.numPartitions / 2));
  const initialMapId = 100 + targetPart;
  const recomputedMapId = 200 + targetPart;
  const state: SimRuntimeState = {
    cfg,
    totalSlots,
    idealWavesPerStage,
    targetPart,
    initialMapId,
    recomputedMapId,
    activeAm,
    activeAttempt,
    amRegistered: b.isAmRegistered(activeAm),
    amNm: amNmInstanceId,
    liveExecs,
    liveBms,
    liveExecNums,
    nextExecNum,
    abortedByFault: false,
    submit,
    rm,
    alloc: activeAlloc,
    schedBackend: activeSchedBackend,
    bmm: activeBmm,
    hbRx: activeHbRx,
  };

  injectExecutorStartupFault(b, state);
  tryInjectPreWorkloadFault(b, state);

  // =========================================================================
  // WORKLOAD EXECUTION STEPS (ALL 8 JOB TYPES)
  // =========================================================================
  const producerExec = state.liveExecs[0];
  const producerBm = state.liveBms[0];
  const producerExecNum = b.instanceExecutorNumber(producerExec);
  const { hostZone: producerZone, hostName: producerHost } = b.instanceWorkerHost(producerExec);
  const producerBmPort = b.instanceHostLabel(producerBm);
  const isSpillOom = cfg.fault === 'task-spill-oom';
  const isTaskResultLost = cfg.fault === 'task-result-lost';
  const isMidExecTaskAbort =
    cfg.fault === 'task-exception' ||
    cfg.fault === 'scala-java-udf-exception' ||
    cfg.fault === 'delegation-token-expired';
  let taskResultLostHandled = false;
  const targetScale =
    cfg.dynamicAllocation && cfg.numPartitions > totalSlots
      ? Math.min(6, Math.ceil(cfg.numPartitions / cfg.executorCores))
      : cfg.numExecutors;
  const willScaleUp = cfg.dynamicAllocation && targetScale > cfg.numExecutors;
  let dynAllocEmitted = false;

  const getConsumerPair = () => {
    const workerBIdx = state.liveExecs.findIndex(
      id => b.instanceWorkerHost(id).hostZone === 'worker-node-b',
    );
    const idx = workerBIdx !== -1 ? workerBIdx : 0;
    const consumerExec = state.liveExecs[idx];
    const consumerBm = state.liveBms[idx];
    const { hostZone, hostName } = b.instanceWorkerHost(consumerExec);
    return { consumerExec, consumerBm, hostZone, hostName };
  };

  const emitDynamicAllocationScaleUp = () => {
    if (!willScaleUp || dynAllocEmitted) return;
    dynAllocEmitted = true;
    const eam = b.std('executor-allocation-manager');
    const yb = b.std('yarn-scheduler-backend');
    const rampRounds = computeDynamicAllocationRamp(cfg.numExecutors, targetScale);
    const rampPath = [cfg.numExecutors, ...rampRounds.map(r => r.nextTarget)].join(' -> ');
    const rampDetail =
      `${cfg.numExecutors}` +
      rampRounds
        .map(
          r =>
            ` -> ${r.nextTarget} (+${r.requestedDelta}${r.capped ? `, capped at max ${targetScale}` : ''})`,
        )
        .join('');
    const sDyn = b.addStep({
      id: 'sim-step-dynalloc',
      title: `Dynamic Allocation exponential ramp: ${rampPath}`,
      explanation: `${cfg.numPartitions} submitted tasks exceed ${totalSlots} initial slots (${cfg.numPartitions - totalSlots} pending in backlog). Spark's ExecutorAllocationManager ramps exponentially (+1, +2, +4, ...) across schedulerBacklogTimeout (1s) and sustainedSchedulerBacklogTimeout (1s): ${rampDetail}.`,
      category: 'yarn-lifecycle',
      keyState: `Exponential ramp ${rampDetail}; ${targetScale} live executors`,
      command: `ExecutorAllocationManager.addExecutors() // ${rampDetail}`,
      detailsPassed: [
        { name: 'initialTarget', value: String(cfg.numExecutors) },
        { name: 'rampSchedule', value: rampDetail },
        {
          name: 'triggers',
          value:
            rampRounds.length > 1
              ? 'schedulerBacklogTimeout (1s) + sustainedSchedulerBacklogTimeout (1s)'
              : 'schedulerBacklogTimeout (1s)',
        },
        { name: 'pendingTasks', value: String(cfg.numPartitions - totalSlots) },
      ],
      deepDiveComponentId: 'executor-allocation-manager',
    });

    for (const r of rampRounds) {
      const capSuffix = r.capped ? `, nominal +${r.nominalDelta} capped at max ${targetScale}` : '';
      b.addEvent({
        stepId: sDyn,
        from: eam,
        to: yb,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM AllocationClient',
        port: `${b.driverHost} in-JVM`,
        label: `requestTotalExecs(${r.nextTarget})`,
        info: `${r.trigger} (round ${r.round}): ExecutorAllocationManager ramps target ${r.prevTarget} -> ${r.nextTarget} (+${r.requestedDelta}${capSuffix}).`,
        size: 'in-memory',
        command: `client.requestTotalExecutors(Map(defaultRP -> ${r.nextTarget})) // ${r.trigger}, +${r.requestedDelta}`,
        fields: [
          { name: 'trigger', value: r.trigger },
          { name: 'oldTarget', value: String(r.prevTarget) },
          { name: 'newTarget', value: String(r.nextTarget) },
          { name: 'increment', value: `+${r.requestedDelta}${capSuffix}` },
        ],
        senderState: `Exponential numExecutorsToAdd=${r.nominalDelta} -> next ${r.nominalDelta * 2}`,
        receiverState: 'Proxying RequestExecutors to active AM',
      });
      const amLink = b.driverToAmLink();
      b.addEvent({
        stepId: sDyn,
        from: yb,
        to: state.activeAm,
        kind: amLink.kind,
        channel: 'rpc',
        protocol: amLink.protocol,
        port: amLink.port,
        label: `RequestExecutors(${r.nextTarget})`,
        info: `YarnSchedulerBackend sends RequestExecutors(${r.nextTarget}) (+${r.requestedDelta}) to active AM on ${r.trigger}.`,
        size: '620 B',
        command: `amEndpoint.askSync[Boolean](RequestExecutors(${r.nextTarget}))`,
        fields: [
          { name: 'trigger', value: r.trigger },
          { name: 'deltaContainers', value: `+${r.requestedDelta}` },
          { name: 'newTarget', value: String(r.nextTarget) },
        ],
        senderState: `Waiting for +${r.requestedDelta} extra executor(s)`,
        receiverState: `Updating YarnAllocator targetNumExecutors=${r.nextTarget}`,
      });
      b.addEvent({
        stepId: sDyn,
        from: state.alloc,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
        port: 'rm01:8030',
        label: `Allocate +${r.requestedDelta} (tot=${r.nextTarget})`,
        info: `AM YarnAllocator requests +${r.requestedDelta} container(s) from RM :8030 (target=${r.nextTarget}, round ${r.round}/${rampRounds.length}).`,
        size: '2.2 KB',
        command: `amClient.allocate() // +${r.requestedDelta}x ResourceRequest(${cfg.executorCores} vcores) -> total=${r.nextTarget}`,
        fields: [
          { name: 'deltaContainers', value: `+${r.requestedDelta}` },
          { name: 'totalTarget', value: String(r.nextTarget) },
        ],
        senderState: 'Waiting for RM grants',
        receiverState: `RM grants +${r.requestedDelta} container(s)`,
      });
      for (let k = 0; k < r.requestedDelta; k++) {
        const { execNum: n, containerId: cid } = b.allocateExec(state.activeAttempt);
        const newExec = b.execInstance(n, state.activeAttempt, cid);
        const newBm = b.execBmInstance(n, state.activeAttempt, cid);
        const nm = b.nmInstance(b.execHostZone(n));
        const host = b.execHostName(n);
        b.addEvent({
          stepId: sDyn,
          from: state.alloc,
          to: nm,
          channel: 'yarn',
          protocol: 'Hadoop RPC (ContainerManagementProtocol)',
          port: `${host}:dynamic`,
          label: state.activeAttempt > 1 ? `startContainer(A${state.activeAttempt} E${n})` : `startContainer(Exec-${n})`,
          info: `ExecutorRunnable launches ramped ${cid} (${newExec}) on ${host}.`,
          size: '9.4 KB',
          command: `nmClient.startContainer(${cid}, ctx[exec=${n}])`,
          fields: [
            { name: 'executorId', value: `"${n}"` },
            { name: 'containerId', value: cid },
          ],
          senderState: `Dispatched ramped ${newExec}`,
          receiverState: `Booting ${cid} JVM`,
        });
        b.addEvent({
          stepId: sDyn,
          from: newExec,
          to: state.schedBackend,
          channel: 'rpc',
          protocol: 'Spark Netty RPC',
          port: b.driverRpc,
          label: state.activeAttempt > 1 ? `RegisterExec(a${state.activeAttempt}-${n})` : `RegisterExecutor(id=${n})`,
          info: `Ramped ${newExec} (${cid}) on ${host} registers ${cfg.executorCores} core(s) with driver and joins live executor pool.`,
          size: '1.8 KB',
          command: `ref.ask[Boolean](RegisterExecutor("${n}", self, "${host}", ${cfg.executorCores}))`,
          fields: [
            { name: 'executorId', value: `"${n}"` },
            { name: 'containerId', value: cid },
            { name: 'cores', value: String(cfg.executorCores) },
          ],
          senderState: `${newExec} ready for pending backlog tasks`,
          receiverState: `Live executors now ${state.liveExecs.length + 1}`,
        });
        state.liveExecs.push(newExec);
        state.liveBms.push(newBm);
        state.liveExecNums.push(n);
        state.nextExecNum = b.peekNextExecNum(state.activeAttempt);
      }
    }
  };

  const emitTaskExceptionStep = (stageLabel: string, failingExec: string) => {
    state.abortedByFault = true;
    const tsm = b.std('task-set-manager');
    const dag = b.std('dag-scheduler');
    const excName = b.isPySpark
      ? 'Python/SQL ValueError'
      : b.isScala
        ? 'Scala/SQL IllegalArgumentException'
        : 'Java/SQL IllegalArgumentException';
    const sTaskFail = b.addStep({
      id: 'sim-step-task-exception',
      title: `Partition ${targetPart} fails 4 deterministic attempts -> TaskSetManager aborts job`,
      explanation: `During ${stageLabel}, a deterministic exception on a malformed record in partition ${targetPart} (valid in 0..${cfg.numPartitions - 1}) fails attempts 0, 1, 2, and 3 across executors. Because each counts toward numFailures(${targetPart}), hitting spark.task.maxFailures (4) causes TaskSetManager to abort the TaskSet and fail the job before stage completion.`,
      category: 'sql-planning-aqe',
      keyState: `ABORTED: Task ${targetPart} failed 4 times (spark.task.maxFailures=4)`,
      command: `TaskSetManager.handleFailedTask() // numFailures(${targetPart}) >= 4 -> abort()`,
      detailsPassed: [
        { name: 'partitionIndex', value: `${targetPart} (valid in 0..${cfg.numPartitions - 1})` },
        { name: 'spark.task.maxFailures', value: '4 (4 total attempts, not 4 retries after 1st)' },
      ],
      deepDiveComponentId: 'task-set-manager',
    });
    b.addEvent({
      stepId: sTaskFail,
      from: failingExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: `Task p=${targetPart} FAIL (4/4)`,
      info: `Attempt 3 for partition ${targetPart} raises deterministic ${excName}; numFailures(${targetPart}) reaches 4.`,
      size: '2.4 KB stack',
      command: `StatusUpdate(taskId=${40 + targetPart}, FAILED, ExceptionFailure("${excName}: bad record"))`,
      fields: [
        { name: 'partitionIndex', value: String(targetPart) },
        { name: 'numFailures', value: '4 / 4 (exhausted)' },
        { name: 'retryAcrossHosts', value: 'Tried multiple executors; same bad row fails every time' },
      ],
      senderState: 'Attempt 3 failed before stage completion',
      receiverState: 'TaskSetManager calls abort() -> DAGScheduler fails ActiveJob',
      isError: true,
    });
    b.addEvent({
      stepId: sTaskFail,
      from: tsm,
      to: dag,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM TaskSetFailed',
      port: `${b.driverHost} in-JVM`,
      label: 'abortStage -> JobFailed',
      info: `TaskSetManager aborts the TaskSet and DAGScheduler fails the active job back to ${b.appEntryLabel}.`,
      size: 'in-memory',
      command: `dagScheduler.taskSetFailed(taskSet, "Task ${targetPart} failed 4 times")`,
      fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException)' }],
      senderState: 'TaskSet marked zombie',
      receiverState: 'ActiveJob failed',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: `Job aborted due to stage failure: Task ${targetPart} failed 4 times (spark.task.maxFailures=4)`,
      stopLiveExecutors: true,
    });
  };

  const emitMidExecTaskAbort = (stageLabel: string, failingExec: string) => {
    if (cfg.fault === 'task-exception') {
      emitTaskExceptionStep(stageLabel, failingExec);
    } else if (cfg.fault === 'scala-java-udf-exception') {
      injectScalaJavaUdfExceptionStep(b, state, stageLabel, failingExec);
    } else if (cfg.fault === 'delegation-token-expired') {
      injectDelegationTokenExpiredStep(b, state, stageLabel, failingExec);
    }
  };

  if (!state.abortedByFault && cfg.fault === 'kryo-unregistered-class') {
    const sc = b.std('spark-context');
    const dag = b.std('dag-scheduler');
    const sKryoSubmit = b.addStep({
      id: 'sim-step-kryo-rdd-submit',
      title: `Submit JVM Pair-RDD shuffle (rdd.reduceByKey / ShuffledRDD[K, OrderEvent]): ${cfg.numPartitions} map tasks`,
      explanation: `${b.isPySpark ? 'Alongside the PySpark job, the driver invokes an auxiliary JVM-side Pair-RDD helper executing rdd.reduceByKey (ShuffledRDD[K, OrderEvent])' : `${b.appEntryLabel}() executes a JVM Pair-RDD shuffle (rdd.reduceByKey / ShuffledRDD[K, OrderEvent])`} with spark.serializer=org.apache.spark.serializer.KryoSerializer and spark.kryo.registrationRequired=true. SQL ShuffleExchangeExec (ShuffleExchangeExec.scala:219-265) explicitly uses UnsafeRowSerializer regardless of spark.serializer, and task closures always use JavaSerializer; this Pair-RDD ShuffleDependency uses KryoSerializer for com.acme.OrderEvent values.`,
      category: 'sql-planning-aqe',
      keyState: `Pair-RDD ShuffleMapStage 0 submitted (${cfg.numPartitions} map tasks; KryoSerializer on ShuffledRDD[K, OrderEvent])`,
      command: 'rdd.reduceByKey(_ + _) -> ShuffledRDD[K, OrderEvent] -> DAGScheduler.submitStage(ShuffleMapStage 0)',
      detailsPassed: [
        { name: 'rddPath', value: 'JVM Pair-RDD shuffle: rdd.reduceByKey / ShuffledRDD[K, OrderEvent]' },
        { name: 'sqlShuffleDistinction', value: 'SQL ShuffleExchangeExec (ShuffleExchangeExec.scala:219-265) uses UnsafeRowSerializer, NEVER KryoSerializer' },
        { name: 'closureSerializer', value: 'SparkEnv.closureSerializer (always JavaSerializer)' },
        ...(b.isPySpark
          ? [{ name: 'pysparkPath', value: 'Auxiliary JVM-side Pair-RDD helper executed alongside the PySpark job' }]
          : []),
      ],
      deepDiveComponentId: 'serializer-manager',
    });
    b.addEvent({
      stepId: sKryoSubmit,
      from: sc,
      to: dag,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Pair-RDD Shuffle',
      port: `${b.driverHost} in-JVM`,
      label: 'Submit ShuffledRDD (Kryo)',
      info: `${b.isPySpark ? 'Auxiliary JVM Pair-RDD helper submits' : 'Driver submits'} ShuffleMapStage 0 for rdd.reduceByKey / ShuffledRDD[K, OrderEvent] using KryoSerializer (SQL ShuffleExchangeExec uses UnsafeRowSerializer, not Kryo).`,
      size: 'in-memory',
      command: 'DAGScheduler.submitStage(ShuffleMapStage 0[ShuffledRDD[K, OrderEvent]])',
      fields: [
        { name: 'serializer', value: 'KryoSerializer (registrationRequired=true)' },
        { name: 'sqlShuffleNote', value: 'SQL ShuffleExchangeExec explicitly uses UnsafeRowSerializer' },
      ],
      senderState: 'Pair-RDD ShuffleDependency created',
      receiverState: 'Submitting ShuffleMapStage 0 tasks',
    });
    b.addEvent({
      stepId: sKryoSubmit,
      from: state.schedBackend,
      to: producerExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: `LaunchTask (${cfg.numPartitions} maps)`,
      info: `Driver dispatches ShuffleMapTask wave for ShuffledRDD[K, OrderEvent] (task closures serialized cleanly via JavaSerializer).`,
      size: '2.2 KB',
      command: `executorEndpoint.send(LaunchTask(ShuffleMapTask[ShuffledRDD[K, OrderEvent]]))`,
      fields: [
        { name: 'partitions', value: String(cfg.numPartitions) },
        { name: 'closureSerializer', value: 'JavaSerializer (unaffected by registrationRequired)' },
      ],
      senderState: 'Pair-RDD ShuffleMapTasks dispatched',
      receiverState: 'Serializing com.acme.OrderEvent via Executor SerializerManager',
    });
    if (willScaleUp) {
      emitDynamicAllocationScaleUp();
    }
    injectKryoUnregisteredClassStep(
      b,
      state,
      'JVM Pair-RDD ShuffleMapStage (rdd.reduceByKey / ShuffledRDD[K, OrderEvent])',
      producerExec,
    );
  } else if (!state.abortedByFault && cfg.jobType === 'narrow-filter') {
    const sNarrow = b.addStep({
      id: 'sim-step-narrow',
      title: `Narrow Filter Stage: Catalyst + WholeStageCodegen *(1) across ${cfg.numExecutors} exec(s) (0 shuffles)`,
      explanation: `Catalyst Analyzer & Optimizer push status='PAID' down to Parquet scan; WholeStageCodegenExec fuses Scan -> Filter -> Project into stage *(1). DAGScheduler creates ONE ResultStage (0 shuffles) with ${cfg.numPartitions} tasks (~${idealWavesPerStage} ideal wave(s) across ${totalSlots} initial slots), compiled via Janino into Tungsten UnsafeRow loops on executors.`,
      category: 'sql-planning-aqe',
      keyState: isMidExecTaskAbort
        ? `ResultStage 0 submitted (${cfg.numPartitions} tasks); executor encounters mid-stage failure`
        : `1 ResultStage | WholeStageCodegen *(1) | 0 ShuffleDependencies | ${cfg.numPartitions} tasks`,
      command: job.actionSink,
      detailsPassed: [
        { name: 'stages', value: '1 (ResultStage 0 only, fused *(1))' },
        { name: 'shuffles', value: '0 (narrow pipeline)' },
        { name: 'codegen', value: 'WholeStageCodegen -> Janino -> Tungsten UnsafeRow' },
        { name: 'idealWaves', value: `${idealWavesPerStage} (${cfg.numPartitions} tasks / ${totalSlots} slots)` },
      ],
      deepDiveComponentId: 'whole-stage-codegen',
    });
    b.emitSqlCatalystCodegenPlan(sNarrow, {
      hasShuffle: false,
      dagLabel: 'Plan 1-stage narrow DAG',
      dagInfo: 'Catalyst pushes down status="PAID" and WholeStageCodegen fuses *(1) with no Exchange; DAGScheduler creates a single ResultStage.',
      dagCommand: 'DAGScheduler.submitJob(ResultStage 0, partitions = 0..' + (cfg.numPartitions - 1) + ')',
      dagFields: [
        { name: 'shuffleCount', value: '0' },
        { name: 'partitions', value: String(cfg.numPartitions) },
      ],
      dagSenderState: 'Single-stage fused codegen physical plan built',
      dagReceiverState: 'Submitting TaskSet 0.0 directly (no map stage)',
    });
    b.addEvent({
      stepId: sNarrow,
      from: state.schedBackend,
      to: producerExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: `LaunchTask (${cfg.numPartitions} parts)`,
      info: `Driver dispatches narrow filter+write tasks across ${state.liveExecs.join(', ')} (${Math.min(cfg.numPartitions, totalSlots)} running, ${Math.max(0, cfg.numPartitions - totalSlots)} pending).`,
      size: '2.2 KB',
      command: 'executorEndpoint.send(LaunchTask(serializedTask))',
      fields: [
        { name: 'slots', value: String(totalSlots) },
        { name: 'idealWaves', value: String(idealWavesPerStage) },
      ],
      senderState: `Running up to ${Math.min(cfg.numPartitions, totalSlots)} concurrent tasks`,
      receiverState: 'Compiling *(1) via Janino & filtering Tungsten UnsafeRows',
    });
    const narrowJanino = b.execLocalComponentInstance(producerExec, 'janino-code-generator', 'Janino');
    const narrowTungsten = b.execLocalComponentInstance(producerExec, 'tungsten-unsafe-row', 'UnsafeRow');
    b.addEvent({
      stepId: sNarrow,
      from: producerExec,
      to: narrowJanino,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'Janino CodeGenerator',
      port: `${producerHost} in-JVM`,
      label: 'Compile *(1) filter loop',
      info: `${producerExec} compiles/hits CodeGenerator.cache (Janino) for WholeStageCodegenExec *(1) Scan -> Filter -> Project.`,
      size: 'in-memory',
      command: 'CodeGenerator.compile(stage1FilterCode) -> GeneratedIterator',
      fields: [{ name: 'compiler', value: 'Janino SimpleCompiler (cached)' }],
      senderState: 'Running compiled BufferedRowIterator on executor JVM',
      receiverState: 'Compiled *(1) bytecode ready',
    });
    b.addEvent({
      stepId: sNarrow,
      from: narrowJanino,
      to: narrowTungsten,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'Tungsten UnsafeRow',
      port: `${producerHost} in-JVM`,
      label: 'Filter binary UnsafeRows',
      info: 'Compiled *(1) loop evaluates status="PAID" directly on 8-byte aligned Tungsten UnsafeRow memory (0 object boxing).',
      size: 'in-memory',
      command: 'GeneratedIterator.processNext() // UnsafeRow zero-copy filter',
      fields: [{ name: 'memoryPool', value: 'TaskMemoryManager binary pages' }],
      senderState: 'Evaluating fused filter predicate',
      receiverState: 'Streaming filtered UnsafeRows to Parquet writer',
    });
    if (willScaleUp) {
      emitDynamicAllocationScaleUp();
    }
    if (isSpillOom) {
      injectTaskSpillOomStep(b, state);
    }
    if (isMidExecTaskAbort) {
      emitMidExecTaskAbort('ResultStage 0', producerExec);
    } else if (willScaleUp || isSpillOom) {
      const dn = b.std('hdfs-datanode');
      const sNarrowRun = b.addStep({
        id: 'sim-step-narrow-run',
        title: isTaskResultLost
          ? `Live executors (${state.liveExecs.length}) stage Parquet parts & emit IndirectTaskResult`
          : `All ${state.liveExecs.length} live executors finish scanning & writing staged Parquet parts`,
        explanation: isTaskResultLost
          ? `With ${state.liveExecs.length} live executor(s), narrow tasks scan Parquet blocks from HDFS DataNode, write staged output parts, and send IndirectTaskResult to driver while ResultStage 0 remains pending final result fetch.`
          : `With ${state.liveExecs.length} live executor(s), remaining narrow tasks finish scanning Parquet blocks from HDFS DataNode and write staged output parts.`,
        category: 'sql-planning-aqe',
        keyState: isTaskResultLost
          ? `ResultStage 0 pending IndirectTaskResult fetch (${state.liveExecs.length} live executors)`
          : `ResultStage 0 completed across ${state.liveExecs.length} live executors`,
        command: 'FileSourceScanExec -> FilterExec -> SingleDirectoryDataWriter.write()',
        detailsPassed: [
          { name: 'liveExecutors', value: String(state.liveExecs.length) },
          { name: 'shuffleBytes', value: '0 B (no shuffle!)' },
        ],
        deepDiveComponentId: 'hdfs-datanode',
      });
      b.addEvent({
        stepId: sNarrowRun,
        from: state.liveExecs[state.liveExecs.length - 1],
        to: dn,
        channel: 'data',
        protocol: 'HDFS DataTransferProtocol',
        port: 'dn01:9866',
        label: 'Scan & write Parquet',
        info: 'Executors read Parquet blocks from HDFS DataNode, filter status="PAID" in-JVM, and write staged output parts.',
        size: `${cfg.numPartitions * 48} MiB read`,
        command: 'FileSourceScanExec -> FilterExec -> SingleDirectoryDataWriter.write()',
        fields: [
          { name: 'shuffleBytes', value: '0 B (no shuffle!)' },
          { name: 'pythonWorkers', value: '0 (built-in SQL runs in JVM)' },
        ],
        senderState: isTaskResultLost
          ? 'Task staged Parquet part; sending IndirectTaskResult to driver'
          : 'All narrow tasks finished',
        receiverState: 'Staged Parquet parts flushed to DataNodes',
      });
      if (isTaskResultLost) {
        taskResultLostHandled = true;
        injectTaskResultLostStep(b, state, {
          inCollectStep: false,
          stageLabel: 'ResultStage 0 (narrow filter)',
        });
      }
    } else {
      const dn = b.std('hdfs-datanode');
      b.addEvent({
        stepId: sNarrow,
        from: producerExec,
        to: dn,
        channel: 'data',
        protocol: 'HDFS DataTransferProtocol',
        port: 'dn01:9866',
        label: 'Scan & write Parquet',
        info: 'Executors read Parquet blocks from HDFS DataNode, filter status="PAID" in-JVM, and write staged output parts.',
        size: `${cfg.numPartitions * 48} MiB read`,
        command: 'FileSourceScanExec -> FilterExec -> SingleDirectoryDataWriter.write()',
        fields: [
          { name: 'shuffleBytes', value: '0 B (no shuffle!)' },
          { name: 'pythonWorkers', value: '0 (built-in SQL runs in JVM)' },
        ],
        senderState: isTaskResultLost
          ? 'Task staged Parquet part; sending IndirectTaskResult to driver'
          : 'All narrow tasks finished',
        receiverState: 'Staged Parquet parts flushed to DataNodes',
      });
      if (isTaskResultLost) {
        taskResultLostHandled = true;
        injectTaskResultLostStep(b, state, {
          inCollectStep: false,
          stageLabel: 'ResultStage 0 (narrow filter)',
        });
      }
    }
  } else if (!state.abortedByFault && (cfg.jobType === 'wide-shuffle' || cfg.jobType === 'sort-merge-skew')) {
    const isSkewJoin = cfg.jobType === 'sort-merge-skew';
    const isSkewedProfile = cfg.dataProfile === 'skewed';
    const aqeSplits = isSkewJoin && cfg.aqeEnabled && isSkewedProfile;
    const dag = b.std('dag-scheduler');

    if (willScaleUp || isMidExecTaskAbort || isSpillOom) {
      const sMapSubmit = b.addStep({
        id: 'sim-step-map-submit',
        title: isSkewJoin
          ? `Catalyst + WholeStageCodegen -> ShuffleMapStage 0 (${cfg.numPartitions} maps; ${isSkewedProfile ? '640 MiB hot p0' : 'uniform 48 MiB'})`
          : `Catalyst + WholeStageCodegen -> ShuffleMapStage 0: ${cfg.numPartitions} map tasks across ${cfg.numExecutors} exec(s)`,
        explanation: `Catalyst Analyzer & Optimizer push predicates down, SQL Planner inserts ShuffleExchangeExec, WholeStageCodegenExec fuses *(1) map & *(2) reduce stages, and DAGScheduler submits ShuffleMapStage 0 (${cfg.numPartitions} tasks; ${Math.min(cfg.numPartitions, totalSlots)} running on initial slots, ${Math.max(0, cfg.numPartitions - totalSlots)} queued in pending backlog).`,
        category: 'sql-planning-aqe',
        keyState: `Catalyst + WholeStageCodegen *(1,2) -> ShuffleMapStage 0 submitted (${cfg.numPartitions} map tasks)`,
        command: 'Analyzer -> Optimizer -> ShuffleExchangeExec -> WholeStageCodegenExec -> DAGScheduler.submitStage(0)',
        detailsPassed: [
          { name: 'partitions', value: String(cfg.numPartitions) },
          { name: 'initialSlots', value: String(totalSlots) },
          { name: 'codegen', value: 'WholeStageCodegen *(1) map & *(2) reduce' },
        ],
        deepDiveComponentId: 'whole-stage-codegen',
      });
      b.emitSqlCatalystCodegenPlan(sMapSubmit, {
        hasShuffle: true,
        isSkewJoin,
        dagLabel: isSkewJoin ? 'Plan SMJ (2 shuffles)' : 'Plan Exchange (2 stages)',
        dagInfo: isSkewJoin
          ? 'Catalyst + WholeStageCodegen plan SortMergeJoinExec with 2 ShuffleExchange boundaries.'
          : 'Catalyst + WholeStageCodegen fuse Partial/Final HashAggregate around ShuffleExchangeExec.',
        dagCommand: 'WholeStageCodegenExec.doCodeGen() -> DAGScheduler.submitJob()',
        dagFields: [{ name: 'aqeEnabled', value: String(cfg.aqeEnabled) }],
        dagSenderState: 'Fused WholeStageCodegen physical plan ready',
        dagReceiverState: 'Submitting ShuffleMapStage 0',
      });
      b.addEvent({
        stepId: sMapSubmit,
        from: state.schedBackend,
        to: producerExec,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `LaunchTask (${cfg.numPartitions} maps)`,
        info: `Driver launches initial wave of ShuffleMapStage 0 tasks (${Math.max(0, cfg.numPartitions - totalSlots)} pending in backlog).`,
        size: '2.2 KB',
        command: `executorEndpoint.send(LaunchTask(ShuffleMapTask[0..${cfg.numPartitions - 1}]))`,
        fields: [
          { name: 'partitions', value: String(cfg.numPartitions) },
          { name: 'initialSlots', value: String(totalSlots) },
        ],
        senderState: 'Initial map wave dispatched',
        receiverState: 'Compiling *(1) via Janino & running ShuffleMapTasks on worker-a',
      });
      if (willScaleUp) {
        emitDynamicAllocationScaleUp();
      }
      if (isSpillOom) {
        injectTaskSpillOomStep(b, state);
      }
    }

    if (isMidExecTaskAbort) {
      emitMidExecTaskAbort('ShuffleMapStage 0', producerExec);
    } else {
      const motm = b.std('map-output-tracker-master');
      const shuffleWriter = b.workerStd('shuffle-writer', producerZone);
      const resolver = b.workerStd('shuffle-block-resolver', producerZone);
      const shuffleServerPort = cfg.externalShuffleService ? `${producerHost}:7337` : producerBmPort;

      const sPlanMap = b.addStep({
        id: 'sim-step-map',
        title: isSkewJoin
          ? `Map Stages compile *(1) via Janino & write shuffle outputs (${isSkewedProfile ? '640 MiB hot p0' : 'uniform 48 MiB'})`
          : `ShuffleMapStage 0: Janino *(1) Partial HashAgg writes .data/.index & reports MapStatus`,
        explanation: `Catalyst Analyzer -> Optimizer -> ShuffleExchange -> WholeStageCodegen *(1) drive ShuffleMapStage 0 across ${state.liveExecs.length} live executor(s). Executors compile *(1) via Janino over Tungsten UnsafeRows (${isSkewJoin ? 'key-partitioned join sides' : 'Partial HashAggregate'}), commit consolidated .data/.index via IndexShuffleBlockResolver (mapIndex=${targetPart}, mapId=${initialMapId}), and return MapStatus to DAGScheduler -> MapOutputTrackerMaster.`,
        category: 'sql-planning-aqe',
        keyState: `Janino *(1) + Map stage complete | serving via ${cfg.externalShuffleService ? `ESS ${producerHost}:7337` : `${producerExec} BM ${producerBmPort}`}`,
        command: 'WholeStageCodegen -> Janino -> Tungsten UnsafeRow -> BypassMergeSortShuffleWriter.write()',
        detailsPassed: [
          { name: 'codegen', value: 'WholeStageCodegen *(1) -> Janino -> Tungsten UnsafeRow' },
          { name: 'shuffleWriter', value: 'BypassMergeSortShuffleWriter (default <= 200 parts)' },
          { name: 'servingEndpoint', value: shuffleServerPort },
          { name: 'mapIndexVsMapId', value: `logical mapIndex=${targetPart}, physical mapId=${initialMapId}` },
          { name: 'dataProfile', value: isSkewedProfile ? 'Partition 0 = 640 MiB (median 48 MiB)' : 'Uniform ~48 MiB/part' },
        ],
        deepDiveComponentId: 'shuffle-writer',
        isMacro: true,
      });
      if (!willScaleUp && !isSpillOom) {
        b.emitSqlCatalystCodegenPlan(sPlanMap, {
          hasShuffle: true,
          isSkewJoin,
          dagLabel: isSkewJoin ? 'Plan SMJ (2 shuffles)' : 'Plan Exchange (2 stages)',
          dagInfo: isSkewJoin
            ? 'Catalyst + WholeStageCodegen plan SortMergeJoinExec with 2 ShuffleExchange boundaries.'
            : 'Catalyst + WholeStageCodegen fuse Partial/Final HashAggregate around ShuffleExchangeExec.',
          dagCommand: 'WholeStageCodegenExec.doCodeGen() -> DAGScheduler.submitJob()',
          dagFields: [{ name: 'aqeEnabled', value: String(cfg.aqeEnabled) }],
          dagSenderState: 'Fused WholeStageCodegen physical plan ready',
          dagReceiverState: 'Submitting ShuffleMapStage 0',
        });
      }
      const mapJanino = b.execLocalComponentInstance(producerExec, 'janino-code-generator', 'Janino');
      const mapTungsten = b.execLocalComponentInstance(producerExec, 'tungsten-unsafe-row', 'UnsafeRow');
      b.addEvent({
        stepId: sPlanMap,
        from: producerExec,
        to: mapJanino,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Janino CodeGenerator',
        port: `${producerHost} in-JVM`,
        label: 'Compile *(1) map loop',
        info: `${producerExec} compiles/hits Janino CodeGenerator.cache for WholeStageCodegenExec *(1) map pipeline.`,
        size: 'in-memory',
        command: 'CodeGenerator.compile(stage1CodeAndComment) -> GeneratedIterator',
        fields: [{ name: 'compiler', value: 'Janino SimpleCompiler (Guava cache)' }],
        senderState: 'Executing fused *(1) map stage',
        receiverState: 'Compiled BufferedRowIterator bytecode active',
      });
      b.addEvent({
        stepId: sPlanMap,
        from: mapJanino,
        to: mapTungsten,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Tungsten UnsafeRow',
        port: `${producerHost} in-JVM`,
        label: isSkewJoin ? 'Project binary join keys' : 'Partial HashAgg UnsafeRow',
        info: isSkewJoin
          ? 'Compiled *(1) projects join keys into compact Tungsten UnsafeRow buffers for UnsafeRowSerializer shuffle.'
          : 'Compiled *(1) accumulates partial region counts in Tungsten UnsafeFixedWidthAggregationMap before shuffle write.',
        size: 'in-memory',
        command: isSkewJoin
          ? 'UnsafeProjection -> UnsafeRowSerializer'
          : 'HashAggregateExec(partial_count) -> UnsafeFixedWidthAggregationMap',
        fields: [{ name: 'format', value: '8-byte aligned binary UnsafeRow' }],
        senderState: 'Processed map partition in zero-boxing binary loop',
        receiverState: 'Feeding UnsafeRows into BypassMergeSortShuffleWriter',
      });
      if (!isSkewJoin) {
        const mapHashAgg = b.execLocalComponentInstance(producerExec, 'hash-aggregate', 'HashAgg');
        b.addEvent({
          stepId: sPlanMap,
          from: mapTungsten,
          to: mapHashAgg,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: 'Two-Level HashAggregate',
          port: `${producerHost} in-JVM`,
          label: 'Partial HashAgg (map)',
          info: 'HashAggregateExec pre-combines rows per region in a two-level Tungsten hash map before writing shuffle blocks.',
          size: 'in-memory',
          command: 'HashAggregateExec(partial_count(1)) -> TungstenAggregationIterator',
          fields: [{ name: 'aggPhase', value: 'Map-side Partial HashAggregate' }],
          senderState: 'Fed filtered UnsafeRows',
          receiverState: 'Emitting compact partial (region, count) rows to ShuffleWriter',
        });
      }
      b.addEvent({
        stepId: sPlanMap,
        from: shuffleWriter,
        to: resolver,
        kind: 'local',
        categoryOverride: 'shuffle-internals',
        channel: 'data',
        protocol: 'Local Disk Atomic Rename',
        port: 'worker-a local-dir',
        label: 'Commit .data & .index',
        info: `Map task (mapIndex=${targetPart}, mapId=${initialMapId}) commits consolidated shuffle_0_${initialMapId}_0.data and .index on worker-a local disk.`,
        size: isSkewedProfile ? '640 MiB hot' : '18.4 MiB',
        command: `IndexShuffleBlockResolver.writeMetadataFileAndCommit(0, ${initialMapId}, lengths, tmp)`,
        fields: [
          { name: 'files', value: `shuffle_0_${initialMapId}_0.data + .index` },
          { name: 'mapIndex', value: String(targetPart) },
        ],
        senderState: 'Map task wrote local shuffle files',
        receiverState: `Files readable via ${shuffleServerPort}`,
      });
      b.addEvent({
        stepId: sPlanMap,
        from: shuffleWriter,
        to: dag,
        categoryOverride: 'shuffle-internals',
        channel: 'rpc',
        protocol: 'Spark Netty RPC (MapStatus)',
        port: b.driverRpc,
        label: 'StatusUpdate(MapStatus)',
        info: `Map task returns MapStatus(${shuffleServerPort}, mapId=${initialMapId}) for mapIndex=${targetPart} to DAGScheduler -> MapOutputTrackerMaster.`,
        size: '640 B',
        command: `DAGScheduler.handleTaskCompletion -> mapOutputTracker.registerMapOutput(0, ${targetPart}, status[mapId=${initialMapId}])`,
        fields: [
          { name: 'shuffleServerId', value: shuffleServerPort },
          { name: 'mapIndex_vs_mapId', value: `mapIndex=${targetPart}, mapId=${initialMapId}` },
        ],
        senderState: 'Map task succeeded',
        receiverState: 'MapStatus recorded; ready for reduce stage',
      });

      // Expandable map-stage internals: (1) Catalyst + WholeStageCodegen + Janino + Tungsten, (2) Shuffle Commit + MapStatus
      const sMapCodegenSub = b.addStep({
        id: 'sim-step-map-codegen',
        parentId: sPlanMap,
        title: 'Catalyst Analyzer -> Optimizer -> WholeStageCodegen *(1) -> Janino -> Tungsten UnsafeRow',
        explanation: `Inside ${b.driverHost} JVM, Catalyst Analyzer resolves schemas, Catalyst Optimizer pushes filters down, ShuffleExchangeExec creates ShuffleDependency(0), and WholeStageCodegenExec.doCodeGen() generates Java stage *(1). On ${producerHost}, Janino compiles *(1) and evaluates rows in binary Tungsten UnsafeRow${isSkewJoin ? ' key projections' : ' + two-level HashAggregate'}.`,
        category: 'sql-planning-aqe',
        keyState: 'Stage *(1) Catalyst + WholeStageCodegen + Janino + Tungsten complete',
        command: 'Analyzer -> Optimizer -> ShuffleExchange -> WholeStageCodegen -> Janino -> UnsafeRow',
        detailsPassed: [
          { name: 'driverCodegen', value: 'WholeStageCodegenExec.doCodeGen() *(1)' },
          { name: 'executorCompile', value: 'Janino CodeGenerator -> Tungsten UnsafeRow' },
        ],
        deepDiveComponentId: 'whole-stage-codegen',
      });
      b.emitSqlCatalystCodegenPlan(sMapCodegenSub, {
        hasShuffle: true,
        isSkewJoin,
        dagLabel: isSkewJoin ? 'Plan SMJ (2 shuffles)' : 'Plan Exchange (2 stages)',
        dagInfo: isSkewJoin
          ? 'Catalyst + WholeStageCodegen plan SortMergeJoinExec with 2 ShuffleExchange boundaries.'
          : 'Catalyst + WholeStageCodegen fuse Partial/Final HashAggregate around ShuffleExchangeExec.',
        dagCommand: 'WholeStageCodegenExec.doCodeGen() -> DAGScheduler.submitJob()',
        dagFields: [{ name: 'aqeEnabled', value: String(cfg.aqeEnabled) }],
        dagSenderState: 'Fused WholeStageCodegen physical plan ready',
        dagReceiverState: 'Submitting ShuffleMapStage 0',
      });
      b.addEvent({
        stepId: sMapCodegenSub,
        from: producerExec,
        to: mapJanino,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Janino CodeGenerator',
        port: `${producerHost} in-JVM`,
        label: 'Compile *(1) map loop',
        info: `${producerExec} compiles/hits Janino CodeGenerator.cache for WholeStageCodegenExec *(1) map pipeline.`,
        size: 'in-memory',
        command: 'CodeGenerator.compile(stage1CodeAndComment) -> GeneratedIterator',
        fields: [{ name: 'compiler', value: 'Janino SimpleCompiler (Guava cache)' }],
        senderState: 'Executing fused *(1) map stage',
        receiverState: 'Compiled BufferedRowIterator bytecode active',
      });
      b.addEvent({
        stepId: sMapCodegenSub,
        from: mapJanino,
        to: mapTungsten,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Tungsten UnsafeRow',
        port: `${producerHost} in-JVM`,
        label: isSkewJoin ? 'Project binary join keys' : 'Partial HashAgg UnsafeRow',
        info: isSkewJoin
          ? 'Compiled *(1) projects join keys into compact Tungsten UnsafeRow buffers for UnsafeRowSerializer shuffle.'
          : 'Compiled *(1) accumulates partial region counts in Tungsten UnsafeFixedWidthAggregationMap before shuffle write.',
        size: 'in-memory',
        command: isSkewJoin
          ? 'UnsafeProjection -> UnsafeRowSerializer'
          : 'HashAggregateExec(partial_count) -> UnsafeFixedWidthAggregationMap',
        fields: [{ name: 'format', value: '8-byte aligned binary UnsafeRow' }],
        senderState: 'Processed map partition in zero-boxing binary loop',
        receiverState: 'Feeding UnsafeRows into BypassMergeSortShuffleWriter',
      });
      if (!isSkewJoin) {
        const mapHashAgg = b.execLocalComponentInstance(producerExec, 'hash-aggregate', 'HashAgg');
        b.addEvent({
          stepId: sMapCodegenSub,
          from: mapTungsten,
          to: mapHashAgg,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: 'Two-Level HashAggregate',
          port: `${producerHost} in-JVM`,
          label: 'Partial HashAgg (map)',
          info: 'HashAggregateExec pre-combines rows per region in a two-level Tungsten hash map before writing shuffle blocks.',
          size: 'in-memory',
          command: 'HashAggregateExec(partial_count(1)) -> TungstenAggregationIterator',
          fields: [{ name: 'aggPhase', value: 'Map-side Partial HashAggregate' }],
          senderState: 'Fed filtered UnsafeRows',
          receiverState: 'Emitting compact partial (region, count) rows to ShuffleWriter',
        });
      }

      const sMapSub = b.addStep({
        id: 'sim-step-map-sub',
        parentId: sPlanMap,
        title: 'BypassMergeSortShuffleWriter commits .data/.index; DAGScheduler registers MapStatus',
        explanation: `For an exchange without map-side combine/ordering (<=200 parts), BypassMergeSortShuffleWriter concatenates temp partition files into shuffle_0_${initialMapId}_0.data via IndexShuffleBlockResolver; DAGScheduler registers MapStatus for logical mapIndex=${targetPart} into MapOutputTrackerMaster.`,
        category: 'shuffle-internals',
        keyState: 'MapOutputTrackerMaster epoch=1 populated',
        command: 'BypassMergeSortShuffleWriter -> IndexShuffleBlockResolver -> mapOutputTracker.registerMapOutput()',
        detailsPassed: [{ name: 'servingAddr', value: shuffleServerPort }],
        deepDiveComponentId: 'map-output-tracker-master',
      });
      b.addEvent({
        stepId: sMapSub,
        from: shuffleWriter,
        to: resolver,
        kind: 'local',
        channel: 'data',
        protocol: 'Local Disk Commit',
        port: 'worker-a local-dir',
        label: 'BypassMerge -> .data/.idx',
        info: `Concatenates per-reducer temp files into shuffle_0_${initialMapId}_0.data and writes 8-byte offset .index.`,
        size: '18.4 MiB',
        command: `resolver.writeMetadataFileAndCommit(0, ${initialMapId}, partitionLengths, checksums, tmp)`,
        fields: [{ name: 'writer', value: 'BypassMergeSortShuffleWriter' }],
        senderState: 'Temp partition files cleaned',
        receiverState: 'Final .data & .index committed',
      });
      b.addEvent({
        stepId: sMapSub,
        from: dag,
        to: motm,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Tracker Register',
        port: `${b.driverHost} in-JVM`,
        label: `registerMapOutput(0,${targetPart})`,
        info: `DAGScheduler registers MapStatus(mapId=${initialMapId}) for mapIndex=${targetPart} in MapOutputTrackerMaster and marks ShuffleMapStage 0 complete.`,
        size: 'in-memory',
        command: `mapOutputTracker.registerMapOutput(0, ${targetPart}, status[mapId=${initialMapId}])`,
        fields: [{ name: 'servingId', value: shuffleServerPort }],
        senderState: 'Stage 0 available',
        receiverState: 'Epoch=1 ready for reducer queries',
      });

      // AQE Skew Split step ONLY when aqeEnabled === true
      if (isSkewJoin && cfg.aqeEnabled) {
        const aqe = b.std('adaptive-query-execution');
        const sAqe = b.addStep({
          id: 'sim-step-aqe',
          title: aqeSplits
            ? 'AQE OptimizeSkewedJoin splits 640 MiB partition into PartialReducerPartitionSpec'
            : 'AQE checks shuffle stats: uniform 48 MiB -> no skew split triggered',
          explanation: aqeSplits
            ? 'After shuffle stages materialize, AQE observes partition 0 = 640 MiB which exceeds BOTH 5 x 48 MiB median (240 MiB) AND 256 MiB advisory threshold. OptimizeSkewedJoin splits mapper ranges into 3 PartialReducerPartitionSpecs and replicates the matching right-side partition.'
            : 'AQE inspects MapOutputStatistics: max partition 48 MiB does not exceed both 5x median and 256 MiB, so OptimizeSkewedJoin leaves partition specs unchanged.',
          category: 'sql-planning-aqe',
          keyState: aqeSplits ? 'Skewed partition 0 split into 3 PartialReducerPartitionSpecs' : 'No skew rewrite applied',
          command: aqeSplits
            ? 'OptimizeSkewedJoin.tryOptimizeJoinChildren() -> PartialReducerPartitionSpec(0, startMap, endMap)'
            : 'AdaptiveSparkPlanExec // no skew split',
          detailsPassed: [
            { name: 'aqeEnabled', value: 'true' },
            { name: 'thresholdRule', value: '> 5x median AND > 256 MiB (defaults)' },
            { name: 'specClass', value: aqeSplits ? 'PartialReducerPartitionSpec' : 'Standard Coalesced/Identity' },
          ],
          deepDiveComponentId: 'adaptive-query-execution',
        });
        b.addEvent({
          stepId: sAqe,
          from: motm,
          to: aqe,
          kind: 'local',
          channel: 'internal',
          protocol: 'In-JVM MapOutputStatistics',
          port: `${b.driverHost} in-JVM`,
          label: aqeSplits ? 'Split -> PartialReducer' : 'Eval MapOutputStats',
          info: aqeSplits
            ? 'AQE reads materialized sizes (640 MiB > 240 MiB & 256 MiB) and rewrites SortMergeJoinExec with PartialReducerPartitionSpec.'
            : 'AQE evaluates runtime shuffle sizes (48 MiB uniform); join keeps original partition layout.',
          size: 'in-memory',
          command: aqeSplits ? 'OptimizeSkewedJoin.apply(plan)' : 'SparkPlan (unsplit)',
          fields: [
            { name: 'partition0Bytes', value: isSkewedProfile ? '640 MiB' : '48 MiB' },
            { name: 'splitCount', value: aqeSplits ? '3 sub-partitions' : '0' },
          ],
          senderState: 'Supplied MapOutputStatistics',
          receiverState: aqeSplits ? 'Re-planned join with skew=true' : 'Kept original join plan',
        });
      }

      // Reduce Fetch Step (or Shuffle/DAG-Shuffle fault!)
      const reduceConsumer = getConsumerPair();
      const isLocalShuffle = state.liveExecs.length === 1;
      const shuffleReader = b.workerStd('shuffle-reader', reduceConsumer.hostZone);
      const motw = b.workerStd('map-output-tracker-worker', reduceConsumer.hostZone);
      const aqeOffNote =
        isSkewJoin && !cfg.aqeEnabled
          ? isSkewedProfile
            ? ' AQE is OFF, so the 640 MiB skewed partition 0 runs unsplit on 1 straggler reduce task.'
            : ' AQE is OFF, so uniform 48 MiB partitions run without runtime re-optimization.'
          : '';

      const handledReduceFault = tryInjectShuffleReduceFault(b, state, {
        aqeOffNote,
        isLocalShuffle,
        shuffleServerPort,
        producerExec,
        producerExecNum,
        producerBm,
        producerZone,
        producerHost,
        reduceConsumer,
      });

      if (handledReduceFault) {
        // Handled by shuffle/DAG fault injector
      } else if (isLocalShuffle) {
        const sFetch = b.addStep({
          id: 'sim-step-reduce-fetch',
          title: 'Reduce stage reads local shuffle blocks directly from worker-a disk',
          explanation: `With 1 live executor (${producerExec}, id=${producerExecNum} on ${producerHost}), all map outputs are local to ${producerHost}. MapOutputTrackerWorker fetches compressed MapOutputStatuses from driver on cache miss, then ShuffleBlockFetcherIterator reads local .data/.index slices directly via IndexShuffleBlockResolver on ${producerHost} (zero remote shuffle network hops).${aqeOffNote}`,
          category: 'shuffle-internals',
          keyState: `Local shuffle read on ${producerHost} (.data/.index); 0 remote bytes${isSkewJoin && !cfg.aqeEnabled ? (isSkewedProfile ? ' | AQE OFF (640 MiB straggler)' : ' | AQE OFF (48 MiB uniform)') : ''}`,
          command: `ShuffleBlockFetcherIterator.fetchLocalBlocks(shuffle_0_${initialMapId}_${targetPart})`,
          detailsPassed: [
            { name: 'readMode', value: `Local disk read on ${producerHost} (single live executor id=${producerExecNum})` },
            { name: 'blockId', value: `shuffle_0_${initialMapId}_${targetPart}` },
            { name: 'partitionSize', value: isSkewedProfile ? '640 MiB hot partition 0' : '48 MiB uniform' },
          ],
          deepDiveComponentId: 'shuffle-reader',
        });
        b.addEvent({
          stepId: sFetch,
          from: motw,
          to: motm,
          channel: 'rpc',
          protocol: 'Spark Netty RPC',
          port: b.driverRpc,
          label: 'GetMapOutputStatuses(0)',
          info: 'Executor tracker cache miss fetches shuffle 0 locations & sizes from driver MapOutputTrackerMaster.',
          size: '1.9 KB',
          command: 'askTracker[Array[Byte]](GetMapOutputStatuses(0))',
          fields: [{ name: 'shuffleId', value: '0' }],
          senderState: `Cached shuffle 0 statuses locally (all on ${producerHost})`,
          receiverState: 'Returned serialized MapStatus array',
        });
        b.addEvent({
          stepId: sFetch,
          from: shuffleReader,
          to: resolver,
          kind: 'local',
          channel: 'data',
          protocol: 'Local Disk Shuffle Read',
          port: `${producerHost} local-dir`,
          label: 'Read local .data/.index',
          info: `ShuffleBlockFetcherIterator reads shuffle_0_${initialMapId}_${targetPart} directly from local .data/.index files on ${producerHost} (no remote network socket).`,
          size: isSkewedProfile ? '640 MiB local' : '4.6 MiB local',
          command: `IndexShuffleBlockResolver.getBlockData(ShuffleBlockId(0, ${initialMapId}, ${targetPart}))`,
          fields: [
            { name: 'transferType', value: `local (${producerHost} disk; 0 remote bytes)` },
            { name: 'blockId', value: `shuffle_0_${initialMapId}_${targetPart}` },
          ],
          senderState: 'Read local shuffle bytes via FileSegmentManagedBuffer',
          receiverState: isTaskResultLost
            ? `Reducer computed partition on ${producerHost}; sending IndirectTaskResult to driver`
            : `Final HashAggregate / SortMergeJoin complete on ${producerHost}`,
        });
        if (isTaskResultLost) {
          taskResultLostHandled = true;
          injectTaskResultLostStep(b, state, {
            inCollectStep: false,
            stageLabel: 'ResultStage 1 (reduce)',
          });
        }
      } else {
        const essNode = cfg.externalShuffleService ? b.essInstance(producerZone) : null;
        const shuffleServer = essNode ?? producerBm;
        const liveMapPort = b.execDataPort(producerExecNum);
        const sFetch = b.addStep({
          id: 'sim-step-reduce-fetch',
          title: `Reduce stage fetches shuffle blocks from ${cfg.externalShuffleService ? 'ESS (:7337)' : `Exec-${producerExecNum} BM (:${liveMapPort})`}`,
          explanation: `MapOutputTrackerWorker fetches compressed MapOutputStatuses from driver on cache miss, then ShuffleReader (OneForOneBlockFetcher) streams remote shuffle bytes directly from ${shuffleServerPort} (live producer executor "${producerExecNum}") to ${reduceConsumer.hostName} (never relayed through the driver).${aqeOffNote}`,
          category: 'shuffle-internals',
          keyState: `Remote shuffle bytes streamed directly from ${shuffleServerPort} -> ${reduceConsumer.hostName}`,
          command: `OneForOneBlockFetcher.start(FetchShuffleBlocks("${shuffleServerPort}", execId="${producerExecNum}", shuffleId=0))`,
          detailsPassed: [
            { name: 'servingEndpoint', value: shuffleServerPort },
            { name: 'mapProducerExecId', value: `"${producerExecNum}" (${producerExec})` },
            { name: 'protocol', value: 'FetchShuffleBlocks -> StreamHandle -> ChunkFetchRequest' },
            { name: 'maxSizeInFlight', value: '48 MiB (spark.reducer.maxSizeInFlight)' },
          ],
          deepDiveComponentId: 'shuffle-reader',
        });
        b.addEvent({
          stepId: sFetch,
          from: motw,
          to: motm,
          channel: 'rpc',
          protocol: 'Spark Netty RPC',
          port: b.driverRpc,
          label: 'GetMapOutputStatuses(0)',
          info: 'Executor tracker cache miss fetches shuffle 0 locations & sizes from driver MapOutputTrackerMaster.',
          size: '1.9 KB',
          command: 'askTracker[Array[Byte]](GetMapOutputStatuses(0))',
          fields: [{ name: 'shuffleId', value: '0' }],
          senderState: 'Cached shuffle 0 statuses locally',
          receiverState: 'Returned serialized MapStatus array',
        });
        b.addEvent({
          stepId: sFetch,
          from: shuffleReader,
          to: shuffleServer,
          channel: 'data',
          protocol: cfg.externalShuffleService ? 'BlockTransfer (ESS :7337)' : `BlockTransfer (Netty :${liveMapPort})`,
          port: shuffleServerPort,
          label: 'FetchShuffleBlocks(0)',
          info: `OneForOneBlockFetcher requests shuffle_0_${initialMapId}_${targetPart} directly from ${shuffleServerPort} (live executor "${producerExecNum}").`,
          size: '420 B req',
          command: `new FetchShuffleBlocks(appId, "${producerExecNum}", 0, [${initialMapId}], [[${targetPart}]], false)`,
          fields: [
            { name: 'target', value: shuffleServerPort },
            { name: 'producerExecId', value: `"${producerExecNum}"` },
          ],
          senderState: 'Awaiting StreamHandle & chunks',
          receiverState: 'Reading slice via IndexShuffleBlockResolver',
        });
        b.addEvent({
          stepId: sFetch,
          from: shuffleServer,
          to: shuffleReader,
          channel: 'data',
          protocol: 'BlockTransfer (ChunkFetchSuccess)',
          port: `${shuffleServerPort} -> ${reduceConsumer.hostName}`,
          label: 'Stream LZ4 shuffle bytes',
          info: `Streams compressed shuffle bytes directly to ${reduceConsumer.hostName} ShuffleReader without touching the driver.`,
          size: isSkewedProfile && !cfg.aqeEnabled ? '640 MiB LZ4' : '4.6 MiB LZ4',
          command: 'ChunkFetchSuccess(streamId, chunkIndex=0, FileSegmentManagedBuffer)',
          fields: [{ name: 'driverRelay', value: 'none (direct peer transfer)' }],
          senderState: 'Closed chunk stream',
          receiverState: isTaskResultLost
            ? 'Reducer computed partition; sending IndirectTaskResult to driver'
            : 'Feeding shuffle stream into stage *(2) Final operator',
        });
        const reduceOpId = isSkewJoin ? 'sort-merge-join' : 'hash-aggregate';
        const reduceOp = b.execLocalComponentInstance(
          reduceConsumer.consumerExec,
          reduceOpId,
          isSkewJoin ? 'SortMergeJoin' : 'Final HashAgg',
        );
        b.addEvent({
          stepId: sFetch,
          from: reduceConsumer.consumerExec,
          to: reduceOp,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: isSkewJoin ? 'WholeStageCodegen SMJ *(2)' : 'WholeStageCodegen Agg *(2)',
          port: `${reduceConsumer.hostName} in-JVM`,
          label: isSkewJoin ? 'Run SortMergeJoin *(2)' : 'Run Final HashAgg *(2)',
          info: isSkewJoin
            ? `Reducer on ${reduceConsumer.hostName} executes compiled WholeStageCodegen *(2) SortExec + SortMergeJoinExec over sorted UnsafeRow streams.`
            : `Reducer on ${reduceConsumer.hostName} executes compiled WholeStageCodegen *(2) Final HashAggregateExec over fetched partial UnsafeRows.`,
          size: 'in-memory',
          command: isSkewJoin
            ? 'SortExec -> SortMergeJoinExec.doExecute() // compiled *(2)'
            : 'HashAggregateExec(final count(1)) // compiled *(2)',
          fields: [
            { name: 'operator', value: isSkewJoin ? 'SortMergeJoinExec' : 'Final HashAggregateExec' },
            { name: 'codegenStage', value: '*(2) Janino BufferedRowIterator' },
          ],
          senderState: 'Completed compiled *(2) reduce stage',
          receiverState: 'Final UnsafeRows ready for Parquet write',
        });
        if (isTaskResultLost) {
          taskResultLostHandled = true;
          injectTaskResultLostStep(b, state, {
            inCollectStep: false,
            stageLabel: 'ResultStage 1 (reduce)',
          });
        }
      }
    }
  } else if (!state.abortedByFault && cfg.jobType === 'broadcast-join') {
    if (tryInjectBroadcastFault(b, state) !== 'handled-abort') {
      const bcastEx = b.std('broadcast-exchange');
      const bcastMgr = b.std('broadcast-manager');
      const driverBm = b.driverBmInstance();
      const isTorrentCorrupt = cfg.fault === 'torrent-piece-corrupt';

      const sBcast = b.addStep({
        id: 'sim-step-bcast',
        title: `Catalyst + WholeStageCodegen -> BroadcastExchange + TorrentBroadcast (${cfg.numExecutors} execs)`,
        explanation: `Catalyst Analyzer & Optimizer select BroadcastHashJoinExec and WholeStageCodegenExec fuses the probe loop. Driver collects small dimension table (10.8 MiB) into a HashedRelation; TorrentBroadcast stores ALL 3 serialized pieces (4 + 4 + 2.8 MiB; 4 MiB blockSize) on Driver BM; executors fetch in randomized order and advertise immediately (${cfg.numExecutors >= 2 ? 'enabling peer-to-peer E1 -> E2 fetches' : 'direct from Driver BM with 1 executor, no E-to-E hop'}). Expand for TorrentBroadcast internals.`,
        category: 'broadcast-torrent',
        keyState: `WholeStageCodegen BHJ + Broadcast HashedRelation ready on ${cfg.numExecutors} exec(s); 0 large shuffle`,
        command: 'Catalyst -> WholeStageCodegen -> BroadcastExchangeExec -> TorrentBroadcast.writeBlocks() -> BroadcastHashJoinExec',
        detailsPassed: [
          { name: 'pieceSizes', value: '4 + 4 + 2.8 MiB (10.8 MiB total; 4 MiB blockSize)' },
          { name: 'driverStores', value: 'ALL 3 serialized pieces (broadcast_0_piece0..2)' },
          { name: 'codegen', value: 'WholeStageCodegen -> Janino BHJ probe -> Tungsten UnsafeRow' },
          { name: 'p2pHops', value: cfg.numExecutors >= 2 ? 'Enabled (illustrative E1 -> E2)' : 'None (1 executor)' },
        ],
        deepDiveComponentId: 'broadcast-manager',
        isMacro: true,
      });
      const bcastWscg = b.std('whole-stage-codegen');
      b.addEvent({
        stepId: sBcast,
        from: b.std('sql-planner'),
        to: bcastWscg,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'JoinSelection + Codegen',
        port: `${b.driverHost} in-JVM`,
        label: 'Plan BHJ + Codegen',
        info: 'JoinSelection picks BroadcastHashJoinExec (build <= 10 MiB threshold) and WholeStageCodegenExec fuses the streamed probe loop.',
        size: 'in-memory',
        command: 'JoinSelection -> BroadcastHashJoinExec + WholeStageCodegenExec.doCodeGen()',
        fields: [{ name: 'joinStrategy', value: 'BroadcastHashJoinExec (0 large-table shuffle)' }],
        senderState: 'Selected BroadcastHashJoinExec',
        receiverState: 'Fused probe stage; triggering BroadcastExchangeExec build',
      });
      b.addEvent({
        stepId: sBcast,
        from: bcastEx,
        to: driverBm,
        kind: 'local',
        channel: 'data',
        protocol: 'In-JVM TorrentBroadcast',
        port: `${b.driverHost} in-JVM`,
        label: 'Publish 3 pieces (10.8M)',
        info: 'Driver builds HashedRelation and stores ALL 3 serialized pieces (4 + 4 + 2.8 MiB) in Driver BM.',
        size: '10.8 MiB (4+4+2.8)',
        command: 'TorrentBroadcast.writeBlocks(hashedRelation)',
        fields: [
          { name: 'pieces', value: 'broadcast_0_piece0..2 (4 + 4 + 2.8 MiB)' },
          { name: 'driverNote', value: 'Driver retains serialized pieces & broadcast handle' },
        ],
        senderState: 'Broadcast handle ready for tasks',
        receiverState: 'Driver BM serving pieces 0..2',
      });
      b.addEvent({
        stepId: sBcast,
        from: driverBm,
        to: producerBm,
        channel: 'data',
        protocol: 'BlockTransfer - TorrentBroadcast',
        port: `${b.driverBmPort} -> ${producerHost}`,
        label: 'Fetch pieces (Driver->E1)',
        info: `${producerBm} visits piece IDs in randomized order, fetches 4 + 4 + 2.8 MiB from Driver BM, and immediately advertises ${producerBmPort}.`,
        size: '10.8 MiB',
        command: 'TorrentBroadcast.readBlocks() -> exec1BM.getRemoteBytes()',
        fields: [{ name: 'fetchOrder', value: 'Randomized per executor' }],
        senderState: `Seeded pieces to ${producerExec}`,
        receiverState: `${producerExec} cached pieces & registered as peer seed`,
        peerTag: 'Driver -> E1',
      });
      if (state.liveBms.length >= 2) {
        const peerBm = state.liveBms[1];
        const peerExec = state.liveExecs[1];
        b.addEvent({
          stepId: sBcast,
          from: producerBm,
          to: peerBm,
          channel: 'data',
          protocol: 'BlockTransfer - TorrentBroadcast P2P',
          port: `${producerBmPort} -> worker-b`,
          label: 'P2P piece0 (E1->E2)',
          info: `Illustrative seeded peer fetch: ${peerExec} fetches broadcast_0_piece0 (4.0 MiB) from locality-ordered peer ${producerBm}, offloading the driver.`,
          size: '4.0 MiB',
          command: `exec2BM.getRemoteBytes(broadcast_0_piece0) // peer ${producerBmPort}`,
          fields: [
            { name: 'peer', value: producerBmPort },
            { name: 'note', value: 'Illustrative P2P hop; remaining 4 + 2.8 MiB pieces fetched from peers or driver' },
          ],
          senderState: `Served cached piece0 to ${peerExec}`,
          receiverState: `${peerExec} reassembled HashedRelation`,
          peerTag: 'E1 -> E2',
        });
      }

      if (!willScaleUp && !isMidExecTaskAbort && !isTorrentCorrupt && !isSpillOom) {
        const probeConsumer = getConsumerPair();
        const bhj = b.workerStd('broadcast-hash-join', probeConsumer.hostZone);
        b.addEvent({
          stepId: sBcast,
          from: probeConsumer.consumerBm,
          to: bhj,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: 'In-JVM HashedRelation Probe',
          port: `${probeConsumer.hostName} in-JVM`,
          label: 'Probe streamed fact rows',
          info: `BroadcastHashJoinExec on ${probeConsumer.hostName} probes read-only HashedRelation per streamed row inside WholeStageCodegen *(2) with zero large-table shuffle.`,
          size: 'in-memory',
          command: 'BroadcastHashJoinExec.doExecute() -> hashedRelation.asReadOnlyCopy()',
          fields: [
            { name: 'probeHost', value: probeConsumer.hostName },
            { name: 'largeSideShuffle', value: '0 B (avoided!)' },
          ],
          senderState: 'Deserialized HashedRelation shared across executor threads',
          receiverState: 'Joined UnsafeRows streaming via compiled WholeStageCodegen loop',
        });
        const bhjJanino = b.execLocalComponentInstance(
          probeConsumer.consumerExec,
          'janino-code-generator',
          'Janino',
        );
        const bhjTungsten = b.execLocalComponentInstance(
          probeConsumer.consumerExec,
          'tungsten-unsafe-row',
          'UnsafeRow',
        );
        b.addEvent({
          stepId: sBcast,
          from: probeConsumer.consumerExec,
          to: bhjJanino,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: 'WholeStageCodegen BHJ',
          port: `${probeConsumer.hostName} in-JVM`,
          label: 'Compile BHJ probe loop',
          info: `Janino compiles WholeStageCodegenExec fusing Scan -> BroadcastHashJoinExec probe -> Project over Tungsten UnsafeRows on ${probeConsumer.hostName}.`,
          size: 'in-memory',
          command: 'CodeGenerator.compile(bhjProbeCode) -> GeneratedIterator',
          fields: [{ name: 'compiler', value: 'Janino SimpleCompiler' }],
          senderState: 'Probing HashedRelation in fused loop',
          receiverState: 'Emitting joined Tungsten UnsafeRows',
        });
        b.addEvent({
          stepId: sBcast,
          from: bhjJanino,
          to: bhjTungsten,
          kind: 'local',
          categoryOverride: 'sql-planning-aqe',
          channel: 'internal',
          protocol: 'Tungsten UnsafeRow',
          port: `${probeConsumer.hostName} in-JVM`,
          label: 'Emit joined UnsafeRows',
          info: 'Joined fact + dimension columns are written directly into binary Tungsten UnsafeRow buffers without JVM object boxing.',
          size: 'in-memory',
          command: 'GenerateUnsafeRowJoiner -> UnsafeRow',
          fields: [{ name: 'format', value: 'Tungsten binary UnsafeRow' }],
          senderState: 'Joined row buffer populated',
          receiverState: 'Streaming to FileFormatWriter',
        });
      }

      // Expandable TorrentBroadcast detail sub-steps
      const sTb1 = b.addStep({
        id: 'sim-step-bcast-1',
        parentId: sBcast,
        title: 'TorrentBroadcast chunks 10.8 MiB (4 + 4 + 2.8 MiB) & updates BlockManagerMaster',
        explanation: 'Driver TorrentBroadcast splits 10.8 MiB using 4 MiB blockSize into 3 serialized pieces (4 + 4 + 2.8 MiB), writes them into Driver BM, and registers their locations in BlockManagerMaster.',
        category: 'broadcast-torrent',
        keyState: 'All 3 pieces (4 + 4 + 2.8 MiB) advertised on driver BM',
        command: 'TorrentBroadcast.writeBlocks() -> master.updateBlockInfo(driverBmId, pieces)',
        detailsPassed: [{ name: 'pieces', value: '4 + 4 + 2.8 MiB (4 MiB blockSize)' }],
        deepDiveComponentId: 'broadcast-manager',
      });
      b.addEvent({
        stepId: sTb1,
        from: bcastMgr,
        to: driverBm,
        kind: 'local',
        channel: 'data',
        protocol: 'In-JVM writeBlocks',
        port: `${b.driverHost} in-JVM`,
        label: 'Write piece0..2 (10.8M)',
        info: 'Stores all 3 serialized pieces (4 + 4 + 2.8 MiB) locally in Driver BM.',
        size: '10.8 MiB',
        command: 'blockManager.putBytes(BroadcastBlockId(0, "piece" + i), bytes, MEMORY_AND_DISK_SER)',
        fields: [{ name: 'pieces', value: '4 + 4 + 2.8 MiB (3 pieces)' }],
        senderState: 'Pieces written',
        receiverState: 'Serving pieces on Driver BM',
      });
      b.addEvent({
        stepId: sTb1,
        from: driverBm,
        to: state.bmm,
        kind: 'local',
        channel: 'rpc',
        protocol: 'In-JVM BlockManagerMaster',
        port: `${b.driverHost} in-JVM`,
        label: 'UpdateBlockInfo(0..2)',
        info: 'Driver BM registers all 3 piece locations in BlockManagerMaster.',
        size: '540 B',
        command: 'master.updateBlockInfo(driverBmId, broadcast_0_piece0..2)',
        fields: [{ name: 'seed', value: b.driverBmPort }],
        senderState: 'Directory updated',
        receiverState: 'Executors can discover seed locations',
      });
      const sTb2 = b.addStep({
        id: 'sim-step-bcast-2',
        parentId: sBcast,
        title: cfg.numExecutors >= 2
          ? 'Randomized executor fetch & immediate peer seeding (Driver -> E1 -> E2)'
          : 'Single executor fetches all 3 pieces directly from Driver BM (0 E-to-E hops)',
        explanation: cfg.numExecutors >= 2
          ? `${producerExec} fetches in randomized order and immediately advertises cached pieces; peer fetches piece0 P2P from ${producerBm} (illustrative seeded peer fetch).`
          : `With 1 executor, there are no peer executors; ${producerExec} fetches all 3 pieces (4 + 4 + 2.8 MiB) directly from Driver BM.`,
        category: 'broadcast-torrent',
        keyState: 'All executors hold deserialized HashedRelation',
        command: 'TorrentBroadcast.readBlocks() // randomized order + immediate UpdateBlockInfo',
        detailsPassed: [{ name: 'executors', value: String(cfg.numExecutors) }],
        deepDiveComponentId: 'block-manager',
      });
      b.addEvent({
        stepId: sTb2,
        from: driverBm,
        to: producerBm,
        channel: 'data',
        protocol: 'BlockTransfer - TorrentBroadcast',
        port: `${b.driverBmPort} -> ${producerHost}`,
        label: 'Fetch piece0 (Driver->E1)',
        info: `${producerExec} fetches piece0 (4.0 MiB) from Driver BM and advertises ${producerBmPort} immediately.`,
        size: '4.0 MiB',
        command: 'exec1BM.getRemoteBytes(broadcast_0_piece0)',
        fields: [{ name: 'blockId', value: 'broadcast_0_piece0' }],
        senderState: 'Served piece0',
        receiverState: `${producerExec} now acts as peer seed`,
        peerTag: 'Driver -> E1',
      });
      if (state.liveBms.length >= 2) {
        const peerBm = state.liveBms[1];
        const peerExec = state.liveExecs[1];
        b.addEvent({
          stepId: sTb2,
          from: producerBm,
          to: peerBm,
          channel: 'data',
          protocol: 'BlockTransfer - TorrentBroadcast P2P',
          port: `${producerBmPort} -> worker-b`,
          label: 'P2P piece0 (E1->E2)',
          info: `${peerExec} fetches piece0 directly from peer ${producerBm}.`,
          size: '4.0 MiB',
          command: 'exec2BM.getRemoteBytes(broadcast_0_piece0)',
          fields: [{ name: 'peer', value: producerBmPort }],
          senderState: 'Served peer piece0',
          receiverState: 'Reassembling HashedRelation',
          peerTag: 'E1 -> E2',
        });
      }

      if (isTorrentCorrupt && state.liveBms.length >= 2) {
        injectTorrentPieceCorruptStep(
          b,
          state,
          driverBm,
          producerBm,
          state.liveBms[1],
          state.liveExecs[1],
        );
      }

      if (willScaleUp || isMidExecTaskAbort || isTorrentCorrupt || isSpillOom) {
        const sqlPlanner = b.std('sql-planner');
        const dag = b.std('dag-scheduler');
        if (willScaleUp || isMidExecTaskAbort || isSpillOom) {
          const sBhjSubmit = b.addStep({
            id: 'sim-step-bhj-submit',
            title: `Submit streamed fact-table BroadcastHashJoin stage (${cfg.numPartitions} tasks)`,
            explanation: `DAGScheduler submits ResultStage with ${cfg.numPartitions} streamed fact-table probe tasks (${Math.max(0, cfg.numPartitions - totalSlots)} pending in backlog).`,
            category: 'sql-planning-aqe',
            keyState: `BroadcastHashJoin stage submitted (${cfg.numPartitions} tasks)`,
            command: `DAGScheduler.submitJob(BroadcastHashJoinExec, partitions=0..${cfg.numPartitions - 1})`,
            detailsPassed: [{ name: 'partitions', value: String(cfg.numPartitions) }],
            deepDiveComponentId: 'dag-scheduler',
          });
          b.addEvent({
            stepId: sBhjSubmit,
            from: sqlPlanner,
            to: dag,
            kind: 'local',
            channel: 'internal',
            protocol: 'In-JVM Catalyst -> DAG',
            port: `${b.driverHost} in-JVM`,
            label: `Plan BHJ (${cfg.numPartitions} parts)`,
            info: 'BroadcastHashJoinExec submits streamed fact-table probe tasks referencing broadcast handle.',
            size: 'in-memory',
            command: 'BroadcastHashJoinExec.doExecute()',
            fields: [{ name: 'partitions', value: String(cfg.numPartitions) }],
            senderState: 'Streamed stage planned',
            receiverState: 'Submitting TaskSet for streamed fact partitions',
          });
          b.addEvent({
            stepId: sBhjSubmit,
            from: state.schedBackend,
            to: producerExec,
            channel: 'rpc',
            protocol: 'Spark Netty RPC',
            port: b.driverRpc,
            label: `LaunchTask (${cfg.numPartitions} parts)`,
            info: `Driver launches streamed BHJ probe tasks (${Math.max(0, cfg.numPartitions - totalSlots)} pending).`,
            size: '2.2 KB',
            command: 'executorEndpoint.send(LaunchTask(serializedTask))',
            fields: [{ name: 'slots', value: String(totalSlots) }],
            senderState: 'Initial probe wave running',
            receiverState: 'Probing broadcast HashedRelation',
          });
          if (willScaleUp) {
            emitDynamicAllocationScaleUp();
          }
          if (isSpillOom) {
            injectTaskSpillOomStep(b, state);
          }
        }
        if (isMidExecTaskAbort) {
          emitMidExecTaskAbort('BroadcastHashJoin ResultStage', producerExec);
        } else {
          const probeConsumer = getConsumerPair();
          const bhj = b.workerStd('broadcast-hash-join', probeConsumer.hostZone);
          const sBhjProbe = b.addStep({
            id: 'sim-step-bhj-probe',
            title: `BroadcastHashJoin probes streamed fact rows on ${probeConsumer.hostName}`,
            explanation: `All ${state.liveExecs.length} live executors probe read-only HashedRelation per streamed row with zero large-table shuffle.`,
            category: 'sql-planning-aqe',
            keyState: `BroadcastHashJoin completed across ${state.liveExecs.length} live executors`,
            command: 'BroadcastHashJoinExec.doExecute() -> hashedRelation.asReadOnlyCopy()',
            detailsPassed: [{ name: 'probeHost', value: probeConsumer.hostName }],
            deepDiveComponentId: 'broadcast-hash-join',
          });
          b.addEvent({
            stepId: sBhjProbe,
            from: probeConsumer.consumerBm,
            to: bhj,
            kind: 'local',
            channel: 'internal',
            protocol: 'In-JVM HashedRelation Probe',
            port: `${probeConsumer.hostName} in-JVM`,
            label: 'Probe streamed fact rows',
            info: `BroadcastHashJoinExec on ${probeConsumer.hostName} probes read-only HashedRelation with zero large-table shuffle.`,
            size: 'in-memory',
            command: 'BroadcastHashJoinExec.doExecute() -> hashedRelation.asReadOnlyCopy()',
            fields: [{ name: 'largeSideShuffle', value: '0 B (avoided!)' }],
            senderState: 'Deserialized HashedRelation shared across executor threads',
            receiverState: 'Joined rows streaming to Parquet writer',
          });
        }
      }
      if (!state.abortedByFault && isTaskResultLost) {
        taskResultLostHandled = true;
        injectTaskResultLostStep(b, state, {
          inCollectStep: false,
          stageLabel: 'BroadcastHashJoin ResultStage',
        });
      }
    }
  } else if (!state.abortedByFault && cfg.jobType === 'python-udf') {
    const isArrow = cfg.pythonUdfMode === 'arrow';
    const isTaskFail = cfg.fault === 'task-exception';
    const isOtherMidAbort = isMidExecTaskAbort && !isTaskFail;

    if (willScaleUp || isMidExecTaskAbort || isSpillOom) {
      const sqlPlanner = b.std('sql-planner');
      const dag = b.std('dag-scheduler');
      const sUdfSubmit = b.addStep({
        id: 'sim-step-udf-submit',
        title: `Submit ${isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec'} stage (${cfg.numPartitions} tasks)`,
        explanation: `Catalyst ExtractPythonUDFs plans ${isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec'} and DAGScheduler submits ${cfg.numPartitions} tasks (${Math.max(0, cfg.numPartitions - totalSlots)} pending in backlog).`,
        category: 'sql-planning-aqe',
        keyState: `Python UDF stage submitted (${cfg.numPartitions} tasks)`,
        command: `DAGScheduler.submitJob(${isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec'})`,
        detailsPassed: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        deepDiveComponentId: 'dag-scheduler',
      });
      b.addEvent({
        stepId: sUdfSubmit,
        from: sqlPlanner,
        to: dag,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Catalyst -> DAG',
        port: `${b.driverHost} in-JVM`,
        label: 'Plan EvalPythonExec',
        info: 'Catalyst isolates score_udf into EvalPythonExec and submits ResultStage 0.',
        size: 'in-memory',
        command: 'ExtractPythonUDFs -> DAGScheduler.submitJob()',
        fields: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        senderState: 'Physical plan ready',
        receiverState: 'Submitting TaskSet 0.0',
      });
      b.addEvent({
        stepId: sUdfSubmit,
        from: state.schedBackend,
        to: producerExec,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `LaunchTask (${cfg.numPartitions} parts)`,
        info: `Driver launches initial EvalPythonExec tasks (${Math.max(0, cfg.numPartitions - totalSlots)} pending).`,
        size: '2.2 KB',
        command: 'executorEndpoint.send(LaunchTask(serializedTask))',
        fields: [{ name: 'slots', value: String(totalSlots) }],
        senderState: 'Initial UDF wave running',
        receiverState: isOtherMidAbort ? 'Executing task on Executor JVM' : 'Checking out local Python worker',
      });
      if (willScaleUp) {
        emitDynamicAllocationScaleUp();
      }
      if (isSpillOom) {
        injectTaskSpillOomStep(b, state);
      }
    }

    if (isOtherMidAbort) {
      emitMidExecTaskAbort('EvalPythonExec ResultStage', producerExec);
    } else {
      const udfConsumer = getConsumerPair();
      const udfOp = b.workerStd('python-udf-operator', udfConsumer.hostZone);
      const pyFactory = b.workerStd('python-worker-factory', udfConsumer.hostZone);
      const pyRunner = b.workerStd('python-runner', udfConsumer.hostZone);
      const pyWorker =
        cfg.fault === 'python-daemon-stdout-corrupt'
          ? undefined
          : b.workerStd('python-worker', udfConsumer.hostZone);
      const pyCtx = { isArrow, udfConsumer, udfOp, pyFactory, pyRunner, pyWorker };

      const isPreBatchPyFault =
        cfg.fault === 'python-daemon-stdout-corrupt' || cfg.fault === 'python-version-mismatch';

      if (isPreBatchPyFault) {
        tryInjectPythonUdfFault(b, state, pyCtx);
      } else {
        const activePyWorker = pyWorker!;
        const sUdf = b.addStep({
          id: 'sim-step-udf',
          title: `Executor (${udfConsumer.hostName}) ${isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec'} <-> Python worker`,
          explanation: `Catalyst ExtractPythonUDFs isolates score_udf into ${isArrow ? 'ArrowEvalPythonExec (Arrow columnar IPC batches)' : 'BatchEvalPythonExec (pickled row batches)'}. ${udfConsumer.consumerExec} on ${udfConsumer.hostName} acquires a local worker via PythonWorkerFactory (pyspark.daemon fork/reuse) and BasePythonRunner interleaves reads/writes via ReaderInputStream (non-blocking NIO selector in Spark dada2a6).`,
          category: 'python-worker-ipc',
          keyState: isTaskFail
            ? `Python worker on ${udfConsumer.hostName} hits corrupt row in partition ${targetPart}`
            : `Python UDF evaluated in local worker on ${udfConsumer.hostName} (${isArrow ? 'Arrow IPC' : 'CPickle'})`,
          command: isArrow ? 'ArrowEvalPythonExec -> ReaderInputStream <-> worker.py' : 'BatchEvalPythonExec -> ReaderInputStream <-> worker.py',
          detailsPassed: [
            { name: 'operator', value: isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec' },
            { name: 'workerHost', value: udfConsumer.hostName },
            { name: 'ioEngine', value: 'BasePythonRunner ReaderInputStream (NIO Selector)' },
            { name: 'reusePolicy', value: 'spark.python.use.daemon=true, worker.reuse=true' },
          ],
          deepDiveComponentId: 'python-runner',
        });
        b.addEvent({
          stepId: sUdf,
          from: udfOp,
          to: pyFactory,
          kind: 'local',
          channel: 'internal',
          protocol: 'In-JVM Pool Checkout',
          port: `${udfConsumer.hostName} in-JVM`,
          label: 'createThroughDaemon()',
          info: `Operator on ${udfConsumer.hostName} checks idle worker pool or connects to pyspark.daemon to fork a local Python worker.`,
          size: 'in-memory',
          command: 'PythonWorkerFactory.createThroughDaemon()',
          fields: [
            { name: 'host', value: udfConsumer.hostName },
            { name: 'reuse', value: 'FIFO idle pool' },
          ],
          senderState: 'Acquired local worker SocketChannel',
          receiverState: 'Checked out worker PID 18422',
        });
        b.addEvent({
          stepId: sUdf,
          from: pyRunner,
          to: activePyWorker,
          channel: 'python',
          protocol: isArrow ? 'Local Socket (Arrow IPC)' : 'Local Socket (CPickle)',
          port: '127.0.0.1:ephemeral',
          label: isArrow ? 'Stream Arrow batch' : 'Stream pickled batch',
          info: `BasePythonRunner ReaderInputStream on ${udfConsumer.hostName} sends UDF & ${isArrow ? 'Arrow RecordBatches' : 'pickled batches'} over the local socket.`,
          size: isArrow ? '256 KiB Arrow' : '128 KiB pickle',
          command: 'ReaderInputStream (non-blocking Selector) -> worker.main()',
          fields: [{ name: 'format', value: isArrow ? 'Arrow IPC' : 'BatchedSerializer(CPickle)' }],
          senderState: 'Interleaving writes & reads on task thread',
          receiverState: 'Running score_udf in Python process',
        });
        if (isTaskFail) {
          emitTaskExceptionStep('EvalPythonExec ResultStage', udfConsumer.consumerExec);
        } else if (tryInjectPythonUdfFault(b, state, pyCtx) === 'none') {
          b.addEvent({
            stepId: sUdf,
            from: activePyWorker,
            to: pyRunner,
            channel: 'python',
            protocol: isArrow ? 'Local Socket (Arrow IPC)' : 'Local Socket (CPickle)',
            port: '127.0.0.1:ephemeral',
            label: 'Return UDF results',
            info: 'worker.py returns evaluated scores in input order; operator rejoins pass-through columns and releases worker to idle pool.',
            size: '96 KiB',
            command: 'outfile.flush() -> PythonWorkerFactory.releaseWorker()',
            fields: [{ name: 'workerReturned', value: 'true (idle pool)' }],
            senderState: 'Waiting for next task batch',
            receiverState: 'UnsafeRow output ready for write',
          });
          if (isTaskResultLost) {
            taskResultLostHandled = true;
            injectTaskResultLostStep(b, state, {
              inCollectStep: false,
              stageLabel: 'EvalPythonExec ResultStage',
            });
          }
        }
      }
    }
  } else if (!state.abortedByFault && cfg.jobType === 'jvm-udf-dataset') {
    const jvmApp = b.std('jvm-user-app');
    const driverEncoder = b.driverEncoderInstance();
    const sqlPlanner = b.std('sql-planner');
    const wscg = b.std('whole-stage-codegen');
    const dag = b.std('dag-scheduler');
    const initialConsumer = getConsumerPair();

    const sJvmPlan = b.addStep({
      id: 'sim-step-jvm-udf-plan',
      title: b.isJava
        ? `Driver derives JavaBeanEncoder (Encoders.bean) + registers Java UDF1 & fuses WholeStageCodegen`
        : `Driver builds ExpressionEncoder[Order] + defines Scala udf(...) & fuses WholeStageCodegen`,
      explanation: b.isJava
        ? `Driver invokes JavaTypeInference.encoderFor(Order.class) -> JavaBeanEncoder (bound by Catalyst into ExpressionEncoder) and registers scalar Java UDF1<Double, Double> via spark.udf().register("norm_amt", (UDF1<Double, Double>) ..., DataTypes.DoubleType) using inputEncoders=Seq(None) (UserDefinedFunction.scala:165-173), receiving a boxed, nullable java.lang.Double (ScalaUDF.scala:1125-1145). Catalyst binds DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) (object.scala:205-217: scalar withColumn UDF runs AFTER SerializeFromObject) and WholeStageCodegenExec fuses Path B directly into JVM bytecode with ZERO BatchEvalPythonExec; DriverSchedulerBackend sends LaunchTask to ${initialConsumer.consumerExec} (${initialConsumer.hostName}).`
        : `Driver builds ExpressionEncoder[Order] via Encoders.product[Order] and defines typed Scala udf((d: Double) => ...), deriving its input encoder from Scala's Double TypeTag (UserDefinedFunction.scala:132-173) so Catalyst inserts an If(IsNull(col), Literal(null), ScalaUDF(...)) null-check guard before calling primitive Double => Double. Catalyst binds DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) (object.scala:205-217: scalar withColumn UDF runs AFTER SerializeFromObject) and WholeStageCodegenExec fuses Path B directly into JVM bytecode with ZERO BatchEvalPythonExec; DriverSchedulerBackend sends LaunchTask to ${initialConsumer.consumerExec} (${initialConsumer.hostName}).`,
      category: 'sql-planning-aqe',
      keyState: `Pure-JVM Dataset[Order] + ${b.isJava ? 'Java UDF1' : 'ScalaUDF'} fused in WholeStageCodegen (0 Python workers)`,
      command: b.isJava
        ? 'JavaTypeInference.encoderFor(Order.class) -> JavaBeanEncoder + UDF1<Double, Double> -> WholeStageCodegenExec'
        : 'Encoders.product[Order] + udf((d: Double) => ...) -> WholeStageCodegenExec -> LaunchTask',
      detailsPassed: [
        {
          name: 'encoder',
          value: b.isJava
            ? 'JavaTypeInference.encoderFor(Order.class) -> JavaBeanEncoder -> ExpressionEncoder'
            : 'Encoders.product[Order] -> ExpressionEncoder[Order]',
        },
        {
          name: 'scalarUdf',
          value: b.isJava
            ? 'Java UDF1<Double, Double> (inputEncoders=Seq(None), boxed nullable java.lang.Double; UserDefinedFunction.scala:165-173, ScalaUDF.scala:1125-1145)'
            : 'Scala typed udf((d: Double) => ...) on nullable column amount (TypeTag[Double] input encoder + If(IsNull(amount), Literal(null), ScalaUDF(...)) guard inserted by Analyzer.scala:3926-3948 when input is nullable; UserDefinedFunction.scala:132-173)',
        },
        {
          name: 'operatorOrder',
          value: 'DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) (object.scala:205-217)',
        },
        { name: 'pythonBoundary', value: 'NONE (0 BatchEvalPythonExec, 0 Python workers)' },
      ],
      deepDiveComponentId: 'dataset-encoder-udf',
    });
    const closureCleaner = b.std('closure-cleaner');
    b.addEvent({
      stepId: sJvmPlan,
      from: jvmApp,
      to: closureCleaner,
      kind: 'local',
      categoryOverride: 'sql-planning-aqe',
      channel: 'internal',
      protocol: 'SparkClosureCleaner (ASM)',
      port: `${b.driverHost} in-JVM`,
      label: b.isJava ? 'Clean Java SerializedLambda' : 'Clean Scala $anonfun$',
      info: b.isJava
        ? 'SparkClosureCleaner validates Java MapFunction / UDF1 SerializedLambda serializability on the driver before task dispatch.'
        : 'SparkClosureCleaner inspects Scala Function1 ASM bytecode, strips unused $outer pointers, and verifies JavaSerializer serializability on the driver.',
      size: 'in-memory',
      command: 'SparkClosureCleaner.clean(userFn, checkSerializable=true)',
      fields: [
        { name: 'cleaner', value: 'SparkClosureCleaner (ASM9 + JavaSerializer check)' },
        { name: 'jvmOnly', value: 'Scala/Java native closure cleaning (0 cloudpickle)' },
      ],
      senderState: 'Cleaned user Dataset.map & UDF closures on driver',
      receiverState: 'Closures verified Serializable for executor shipping',
    });
    b.addEvent({
      stepId: sJvmPlan,
      from: jvmApp,
      to: driverEncoder,
      kind: 'local',
      channel: 'internal',
      protocol: b.isJava ? 'JavaTypeInference + UDF1' : 'ScalaReflection + ScalaUDF',
      port: `${b.driverHost} in-JVM`,
      label: b.isJava ? 'Encoders.bean(Order)' : 'Encoders.product[Order]',
      info: b.isJava
        ? 'JavaTypeInference.encoderFor(Order.class) derives JavaBeanEncoder (AgnosticEncoder bound to ExpressionEncoder) and spark.udf().register("norm_amt", (UDF1<Double, Double>) ..., DataTypes.DoubleType) uses inputEncoders=Seq(None) (UserDefinedFunction.scala:165-173), receiving boxed, nullable java.lang.Double (ScalaUDF.scala:1125-1145).'
        : 'Encoders.product[Order] builds ExpressionEncoder[Order] and typed udf((d: Double) => ...) on nullable column amount derives its input encoder from Scala Double TypeTag (UserDefinedFunction.scala:132-173); because amount is nullable, Catalyst Analyzer (Analyzer.scala:3926-3948) inserts If(IsNull(amount), Literal(null), ScalaUDF(...)) before primitive Double => Double.',
      size: 'in-memory',
      command: b.isJava
        ? 'Encoders.bean(Order.class); spark.udf().register("norm_amt", (UDF1<Double, Double>) ..., DataTypes.DoubleType)'
        : 'Encoders.product[Order]; val norm_amt = udf((d: Double) => ...) // TypeTag[Double] encoder',
      fields: [
        ...(b.isJava
          ? [{ name: 'inputEncoders', value: 'Seq(None) (boxed nullable java.lang.Double)' }]
          : [
              { name: 'inputEncoders', value: 'Derived from Scala Double TypeTag (UserDefinedFunction.scala:132-173)' },
              { name: 'nullGuard', value: 'If(IsNull(col), Literal(null), ScalaUDF(...)) before primitive Double => Double' },
            ]),
        {
          name: 'closureFormat',
          value: b.isJava ? 'Java Serializable / SerializedLambda MapFunction' : 'Scala Function1 closure',
        },
      ],
      senderState: 'Derived typed encoder & registered JVM scalar UDF',
      receiverState: 'Encoder expressions & ScalaUDF ready for Catalyst binding',
    });
    b.addEvent({
      stepId: sJvmPlan,
      from: driverEncoder,
      to: sqlPlanner,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Catalyst Binding',
      port: `${b.driverHost} in-JVM`,
      label: 'Bind Encoder + JVM UDF',
      info: b.isJava
        ? 'Catalyst binds DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) (object.scala:205-217: scalar Java UDF1 runs AFTER SerializeFromObject consumes mapped output, receiving boxed nullable java.lang.Double; zero BatchEvalPythonExec).'
        : 'Catalyst binds DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) (object.scala:205-217: scalar withColumn UDF runs AFTER SerializeFromObject consumes mapped output, guarded by If(IsNull(col), Literal(null), ScalaUDF(...)); zero BatchEvalPythonExec).',
      size: 'in-memory',
      command: 'Catalyst Optimizer -> DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF)',
      fields: [
        { name: 'operatorOrder', value: 'DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF)' },
        { name: 'batchEvalPythonExec', value: '0 (pure JVM Path B)' },
      ],
      senderState: 'Bound ExpressionEncoder & ScalaUDF',
      receiverState: 'Handing physical plan to WholeStageCodegenExec',
    });
    b.addEvent({
      stepId: sJvmPlan,
      from: sqlPlanner,
      to: wscg,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM WholeStageCodegen',
      port: `${b.driverHost} in-JVM`,
      label: 'Fuse WholeStageCodegen',
      info: 'WholeStageCodegenExec fuses Parquet scan, typed Dataset.map(Order), and scalar JVM UDF into a single Janino BufferedRowIterator without any Python boundary.',
      size: 'in-memory',
      command: 'WholeStageCodegenExec.doCodeGen() // Path B fused without BatchEvalPythonExec',
      fields: [{ name: 'compiler', value: 'Janino default (in-JVM BufferedRowIterator)' }],
      senderState: 'Fused physical operator pipeline',
      receiverState: 'Submitting 1-stage ResultStage to DAGScheduler',
    });
    b.addEvent({
      stepId: sJvmPlan,
      from: wscg,
      to: dag,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM DAG Submit',
      port: `${b.driverHost} in-JVM`,
      label: 'Submit 1-stage JVM DAG',
      info: `DAGScheduler creates ResultStage 0 (${cfg.numPartitions} tasks, 0 shuffles, 0 Python workers).`,
      size: 'in-memory',
      command: `DAGScheduler.submitJob(ResultStage 0, partitions=0..${cfg.numPartitions - 1})`,
      fields: [
        { name: 'partitions', value: String(cfg.numPartitions) },
        { name: 'pythonWorkers', value: '0' },
      ],
      senderState: 'Stage 0 submitted',
      receiverState: 'Dispatching serialized tasks via DriverSchedulerBackend',
    });
    b.addEvent({
      stepId: sJvmPlan,
      from: state.schedBackend,
      to: initialConsumer.consumerExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: `LaunchTask (${cfg.numPartitions} parts)`,
      info: `DriverSchedulerBackend sends LaunchTask to live executor ${initialConsumer.consumerExec} (${initialConsumer.hostName}) carrying ${b.isJava ? 'JavaBeanEncoder + Java UDF1' : 'ExpressionEncoder[Order] + ScalaUDF'}.`,
      size: '3.4 KB',
      command: 'executorEndpoint.send(LaunchTask(serializedTask))',
      fields: [
        { name: 'targetExecutor', value: `${initialConsumer.consumerExec} (${initialConsumer.hostName})` },
        { name: 'slots', value: String(totalSlots) },
      ],
      senderState: `Running up to ${Math.min(cfg.numPartitions, totalSlots)} concurrent tasks`,
      receiverState: 'Executing fused Dataset.map & JVM UDF on Executor JVM thread',
    });
    if (willScaleUp) {
      emitDynamicAllocationScaleUp();
    }
    if (isSpillOom) {
      injectTaskSpillOomStep(b, state);
    }

    const execConsumer = getConsumerPair();
    if (isMidExecTaskAbort) {
      emitMidExecTaskAbort('Typed Dataset + JVM UDF ResultStage', execConsumer.consumerExec);
    } else {
      const execTaskAttempt = b.execTaskAttemptInstance(execConsumer.consumerExec);
      const execEncoder = b.execEncoderInstance(execConsumer.consumerExec);
      const dn = b.std('hdfs-datanode');
      const sJvmExec = b.addStep({
        id: 'sim-step-jvm-udf-exec',
        title: `Executor (${execConsumer.hostName}) evaluates Dataset.map(Order) & ${b.isJava ? 'Java UDF1' : 'ScalaUDF'} in-JVM`,
        explanation: `${execConsumer.consumerExec} on ${execConsumer.hostName} executes Dataset.map(Order) and ${b.isJava ? 'Java UDF1<Double, Double>.call(java.lang.Double)' : 'ScalaUDF'} directly on the Executor JVM task thread via ${execEncoder} (ZERO Python workers, ZERO socket IPC) and writes staged Parquet parts to HDFS DataNode.`,
        category: 'sql-planning-aqe',
        keyState: isTaskResultLost
          ? `Executor ${execConsumer.consumerExec} staged Parquet parts; IndirectTaskResult queued`
          : `All ${cfg.numPartitions} pure-JVM Dataset/UDF tasks finished (0 Python workers)`,
        command: b.isJava
          ? 'TaskRunner -> JavaBeanEncoder + UDF1.call(Double) -> ParquetWriter'
          : 'TaskRunner -> ExpressionEncoder[Order] + ScalaUDF -> ParquetWriter',
        detailsPassed: [
          { name: 'executorInstance', value: `${execConsumer.consumerExec} (${execConsumer.hostName})` },
          { name: 'encoderInstance', value: execEncoder },
          { name: 'pythonWorkersSpawned', value: '0' },
        ],
        deepDiveComponentId: 'dataset-encoder-udf',
      });
      b.addEvent({
        stepId: sJvmExec,
        from: execConsumer.consumerExec,
        to: execTaskAttempt,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM TaskRunner',
        port: `${execConsumer.hostName} in-JVM`,
        label: 'Run fused task thread',
        info: `${execConsumer.consumerExec} invokes TaskRunner on ${execConsumer.hostName} JVM thread pool.`,
        size: 'in-memory',
        command: 'Executor.TaskRunner.run() // pure-JVM WholeStageCodegen',
        fields: [{ name: 'host', value: execConsumer.hostName }],
        senderState: 'Task thread executing fused pipeline',
        receiverState: 'Invoking typed Dataset encoder & JVM UDF in-JVM',
      });
      b.addEvent({
        stepId: sJvmExec,
        from: execTaskAttempt,
        to: execEncoder,
        kind: 'local',
        channel: 'internal',
        protocol: b.isJava ? 'In-JVM BeanEnc + UDF1' : 'In-JVM Encoder + ScalaUDF',
        port: `${execConsumer.hostName} in-JVM`,
        label: b.isJava ? 'BeanEnc + UDF1(Double)' : 'Dataset.map + ScalaUDF',
        info: b.isJava
          ? `Executor task thread runs DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) on ${execConsumer.hostName} JVM (object.scala:205-217): JavaBeanEncoder maps Order, then Project invokes Java UDF1<Double, Double>.call(java.lang.Double) on boxed nullable Double AFTER SerializeFromObject (0 Python workers).`
          : `Executor task thread runs DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF) on ${execConsumer.hostName} JVM (object.scala:205-217): ExpressionEncoder[Order] maps Order, then Project runs If(IsNull(col), Literal(null), ScalaUDF(...)) before primitive Double => Double AFTER SerializeFromObject (0 Python workers).`,
        size: 'in-memory',
        command: b.isJava
          ? 'DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF[UDF1.call(java.lang.Double)])'
          : 'DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF)',
        fields: [
          { name: 'operatorOrder', value: 'DeserializeToObject -> MapElements -> SerializeFromObject -> Project(ScalaUDF)' },
          { name: 'pythonWorkers', value: '0 (pure JVM task thread)' },
          { name: 'ipcSockets', value: '0 (in-JVM call)' },
        ],
        senderState: 'Evaluated typed Dataset & scalar JVM UDF in-JVM',
        receiverState: 'Produced Tungsten UnsafeRow output for Parquet write',
      });
      const jvmJanino = b.execLocalComponentInstance(execConsumer.consumerExec, 'janino-code-generator', 'Janino');
      const jvmTungsten = b.execLocalComponentInstance(execConsumer.consumerExec, 'tungsten-unsafe-row', 'UnsafeRow');
      b.addEvent({
        stepId: sJvmExec,
        from: execEncoder,
        to: jvmJanino,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Janino CodeGenerator',
        port: `${execConsumer.hostName} in-JVM`,
        label: 'Compile inline UDF loop',
        info: `Janino compiles WholeStageCodegenExec fusing MapElements + inline ${b.isJava ? 'Java UDF1' : 'ScalaUDF'} inside one BufferedRowIterator on ${execConsumer.hostName}.`,
        size: 'in-memory',
        command: 'CodeGenerator.compile(fusedDatasetUdfCode) -> GeneratedIterator',
        fields: [{ name: 'compiler', value: 'Janino SimpleCompiler (cached)' }],
        senderState: 'Inlined ScalaUDF/Java UDF1 into generated Java bytecode',
        receiverState: 'Compiled fused iterator writing Tungsten UnsafeRows',
      });
      b.addEvent({
        stepId: sJvmExec,
        from: jvmJanino,
        to: jvmTungsten,
        kind: 'local',
        categoryOverride: 'sql-planning-aqe',
        channel: 'internal',
        protocol: 'Tungsten UnsafeRow',
        port: `${execConsumer.hostName} in-JVM`,
        label: 'Write scored UnsafeRow',
        info: 'Scored Dataset + scalar JVM UDF results are serialized directly into binary Tungsten UnsafeRow pages for Parquet encoding.',
        size: 'in-memory',
        command: 'SerializeFromObject -> UnsafeRow -> SingleDirectoryDataWriter',
        fields: [{ name: 'format', value: 'Tungsten binary UnsafeRow (0 Python IPC)' }],
        senderState: 'Evaluated inline UDF inside compiled loop',
        receiverState: 'Output UnsafeRows ready for Parquet write',
      });
      b.addEvent({
        stepId: sJvmExec,
        from: execTaskAttempt,
        to: dn,
        channel: 'data',
        protocol: 'HDFS DataTransferProtocol',
        port: 'dn01:9866',
        label: 'Write staged Parquet',
        info: `Executors write staged Parquet output blocks directly to HDFS DataNode :9866 with zero Python workers.`,
        size: `${cfg.numPartitions * 48} MiB`,
        command: 'SingleDirectoryDataWriter.write() -> DFSOutputStream (dn01:9866)',
        fields: [
          { name: 'shuffleBytes', value: '0 B' },
          { name: 'pythonWorkers', value: '0' },
        ],
        senderState: isTaskResultLost
          ? 'Task staged Parquet part; sending IndirectTaskResult to driver'
          : 'All JVM Dataset/UDF tasks finished',
        receiverState: 'Staged Parquet parts flushed to DataNodes',
      });
      if (isTaskResultLost) {
        taskResultLostHandled = true;
        injectTaskResultLostStep(b, state, {
          inCollectStep: false,
          stageLabel: 'Typed Dataset + JVM UDF ResultStage',
        });
      }
    }
  } else if (!state.abortedByFault && cfg.jobType === 'cached-rdd') {
    if (willScaleUp || isMidExecTaskAbort || isSpillOom) {
      const dag = b.std('dag-scheduler');
      const sCacheSubmit = b.addStep({
        id: 'sim-step-cache-submit',
        title: `Submit Action 1 (${b.isJava ? 'javaRdd.count()' : 'rdd.count()'}) for ${cfg.numPartitions} partitions`,
        explanation: `DAGScheduler submits Action 1 across ${cfg.numPartitions} partitions (${Math.max(0, cfg.numPartitions - totalSlots)} pending in backlog).`,
        category: 'sql-planning-aqe',
        keyState: `Action 1 submitted (${cfg.numPartitions} partitions)`,
        command: `DAGScheduler.submitJob(${b.isJava ? 'javaRdd.count()' : 'rdd.count()'}, partitions=0..${cfg.numPartitions - 1})`,
        detailsPassed: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        deepDiveComponentId: 'dag-scheduler',
      });
      b.addEvent({
        stepId: sCacheSubmit,
        from: dag,
        to: state.schedBackend,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM TaskSet Submit',
        port: `${b.driverHost} in-JVM`,
        label: `Submit Action 1 (${cfg.numPartitions}p)`,
        info: `DAGScheduler submits Action 1 TaskSet (${cfg.numPartitions} partitions) for MEMORY_ONLY caching.`,
        size: 'in-memory',
        command: 'taskScheduler.submitTasks(TaskSet 0.0)',
        fields: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        senderState: 'Action 1 active',
        receiverState: 'Launching initial tasks',
      });
      b.addEvent({
        stepId: sCacheSubmit,
        from: state.schedBackend,
        to: producerExec,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `LaunchTask (${cfg.numPartitions} parts)`,
        info: `Driver dispatches initial Action 1 wave (${Math.max(0, cfg.numPartitions - totalSlots)} pending).`,
        size: '2.2 KB',
        command: 'executorEndpoint.send(LaunchTask(serializedTask))',
        fields: [{ name: 'slots', value: String(totalSlots) }],
        senderState: 'Action 1 running',
        receiverState: 'Computing RDD partitions',
      });
      if (willScaleUp) {
        emitDynamicAllocationScaleUp();
      }
      if (isSpillOom) {
        injectTaskSpillOomStep(b, state);
      }
    }

    if (isMidExecTaskAbort) {
      emitMidExecTaskAbort('Action 1 (rdd.count())', producerExec);
    } else {
      const rddCache = b.workerStd('rdd-cache', producerZone);
      const dn = b.std('hdfs-datanode');
      const sCache = b.addStep({
        id: 'sim-step-cache',
        title: `Action 1 caches MEMORY_ONLY; Action 2 hits p=0 & recomputes evicted p=${targetPart}`,
        explanation: b.isPySpark
          ? `persist(MEMORY_ONLY) is lazy until Action 1 (PySpark RDD.count() runs mapPartitions via Python worker). Each executor computes partitions via RDD.getOrCompute, stores rdd_17_p in local MemoryStore, and updates BlockManagerMaster. Under MEMORY_ONLY, RAM eviction drops blocks instead of spilling to disk: Action 2 hits partition 0 (rdd_17_0) in MemoryStore, while evicted partition ${targetPart} (rdd_17_${targetPart}) misses and recomputes from HDFS lineage via RDD.getOrCompute.`
          : `persist(MEMORY_ONLY) is lazy until Action 1 (${b.isJava ? 'JavaRDD.count()' : 'RDD.count()'} directly in-JVM, ZERO Python workers). Each executor computes partitions via RDD.getOrCompute, stores rdd_17_p in local MemoryStore, and updates BlockManagerMaster. Under MEMORY_ONLY, RAM eviction drops blocks instead of spilling to disk: Action 2 hits partition 0 (rdd_17_0) in MemoryStore, while evicted partition ${targetPart} (rdd_17_${targetPart}) misses and recomputes from HDFS lineage via RDD.getOrCompute.`,
        category: 'yarn-lifecycle',
        keyState: `MEMORY_ONLY: Action 2 hit rdd_17_0; evicted rdd_17_${targetPart} recomputed via RDD.getOrCompute`,
        command: `RDD.iterator() -> BlockManager.getOrElseUpdateRDDBlock(rdd_17_0 / rdd_17_${targetPart}, MEMORY_ONLY)`,
        detailsPassed: [
          { name: 'storageLevel', value: 'MEMORY_ONLY (RAM eviction triggers lineage recompute, no disk spill)' },
          {
            name: 'executionRuntime',
            value: b.isPySpark ? 'PySpark PythonRDD mapPartitions via local Python worker' : `Pure-JVM ${b.langDisplay} RDD.count() (0 Python workers)`,
          },
          { name: 'cacheHit', value: 'Partition 0 (rdd_17_0) served from MemoryStore' },
          { name: 'evictedRecompute', value: `Partition ${targetPart} (rdd_17_${targetPart}) missed -> RDD.getOrCompute` },
        ],
        deepDiveComponentId: 'rdd-cache',
      });
      if (b.isPySpark) {
        const pyRunner = b.workerStd('python-runner', producerZone);
        const pyWorker = b.workerStd('python-worker', producerZone);
        b.addEvent({
          stepId: sCache,
          from: pyRunner,
          to: pyWorker,
          categoryOverride: 'python-worker-ipc',
          channel: 'python',
          protocol: 'Local Socket (CPickle)',
          port: '127.0.0.1:ephemeral',
          label: 'PyRDD mapPartitions count',
          info: `PySpark RDD.count() evaluates mapPartitions(lambda it: [sum(1 for _ in it)]) in the local Python worker on ${producerHost}.`,
          size: '64 KiB',
          command: 'PythonRDD.compute() -> worker.py mapPartitions (RDD.count())',
          fields: [{ name: 'api', value: 'PySpark RDD.count() via Python worker' }],
          senderState: 'Streamed partition iterator to local Python worker',
          receiverState: 'Returned partition element count',
        });
      } else {
        const jvmApp = b.std('jvm-user-app');
        const dag = b.std('dag-scheduler');
        b.addEvent({
          stepId: sCache,
          from: jvmApp,
          to: dag,
          kind: 'local',
          channel: 'internal',
          protocol: 'In-JVM RDD.count()',
          port: `${b.driverHost} in-JVM`,
          label: b.isJava ? 'JavaRDD.persist().count()' : 'RDD.persist().count()',
          info: `${b.appEntryLabel}() triggers Action 1 & Action 2 directly in-JVM with ZERO Python workers or Py4J calls.`,
          size: 'in-memory',
          command: b.isJava
            ? 'javaRdd.persist(StorageLevel.MEMORY_ONLY()).count()'
            : 'rdd.persist(StorageLevel.MEMORY_ONLY).count()',
          fields: [{ name: 'pythonWorkers', value: '0 (pure JVM RDD execution)' }],
          senderState: 'Submitted in-JVM RDD count actions',
          receiverState: 'Executing directly on Executor JVM task threads',
        });
      }
      b.addEvent({
        stepId: sCache,
        from: rddCache,
        to: producerBm,
        kind: 'local',
        channel: 'data',
        protocol: 'In-JVM BlockManager',
        port: `${producerHost} in-JVM`,
        label: 'Action 1: compute & store',
        info: `Action 1 misses cache, computes partitions from HDFS lineage, and stores rdd_17_0 and rdd_17_${targetPart} in MemoryStore (MEMORY_ONLY).`,
        size: '48 MiB cached',
        command: 'blockManager.getOrElseUpdateRDDBlock(taskAttemptId, rdd_17_0, MEMORY_ONLY)',
        fields: [
          { name: 'storageLevel', value: 'MEMORY_ONLY' },
          { name: 'blockId', value: `rdd_17_0, rdd_17_${targetPart}` },
        ],
        senderState: 'Action 1 returned count',
        receiverState: 'Blocks stored in MemoryStore (MEMORY_ONLY)',
      });
      b.addEvent({
        stepId: sCache,
        from: producerBm,
        to: state.bmm,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: 'UpdateBlockInfo(rdd_17)',
        info: 'Executor reports MEMORY_ONLY cached partition locations to driver BlockManagerMaster (metadata only; bytes stay on worker).',
        size: '520 B',
        command: 'master.updateBlockInfo(bmId, rdd_17_0, MEMORY_ONLY, memSize=48MB)',
        fields: [
          { name: 'storageLevel', value: 'MEMORY_ONLY' },
          { name: 'dataProxiedByDriver', value: 'false (metadata only)' },
        ],
        senderState: 'Block visible to DAGScheduler locality',
        receiverState: `Recorded cache locations on ${producerHost}`,
      });
      b.addEvent({
        stepId: sCache,
        from: rddCache,
        to: producerBm,
        kind: 'local',
        channel: 'data',
        protocol: 'In-JVM Cache Hit',
        port: `${producerHost} in-JVM`,
        label: 'Action 2: hit rdd_17_0',
        info: 'Action 2 finds partition 0 (rdd_17_0) locally in MemoryStore without rescanning HDFS.',
        size: '48 MiB hit',
        command: 'blockManager.getLocalValues(rdd_17_0) -> Some(BlockResult)',
        fields: [
          { name: 'partition', value: '0 (rdd_17_0)' },
          { name: 'hdfsReadOnHit', value: '0 B' },
        ],
        senderState: 'Partition 0 served from RAM cache',
        receiverState: 'MemoryStore hit on rdd_17_0',
      });
      b.addEvent({
        stepId: sCache,
        from: rddCache,
        to: producerBm,
        kind: 'local',
        channel: 'data',
        protocol: 'In-JVM Cache Miss (Evicted)',
        port: `${producerHost} in-JVM`,
        label: `Miss evicted rdd_17_${targetPart}`,
        info: `Under MEMORY_ONLY, partition ${targetPart} (rdd_17_${targetPart}) was evicted under memory pressure without spilling to disk; getLocalValues returns None and falls back to RDD.getOrCompute.`,
        size: 'Cache miss',
        command: `blockManager.getLocalValues(rdd_17_${targetPart}) -> None -> RDD.getOrCompute(split=${targetPart})`,
        fields: [
          { name: 'evictedBlock', value: `rdd_17_${targetPart}` },
          { name: 'storageLevel', value: 'MEMORY_ONLY (no disk spill)' },
        ],
        senderState: `Falling back to lineage recomputation for p=${targetPart}`,
        receiverState: `rdd_17_${targetPart} not present in MemoryStore`,
      });
      b.addEvent({
        stepId: sCache,
        from: rddCache,
        to: dn,
        channel: 'data',
        protocol: 'HDFS DataTransferProtocol',
        port: 'dn01:9866',
        label: `Recompute p=${targetPart} lineage`,
        info: `RDD.getOrCompute re-reads evicted partition ${targetPart} from HDFS DataNode lineage and re-caches rdd_17_${targetPart} in MemoryStore.`,
        size: '48 MiB HDFS',
        command: `RDD.getOrCompute(split=${targetPart}) -> computeOrReadCheckpoint() -> HDFS DataNode`,
        fields: [
          { name: 'partition', value: String(targetPart) },
          { name: 'recomputeCause', value: 'MEMORY_ONLY RAM eviction' },
        ],
        senderState: isTaskResultLost
          ? `Recomputed p=${targetPart}; sending IndirectTaskResult for Action 2`
          : 'Action 2 completed (hit p=0 + recomputed evicted p=' + targetPart + ')',
        receiverState: 'Served lineage blocks for recomputation',
      });
      if (isTaskResultLost) {
        taskResultLostHandled = true;
        injectTaskResultLostStep(b, state, {
          inCollectStep: false,
          stageLabel: 'Action 2 (rdd.count())',
          actionCompleteLabel: 'Action 2 completed',
          actionCompleteInfo: `Driver TaskResultGetter delivers retry attempt 1 result for Action 2 (rdd.count()); Action 2 completes cleanly after TaskResultLost recovery (hit p=0 + recomputed evicted p=${targetPart}).`,
          actionCompleteSenderState:
            'Action 2 completed (hit p=0 + recomputed evicted p=' + targetPart + ' + retry OK)',
        });
      }
    }
  } else if (!state.abortedByFault && cfg.jobType === 'collect-action') {
    if (willScaleUp || isMidExecTaskAbort || isSpillOom) {
      const dag = b.std('dag-scheduler');
      const sCollectSubmit = b.addStep({
        id: 'sim-step-collect-submit',
        title: `Submit collect() ResultStage (${cfg.numPartitions} partitions)`,
        explanation: `DAGScheduler submits collect() ResultStage across ${cfg.numPartitions} partitions (${Math.max(0, cfg.numPartitions - totalSlots)} pending in backlog).`,
        category: 'sql-planning-aqe',
        keyState: `collect() stage submitted (${cfg.numPartitions} tasks)`,
        command: `DAGScheduler.submitJob(rdd.collect(), partitions=0..${cfg.numPartitions - 1})`,
        detailsPassed: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        deepDiveComponentId: 'dag-scheduler',
      });
      b.addEvent({
        stepId: sCollectSubmit,
        from: dag,
        to: state.schedBackend,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM TaskSet Submit',
        port: `${b.driverHost} in-JVM`,
        label: `Submit collect() (${cfg.numPartitions}p)`,
        info: `DAGScheduler submits collect() TaskSet (${cfg.numPartitions} partitions).`,
        size: 'in-memory',
        command: 'taskScheduler.submitTasks(TaskSet 0.0)',
        fields: [{ name: 'partitions', value: String(cfg.numPartitions) }],
        senderState: 'collect() active',
        receiverState: 'Dispatching initial collect tasks',
      });
      b.addEvent({
        stepId: sCollectSubmit,
        from: state.schedBackend,
        to: producerExec,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `LaunchTask (${cfg.numPartitions} parts)`,
        info: `Driver dispatches initial collect() wave (${Math.max(0, cfg.numPartitions - totalSlots)} pending).`,
        size: '2.2 KB',
        command: 'executorEndpoint.send(LaunchTask(serializedTask))',
        fields: [{ name: 'slots', value: String(totalSlots) }],
        senderState: 'Tasks running on executors',
        receiverState: 'Serializing partition results',
      });
      if (willScaleUp) {
        emitDynamicAllocationScaleUp();
      }
      if (isSpillOom) {
        injectTaskSpillOomStep(b, state);
      }
    }

    if (isMidExecTaskAbort) {
      emitMidExecTaskAbort('collect() ResultStage', producerExec);
    } else {
      const trg = b.std('task-result-getter');
      const isLarge =
        cfg.dataProfile === 'large-result' || cfg.fault === 'max-result-size' || isTaskResultLost;
      const perPartMiB = Math.ceil(600 / cfg.numPartitions);
      const totalMiB = perPartMiB * cfg.numPartitions;
      const perPartBytes = perPartMiB * 1024 * 1024;
      const crossingCount = Math.floor(512 / perPartMiB) + 1;
      const crossingTotalMiB = crossingCount * perPartMiB;
      const maxResultSizeLabel = cfg.fault === 'max-result-size' ? '512 MiB' : '1 GiB';
      const deliveryDesc = b.isPySpark
        ? 'Python collect() via PythonRDD.collectAndServe'
        : `${b.appEntryLabel}() in-JVM Array/List collect()`;

      const sCollect = b.addStep({
        id: 'sim-step-collect',
        title: isLarge
          ? `collect() uses IndirectTaskResult (${perPartMiB} MiB/part, ${totalMiB} MiB total) & checks ${maxResultSizeLabel} limit`
          : 'collect() returns serialized partition arrays directly in StatusUpdate',
        explanation: isLarge
          ? cfg.fault === 'max-result-size'
            ? `Each of ${cfg.numPartitions} partitions serializes to ${perPartMiB} MiB (${totalMiB} MiB total) and is stored as a TaskResultBlock (StorageLevel.MEMORY_AND_DISK_SER) via IndirectTaskResult. With spark.driver.maxResultSize=512 MiB, TaskSetManager.canFetchMoreResults crosses the limit on partition #${crossingCount} (${crossingTotalMiB} MiB > 512 MiB) and aborts BEFORE fetching that TaskResultBlock.`
            : isTaskResultLost
              ? `Each of ${cfg.numPartitions} partitions serializes to ${perPartMiB} MiB (${totalMiB} MiB total) and is stored as a TaskResultBlock with StorageLevel.MEMORY_AND_DISK_SER before sending IndirectTaskResult to Driver TaskResultGetter. Before result delivery, TaskResultGetter fails to fetch the lost TaskResultBlock, reports TaskResultLost, and retries the task before delivering results to ${deliveryDesc}.`
              : `Each of ${cfg.numPartitions} partitions serializes to ${perPartMiB} MiB (${totalMiB} MiB total) and is stored as a TaskResultBlock (StorageLevel.MEMORY_AND_DISK_SER) via IndirectTaskResult. With spark.driver.maxResultSize=1 GiB (1024 MiB), totalMiB (${totalMiB} MiB <= 1024 MiB) passes TaskSetManager.canFetchMoreResults and Driver TaskResultGetter fetches TaskResultBlocks over ${producerBmPort}.`
          : `Small serialized partition results fit inside StatusUpdate RPC messages directly to the driver and are delivered to ${deliveryDesc}.`,
        category: 'yarn-lifecycle',
        keyState:
          cfg.fault === 'max-result-size'
            ? `ABORTED on part #${crossingCount}: ${crossingTotalMiB} MiB > 512 MiB (total ${cfg.numPartitions}x${perPartMiB}=${totalMiB} MiB)`
            : isTaskResultLost
              ? `IndirectTaskResult (${perPartMiB} MiB, MEMORY_AND_DISK_SER) queued for TaskResultGetter fetch`
              : isLarge
                ? `collect() succeeded: ${totalMiB} MiB (${cfg.numPartitions}x${perPartMiB} MiB) <= 1 GiB maxResultSize`
                : `Driver assembled collect() result for ${deliveryDesc}`,
        command: b.isPySpark
          ? 'RDD.collect() (RDD.scala:1122-1125) -> TaskResultGetter -> DAGScheduler -> PythonRDD.collectAndServe()'
          : `RDD.collect() (RDD.scala:1122-1125) -> TaskResultGetter -> DAGScheduler -> ${b.isJava ? 'List<Row>' : 'Array[Row]'} in-JVM`,
        detailsPassed: [
          {
            name: 'transport',
            value: isLarge
              ? 'IndirectTaskResult (StorageLevel.MEMORY_AND_DISK_SER) -> BlockManager fetch'
              : 'DirectTaskResult in StatusUpdate',
          },
          ...(isLarge
            ? [{ name: 'resultSizing', value: `${cfg.numPartitions} parts x ${perPartMiB} MiB = ${totalMiB} MiB` }]
            : []),
          { name: 'maxResultSize', value: `${maxResultSizeLabel} (spark.driver.maxResultSize)` },
          { name: 'driverDelivery', value: deliveryDesc },
        ],
        deepDiveComponentId: 'task-result-getter',
      });
      b.addEvent({
        stepId: sCollect,
        from: producerExec,
        to: trg,
        channel: 'rpc',
        protocol: 'Spark Netty RPC (StatusUpdate)',
        port: b.driverRpc,
        label: isLarge ? `IndirectTaskResult(${perPartMiB}M)` : 'DirectTaskResult(120 KB)',
        info: isLarge
          ? `Executor stores TaskResultBlock locally with StorageLevel.MEMORY_AND_DISK_SER and sends IndirectTaskResult(blockId, ${perPartMiB} MiB) reference in StatusUpdate (${cfg.numPartitions} parts = ${totalMiB} MiB total).`
          : 'Executor embeds small serialized partition array directly inside StatusUpdate.',
        size: isLarge ? '280 B ref' : '120 KB',
        command: isLarge
          ? `new IndirectTaskResult(TaskResultBlockId(tid), ${perPartBytes}) // StorageLevel.MEMORY_AND_DISK_SER`
          : 'new DirectTaskResult(bytes)',
        fields: isLarge
          ? [
              { name: 'storageLevel', value: 'StorageLevel.MEMORY_AND_DISK_SER' },
              { name: 'perPartitionSize', value: `${perPartMiB} MiB` },
              { name: 'totalProjected', value: `${totalMiB} MiB (${cfg.numPartitions} x ${perPartMiB} MiB)` },
            ]
          : [{ name: 'resultSize', value: '120 KB' }],
        senderState: 'Task finished',
        receiverState: isLarge ? 'Checking TaskSetManager.canFetchMoreResults(size)' : 'Deserializing result on driver',
      });
      if (cfg.fault === 'max-result-size') {
        const tsm = b.std('task-set-manager');
        state.abortedByFault = true;
        b.addEvent({
          stepId: sCollect,
          from: trg,
          to: tsm,
          kind: 'local',
          channel: 'error',
          protocol: 'In-JVM Quota Abort',
          port: `${b.driverHost} in-JVM`,
          label: `Abort: ${crossingTotalMiB}MiB > 512MiB`,
          info: `TaskSetManager.canFetchMoreResults aborts TaskSet on partition #${crossingCount} BEFORE fetching its TaskResultBlock: totalSoFar (${crossingTotalMiB} MiB = ${crossingCount} x ${perPartMiB} MiB; full ${cfg.numPartitions} parts = ${totalMiB} MiB) > spark.driver.maxResultSize (512 MiB).`,
          size: 'SparkException',
          command: `TaskSetManager.canFetchMoreResults(${perPartBytes}) // ${crossingTotalMiB} MiB > 512 MiB -> abort()`,
          fields: [
            { name: 'crossingPartition', value: `#${crossingCount} of ${cfg.numPartitions} (${crossingTotalMiB} MiB > 512 MiB)` },
            { name: 'cumulativeJobBytes', value: `${totalMiB} MiB (${cfg.numPartitions} x ${perPartMiB} MiB)` },
            { name: 'limit', value: '512 MiB (spark.driver.maxResultSize)' },
          ],
          senderState: 'Aborted BEFORE fetching crossing TaskResultBlock',
          receiverState: `Job failed; ${b.appEntryLabel} receives SparkException (no partial list)`,
          isError: true,
        });
        emitTerminalAppOutcome(b, state, {
          origin: 'user-app-failure',
          diagnostic: `Total serialized results (${crossingTotalMiB} MiB) exceeded spark.driver.maxResultSize (512 MiB)`,
          stopLiveExecutors: true,
        });
      } else {
        let deliveryStepId = sCollect;
        if (isTaskResultLost) {
          taskResultLostHandled = true;
          injectTaskResultLostStep(b, state, {
            inCollectStep: true,
            perPartMiB,
            totalMiB,
          });
          deliveryStepId = 'sim-step-task-result-lost';
        } else if (isLarge) {
          b.addEvent({
            stepId: sCollect,
            from: trg,
            to: producerBm,
            channel: 'data',
            protocol: 'BlockTransfer (TaskResultBlock)',
            port: producerBmPort,
            label: 'getRemoteBytes(taskres)',
            info: `Driver TaskResultGetter fetches TaskResultBlock (${perPartMiB} MiB; ${cfg.numPartitions} parts = ${totalMiB} MiB <= 1 GiB limit) from ${producerBmPort} after quota check passes.`,
            size: `${perPartMiB} MiB`,
            command: 'sparkEnv.blockManager.getRemoteBytes(TaskResultBlockId(tid))',
            fields: [{ name: 'quotaCheck', value: `Passed (${totalMiB} MiB <= 1 GiB)` }],
            senderState: 'Fetched TaskResultBlock; handing partition result to TaskScheduler / DAGScheduler',
            receiverState: 'TaskResultBlock removed after fetch',
          });
        }
        const dagCollect = b.std('dag-scheduler');
        b.addEvent({
          stepId: deliveryStepId,
          from: trg,
          to: dagCollect,
          kind: 'local',
          channel: 'internal',
          protocol: 'In-JVM TaskScheduler/DAG',
          port: `${b.driverHost} in-JVM`,
          label: 'Deliver part results',
          info: 'TaskResultGetter fetches and deserializes individual partition results into TaskScheduler / DAGScheduler before RDD.collect() (RDD.scala:1122-1125) concatenates partition arrays.',
          size: 'in-memory',
          command: 'scheduler.handleSuccessfulTask(tsm, tid, result) -> DAGScheduler JobWaiter',
          fields: [{ name: 'handoff', value: 'TaskResultGetter -> TaskScheduler / DAGScheduler (per-partition arrays)' }],
          senderState: 'Individual partition results delivered to TaskScheduler / DAGScheduler',
          receiverState: 'All partition arrays collected for RDD.collect() concatenation',
        });
        if (b.isPySpark) {
          const pyDriver = b.std('python-driver');
          b.addEvent({
            stepId: deliveryStepId,
            from: dagCollect,
            to: pyDriver,
            channel: 'python',
            protocol: 'PythonRDD.collectAndServe',
            port: b.isClient ? '127.0.0.1:44108' : '127.0.0.1:43122',
            label: 'collectAndServe -> Py',
            info: 'After TaskResultGetter delivers partition results to TaskScheduler / DAGScheduler, RDD.collect() (RDD.scala:1122-1125) / PythonRDD.collectAndServe streams collected pickled partition batches over local socket to python-driver (orders.py).',
            size: isLarge ? `${totalMiB} MiB` : '120 KB',
            command: 'RDD.collect() (RDD.scala:1122-1125) -> PythonRDD.collectAndServe(rdd) -> local auth socket -> orders.py',
            fields: [{ name: 'bridge', value: 'PythonRDD.collectAndServe (driver loopback socket)' }],
            senderState: 'Result delivered to Python collect()',
            receiverState: 'orders.py materialized Python list',
          });
        } else {
          const jvmApp = b.std('jvm-user-app');
          b.addEvent({
            stepId: deliveryStepId,
            from: dagCollect,
            to: jvmApp,
            kind: 'local',
            channel: 'internal',
            protocol: 'In-JVM RDD.collect()',
            port: `${b.driverHost} in-JVM`,
            label: 'RDD.collect() concat',
            info: `After TaskResultGetter feeds individual partition results into TaskScheduler / DAGScheduler, RDD.collect() (RDD.scala:1122-1125, Array.concat(results: _*)) concatenates the partition arrays into the final ${b.isJava ? 'Java List<Row>' : 'Scala Array[Row]'} in-JVM for ${b.appEntryLabel}() (zero loopback IPC).`,
            size: 'in-memory',
            command: b.isJava
              ? 'RDD.collect() (RDD.scala:1122-1125) -> Array.concat -> List<Row> in-JVM'
              : 'RDD.collect() (RDD.scala:1122-1125) -> Array.concat(results: _*) -> Array[Row] in-JVM',
            fields: [
              { name: 'concatenation', value: 'RDD.collect() (RDD.scala:1122-1125) concatenates per-partition arrays' },
              { name: 'returnType', value: b.isJava ? 'Java List<Row>' : 'Scala Array[Row]' },
              { name: 'pythonBridge', value: '0 (direct JVM heap return)' },
            ],
            senderState: `Result delivered in-JVM to ${b.appEntryLabel} collect()`,
            receiverState: `${b.appEntryLabel} received concatenated ${b.isJava ? 'List<Row>' : 'Array[Row]'}`,
          });
        }
      }
    }
  }

  // =========================================================================
  // PHASE 9: Mid-Run Injected Recoverable & Degraded Faults
  // =========================================================================
  if (!state.abortedByFault) {
    injectPostWorkloadMidRunFault(b, state, { taskResultLostHandled });
  }

  // =========================================================================
  // V1 OUTPUT COMMIT STEP (For file-write jobs not already aborted)
  // =========================================================================
  if (job.hasFileCommit && !state.abortedByFault) {
    const commitConsumer = getConsumerPair();
    const ffw = b.workerStd('file-format-writer', commitConsumer.hostZone);
    const workerCommit = b.workerStd('file-commit-protocol', commitConsumer.hostZone);
    const driverCommit = b.driverCommitInstance();
    const commitNn = b.std('hdfs-namenode');

    if (cfg.fault === 'commit-job-failure') {
      state.abortedByFault = true;
      const sCommitFail = b.addStep({
        id: 'sim-step-commit-fail',
        title: 'All write tasks commit to _temporary/0/task_*, then driver commitJob() fails -> abortJob()',
        explanation:
          'Under Spark dada2a6 FileOutputCommitter algorithm 1 (SparkHadoopUtil.scala:517-526), every output task stages into _temporary/0/_temporary/attempt_* and commitTask() moves files into _temporary/0/task_*. Driver FileFormatWriter then invokes HadoopMapReduceCommitProtocol.commitJob() to move task files into the final directory, which throws an HDFS IOException. FileFormatWriter catches it, logs "Aborting job", calls best-effort abortJob(), and rethrows (NOT universal atomic rollback).',
        category: 'output-commit',
        keyState: 'All tasks committed _temporary/0/task_*, then driver commitJob FAILED -> best-effort abortJob()',
        command: 'FileFormatWriter.write() -> commitJob() throws -> committer.abortJob()',
        detailsPassed: [
          { name: 'committerAlgorithm', value: 'FileOutputCommitter algorithm 1 (SparkHadoopUtil.scala:517-526)' },
          { name: 'taskStatus', value: 'All tasks SUCCEEDED (_temporary/0/task_*)' },
          { name: 'failurePoint', value: 'Driver-side commitJob() moving task_* -> final dir on NameNode :8020' },
          { name: 'rollbackGuarantee', value: 'Best-effort cleanup only (not universal atomic rollback)' },
        ],
        deepDiveComponentId: 'file-format-writer',
      });
      b.addEvent({
        stepId: sCommitFail,
        from: ffw,
        to: workerCommit,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Task Commit (Algo 1)',
        port: `${commitConsumer.hostName} in-JVM`,
        label: 'commitTask() -> task_* OK',
        info: 'All write tasks stage in _temporary/0/_temporary/attempt_* and commitTask() moves output into _temporary/0/task_* under FileOutputCommitter algorithm 1.',
        size: 'in-memory',
        command: 'committer.commitTask(taskAttemptContext) // attempt_* -> _temporary/0/task_*',
        fields: [
          { name: 'tasksSucceeded', value: `${cfg.numPartitions}/${cfg.numPartitions}` },
          { name: 'taskCommittedDir', value: '_temporary/0/task_*' },
        ],
        senderState: 'Tasks finished cleanly',
        receiverState: 'Sent TaskCommitMessages to driver',
      });
      b.addEvent({
        stepId: sCommitFail,
        from: driverCommit,
        to: commitNn,
        channel: 'error',
        protocol: 'Hadoop RPC (commitJob -> IOException)',
        port: 'nn01:8020',
        label: 'commitJob() FAILS',
        info: 'Driver commitJob encounters NameNode permission/quota/IO failure while moving _temporary/0/task_* into final output directory.',
        size: 'IOException',
        command: 'HadoopMapReduceCommitProtocol.commitJob(jobContext, messages) // throws IOException',
        fields: [{ name: 'error', value: 'IOException during algorithm 1 final move' }],
        senderState: 'Entering catch block -> calling abortJob()',
        receiverState: 'Output directory may contain partial moves depending on committer',
        isError: true,
      });
      b.addEvent({
        stepId: sCommitFail,
        from: driverCommit,
        to: commitNn,
        channel: 'error',
        protocol: 'Hadoop RPC (abortJob cleanup)',
        port: 'nn01:8020',
        label: 'abortJob() cleanup',
        info: `FileFormatWriter invokes best-effort abortJob() to delete _temporary/ and propagates original exception to ${b.appEntryLabel}.`,
        size: '1.4 KB',
        command: 'committer.abortJob(jobContext); throw originalException',
        fields: [{ name: 'stagingCleanup', value: 'Best-effort delete of _temporary/' }],
        senderState: `Rethrew failure to ${b.appEntryLabel}`,
        receiverState: 'Removed staging directory (verify target path before retry)',
        isError: true,
      });
      emitTerminalAppOutcome(b, state, {
        origin: 'user-app-failure',
        diagnostic: 'Driver V1 commitJob() failed with HDFS IOException after best-effort abortJob()',
        stopLiveExecutors: true,
      });
    } else {
      const sCommit = b.addStep({
        id: 'sim-step-commit',
        title: 'V1 Parquet commit (FileOutputCommitter algo 1): commitTask -> commitJob',
        explanation:
          'Spark dada2a6 (SparkHadoopUtil.scala:517-526) explicitly sets mapreduce.fileoutputcommitter.algorithm.version=1. Tasks stage files into _temporary/0/_temporary/attempt_*, commitTask() moves them into task-committed dir _temporary/0/task_*, and driver FileFormatWriter calls commitJob() on NameNode :8020 to move task files into the final output directory, remove _temporary/, and write configurable _SUCCESS.',
        category: 'output-commit',
        keyState: 'Algorithm 1 commitJob moved _temporary/0/task_* into final dir & wrote _SUCCESS',
        command: 'HadoopMapReduceCommitProtocol.commitJob(jobContext, taskCommitMessages)',
        detailsPassed: [
          { name: 'committerAlgorithm', value: 'FileOutputCommitter algorithm 1 (SparkHadoopUtil.scala:517-526)' },
          { name: 'taskStaging', value: '_temporary/0/_temporary/attempt_* -> _temporary/0/task_*' },
          { name: 'markerFile', value: '_SUCCESS (configurable)' },
        ],
        deepDiveComponentId: 'file-commit-protocol',
      });
      b.addEvent({
        stepId: sCommit,
        from: ffw,
        to: workerCommit,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Task Commit (Algo 1)',
        port: `${commitConsumer.hostName} in-JVM`,
        label: 'commitTask() -> task_*',
        info: 'Tasks stage Parquet files in _temporary/0/_temporary/attempt_* and commitTask() moves them into _temporary/0/task_* under FileOutputCommitter algorithm 1.',
        size: 'in-memory',
        command: 'committer.commitTask(taskAttemptContext) // attempt_* -> _temporary/0/task_*',
        fields: [
          { name: 'partsCommitted', value: String(cfg.numPartitions) },
          { name: 'taskCommittedDir', value: '_temporary/0/task_*' },
        ],
        senderState: 'All write tasks succeeded',
        receiverState: 'Returning TaskCommitMessages to driver',
      });
      b.addEvent({
        stepId: sCommit,
        from: driverCommit,
        to: commitNn,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ClientProtocol)',
        port: 'nn01:8020',
        label: 'commitJob() + _SUCCESS',
        info: 'Driver FileCommitProtocol moves _temporary/0/task_* files into final output directory, cleans _temporary/, and writes configurable _SUCCESS on NameNode :8020.',
        size: '2.1 KB',
        command: 'committer.commitJob(jobContext) // _temporary/0/task_* -> final dir + _SUCCESS',
        fields: [
          { name: 'algorithm', value: 'FileOutputCommitter algorithm 1' },
          { name: 'finalPublish', value: 'Moved _temporary/0/task_* -> final dir & removed _temporary/' },
        ],
        senderState: `Write action returned cleanly to ${b.appEntryLabel}`,
        receiverState: 'Dataset published on HDFS',
      });
    }
  }

  // =========================================================================
  // ORDERLY SHUTDOWN (if not aborted by terminal fault)
  // =========================================================================
  if (!state.abortedByFault) {
    const stopCall = b.isJava ? 'spark.close()' : 'spark.stop()';
    const sStop = b.addStep({
      id: 'sim-step-shutdown',
      title: `${stopCall} halts ${state.liveExecs.length} live executor(s) & active AM unregisters SUCCEEDED`,
      explanation: `${b.appEntryLabel} calls ${b.isJava ? 'spark.close() -> SparkContext.stop(0)' : 'spark.stop()'}. CoarseGrainedSchedulerBackend sends StopExecutor ONLY to the ${state.liveExecs.length} live executor(s) (${state.liveExecs.join(', ')}), ${b.appEntryLabel} exits 0, and active AM (${state.activeAm}) unregisters FinalApplicationStatus.SUCCEEDED on RM :8030.`,
      category: 'yarn-lifecycle',
      keyState: `Stopped ${state.liveExecs.length} live executor(s) | YARN state=FINISHED, finalStatus=SUCCEEDED`,
      command: `${stopCall} -> StopExecutors -> amClient.unregister(SUCCEEDED)`,
      detailsPassed: [
        { name: 'liveExecutorsStopped', value: `${state.liveExecs.length} (${state.liveExecs.join(', ')})` },
        { name: 'activeAm', value: state.activeAm },
        { name: 'yarnState', value: 'FINISHED' },
        { name: 'finalStatus', value: 'SUCCEEDED' },
      ],
      deepDiveComponentId: 'application-master',
    });
    for (const liveExecId of state.liveExecs) {
      b.addEvent({
        stepId: sStop,
        from: state.schedBackend,
        to: liveExecId,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `StopExecutor(${liveExecId})`,
        info: `Driver sends StopExecutor to live executor ${liveExecId} so it releases its BlockManager and exits cleanly.`,
        size: '340 B',
        command: `driverEndpoint.askSync[Boolean](StopExecutors) // target=${liveExecId}`,
        fields: [
          { name: 'executorId', value: liveExecId },
          { name: 'stoppedLiveExecutors', value: String(state.liveExecs.length) },
        ],
        senderState: 'SparkContext stopping live executors',
        receiverState: `${liveExecId} releasing resources and exiting`,
      });
    }
    b.addEvent({
      stepId: sStop,
      from: state.activeAm,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: 'unregisterAM(SUCCEEDED)',
      info: `Active AM (${state.activeAm}) calls finishApplicationMaster(SUCCEEDED), cleans .sparkStaging/, and exits 0.`,
      size: '920 B',
      command: 'amClient.unregister(FinalApplicationStatus.SUCCEEDED, "")',
      fields: [
        { name: 'amInstance', value: state.activeAm },
        { name: 'finalStatus', value: 'SUCCEEDED' },
      ],
      senderState: 'AM JVM exiting',
      receiverState: 'RM marks app FINISHED/SUCCEEDED',
    });
  }

  return b.finalize(
    runId,
    runName,
    runSubtitle,
    runSummary,
    cfg.fault === 'none' ? job.short.toUpperCase() : fault.short.toUpperCase(),
    telemetry,
  );
}
