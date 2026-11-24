import type {
  ChannelKind,
  ComponentId,
  HostZoneId,
  SubsystemId,
  TraceField,
  TraceKind,
} from './types';

export type StepCategory =
  | 'yarn-lifecycle'
  | 'staging-localization'
  | 'driver-python-bridge'
  | 'sql-planning-aqe'
  | 'broadcast-torrent'
  | 'shuffle-internals'
  | 'python-worker-ipc'
  | 'heartbeats-metrics'
  | 'output-commit';

export interface StepCategoryMeta {
  id: StepCategory;
  label: string;
  short: string;
  description: string;
  defaultHighLevel: boolean;
  defaultStandard: boolean;
}

export const STEP_CATEGORIES: StepCategoryMeta[] = [
  {
    id: 'yarn-lifecycle',
    label: 'YARN Submission & Containers',
    short: 'YARN Lifecycle',
    description: 'RM application ID, AM launch, container grants, executor registration, and shutdown.',
    defaultHighLevel: true,
    defaultStandard: true,
  },
  {
    id: 'staging-localization',
    label: 'HDFS Staging & Localization',
    short: 'Staging / Cache',
    description: 'Uploading __app__.jar, pyspark.zip, __spark_conf__.zip to .sparkStaging and NM localization.',
    defaultHighLevel: false,
    defaultStandard: true,
  },
  {
    id: 'driver-python-bridge',
    label: 'Driver Python & Py4J Bridge',
    short: 'Py4J / Driver',
    description: 'deploy.PythonRunner, Py4JServer loopback auth secret, SparkSession/SparkContext init.',
    defaultHighLevel: true,
    defaultStandard: true,
  },
  {
    id: 'sql-planning-aqe',
    label: 'SQL Catalyst Planning & AQE',
    short: 'SQL / AQE',
    description: 'Logical/physical query planning, DAG stage splits, TaskScheduler offers, and AQE re-optimization.',
    defaultHighLevel: true,
    defaultStandard: true,
  },
  {
    id: 'broadcast-torrent',
    label: 'Broadcast & TorrentBroadcast',
    short: 'TorrentBroadcast',
    description: '4 MiB piece chunking, driver BlockManager publish, randomized P2P block fetching, and HashedRelation.',
    defaultHighLevel: false,
    defaultStandard: false,
  },
  {
    id: 'shuffle-internals',
    label: 'Shuffle Files, Tracker & Fetch',
    short: 'Shuffle Internals',
    description: 'SortShuffleWriter spill/merge, .data/.index commit, MapOutputTracker, FetchShuffleBlocks, ESS.',
    defaultHighLevel: true,
    defaultStandard: true,
  },
  {
    id: 'python-worker-ipc',
    label: 'Executor Python Worker IPC',
    short: 'Python Worker IPC',
    description: 'PythonWorkerFactory, pyspark.daemon fork/reuse, local socket framing, pickle/Arrow batches.',
    defaultHighLevel: false,
    defaultStandard: true,
  },
  {
    id: 'heartbeats-metrics',
    label: 'Heartbeats, Events & UI',
    short: 'Heartbeats / UI',
    description: 'Executor heartbeats, LiveListenerBus queues, EventLoggingListener, SparkUI, History Server.',
    defaultHighLevel: false,
    defaultStandard: false,
  },
  {
    id: 'output-commit',
    label: 'V1 Output Commit & HDFS',
    short: 'Output Commit',
    description: 'OutputCommitCoordinator.canCommit, Hadoop commitTask, FileFormatWriter commitJob/abortJob.',
    defaultHighLevel: true,
    defaultStandard: true,
  },
];

export const ALL_STEP_CATEGORIES: ReadonlySet<StepCategory> = new Set(
  STEP_CATEGORIES.map(c => c.id),
);

export const HIGH_LEVEL_CATEGORIES: ReadonlySet<StepCategory> = new Set(
  STEP_CATEGORIES.filter(c => c.defaultHighLevel).map(c => c.id),
);

export const STANDARD_CATEGORIES: ReadonlySet<StepCategory> = new Set(
  STEP_CATEGORIES.filter(c => c.defaultStandard).map(c => c.id),
);

export const CONTROL_ONLY_CATEGORIES: ReadonlySet<StepCategory> = new Set<StepCategory>([
  'yarn-lifecycle',
  'sql-planning-aqe',
]);

export type CategoryPresetId = 'all' | 'standard' | 'high-level' | 'control-only' | 'custom';

export interface CategoryPresetMeta {
  id: Exclude<CategoryPresetId, 'custom'>;
  label: string;
  description: string;
  categories: ReadonlySet<StepCategory>;
}

export const CATEGORY_PRESETS: CategoryPresetMeta[] = [
  {
    id: 'all',
    label: 'All Internals',
    description: 'Include every step category (TorrentBroadcast, staging, Py4J, shuffle, Python IPC, heartbeats, commit).',
    categories: ALL_STEP_CATEGORIES,
  },
  {
    id: 'standard',
    label: 'Standard',
    description: 'Core lifecycle, staging, SQL, shuffle, Python IPC, and commit; skips TorrentBroadcast chunks and heartbeats/UI.',
    categories: STANDARD_CATEGORIES,
  },
  {
    id: 'high-level',
    label: 'High-Level',
    description: 'Macro control & data flow; skips HDFS staging, TorrentBroadcast internals, Python worker IPC, and heartbeats.',
    categories: HIGH_LEVEL_CATEGORIES,
  },
  {
    id: 'control-only',
    label: 'Control Only',
    description: 'Only YARN container lifecycle and SQL/DAG scheduling control flow.',
    categories: CONTROL_ONLY_CATEGORIES,
  },
];

export function detectCategoryPreset(enabled: ReadonlySet<StepCategory>): CategoryPresetId {
  for (const preset of CATEGORY_PRESETS) {
    if (
      preset.categories.size === enabled.size &&
      [...preset.categories].every(cat => enabled.has(cat))
    ) {
      return preset.id;
    }
  }
  return 'custom';
}

export const STEP_CATEGORY_MAP: Record<StepCategory, StepCategoryMeta> = Object.fromEntries(
  STEP_CATEGORIES.map(c => [c.id, c]),
) as Record<StepCategory, StepCategoryMeta>;

export interface RuntimeInstance {
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

export interface RunStep {
  id: string;
  parentId?: string;
  title: string;
  explanation: string;
  category: StepCategory;
  keyState?: string;
  command?: string;
  detailsPassed?: TraceField[];
  deepDiveComponentId?: ComponentId;
  /** Marks a macro step that summarizes child sub-steps when collapsed. */
  isMacro?: boolean;
}

export interface RunEvent {
  id: string;
  stepId: string;
  kind: 'wire' | 'local';
  categoryOverride?: StepCategory;
  fromInstance: string;
  toInstance: string;
  fromComponent: ComponentId;
  toComponent: ComponentId;
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
  /** Shown on All-54 map when two distinct runtime instances share the same ComponentId (e.g. 'E1 -> E2'). */
  peerTag?: string;
}

export interface RunTelemetry {
  totalSlots: number;
  idealWavesPerStage: number;
  containers: number;
  numExecutors: number;
  executorCores: number;
  numPartitions: number;
  externalShuffleService: boolean;
  dynamicAllocation: boolean;
  aqeEnabled: boolean;
  faultLabel: string;
  actionSummary: string;
}

export interface ExecutionRun {
  id: string;
  sourceKind: 'catalog' | 'simulation';
  traceKind: TraceKind;
  name: string;
  subtitle: string;
  summary: string;
  deployMode: 'cluster' | 'client' | 'both';
  layerTag: string;
  componentId?: ComponentId;
  scenarioNumber?: number;
  subsystem?: SubsystemId;
  instances: Record<string, RuntimeInstance>;
  focusInstances: string[];
  steps: RunStep[];
  events: RunEvent[];
  telemetry?: RunTelemetry;
}

export interface ProjectedStep {
  step: RunStep;
  depth: number;
  displayNumber: number;
  hasChildren: boolean;
  isExpanded: boolean;
  childCount: number;
  firstFrameIndex: number;
  frameIndices: number[];
}

export interface ProjectedFrame {
  frameIndex: number;
  canonicalEventIndex: number;
  event: RunEvent;
  step: RunStep;
  ancestors: RunStep[];
  visibleStepIndex: number;
  displayStepNumber: number;
  effectiveCategory: StepCategory;
  isSummaryFrame: boolean;
}

export interface SkippedSpan {
  id: string;
  /** Appears right before this frameIndex (-1 means after the last visible frame, or the whole run is empty). */
  beforeFrameIndex: number;
  categories: StepCategory[];
  eventCount: number;
  stepIds: string[];
}

export interface ProjectedTimeline {
  runId: string;
  visibleSteps: ProjectedStep[];
  visibleFrames: ProjectedFrame[];
  skippedSpans: SkippedSpan[];
  frameIndexByEventId: Map<string, number>;
  categoryCounts: Record<StepCategory, number>;
  totalCanonicalEvents: number;
  /** Expanded macro step IDs whose descendants produced 0 visible events under current category filters. */
  emptyExpandedStepIds: string[];
}

export interface DeepStepEnrichment {
  traceId: string;
  extraInstances?: RuntimeInstance[];
  /**
   * Maps a top-level step index (0-based in TraceLesson.steps) to its hierarchical
   * descendant RunSteps and RunEvents. Descendant steps may themselves nest further
   * via parentId (e.g., 3-level trees).
   */
  expansions: Record<
    number,
    {
      steps: RunStep[];
      events: RunEvent[];
    }
  >;
}

export interface RunCanvasProps {
  run: ExecutionRun;
  projection: ProjectedTimeline;
  activeEventId: string | null;
  progress: number;
  reducedMotion: boolean;
  selectedComponentId: ComponentId | null;
  onSelectComponent: (id: ComponentId) => void;
  onSelectEvent: (eventId: string) => void;
}
