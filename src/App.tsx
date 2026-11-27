import { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  ArrowLeft,
  ChevronLeft,
  ChevronRight,
  Eye,
  FlaskConical,
  Keyboard,
  Layers,
  ListOrdered,
  Map as MapIcon,
  Maximize,
  Minus,
  Moon,
  Network,
  PanelLeft,
  Plus,
  RotateCcw,
  Search,
  SlidersHorizontal,
  Sun,
  X,
} from 'lucide-react';
import CatalogSidebar from './components/CatalogSidebar';
import CategoryFilterBar from './components/CategoryFilterBar';
import ClusterMapCanvas from './components/ClusterMapCanvas';
import InspectorPanel from './components/InspectorPanel';
import type { InspectorTab } from './components/InspectorPanel';
import MessageTable from './components/MessageTable';
import SequenceCanvas from './components/SequenceCanvas';
import SimulatorPanel from './components/SimulatorPanel';
import TopologyCanvas from './components/TopologyCanvas';
import { COMPONENTS, COMPONENT_LIST } from './data/components';
import { resolveTraceForLanguage } from './data/jvmTraceOverrides';
import {
  ALL_STEP_CATEGORIES,
  CATEGORY_PRESETS,
  STEP_CATEGORIES,
  STEP_CATEGORY_MAP,
} from './data/runModel';
import type {
  CategoryPresetId,
  ExecutionRun,
  ProjectedStep,
  ProjectedTimeline,
  RunCanvasProps,
  RunEvent,
  StepCategory,
} from './data/runModel';
import {
  ALL_TRACES,
  COMPONENT_TRACES,
  COMPONENT_TRACE_BY_ID,
  LIFECYCLE_TRACES,
  SCENARIO_TRACES,
  TRACE_BY_ID,
} from './data/traces';
import { CHANNEL_META, SPARK_LANGUAGES, SUBSYSTEM_META } from './data/types';
import type {
  ChannelKind,
  ComponentId,
  SparkLanguage,
  SubsystemId,
  TraceKind,
} from './data/types';
import { adaptTraceToRun } from './engine/adaptTrace';
import { isDriverRuntimeInstance } from './engine/hostPlacement';
import {
  collectStepIdsToExpandForStep,
  computeDeepOnlyCategoryCounts,
  computeHiddenCategoriesForStep,
  getExpandableStepIds,
  projectRun,
  reconcileSameTraceLanguageTransition,
  resolveEventAfterProjectionChange,
  resolveEventAfterToggleExpand,
  stepJumpEventId,
} from './engine/projection';
import {
  buildSimulatedRun,
  resolveValidSimulationConfig,
} from './engine/simulator';
import {
  DEFAULT_SIMULATION_CONFIG,
  FAULT_MAP,
  JOB_TYPE_MAP,
  adaptSimConfigForLanguage,
  validateSimulationConfig,
} from './engine/simulatorTypes';
import type { SimulationConfig } from './engine/simulatorTypes';
import {
  encodeCatalogHash,
  encodeSimHash,
  parseSimHash,
  parseUrlHash,
  resolveExitSimulationTarget,
  resolveInitialCatalogTraceId,
  resolveLanguageAfterTraceSelect,
  resolveLanguageVariantTraceId,
} from './engine/urlRoute';
import type { CanvasView } from './engine/urlRoute';
import { advanceTimeline } from './playback';

export { encodeSimHash, parseSimHash };

type ModalKind = 'search' | 'shortcuts' | null;
type PlaneId = 'yarn' | 'driver' | 'rpc' | 'data' | 'python' | 'hdfs';

interface PlaneMeta {
  id: PlaneId;
  label: string;
  description: string;
}

interface ReturnStackEntry {
  traceId: string;
  traceName: string;
  catalogTraceId?: string;
  run?: ExecutionRun;
  simConfig?: SimulationConfig;
  eventId: string | null;
  view: CanvasView;
  language: SparkLanguage;
  expandedStepIds: string[];
  enabledCategories: Set<StepCategory>;
}

interface PlayScope {
  start: number;
  end: number;
}

const ARCHITECTURE_PLANES: PlaneMeta[] = [
  { id: 'yarn', label: 'YARN', description: 'YARN ResourceManager, NodeManager, AM & containers' },
  { id: 'driver', label: 'Driver', description: 'Spark Driver JVM, SparkContext, SQL Planner & schedulers' },
  { id: 'rpc', label: 'RPC', description: 'Spark Netty RPC control plane & heartbeats' },
  { id: 'data', label: 'Data', description: 'BlockManager, Shuffle, Broadcast & data transfer plane' },
  { id: 'python', label: 'Python', description: 'Py4J Gateway, Python driver & worker sockets' },
  { id: 'hdfs', label: 'HDFS', description: 'HDFS NameNode/DataNode staging, data & event logs' },
];

const CHANNEL_ORDER: ChannelKind[] = [
  'yarn',
  'rpc',
  'data',
  'python',
  'internal',
  'error',
];

const SPEEDS = [0.5, 1, 1.5, 2] as const;

const YARN_COMPONENTS = new Set<ComponentId>([
  'yarn-client',
  'resource-manager',
  'node-manager',
  'yarn-container',
  'application-master',
  'yarn-allocator',
  'yarn-scheduler-backend',
  'executor-allocation-manager',
  'executor-monitor',
  'delegation-token-manager',
  'health-tracker',
  'block-manager-decommissioner',
]);

const RPC_COMPONENTS = new Set<ComponentId>([
  'rpc-env',
  'rpc-file-server',
  'transport-context',
  'delegation-token-manager',
  'driver-scheduler-backend',
  'executor-backend',
  'heartbeat-receiver',
  'block-manager-master',
  'map-output-tracker-master',
  'map-output-tracker-worker',
  'context-cleaner',
  'output-commit-coordinator',
]);

const DATA_COMPONENTS = new Set<ComponentId>([
  'block-manager',
  'block-transfer-service',
  'memory-manager',
  'serializer-manager',
  'kryo-registrator',
  'context-cleaner',
  'block-manager-decommissioner',
  'transport-context',
  'rdd-cache',
  'columnar-cache',
  'broadcast-manager',
  'shuffle-exchange',
  'shuffle-writer',
  'shuffle-reader',
  'shuffle-block-resolver',
  'external-shuffle-service',
  'push-based-shuffle',
  'jvm-partitioner-pair-rdd',
  'broadcast-exchange',
  'broadcast-hash-join',
  'sort-merge-join',
  'hash-aggregate',
  'window-sort-exec',
  'tungsten-unsafe-row',
  'janino-code-generator',
  'whole-stage-codegen',
  'dataset-encoder-udf',
  'typed-dataset-ops',
  'key-value-grouped-dataset',
  'scala-java-udf',
  'typed-aggregator',
  'file-format-writer',
  'file-commit-protocol',
  'datasource-v2',
]);

const PYTHON_COMPONENTS = new Set<ComponentId>([
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

const HDFS_COMPONENTS = new Set<ComponentId>([
  'hdfs-namenode',
  'hdfs-datanode',
  'delegation-token-manager',
  'event-log',
  'history-server',
  'file-format-writer',
  'file-commit-protocol',
  'datasource-v2',
]);

const THEME_STORAGE_KEY = 'spark-visualizer-theme';

function readTheme(): 'dark' | 'light' {
  try {
    return localStorage.getItem(THEME_STORAGE_KEY) === 'light' ? 'light' : 'dark';
  } catch {
    return 'dark';
  }
}

function planesForEvent(event: RunEvent, run: ExecutionRun): Set<PlaneId> {
  const planes = new Set<PlaneId>();
  const endpoints: ComponentId[] = [event.fromComponent, event.toComponent];

  if (event.channel === 'yarn' || endpoints.some((id) => YARN_COMPONENTS.has(id))) {
    planes.add('yarn');
  }
  if (
    isDriverRuntimeInstance(run.instances[event.fromInstance]) ||
    isDriverRuntimeInstance(run.instances[event.toInstance])
  ) {
    planes.add('driver');
  }
  if (event.channel === 'rpc' || endpoints.some((id) => RPC_COMPONENTS.has(id))) {
    planes.add('rpc');
  }
  if (event.channel === 'data' || endpoints.some((id) => DATA_COMPONENTS.has(id))) {
    planes.add('data');
  }
  if (event.channel === 'python' || endpoints.some((id) => PYTHON_COMPONENTS.has(id))) {
    planes.add('python');
  }
  if (
    endpoints.some((id) => HDFS_COMPONENTS.has(id)) ||
    /hdfs|event\s*log/i.test(event.protocol)
  ) {
    planes.add('hdfs');
  }
  return planes;
}

function planesForProjection(
  projection: ProjectedTimeline,
  run: ExecutionRun,
): Set<PlaneId> {
  const planes = new Set<PlaneId>();
  for (const frame of projection.visibleFrames) {
    for (const p of planesForEvent(frame.event, run)) planes.add(p);
  }
  return planes;
}

function collectDescendantStepIds(run: ExecutionRun, stepId: string): string[] {
  const childrenByParent = new Map<string, string[]>();
  for (const step of run.steps) {
    if (step.parentId) {
      const list = childrenByParent.get(step.parentId) ?? [];
      list.push(step.id);
      childrenByParent.set(step.parentId, list);
    }
  }
  const descendants: string[] = [];
  const stack = [...(childrenByParent.get(stepId) ?? [])];
  const seen = new Set<string>([stepId]);
  while (stack.length > 0) {
    const cur = stack.pop()!;
    if (seen.has(cur)) continue;
    seen.add(cur);
    descendants.push(cur);
    for (const kid of childrenByParent.get(cur) ?? []) {
      stack.push(kid);
    }
  }
  return descendants;
}

function simConfigsEqual(a: SimulationConfig, b: SimulationConfig): boolean {
  return (
    a.language === b.language &&
    a.jobType === b.jobType &&
    a.dataProfile === b.dataProfile &&
    a.deployMode === b.deployMode &&
    a.numExecutors === b.numExecutors &&
    a.executorCores === b.executorCores &&
    a.numPartitions === b.numPartitions &&
    a.externalShuffleService === b.externalShuffleService &&
    a.dynamicAllocation === b.dynamicAllocation &&
    a.aqeEnabled === b.aqeEnabled &&
    a.pythonUdfMode === b.pythonUdfMode &&
    a.fault === b.fault
  );
}

interface SearchEntry {
  key: string;
  title: string;
  subtitle: string;
  badge: string;
  kind: 'lifecycle' | 'scenario' | 'component' | 'step';
  traceId: string;
  stepId: string | null;
  componentId?: ComponentId;
  searchText: string;
}

const DEFAULT_TRACE =
  TRACE_BY_ID['flow-cluster-lifecycle'] ?? LIFECYCLE_TRACES[0] ?? ALL_TRACES[0];

export default function App() {
  const initialReducedMotion = useMemo(
    () => window.matchMedia('(prefers-reduced-motion: reduce)').matches,
    [],
  );

  const initialBoot = useMemo(() => {
    const parsed = parseUrlHash(
      window.location.hash,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    );
    const initialCatalogTraceId = resolveInitialCatalogTraceId(
      parsed,
      window.history.state,
      TRACE_BY_ID,
      DEFAULT_TRACE.id,
    );
    const trace = TRACE_BY_ID[initialCatalogTraceId] ?? DEFAULT_TRACE;
    const simConfig = parsed.kind === 'sim' ? parsed.simConfig : null;
    const simRun = simConfig ? buildSimulatedRun(simConfig) : null;
    const run = simRun ?? adaptTraceToRun(trace, parsed.language);
    const proj = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
    return {
      parsed,
      trace,
      simConfig,
      simRun,
      firstEventId: proj.visibleFrames[0]?.event.id ?? null,
    };
  }, []);

  const [traceId, setTraceId] = useState<string>(initialBoot.trace.id);
  const [activeLanguage, setActiveLanguage] = useState<SparkLanguage>(
    initialBoot.parsed.language,
  );
  const [catalogMode, setCatalogMode] = useState<TraceKind>(initialBoot.trace.kind);
  const [subsystemFilter, setSubsystemFilter] = useState<SubsystemId | 'all'>('all');
  const [view, setView] = useState<CanvasView>(initialBoot.parsed.view);
  const [theme, setTheme] = useState<'dark' | 'light'>(readTheme);
  const [sidebarOpen, setSidebarOpen] = useState(
    () => !window.matchMedia('(max-width: 760px)').matches,
  );
  const [diagramOnly, setDiagramOnly] = useState(false);
  const [floatingInspectorOpen, setFloatingInspectorOpen] = useState(false);
  const [inspectorTab, setInspectorTab] = useState<InspectorTab>('step');
  const [selectedComponentId, setSelectedComponentId] = useState<ComponentId | null>(
    initialBoot.simRun
      ? (initialBoot.simRun.componentId ?? null)
      : (initialBoot.trace.componentId ?? null),
  );

  const [expandedStepIds, setExpandedStepIds] = useState<Set<string>>(() => new Set());
  const [enabledCategories, setEnabledCategories] = useState<Set<StepCategory>>(
    () => new Set(ALL_STEP_CATEGORIES),
  );
  const [categoryBarOpen, setCategoryBarOpen] = useState<boolean>(false);

  const [simulatorOpen, setSimulatorOpen] = useState<boolean>(false);
  const [simDraft, setSimDraft] = useState<SimulationConfig>(() =>
    initialBoot.simConfig
      ? { ...initialBoot.simConfig }
      : adaptSimConfigForLanguage(
          DEFAULT_SIMULATION_CONFIG,
          initialBoot.parsed.language,
        ),
  );
  const [activeSimConfig, setActiveSimConfig] = useState<SimulationConfig | null>(
    initialBoot.simConfig,
  );
  const [activeSimRun, setActiveSimRun] = useState<ExecutionRun | null>(
    initialBoot.simRun,
  );

  const [activeEventId, setActiveEventId] = useState<string | null>(
    initialBoot.firstEventId,
  );
  const [progress, setProgress] = useState<number>(
    initialBoot.firstEventId ? 1 : 0,
  );
  const [isReadyAtStart, setIsReadyAtStart] = useState<boolean>(
    initialBoot.firstEventId !== null,
  );
  const [playing, setPlaying] = useState<boolean>(false);
  const [speed, setSpeed] = useState<number>(1);
  const [zoom, setZoom] = useState<number>(1);
  const [reducedMotion, setReducedMotion] = useState<boolean>(initialReducedMotion);
  const [returnStack, setReturnStack] = useState<ReturnStackEntry[]>([]);
  const [modal, setModal] = useState<ModalKind>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchIndex, setSearchIndex] = useState(0);
  const [notice, setNotice] = useState('');

  const playbackRef = useRef<{ eventId: string | null; progress: number }>({
    eventId: initialBoot.firstEventId,
    progress: initialBoot.firstEventId ? 1 : 0,
  });
  const isReadyAtStartRef = useRef<boolean>(initialBoot.firstEventId !== null);
  const playScopeRef = useRef<PlayScope | null>(null);
  const speedRef = useRef<number>(speed);
  speedRef.current = speed;
  const navHistoryModeRef = useRef<'replace' | 'push'>('replace');
  const lastAppliedHashRef = useRef<string>(window.location.hash);

  const dialogRef = useRef<HTMLDialogElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  const selectedTrace = TRACE_BY_ID[traceId] ?? DEFAULT_TRACE;
  const catalogRun = useMemo(
    () => adaptTraceToRun(selectedTrace, activeLanguage),
    [selectedTrace, activeLanguage],
  );
  const activeRun: ExecutionRun = activeSimRun ?? catalogRun;

  const projection = useMemo(
    () => projectRun(activeRun, expandedStepIds, enabledCategories),
    [activeRun, expandedStepIds, enabledCategories],
  );

  const activeFrameIndex = activeEventId
    ? (projection.frameIndexByEventId.get(activeEventId) ?? -1)
    : -1;
  const activeFrame =
    activeFrameIndex >= 0 ? projection.visibleFrames[activeFrameIndex] : null;
  const activeProjectedStep = activeFrame
    ? (projection.visibleSteps[activeFrame.visibleStepIndex] ?? null)
    : null;
  const activeStepObj = activeFrame?.step ?? null;
  const isLocalEvent = activeFrame?.event.kind === 'local';

  const expandableStepIds = useMemo(
    () => getExpandableStepIds(activeRun),
    [activeRun],
  );
  const allExpandedStepCount = useMemo(() => {
    if (expandableStepIds.length === 0) return projection.visibleSteps.length;
    return projectRun(
      activeRun,
      new Set(expandableStepIds),
      enabledCategories,
    ).visibleSteps.length;
  }, [activeRun, expandableStepIds, enabledCategories, projection.visibleSteps.length]);
  const isHighLevelDepth = expandedStepIds.size === 0;
  const isAllExpandedDepth =
    expandableStepIds.length > 0 &&
    expandedStepIds.size >= expandableStepIds.length &&
    expandableStepIds.every((id) => expandedStepIds.has(id));
  const emptyExpandedStepIds = projection.emptyExpandedStepIds ?? [];

  const deepOnlyCategoryCounts = useMemo(
    () => computeDeepOnlyCategoryCounts(activeRun, expandedStepIds),
    [activeRun, expandedStepIds],
  );
  const totalSkippedEvents = useMemo(
    () => projection.skippedSpans.reduce((sum, span) => sum + span.eventCount, 0),
    [projection.skippedSpans],
  );
  const skippedCategoryIds = useMemo<StepCategory[]>(() => {
    if (projection.skippedSpans.length === 0) return [];
    const seen = new Set<StepCategory>();
    for (const span of projection.skippedSpans) {
      for (const cat of span.categories) seen.add(cat);
    }
    return STEP_CATEGORIES.map((c) => c.id).filter((id) => seen.has(id));
  }, [projection.skippedSpans]);
  const emptyExpandedHiddenCategories = useMemo<StepCategory[]>(() => {
    if (emptyExpandedStepIds.length === 0) return [];
    const hidden = new Set<StepCategory>();
    for (const stepId of emptyExpandedStepIds) {
      for (const cat of computeHiddenCategoriesForStep(
        activeRun,
        stepId,
        enabledCategories,
        expandedStepIds,
      )) {
        hidden.add(cat);
      }
    }
    return STEP_CATEGORIES.map((c) => c.id).filter((id) => hidden.has(id));
  }, [activeRun, emptyExpandedStepIds, enabledCategories, expandedStepIds]);
  const isCategoryFiltered = enabledCategories.size < STEP_CATEGORIES.length;

  const activeMessagePlanes = useMemo(
    () => (activeFrame ? planesForEvent(activeFrame.event, activeRun) : new Set<PlaneId>()),
    [activeFrame, activeRun],
  );
  const runPlanes = useMemo(
    () => planesForProjection(projection, activeRun),
    [projection, activeRun],
  );

  function updateReadyAtStart(ready: boolean) {
    isReadyAtStartRef.current = ready;
    setIsReadyAtStart(ready);
  }

  function commitPlayback(nextEventId: string | null, nextProgress: number) {
    playbackRef.current = { eventId: nextEventId, progress: nextProgress };
    setActiveEventId(nextEventId);
    setProgress(nextProgress);
  }

  function playStep(
    targetStep: ProjectedStep,
    targetProjection: ProjectedTimeline = projection,
  ) {
    const start = targetStep.firstFrameIndex;
    const end =
      targetStep.frameIndices[targetStep.frameIndices.length - 1] ?? start;
    const firstEventId = targetProjection.visibleFrames[start]?.event.id ?? null;
    if (!firstEventId) return;

    updateReadyAtStart(false);
    playScopeRef.current = { start, end };
    if (reducedMotion) {
      setPlaying(false);
      commitPlayback(firstEventId, 1);
    } else {
      commitPlayback(firstEventId, 0);
      setPlaying(true);
    }
  }

  // Keep activeEventId synchronized whenever run or projection changes externally
  useEffect(() => {
    if (projection.visibleFrames.length === 0) {
      if (activeEventId !== null || progress !== 0) {
        playScopeRef.current = null;
        updateReadyAtStart(false);
        commitPlayback(null, 0);
      }
      if (playing) setPlaying(false);
      return;
    }
    if (!activeEventId || !projection.frameIndexByEventId.has(activeEventId)) {
      const resolved = resolveEventAfterProjectionChange(
        activeEventId,
        activeRun,
        projection,
      );
      playScopeRef.current = null;
      setPlaying(false);
      commitPlayback(resolved, resolved ? 1 : 0);
      const resolvedIdx = resolved
        ? (projection.frameIndexByEventId.get(resolved) ?? -1)
        : -1;
      updateReadyAtStart(activeEventId === null && resolvedIdx === 0);
    }
  }, [activeRun, projection, activeEventId, progress, playing]);

  // Build search index once across all lifecycle flows, scenarios, components, and steps
  const catalogSearchEntries = useMemo<SearchEntry[]>(() => {
    const entries: SearchEntry[] = [];
    for (const trace of LIFECYCLE_TRACES) {
      const resolved = resolveTraceForLanguage(trace, activeLanguage);
      entries.push({
        key: `trace-${trace.id}`,
        title: resolved.name,
        subtitle: resolved.subtitle,
        badge: 'LIFECYCLE',
        kind: 'lifecycle',
        traceId: trace.id,
        stepId: null,
        searchText: `${resolved.name} ${resolved.subtitle} ${resolved.summary} ${resolved.layerTag} ${trace.name} lifecycle`.toLowerCase(),
      });
    }
    for (const trace of SCENARIO_TRACES) {
      const resolved = resolveTraceForLanguage(trace, activeLanguage);
      const num = trace.scenarioNumber ?? 0;
      entries.push({
        key: `trace-${trace.id}`,
        title: resolved.name,
        subtitle: resolved.subtitle,
        badge: `SCENARIO #${String(num).padStart(2, '0')}`,
        kind: 'scenario',
        traceId: trace.id,
        stepId: null,
        searchText: `${resolved.name} ${resolved.subtitle} ${resolved.summary} ${resolved.layerTag} ${trace.name} scenario ${num}`.toLowerCase(),
      });
    }
    for (const comp of COMPONENT_LIST) {
      const compTrace =
        COMPONENT_TRACE_BY_ID[comp.id] ?? TRACE_BY_ID[`comp-${comp.id}`];
      if (!compTrace) continue;
      entries.push({
        key: `comp-${comp.id}`,
        title: comp.name,
        subtitle: `${SUBSYSTEM_META[comp.subsystem].label} · ${comp.process}${comp.port ? ` (${comp.port})` : ''}`,
        badge: 'COMPONENT',
        kind: 'component',
        traceId: compTrace.id,
        stepId: null,
        componentId: comp.id,
        searchText: `${comp.name} ${comp.shortName} ${comp.id} ${comp.role} ${comp.process} ${comp.port ?? ''} ${SUBSYSTEM_META[comp.subsystem].label}`.toLowerCase(),
      });
    }
    for (const trace of ALL_TRACES) {
      const run = adaptTraceToRun(trace, activeLanguage);
      run.steps.forEach((step, idx) => {
        entries.push({
          key: `step-${step.id}`,
          title: step.title,
          subtitle: `${run.name} - Step ${idx + 1} of ${run.steps.length}`,
          badge: `STEP ${String(idx + 1).padStart(2, '0')}`,
          kind: 'step',
          traceId: trace.id,
          stepId: step.id,
          componentId: trace.componentId,
          searchText: `${step.title} ${step.explanation} ${step.keyState ?? ''} ${step.command ?? ''} ${run.name}`.toLowerCase(),
        });
      });
    }
    return entries;
  }, [activeLanguage]);

  const allSearchEntries = useMemo<SearchEntry[]>(() => {
    if (activeRun.sourceKind !== 'simulation') return catalogSearchEntries;
    const simStepEntries: SearchEntry[] = activeRun.steps.map((step, idx) => ({
      key: `step-${activeRun.id}-${step.id}`,
      title: step.title,
      subtitle: `${activeRun.name} · Step ${idx + 1} of ${activeRun.steps.length}`,
      badge: `STEP ${String(idx + 1).padStart(2, '0')}`,
      kind: 'step',
      traceId: activeRun.id,
      stepId: step.id,
      componentId: activeRun.componentId,
      searchText: `${step.title} ${step.explanation} ${step.keyState ?? ''} ${step.command ?? ''} ${activeRun.name}`.toLowerCase(),
    }));
    return [...simStepEntries, ...catalogSearchEntries];
  }, [activeRun, catalogSearchEntries]);

  const searchResults = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    if (!q) {
      return allSearchEntries.filter((e) => e.kind !== 'step');
    }
    const tokens = q.split(/\s+/).filter(Boolean);
    return allSearchEntries.filter((entry) =>
      tokens.every((tok) => entry.searchText.includes(tok)),
    );
  }, [allSearchEntries, searchQuery]);

  const visibleSearchResults = useMemo(
    () => searchResults.slice(0, 60),
    [searchResults],
  );

  function selectTrace(
    id: string,
    opts?: {
      componentId?: ComponentId | null;
      stepId?: string;
      tab?: InspectorTab;
      closeMobile?: boolean;
      preserveReturnStack?: boolean;
      animate?: boolean;
      languageOverride?: SparkLanguage;
      historyMode?: 'replace' | 'push';
    },
  ) {
    const target = TRACE_BY_ID[id] ?? DEFAULT_TRACE;
    const nextLanguage: SparkLanguage = opts?.languageOverride
      ? opts.languageOverride
      : TRACE_BY_ID[id]
        ? resolveLanguageAfterTraceSelect(target, activeLanguage)
        : activeLanguage;
    const isCurrentRun =
      id === activeRun.id && nextLanguage === activeLanguage;
    const nextRun = isCurrentRun
      ? activeRun
      : adaptTraceToRun(target, nextLanguage);

    if (
      opts?.historyMode !== 'replace' &&
      (!isCurrentRun || nextLanguage !== activeLanguage)
    ) {
      navHistoryModeRef.current = 'push';
    }

    const nextExpanded = new Set<string>();
    if (opts?.stepId) {
      for (const ancestorId of collectStepIdsToExpandForStep(nextRun, opts.stepId)) {
        nextExpanded.add(ancestorId);
      }
    }

    let nextCategories = enabledCategories;
    let nextProj = projectRun(nextRun, nextExpanded, enabledCategories);
    let reenabledCategories: StepCategory[] = [];

    if (
      opts?.stepId &&
      !nextProj.visibleSteps.some((ps) => ps.step.id === opts.stepId)
    ) {
      const targetRunStep = nextRun.steps.find((s) => s.id === opts.stepId);
      const stepEvents = nextRun.events.filter((ev) => ev.stepId === opts.stepId);
      const neededCats = new Set<StepCategory>();
      for (const ev of stepEvents) {
        const eff = ev.categoryOverride ?? targetRunStep?.category;
        if (eff && !enabledCategories.has(eff)) {
          neededCats.add(eff);
        }
      }
      if (
        neededCats.size === 0 &&
        targetRunStep &&
        !enabledCategories.has(targetRunStep.category)
      ) {
        neededCats.add(targetRunStep.category);
      }
      if (neededCats.size > 0) {
        nextCategories = new Set(enabledCategories);
        for (const c of neededCats) nextCategories.add(c);
        reenabledCategories = STEP_CATEGORIES.map((c) => c.id).filter((catId) =>
          neededCats.has(catId),
        );
        nextProj = projectRun(nextRun, nextExpanded, nextCategories);
      }
    }

    const targetStep: ProjectedStep | null = opts?.stepId
      ? (nextProj.visibleSteps.find((ps) => ps.step.id === opts.stepId) ??
        nextProj.visibleSteps[0] ??
        null)
      : (nextProj.visibleSteps[0] ?? null);
    const targetEventId: string | null = targetStep
      ? (nextProj.visibleFrames[targetStep.firstFrameIndex]?.event.id ?? null)
      : (nextProj.visibleFrames[0]?.event.id ?? null);

    if (!opts?.preserveReturnStack) {
      setReturnStack([]);
    }

    const leavingSim = !isCurrentRun && activeSimRun !== null;
    if (!isCurrentRun) {
      setActiveSimRun(null);
      setActiveSimConfig(null);
      setSimulatorOpen(false);
    }

    if (TRACE_BY_ID[id]) {
      setTraceId(target.id);
    }
    if (nextLanguage !== activeLanguage) {
      setActiveLanguage(nextLanguage);
    }
    if (
      leavingSim ||
      (nextLanguage !== activeLanguage && activeSimConfig === null)
    ) {
      setSimDraft((prev) => adaptSimConfigForLanguage(prev, nextLanguage));
    }
    setCatalogMode(nextRun.traceKind);
    if (
      nextRun.traceKind === 'component' &&
      nextRun.subsystem &&
      subsystemFilter !== 'all' &&
      subsystemFilter !== nextRun.subsystem
    ) {
      setSubsystemFilter('all');
    }
    setExpandedStepIds(nextExpanded);
    if (nextCategories !== enabledCategories) {
      setEnabledCategories(nextCategories);
      if (reenabledCategories.length > 0) {
        const labels = reenabledCategories
          .map((c) => STEP_CATEGORY_MAP[c]?.short ?? c)
          .join(', ');
        setNotice(
          `Re-enabled ${labels} to show "${
            nextRun.steps.find((s) => s.id === opts?.stepId)?.title ?? 'selected step'
          }".`,
        );
      }
    }
    setSelectedComponentId(
      opts?.componentId !== undefined
        ? opts.componentId
        : (nextRun.componentId ?? null),
    );
    setInspectorTab(opts?.tab ?? 'step');
    setModal(null);
    setZoom(1);

    if (targetEventId && targetStep && opts?.animate !== false && !reducedMotion) {
      const start = targetStep.firstFrameIndex;
      const end =
        targetStep.frameIndices[targetStep.frameIndices.length - 1] ?? start;
      updateReadyAtStart(false);
      playScopeRef.current = { start, end };
      commitPlayback(targetEventId, 0);
      setPlaying(true);
    } else {
      playScopeRef.current = null;
      setPlaying(false);
      commitPlayback(targetEventId, targetEventId ? 1 : 0);
      const landingIdx = targetEventId
        ? (nextProj.frameIndexByEventId.get(targetEventId) ?? -1)
        : -1;
      updateReadyAtStart(landingIdx === 0);
    }

    if (opts?.closeMobile && window.matchMedia('(max-width: 760px)').matches) {
      setSidebarOpen(false);
    }
  }

  function handleLanguageChange(nextLang: SparkLanguage) {
    if (nextLang === activeLanguage) return;

    if (activeSimRun !== null && activeSimConfig !== null) {
      const adapted = adaptSimConfigForLanguage(activeSimConfig, nextLang);
      const adaptedDraft = simConfigsEqual(simDraft, activeSimConfig)
        ? adapted
        : adaptSimConfigForLanguage(simDraft, nextLang);
      handleRunSimulation(adapted, {
        historyMode: 'push',
        nextDraft: adaptedDraft,
      });
      return;
    }

    navHistoryModeRef.current = 'push';
    setActiveLanguage(nextLang);
    if (activeSimConfig === null) {
      setSimDraft((prev) => adaptSimConfigForLanguage(prev, nextLang));
    }

    const resolvedId = resolveLanguageVariantTraceId(
      selectedTrace.id,
      nextLang,
      ALL_TRACES,
    );
    if (resolvedId !== selectedTrace.id) {
      selectTrace(resolvedId, {
        tab: 'step',
        animate: false,
        languageOverride: nextLang,
      });
    } else {
      const nextRun = adaptTraceToRun(selectedTrace, nextLang);
      const transition = reconcileSameTraceLanguageTransition({
        nextRun,
        nextLanguage: nextLang,
        currentExpandedStepIds: expandedStepIds,
        enabledCategories,
        currentEventId: activeEventId,
        currentFrame: activeFrame,
        selectedComponentId,
        componentLanguages: selectedComponentId
          ? COMPONENTS[selectedComponentId]?.languages
          : undefined,
      });
      if (
        transition.nextExpandedStepIds.size !== expandedStepIds.size ||
        [...transition.nextExpandedStepIds].some((id) => !expandedStepIds.has(id))
      ) {
        setExpandedStepIds(transition.nextExpandedStepIds);
      }
      if (transition.nextSelectedComponentId !== selectedComponentId) {
        setSelectedComponentId(transition.nextSelectedComponentId);
      }
      playScopeRef.current = null;
      setPlaying(false);
      commitPlayback(
        transition.nextEventId,
        transition.nextEventId ? 1 : 0,
      );
      const landingIdx = transition.nextEventId
        ? (transition.nextProjection.frameIndexByEventId.get(
            transition.nextEventId,
          ) ?? -1)
        : -1;
      updateReadyAtStart(landingIdx === 0 && isReadyAtStartRef.current);
    }
  }

  function handleViewChange(nextView: CanvasView) {
    if (nextView === view) {
      setZoom(1);
      return;
    }
    navHistoryModeRef.current = 'push';
    setView(nextView);
    setZoom(1);
  }

  function handleSelectStep(stepId: string) {
    const projStep = projection.visibleSteps.find((ps) => ps.step.id === stepId);
    if (!projStep) return;
    setInspectorTab('step');
    playStep(projStep);
    if (window.matchMedia('(max-width: 760px)').matches) {
      setSidebarOpen(false);
    }
  }

  function handleSelectSidebarEvent(eventId: string) {
    const idx = projection.frameIndexByEventId.get(eventId);
    if (idx === undefined) return;
    const frame = projection.visibleFrames[idx];
    if (!frame) return;
    handleSelectStep(frame.step.id);
  }

  function jumpStep(direction: 1 | -1) {
    if (!activeFrame) return;
    if (direction < 0 && activeFrame.visibleStepIndex <= 0) return;
    if (
      direction > 0 &&
      activeFrame.visibleStepIndex >= projection.visibleSteps.length - 1
    ) {
      return;
    }
    const targetEventId = stepJumpEventId(activeEventId, projection, direction);
    if (!targetEventId) return;
    const targetIdx = projection.frameIndexByEventId.get(targetEventId);
    if (targetIdx === undefined) return;
    const targetFrame = projection.visibleFrames[targetIdx];
    if (!targetFrame) return;
    const targetStep = projection.visibleSteps[targetFrame.visibleStepIndex];
    if (!targetStep) return;
    setInspectorTab('step');
    playStep(targetStep);
  }

  function handlePrevStep() {
    jumpStep(-1);
  }

  function handleNextStep() {
    jumpStep(1);
  }

  function handleSelectEvent(eventId: string) {
    const idx = projection.frameIndexByEventId.get(eventId);
    if (idx === undefined) return;
    updateReadyAtStart(false);
    playScopeRef.current = { start: idx, end: idx };
    setPlaying(false);
    commitPlayback(eventId, 1);
    setInspectorTab('step');
    if (diagramOnly) setFloatingInspectorOpen(true);
  }

  function handleSelectComponent(id: ComponentId) {
    if (!COMPONENTS[id]) return;
    setSelectedComponentId(id);
    setInspectorTab('component');
    if (diagramOnly) setFloatingInspectorOpen(true);
  }

  function handleRunSimulation(
    config: SimulationConfig,
    opts?: { historyMode?: 'replace' | 'push'; nextDraft?: SimulationConfig },
  ) {
    const validation = validateSimulationConfig(config);
    if (!validation.valid) return;

    const resolved: SimulationConfig = resolveValidSimulationConfig(config);
    const isSameActiveSim =
      activeSimRun !== null &&
      activeSimConfig !== null &&
      simConfigsEqual(resolved, activeSimConfig);
    if (
      opts?.historyMode !== 'replace' &&
      (!isSameActiveSim || resolved.language !== activeLanguage)
    ) {
      navHistoryModeRef.current = 'push';
    }
    const simRun = buildSimulatedRun(resolved);
    const nextExpanded = new Set<string>();
    const nextProj = projectRun(simRun, nextExpanded, enabledCategories);
    const firstStep = nextProj.visibleSteps[0] ?? null;
    const firstEventId = firstStep
      ? (nextProj.visibleFrames[firstStep.firstFrameIndex]?.event.id ?? null)
      : (nextProj.visibleFrames[0]?.event.id ?? null);

    setActiveLanguage(resolved.language);
    setSimDraft(opts?.nextDraft ? { ...opts.nextDraft } : resolved);
    setActiveSimConfig(resolved);
    setActiveSimRun(simRun);
    setReturnStack([]);
    setExpandedStepIds(nextExpanded);
    setSelectedComponentId(simRun.componentId ?? null);
    setInspectorTab('step');
    setSimulatorOpen(false);
    setModal(null);
    setZoom(1);

    if (firstEventId && firstStep && !reducedMotion) {
      const start = firstStep.firstFrameIndex;
      const end =
        firstStep.frameIndices[firstStep.frameIndices.length - 1] ?? start;
      updateReadyAtStart(false);
      playScopeRef.current = { start, end };
      commitPlayback(firstEventId, 0);
      setPlaying(true);
    } else {
      playScopeRef.current = null;
      setPlaying(false);
      commitPlayback(firstEventId, firstEventId ? 1 : 0);
      const landingIdx = firstEventId
        ? (nextProj.frameIndexByEventId.get(firstEventId) ?? -1)
        : -1;
      updateReadyAtStart(landingIdx === 0);
    }
  }

  function handleExitSimulation() {
    if (!activeSimRun) return;
    const exitTarget = resolveExitSimulationTarget(
      selectedTrace.id,
      activeLanguage,
      ALL_TRACES,
    );
    const exitTrace = TRACE_BY_ID[exitTarget.traceId] ?? selectedTrace;
    const destLang = resolveLanguageAfterTraceSelect(
      exitTrace,
      exitTarget.language,
    );
    setSimulatorOpen(false);
    setSimDraft((prev) => adaptSimConfigForLanguage(prev, destLang));
    selectTrace(exitTrace.id, {
      componentId: exitTrace.componentId ?? null,
      tab: 'step',
      animate: false,
      languageOverride: destLang,
    });
  }

  function handleResetSimDraft() {
    setSimDraft({ ...DEFAULT_SIMULATION_CONFIG });
  }

  function handleTraceComponent(id: ComponentId) {
    const compTrace = COMPONENT_TRACE_BY_ID[id] ?? TRACE_BY_ID[`comp-${id}`];
    if (!compTrace) return;
    if (activeRun.id !== compTrace.id) {
      setReturnStack((prev) => [
        ...prev,
        {
          traceId: activeRun.id,
          traceName: activeRun.name,
          catalogTraceId:
            activeRun.sourceKind === 'simulation'
              ? selectedTrace.id
              : undefined,
          run: activeRun.sourceKind === 'simulation' ? activeRun : undefined,
          simConfig:
            activeRun.sourceKind === 'simulation'
              ? (activeSimConfig ?? undefined)
              : undefined,
          eventId: activeEventId,
          view,
          language: activeLanguage,
          expandedStepIds: [...expandedStepIds],
          enabledCategories: new Set(enabledCategories),
        },
      ]);
    }
    selectTrace(compTrace.id, {
      componentId: id,
      tab: 'step',
      closeMobile: true,
      preserveReturnStack: true,
    });
  }

  function handleReturnToPreviousRun() {
    if (returnStack.length === 0) return;
    const entry = returnStack[returnStack.length - 1];
    setReturnStack((prev) => prev.slice(0, -1));

    const targetTrace = TRACE_BY_ID[entry.traceId] ?? DEFAULT_TRACE;
    const restoredExpanded = new Set<string>(entry.expandedStepIds);
    const restoredCategories = new Set<StepCategory>(entry.enabledCategories);
    const restoredRun = entry.run ?? adaptTraceToRun(targetTrace, entry.language);
    const restoredProj = projectRun(
      restoredRun,
      restoredExpanded,
      restoredCategories,
    );
    const restoredEventId = resolveEventAfterProjectionChange(
      entry.eventId,
      restoredRun,
      restoredProj,
    );

    if (
      restoredRun.id !== activeRun.id ||
      entry.view !== view ||
      entry.language !== activeLanguage
    ) {
      navHistoryModeRef.current = 'push';
    }

    if (entry.run) {
      setActiveSimRun(entry.run);
      setActiveSimConfig(entry.simConfig ?? null);
      if (entry.catalogTraceId && TRACE_BY_ID[entry.catalogTraceId]) {
        setTraceId(entry.catalogTraceId);
      }
    } else {
      setActiveSimRun(null);
      setActiveSimConfig(null);
      setTraceId(targetTrace.id);
    }
    setActiveLanguage(entry.language);
    setCatalogMode(restoredRun.traceKind);
    setView(entry.view);
    setExpandedStepIds(restoredExpanded);
    setEnabledCategories(restoredCategories);
    setSelectedComponentId(restoredRun.componentId ?? null);
    setInspectorTab('step');
    setZoom(1);
    playScopeRef.current = null;
    setPlaying(false);
    updateReadyAtStart(false);
    commitPlayback(restoredEventId, restoredEventId ? 1 : 0);
  }

  function handleViewOnClusterMap(id: ComponentId) {
    setSelectedComponentId(id);
    handleViewChange('cluster');
  }

  function handleToggleExpandStep(stepId: string) {
    const wasExpanded = expandedStepIds.has(stepId);
    const nextExpanded = new Set(expandedStepIds);
    if (wasExpanded) {
      nextExpanded.delete(stepId);
      for (const descId of collectDescendantStepIds(activeRun, stepId)) {
        nextExpanded.delete(descId);
      }
    } else {
      for (const ancId of collectStepIdsToExpandForStep(activeRun, stepId)) {
        nextExpanded.add(ancId);
      }
      nextExpanded.add(stepId);
    }
    const nextProj = projectRun(activeRun, nextExpanded, enabledCategories);
    const nextEventId = resolveEventAfterToggleExpand(
      stepId,
      wasExpanded,
      activeRun,
      nextProj,
      activeEventId,
    );
    setExpandedStepIds(nextExpanded);
    setInspectorTab('step');

    if (!wasExpanded && nextEventId && !reducedMotion) {
      const targetIdx = nextProj.frameIndexByEventId.get(nextEventId);
      const targetFrame =
        targetIdx !== undefined ? nextProj.visibleFrames[targetIdx] : null;
      const targetStep = targetFrame
        ? (nextProj.visibleSteps[targetFrame.visibleStepIndex] ?? null)
        : null;
      if (
        targetStep &&
        targetFrame &&
        targetFrame.ancestors.some((a) => a.id === stepId)
      ) {
        playStep(targetStep, nextProj);
        return;
      }
    }

    playScopeRef.current = null;
    setPlaying(false);
    updateReadyAtStart(false);
    commitPlayback(nextEventId, nextEventId ? 1 : 0);
  }

  function handleCollapseAll() {
    if (expandedStepIds.size === 0) return;
    const nextExpanded = new Set<string>();
    const nextProj = projectRun(activeRun, nextExpanded, enabledCategories);
    const rootAncestorId = activeFrame?.ancestors[0]?.id ?? null;
    const nextEventId = rootAncestorId
      ? resolveEventAfterToggleExpand(
          rootAncestorId,
          true,
          activeRun,
          nextProj,
          activeEventId,
        )
      : resolveEventAfterProjectionChange(activeEventId, activeRun, nextProj);
    setExpandedStepIds(nextExpanded);
    playScopeRef.current = null;
    setPlaying(false);
    updateReadyAtStart(false);
    commitPlayback(nextEventId, nextEventId ? 1 : 0);
  }

  function handleExpandAll() {
    if (expandableStepIds.length === 0 || isAllExpandedDepth) return;
    const nextExpanded = new Set<string>(expandableStepIds);
    const nextProj = projectRun(activeRun, nextExpanded, enabledCategories);
    const currentWasCollapsedMacro = Boolean(
      activeStepObj &&
        nextExpanded.has(activeStepObj.id) &&
        !expandedStepIds.has(activeStepObj.id),
    );
    const nextEventId =
      currentWasCollapsedMacro && activeStepObj
        ? resolveEventAfterToggleExpand(
            activeStepObj.id,
            false,
            activeRun,
            nextProj,
            activeEventId,
          )
        : resolveEventAfterProjectionChange(activeEventId, activeRun, nextProj);
    setExpandedStepIds(nextExpanded);
    playScopeRef.current = null;
    setPlaying(false);
    updateReadyAtStart(false);
    commitPlayback(nextEventId, nextEventId ? 1 : 0);
  }

  function handleCollapseEmptyExpanded() {
    if (emptyExpandedStepIds.length === 0) return;
    const nextExpanded = new Set(expandedStepIds);
    for (const id of emptyExpandedStepIds) {
      nextExpanded.delete(id);
      for (const descId of collectDescendantStepIds(activeRun, id)) {
        nextExpanded.delete(descId);
      }
    }
    const nextProj = projectRun(activeRun, nextExpanded, enabledCategories);
    const firstCollapsed = emptyExpandedStepIds[0];
    const nextEventId = firstCollapsed
      ? resolveEventAfterToggleExpand(
          firstCollapsed,
          true,
          activeRun,
          nextProj,
          activeEventId,
        )
      : resolveEventAfterProjectionChange(activeEventId, activeRun, nextProj);
    setExpandedStepIds(nextExpanded);
    playScopeRef.current = null;
    setPlaying(false);
    updateReadyAtStart(false);
    commitPlayback(nextEventId, nextEventId ? 1 : 0);
  }

  function applyEnabledCategories(nextEnabled: Set<StepCategory>) {
    const nextProj = projectRun(activeRun, expandedStepIds, nextEnabled);
    const nextEventId = resolveEventAfterProjectionChange(
      activeEventId,
      activeRun,
      nextProj,
    );
    setEnabledCategories(nextEnabled);
    playScopeRef.current = null;
    setPlaying(false);
    if (nextEventId === activeEventId && nextEventId !== null) {
      const nextIdx = nextProj.frameIndexByEventId.get(nextEventId) ?? -1;
      if (isReadyAtStartRef.current && nextIdx !== 0) {
        updateReadyAtStart(false);
      }
      return;
    }
    commitPlayback(nextEventId, nextEventId ? 1 : 0);
    const nextIdx = nextEventId
      ? (nextProj.frameIndexByEventId.get(nextEventId) ?? -1)
      : -1;
    updateReadyAtStart(activeEventId === null && nextIdx === 0);
  }

  function handleToggleCategory(category: StepCategory) {
    const nextEnabled = new Set(enabledCategories);
    if (nextEnabled.has(category)) {
      nextEnabled.delete(category);
    } else {
      nextEnabled.add(category);
    }
    applyEnabledCategories(nextEnabled);
  }

  function handleApplyCategoryPreset(
    presetId: Exclude<CategoryPresetId, 'custom'>,
  ) {
    const preset = CATEGORY_PRESETS.find((p) => p.id === presetId);
    if (!preset) return;
    applyEnabledCategories(new Set<StepCategory>(preset.categories));
  }

  function handleClearCategories() {
    applyEnabledCategories(new Set<StepCategory>());
  }

  function handleEnableCategories(categories: StepCategory[]) {
    const nextEnabled = new Set(enabledCategories);
    for (const cat of categories) nextEnabled.add(cat);
    applyEnabledCategories(nextEnabled);
  }

  function handleResetFilters() {
    applyEnabledCategories(new Set<StepCategory>(ALL_STEP_CATEGORIES));
  }

  function handleTogglePlay() {
    const total = projection.visibleFrames.length;
    if (total === 0) return;
    if (playing) {
      playScopeRef.current = null;
      setPlaying(false);
      return;
    }
    playScopeRef.current = null;
    const pb = playbackRef.current;
    const curIndex = pb.eventId
      ? (projection.frameIndexByEventId.get(pb.eventId) ?? -1)
      : -1;
    if (
      curIndex < 0 ||
      (curIndex >= total - 1 && pb.progress >= 1) ||
      (isReadyAtStartRef.current && curIndex === 0)
    ) {
      commitPlayback(projection.visibleFrames[0].event.id, 0);
    } else if (pb.progress >= 1) {
      const nextIdx = Math.min(curIndex + 1, total - 1);
      commitPlayback(projection.visibleFrames[nextIdx].event.id, 0);
    }
    updateReadyAtStart(false);
    setPlaying(true);
  }

  function handleReset() {
    setPlaying(false);
    playScopeRef.current = null;
    setInspectorTab('step');
    const firstId = projection.visibleFrames[0]?.event.id ?? null;
    commitPlayback(firstId, firstId ? 1 : 0);
    updateReadyAtStart(firstId !== null);
  }

  function handleNextEvent() {
    const total = projection.visibleFrames.length;
    if (total === 0) return;
    updateReadyAtStart(false);
    setPlaying(false);
    playScopeRef.current = null;
    const curIdx = playbackRef.current.eventId
      ? (projection.frameIndexByEventId.get(playbackRef.current.eventId) ?? -1)
      : -1;
    const nextIdx = Math.min(curIdx + 1, total - 1);
    commitPlayback(projection.visibleFrames[nextIdx].event.id, 1);
  }

  function handlePrevEvent() {
    const total = projection.visibleFrames.length;
    if (total === 0) return;
    updateReadyAtStart(false);
    setPlaying(false);
    playScopeRef.current = null;
    const curIdx = playbackRef.current.eventId
      ? (projection.frameIndexByEventId.get(playbackRef.current.eventId) ?? 0)
      : 0;
    const prevIdx = Math.max(0, curIdx - 1);
    commitPlayback(projection.visibleFrames[prevIdx].event.id, 1);
  }

  function handleToggleTheme() {
    const next = theme === 'dark' ? 'light' : 'dark';
    setTheme(next);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, next);
    } catch {
      setNotice('Theme switched, but browser storage is unavailable.');
    }
  }

  async function handleToggleFullscreen() {
    try {
      if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else {
        await document.documentElement.requestFullscreen();
      }
    } catch {
      setNotice('Fullscreen is unavailable here. Try Diagram-Only Focus Mode (D).');
    }
  }

  function handlePickSearchResult(entry: SearchEntry) {
    selectTrace(entry.traceId, {
      componentId: entry.componentId ?? null,
      stepId: entry.stepId ?? undefined,
      tab: 'step',
      closeMobile: true,
    });
  }

  // Sync theme attribute & theme-color meta
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    const metaTheme = document.querySelector<HTMLMetaElement>(
      'meta[name="theme-color"]',
    );
    if (metaTheme) {
      metaTheme.content = theme === 'dark' ? '#0b1020' : '#edf0f6';
    }
  }, [theme]);

  // Keep URL hash in sync with active run, canvas view, and language
  useEffect(() => {
    const nextHash =
      activeRun.sourceKind === 'simulation'
        ? activeSimConfig
          ? encodeSimHash(activeSimConfig, view, activeLanguage)
          : `#sim=${encodeURIComponent(activeRun.id)}${
              activeLanguage !== 'pyspark' ? `&lang=${activeLanguage}` : ''
            }${view !== 'topology' ? `&view=${view}` : ''}`
        : encodeCatalogHash(selectedTrace, view, activeLanguage);
    const historyMode = navHistoryModeRef.current;
    navHistoryModeRef.current = 'replace';
    const historyState = { catalogTraceId: traceId };
    if (window.location.hash !== nextHash) {
      try {
        if (historyMode === 'push') {
          window.history.pushState(historyState, '', nextHash);
        } else {
          window.history.replaceState(historyState, '', nextHash);
        }
      } catch {
        // Ignore restricted history origin errors
      }
    } else if (
      window.history.state?.catalogTraceId !== traceId
    ) {
      try {
        window.history.replaceState(historyState, '', nextHash);
      } catch {
        // Ignore restricted history origin errors
      }
    }
    lastAppliedHashRef.current = window.location.hash;
  }, [activeRun, selectedTrace, traceId, view, activeSimConfig, activeLanguage]);

  // Listen to external hashchange/popstate navigation (deep links / browser history)
  const hashChangeHandlerRef = useRef<() => void>(() => {});
  hashChangeHandlerRef.current = () => {
    navHistoryModeRef.current = 'replace';
    const parsed = parseUrlHash(
      window.location.hash,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    );
    const historyCatalogTraceId =
      typeof window.history.state?.catalogTraceId === 'string' &&
      Object.prototype.hasOwnProperty.call(
        TRACE_BY_ID,
        window.history.state.catalogTraceId,
      )
        ? window.history.state.catalogTraceId
        : null;
    if (parsed.language !== activeLanguage) {
      setActiveLanguage(parsed.language);
    }
    if (parsed.view !== view) {
      setView(parsed.view);
      setZoom(1);
    }
    if (parsed.kind === 'sim') {
      if (historyCatalogTraceId && historyCatalogTraceId !== traceId) {
        setTraceId(historyCatalogTraceId);
      }
      const sameActiveSim =
        activeSimRun !== null &&
        activeSimConfig !== null &&
        simConfigsEqual(parsed.simConfig, activeSimConfig);
      if (!sameActiveSim) {
        handleRunSimulation(parsed.simConfig, { historyMode: 'replace' });
      }
      const canonicalHash = encodeSimHash(
        parsed.simConfig,
        parsed.view,
        parsed.language,
      );
      if (window.location.hash !== canonicalHash) {
        try {
          window.history.replaceState(
            { catalogTraceId: historyCatalogTraceId ?? traceId },
            '',
            canonicalHash,
          );
          lastAppliedHashRef.current = window.location.hash;
        } catch {
          // Ignore restricted history origin errors
        }
      }
    } else {
      const nextTrace = TRACE_BY_ID[parsed.traceId] ?? DEFAULT_TRACE;
      if (
        activeSimRun !== null ||
        (parsed.language !== activeLanguage && activeSimConfig === null)
      ) {
        setSimDraft((prev) => adaptSimConfigForLanguage(prev, parsed.language));
      }
      if (nextTrace.id !== traceId || activeSimRun !== null) {
        selectTrace(nextTrace.id, {
          componentId: nextTrace.componentId ?? null,
          tab: 'step',
          animate: true,
          languageOverride: parsed.language,
          historyMode: 'replace',
        });
      } else if (parsed.language !== activeLanguage) {
        // Same-trace language history traversal: preserve valid expansions, surviving activeEventId, and reconcile inspector component!
        const nextRun = adaptTraceToRun(nextTrace, parsed.language);
        const transition = reconcileSameTraceLanguageTransition({
          nextRun,
          nextLanguage: parsed.language,
          currentExpandedStepIds: expandedStepIds,
          enabledCategories,
          currentEventId: activeEventId,
          currentFrame: activeFrame,
          selectedComponentId,
          componentLanguages: selectedComponentId
            ? COMPONENTS[selectedComponentId]?.languages
            : undefined,
        });
        if (
          transition.nextExpandedStepIds.size !== expandedStepIds.size ||
          [...transition.nextExpandedStepIds].some((id) => !expandedStepIds.has(id))
        ) {
          setExpandedStepIds(transition.nextExpandedStepIds);
        }
        if (transition.nextSelectedComponentId !== selectedComponentId) {
          setSelectedComponentId(transition.nextSelectedComponentId);
        }
        playScopeRef.current = null;
        setPlaying(false);
        commitPlayback(
          transition.nextEventId,
          transition.nextEventId ? 1 : 0,
        );
      } else if (
        nextTrace.componentId &&
        nextTrace.componentId !== selectedComponentId
      ) {
        setSelectedComponentId(nextTrace.componentId);
      }
      const canonicalHash = encodeCatalogHash(
        nextTrace,
        parsed.view,
        parsed.language,
      );
      if (window.location.hash !== canonicalHash) {
        try {
          window.history.replaceState(
            { catalogTraceId: nextTrace.id },
            '',
            canonicalHash,
          );
          lastAppliedHashRef.current = window.location.hash;
        } catch {
          // Ignore restricted history origin errors
        }
      }
    }
  };

  useEffect(() => {
    const listener = () => {
      if (window.location.hash === lastAppliedHashRef.current) return;
      lastAppliedHashRef.current = window.location.hash;
      navHistoryModeRef.current = 'replace';
      hashChangeHandlerRef.current();
    };
    window.addEventListener('hashchange', listener);
    window.addEventListener('popstate', listener);
    return () => {
      window.removeEventListener('hashchange', listener);
      window.removeEventListener('popstate', listener);
    };
  }, []);

  // Sync viewport & reduced-motion media queries
  useEffect(() => {
    const mobileMq = window.matchMedia('(max-width: 760px)');
    const onMobileChange = () => setSidebarOpen(!mobileMq.matches);
    mobileMq.addEventListener('change', onMobileChange);

    const motionMq = window.matchMedia('(prefers-reduced-motion: reduce)');
    const onMotionChange = () => setReducedMotion(motionMq.matches);
    motionMq.addEventListener('change', onMotionChange);

    return () => {
      mobileMq.removeEventListener('change', onMobileChange);
      motionMq.removeEventListener('change', onMotionChange);
    };
  }, []);

  // Playback animation loop strictly over projection.visibleFrames
  useEffect(() => {
    if (!playing) return;
    if (projection.visibleFrames.length === 0) {
      playScopeRef.current = null;
      setPlaying(false);
      return;
    }
    let frameId = 0;
    let prevTime: number | null = null;

    function tick(now: number) {
      const elapsed = prevTime === null ? 0 : Math.min(now - prevTime, 100);
      prevTime = now;
      const curEventId = playbackRef.current.eventId;
      const curIndex = curEventId
        ? (projection.frameIndexByEventId.get(curEventId) ?? -1)
        : -1;
      if (curIndex < 0) {
        playScopeRef.current = null;
        setPlaying(false);
        return;
      }
      const maxIdx = projection.visibleFrames.length - 1;
      const scope = playScopeRef.current;
      const startIndex = scope ? Math.max(0, Math.min(scope.start, maxIdx)) : 0;
      const endIndex = scope
        ? Math.max(startIndex, Math.min(scope.end, maxIdx))
        : maxIdx;
      const clampedCur = Math.max(startIndex, Math.min(curIndex, endIndex));
      const next = advanceTimeline(
        { cursor: clampedCur, progress: playbackRef.current.progress },
        elapsed,
        speedRef.current,
        endIndex + 1,
      );
      const nextFrame = projection.visibleFrames[next.cursor];
      if (!nextFrame) {
        playScopeRef.current = null;
        setPlaying(false);
        return;
      }
      commitPlayback(nextFrame.event.id, next.progress);
      if (next.cursor >= endIndex && next.progress >= 1) {
        playScopeRef.current = null;
        setPlaying(false);
      } else {
        frameId = window.requestAnimationFrame(tick);
      }
    }

    frameId = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frameId);
  }, [playing, projection]);

  // Native <dialog> open/close sync
  useEffect(() => {
    const el = dialogRef.current;
    if (!el) return;
    if (modal && !el.open) {
      el.showModal();
      if (modal === 'search') {
        window.requestAnimationFrame(() => searchInputRef.current?.focus());
      }
    } else if (!modal && el.open) {
      el.close();
    }
  }, [modal]);

  // Keep highlighted search item in view
  useEffect(() => {
    dialogRef.current
      ?.querySelector('.search-results .highlighted')
      ?.scrollIntoView({ block: 'nearest' });
  }, [searchIndex, searchQuery]);

  // Auto-dismiss notice toast
  useEffect(() => {
    if (!notice) return;
    const id = window.setTimeout(() => setNotice(''), 5500);
    return () => window.clearTimeout(id);
  }, [notice]);

  // Keyboard shortcuts
  const keyHandlerRef = useRef<(event: KeyboardEvent) => void>(() => {});
  keyHandlerRef.current = (event: KeyboardEvent) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      setModal((prev) => (prev === 'search' ? null : 'search'));
      setSearchQuery('');
      setSearchIndex(0);
      return;
    }

    if (event.key === 'Escape' && !modal) {
      if (floatingInspectorOpen) {
        setFloatingInspectorOpen(false);
      } else if (simulatorOpen) {
        setSimulatorOpen(false);
      } else if (categoryBarOpen) {
        setCategoryBarOpen(false);
      } else if (diagramOnly) {
        setDiagramOnly(false);
      } else if (window.matchMedia('(max-width: 760px)').matches && sidebarOpen) {
        setSidebarOpen(false);
      }
      return;
    }

    if (modal || event.ctrlKey || event.metaKey || event.altKey) return;

    const target = event.target;
    if (
      target instanceof HTMLElement &&
      (target.matches('input, textarea, select') || target.isContentEditable)
    ) {
      return;
    }
    if (
      event.key === ' ' &&
      target instanceof HTMLElement &&
      target.matches('button, a, summary')
    ) {
      return;
    }

    if (/^[1-9]$/.test(event.key)) {
      const idx = Number(event.key) - 1;
      const projStep = projection.visibleSteps[idx];
      if (projStep) {
        event.preventDefault();
        handleSelectStep(projStep.step.id);
      }
      return;
    }

    switch (event.key.toLowerCase()) {
      case '/':
        event.preventDefault();
        setModal('search');
        setSearchQuery('');
        setSearchIndex(0);
        break;
      case '?':
        event.preventDefault();
        setModal((prev) => (prev === 'shortcuts' ? null : 'shortcuts'));
        break;
      case 'l':
        setSidebarOpen((v) => !v);
        break;
      case 'd':
        setDiagramOnly((v) => {
          const next = !v;
          if (!next) setFloatingInspectorOpen(false);
          return next;
        });
        break;
      case 't':
        handleToggleTheme();
        break;
      case 'f':
        void handleToggleFullscreen();
        break;
      case 'v': {
        const nextView: CanvasView =
          view === 'topology'
            ? 'cluster'
            : view === 'cluster'
              ? 'sequence'
              : 'topology';
        handleViewChange(nextView);
        break;
      }
      case 'c':
        setCategoryBarOpen((v) => {
          const next = !v;
          if (next) setSimulatorOpen(false);
          return next;
        });
        break;
      case 'g':
        setSimulatorOpen((v) => {
          const next = !v;
          if (next) setCategoryBarOpen(false);
          return next;
        });
        break;
      case '[':
        setSpeed((v) => {
          const idx = SPEEDS.indexOf(v as (typeof SPEEDS)[number]);
          return SPEEDS[Math.max(0, (idx === -1 ? 1 : idx) - 1)];
        });
        break;
      case ']':
        setSpeed((v) => {
          const idx = SPEEDS.indexOf(v as (typeof SPEEDS)[number]);
          return SPEEDS[Math.min(SPEEDS.length - 1, (idx === -1 ? 1 : idx) + 1)];
        });
        break;
      case ' ':
        event.preventDefault();
        handleTogglePlay();
        break;
      case 'arrowright':
        event.preventDefault();
        handleNextStep();
        break;
      case 'arrowleft':
        event.preventDefault();
        handlePrevStep();
        break;
      case 's':
        handleNextEvent();
        break;
      case 'r':
        handleReset();
        break;
    }
  };

  useEffect(() => {
    const listener = (event: KeyboardEvent) => keyHandlerRef.current(event);
    document.addEventListener('keydown', listener);
    return () => document.removeEventListener('keydown', listener);
  }, []);

  const playbackStatus: 'PLAYING' | 'INSPECTING' | 'READY' = playing
    ? 'PLAYING'
    : isReadyAtStart || activeFrameIndex < 0
      ? 'READY'
      : 'INSPECTING';

  const minZoom = view === 'cluster' ? 0.55 : 0.7;
  const maxZoom = view === 'cluster' ? 2.6 : 1.5;

  const canvasProps: RunCanvasProps = {
    run: activeRun,
    projection,
    activeEventId,
    progress,
    reducedMotion,
    selectedComponentId,
    onSelectComponent: handleSelectComponent,
    onSelectEvent: handleSelectEvent,
  };

  const inspectorProps = {
    tab: inspectorTab,
    onTabChange: setInspectorTab,
    run: activeRun,
    projection,
    activeEventId,
    selectedComponentId,
    activeLanguage,
    onPrevStep: handlePrevStep,
    onNextStep: handleNextStep,
    onSelectStep: handleSelectStep,
    onSelectEvent: handleSelectEvent,
    onSelectComponent: handleSelectComponent,
    onTraceComponent: handleTraceComponent,
    onViewOnClusterMap: handleViewOnClusterMap,
    onToggleExpandStep: handleToggleExpandStep,
    onToggleCategory: handleToggleCategory,
    onEnableCategories: handleEnableCategories,
    onResetFilters: handleResetFilters,
  };

  const topReturnEntry =
    returnStack.length > 0 ? returnStack[returnStack.length - 1] : null;
  const exitSimulationTarget = useMemo(
    () => resolveExitSimulationTarget(selectedTrace.id, activeLanguage, ALL_TRACES),
    [selectedTrace.id, activeLanguage],
  );
  const exitSimulationTrace =
    TRACE_BY_ID[exitSimulationTarget.traceId] ?? selectedTrace;

  const simTelemetry = activeSimRun?.telemetry ?? null;
  const simLang = activeSimConfig?.language ?? activeLanguage;
  const simLangBadgeClass =
    simLang === 'pyspark'
      ? 'lang-badge-py'
      : simLang === 'scala'
        ? 'lang-badge-scala'
        : 'lang-badge-java';
  const simLangBadgeText =
    simLang === 'pyspark' ? 'PYSPARK' : simLang === 'scala' ? 'SCALA' : 'JAVA';
  const simLangMeta = SPARK_LANGUAGES.find((l) => l.id === simLang);
  const simJobShort = activeSimConfig
    ? JOB_TYPE_MAP[activeSimConfig.jobType].short
    : (activeSimRun?.name ?? '');
  const simFaultId = activeSimConfig?.fault ?? null;
  const simHasFault =
    simFaultId !== null
      ? simFaultId !== 'none'
      : Boolean(
          simTelemetry?.faultLabel && !/^none\b/i.test(simTelemetry.faultLabel),
        );
  const simFaultSeverity =
    simFaultId !== null ? FAULT_MAP[simFaultId].severity : null;
  const simFaultBadgeText =
    simFaultId !== null
      ? simFaultId === 'none'
        ? 'Fault: None'
        : FAULT_MAP[simFaultId].short
      : (simTelemetry?.faultLabel ?? 'Fault: None');

  return (
    <div
      className={[
        'app',
        sidebarOpen && !diagramOnly ? '' : 'without-sidebar',
        sidebarOpen ? 'catalog-open' : '',
        diagramOnly ? 'diagram-only' : '',
      ]
        .filter(Boolean)
        .join(' ')}
    >
      <header className="topbar">
        <a
          className="brand"
          href={encodeCatalogHash(
            TRACE_BY_ID[
              resolveLanguageVariantTraceId(
                DEFAULT_TRACE.id,
                activeLanguage,
                ALL_TRACES,
              )
            ] ?? DEFAULT_TRACE,
            'topology',
            activeLanguage,
          )}
          onClick={(e) => {
            e.preventDefault();
            const homeId = resolveLanguageVariantTraceId(
              DEFAULT_TRACE.id,
              activeLanguage,
              ALL_TRACES,
            );
            if (view !== 'topology') {
              navHistoryModeRef.current = 'push';
              setView('topology');
            }
            selectTrace(homeId, {
              componentId: null,
              tab: 'step',
              animate: false,
              languageOverride: activeLanguage,
            });
          }}
        >
          <span className="brand-mark" aria-hidden="true">
            <svg viewBox="0 0 36 36" width="28" height="28">
              <path
                d="M8 26 L18 8 L28 26 M11.5 20.5 H24.5"
                fill="none"
                stroke="var(--ch-rpc)"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <circle cx="18" cy="8" r="3" fill="var(--ch-yarn)" />
              <circle cx="8" cy="26" r="3" fill="var(--ch-data)" />
              <circle cx="28" cy="26" r="3" fill="var(--ch-python)" />
            </svg>
          </span>
          <span className="brand-copy">
            <span className="brand-title">
              SPARK <em>VISUALIZER</em>
            </span>
            <span className="brand-tagline">
              {(SPARK_LANGUAGES.find((l) => l.id === activeLanguage)?.label ?? 'PySpark').toUpperCase()} ON YARN |{' '}
              {COMPONENT_LIST.filter((c) => !c.languages || c.languages.includes(activeLanguage)).length}/
              {COMPONENT_LIST.length} COMPONENTS | {ALL_TRACES.length} TRACES
            </span>
          </span>
        </a>

        <div
          className="lang-switcher"
          role="group"
          aria-label="Spark application language"
        >
          {SPARK_LANGUAGES.map((langMeta) => {
            const isActive = activeLanguage === langMeta.id;
            const langCompCount = COMPONENT_LIST.filter(
              (c) => !c.languages || c.languages.includes(langMeta.id),
            ).length;
            return (
              <button
                key={langMeta.id}
                type="button"
                className={`lang-btn lang-${langMeta.id} ${isActive ? 'active' : ''}`}
                aria-pressed={isActive}
                title={`${langMeta.label} (${langCompCount}/${COMPONENT_LIST.length} components): ${langMeta.description}`}
                onClick={() => handleLanguageChange(langMeta.id)}
              >
                <span>{langMeta.label}</span>
                <span className="lang-btn-count">{langCompCount}</span>
              </button>
            );
          })}
        </div>

        <div className="arch-planes" aria-label="Active architecture planes">
          <span className="small-label">PLANES</span>
          <div className="plane-pills">
            {ARCHITECTURE_PLANES.map((plane) => {
              const isActive = activeMessagePlanes.has(plane.id);
              const isInRun = runPlanes.has(plane.id);
              const planeTitle =
                plane.id === 'python' && activeLanguage !== 'pyspark' && !isInRun
                  ? 'PySpark only (0 Python/Py4J processes in pure Scala/Java jobs)'
                  : plane.description;
              return (
                <span
                  key={plane.id}
                  title={planeTitle}
                  className={[
                    'plane-pill',
                    `plane-${plane.id}`,
                    isActive ? 'is-active' : isInRun ? 'in-lesson' : '',
                  ]
                    .filter(Boolean)
                    .join(' ')}
                >
                  {plane.label}
                </span>
              );
            })}
          </div>
        </div>

        <div className="header-actions">
          <button
            type="button"
            className="search-trigger"
            onClick={() => {
              setModal('search');
              setSearchQuery('');
              setSearchIndex(0);
            }}
          >
            <Search size={14} aria-hidden="true" />
            <span>
              Search {ALL_TRACES.length} traces &amp; {COMPONENT_LIST.length} components
            </span>
            <kbd>Ctrl K</kbd>
          </button>

          <button
            type="button"
            className={`icon-button ${sidebarOpen && !diagramOnly ? 'is-on' : ''}`}
            title="Toggle catalog sidebar (L)"
            aria-label="Toggle catalog sidebar"
            aria-pressed={sidebarOpen && !diagramOnly}
            onClick={() => {
              if (diagramOnly) setDiagramOnly(false);
              setSidebarOpen((v) => !v);
            }}
          >
            <PanelLeft size={16} />
          </button>

          <button
            type="button"
            className={`icon-button ${diagramOnly ? 'is-on' : ''}`}
            title="Diagram-only focus mode (D)"
            aria-label="Toggle diagram-only focus mode"
            aria-pressed={diagramOnly}
            onClick={() =>
              setDiagramOnly((v) => {
                const next = !v;
                if (!next) setFloatingInspectorOpen(false);
                return next;
              })
            }
          >
            <Network size={16} />
          </button>

          <button
            type="button"
            className="icon-button"
            title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme (T)`}
            aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
            onClick={handleToggleTheme}
          >
            {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
          </button>

          <button
            type="button"
            className="icon-button fullscreen-button"
            title="Toggle fullscreen (F)"
            aria-label="Toggle fullscreen"
            onClick={() => void handleToggleFullscreen()}
          >
            <Maximize size={16} />
          </button>

          <button
            type="button"
            className="icon-button"
            title="Keyboard shortcuts (?)"
            aria-label="Keyboard shortcuts"
            onClick={() => setModal('shortcuts')}
          >
            <Keyboard size={16} />
          </button>
        </div>
      </header>

      <main className="workspace">
        {sidebarOpen && !diagramOnly && (
          <CatalogSidebar
            mode={catalogMode}
            onModeChange={setCatalogMode}
            subsystemFilter={subsystemFilter}
            onSubsystemFilterChange={setSubsystemFilter}
            lifecycleTraces={LIFECYCLE_TRACES}
            scenarioTraces={SCENARIO_TRACES}
            componentTraces={COMPONENT_TRACES}
            activeRun={activeRun}
            activeRunId={activeRun.id}
            activeLanguage={activeLanguage}
            projection={projection}
            expandedStepIds={expandedStepIds}
            activeEventId={activeEventId}
            deployMode={activeRun.deployMode}
            playbackStatus={playbackStatus}
            onSelectTrace={(id) => selectTrace(id, { closeMobile: true })}
            onSelectEvent={handleSelectSidebarEvent}
            onToggleExpandStep={handleToggleExpandStep}
            onResetFilters={handleResetFilters}
            onCloseMobile={() => setSidebarOpen(false)}
          />
        )}

        <section className="diagram-panel panel" aria-label="Interactive cluster canvas">
          <div className="diagram-toolbar">
            <div className="toolbar-left-group">
              <div className="view-tabs" role="tablist" aria-label="Canvas visualization mode">
                <button
                  type="button"
                  role="tab"
                  aria-selected={view === 'topology'}
                  className={view === 'topology' ? 'active' : ''}
                  onClick={() => handleViewChange('topology')}
                >
                  <Network size={13} aria-hidden="true" />
                  <span>Topology</span>
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={view === 'cluster'}
                  className={view === 'cluster' ? 'active' : ''}
                  onClick={() => handleViewChange('cluster')}
                >
                  <MapIcon size={13} aria-hidden="true" />
                  <span>
                    {SPARK_LANGUAGES.find((l) => l.id === activeLanguage)?.short ?? 'PySpark'} Map (
                    {COMPONENT_LIST.filter((c) => !c.languages || c.languages.includes(activeLanguage)).length}/
                    {COMPONENT_LIST.length})
                  </span>
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={view === 'sequence'}
                  className={view === 'sequence' ? 'active' : ''}
                  onClick={() => handleViewChange('sequence')}
                >
                  <ListOrdered size={13} aria-hidden="true" />
                  <span>Sequence</span>
                </button>
              </div>

              {topReturnEntry && (
                <button
                  type="button"
                  className="return-run-btn"
                  title={`Return to ${topReturnEntry.traceName}`}
                  onClick={handleReturnToPreviousRun}
                >
                  <ArrowLeft size={13} aria-hidden="true" />
                  <span>Back to {topReturnEntry.traceName}</span>
                </button>
              )}
            </div>

            <div className="channel-legend" aria-label="Protocol channel legend">
              {CHANNEL_ORDER.map((ch) => {
                const meta = CHANNEL_META[ch];
                return (
                  <span key={ch} className="channel-legend-pill" title={meta.label}>
                    <svg
                      width="18"
                      height="8"
                      viewBox="0 0 18 8"
                      aria-hidden="true"
                    >
                      <line
                        x1="1"
                        y1="4"
                        x2="17"
                        y2="4"
                        stroke={meta.cssVar}
                        strokeWidth="2"
                        strokeDasharray={meta.dash}
                        strokeLinecap="round"
                      />
                    </svg>
                    <span>{meta.short}</span>
                  </span>
                );
              })}
            </div>

            <div className="toolbar-right-group">
              <button
                type="button"
                className={[
                  'sim-trigger-btn',
                  simulatorOpen ? 'is-open' : '',
                  activeSimRun !== null ? 'is-sim-active' : '',
                ]
                  .filter(Boolean)
                  .join(' ')}
                aria-expanded={simulatorOpen}
                aria-controls="simulator-panel"
                title={
                  activeSimRun !== null
                    ? `Toggle job, data & fault simulator (G) — Active: ${activeSimRun.name}`
                    : 'Open job, data & fault simulator (G)'
                }
                onClick={() =>
                  setSimulatorOpen((v) => {
                    const next = !v;
                    if (next) setCategoryBarOpen(false);
                    return next;
                  })
                }
              >
                <FlaskConical size={12} aria-hidden="true" />
                <span>Simulator</span>
                {activeSimRun !== null && (
                  <span className="sim-trigger-badge">
                    <span className="sim-trigger-dot" aria-hidden="true" />
                    <span>SIM</span>
                  </span>
                )}
              </button>

              <button
                type="button"
                className={[
                  'category-trigger-btn',
                  categoryBarOpen ? 'is-open' : '',
                  isCategoryFiltered ? 'is-filtered' : '',
                ]
                  .filter(Boolean)
                  .join(' ')}
                aria-expanded={categoryBarOpen}
                aria-controls="category-filter-bar"
                title={`Toggle step category filters (C) — ${enabledCategories.size}/${STEP_CATEGORIES.length} categories enabled${
                  totalSkippedEvents > 0
                    ? `, ${totalSkippedEvents} ${
                        totalSkippedEvents === 1 ? 'event' : 'events'
                      } skipped`
                    : ''
                }`}
                onClick={() =>
                  setCategoryBarOpen((v) => {
                    const next = !v;
                    if (next) setSimulatorOpen(false);
                    return next;
                  })
                }
              >
                <SlidersHorizontal size={12} aria-hidden="true" />
                <span>Categories</span>
                <span className="category-trigger-badge">
                  {isCategoryFiltered && totalSkippedEvents > 0
                    ? `${enabledCategories.size}/${STEP_CATEGORIES.length} · ${totalSkippedEvents} skipped`
                    : `${enabledCategories.size}/${STEP_CATEGORIES.length}`}
                </span>
              </button>

              {diagramOnly && (
                <button
                  type="button"
                  className={`control-button floating-toggle-btn ${floatingInspectorOpen ? 'is-on' : ''}`}
                  onClick={() => setFloatingInspectorOpen((v) => !v)}
                >
                  <span>
                    {floatingInspectorOpen ? 'Hide Inspector' : 'Step Inspector'}
                  </span>
                </button>
              )}
            </div>
          </div>

          {activeSimRun !== null && (
            <div
              className="sim-status-strip"
              role="status"
              aria-label="Active simulation summary"
            >
              <div className="sim-status-info">
                <span className="sim-status-pill">
                  <FlaskConical size={10} aria-hidden="true" />
                  <span>SIMULATION</span>
                </span>
                <strong className="sim-status-job" title={activeSimRun.name}>
                  {simJobShort}
                </strong>
                <span className={`deploy-badge mode-${activeSimRun.deployMode}`}>
                  {activeSimRun.deployMode.toUpperCase()}
                </span>
                <span
                  className={`lang-badge ${simLangBadgeClass} sim-strip-lang-badge`}
                  title={simLangMeta?.description}
                >
                  {simLangBadgeText}
                </span>
                {simTelemetry && (
                  <>
                    <span className="sim-status-chip">
                      {simTelemetry.numExecutors}{' '}
                      {simTelemetry.numExecutors === 1 ? 'Exec' : 'Execs'} ×{' '}
                      {simTelemetry.executorCores}{' '}
                      {simTelemetry.executorCores === 1 ? 'Core' : 'Cores'}
                    </span>
                    <span className="sim-status-chip">
                      Input-stage ideal waves: {simTelemetry.idealWavesPerStage}
                    </span>
                    <span
                      className={`sim-status-chip ${
                        simTelemetry.externalShuffleService ? 'is-ess-on' : ''
                      }`}
                    >
                      ESS {simTelemetry.externalShuffleService ? 'ON' : 'OFF'}
                    </span>
                  </>
                )}
                <span
                  className={[
                    'sim-strip-fault-badge',
                    simHasFault ? 'is-fault' : 'is-ok',
                    simFaultSeverity ? `sev-${simFaultSeverity}` : '',
                  ]
                    .filter(Boolean)
                    .join(' ')}
                  title={
                    simFaultId !== null && simFaultId !== 'none'
                      ? FAULT_MAP[simFaultId].label
                      : undefined
                  }
                >
                  {simHasFault && <AlertTriangle size={10} aria-hidden="true" />}
                  <span>{simFaultBadgeText}</span>
                </span>
              </div>

              <div className="sim-status-actions">
                <button
                  type="button"
                  className={`sim-strip-btn ${simulatorOpen ? 'is-active' : ''}`}
                  aria-expanded={simulatorOpen}
                  aria-controls="simulator-panel"
                  onClick={() =>
                    setSimulatorOpen((v) => {
                      const next = !v;
                      if (next) setCategoryBarOpen(false);
                      return next;
                    })
                  }
                >
                  <SlidersHorizontal size={11} aria-hidden="true" />
                  <span>Configure</span>
                </button>
                <button
                  type="button"
                  className="sim-strip-btn exit"
                  title={`Exit simulation and return to ${exitSimulationTrace.name}`}
                  onClick={handleExitSimulation}
                >
                  <ArrowLeft size={11} aria-hidden="true" />
                  <span>Back to Catalog</span>
                </button>
              </div>
            </div>
          )}

          {simulatorOpen && (
            <SimulatorPanel
              id="simulator-panel"
              draft={simDraft}
              activeConfig={activeSimConfig}
              onDraftChange={setSimDraft}
              onRun={handleRunSimulation}
              onRunPreset={handleRunSimulation}
              onResetDraft={handleResetSimDraft}
              onClose={() => setSimulatorOpen(false)}
            />
          )}

          {categoryBarOpen && (
            <CategoryFilterBar
              id="category-filter-bar"
              enabledCategories={enabledCategories}
              categoryCounts={projection.categoryCounts}
              deepOnlyCounts={deepOnlyCategoryCounts}
              skippedEventCount={totalSkippedEvents}
              onToggleCategory={handleToggleCategory}
              onApplyPreset={handleApplyCategoryPreset}
              onSelectAll={handleResetFilters}
              onClearAll={handleClearCategories}
              onClose={() => setCategoryBarOpen(false)}
            />
          )}

          {expandableStepIds.length > 0 && (
            <div
              className="depth-breadcrumb-bar"
              aria-label="Execution depth and step breadcrumbs"
            >
              <nav className="breadcrumb-trail" aria-label="Step hierarchy breadcrumb">
                <button
                  type="button"
                  className={`breadcrumb-chip root ${
                    (activeFrame?.ancestors.length ?? 0) === 0 && isHighLevelDepth
                      ? 'is-current'
                      : ''
                  }`}
                  title={
                    isHighLevelDepth
                      ? 'Viewing top-level Master Flow'
                      : 'Collapse all expanded steps and return to top-level Master Flow'
                  }
                  onClick={handleCollapseAll}
                >
                  <Layers size={11} aria-hidden="true" />
                  <span>Master Flow</span>
                </button>

                {(activeFrame?.ancestors ?? []).map((ancestor) => (
                  <span key={ancestor.id} className="breadcrumb-segment">
                    <ChevronRight
                      size={11}
                      className="breadcrumb-sep"
                      aria-hidden="true"
                    />
                    <button
                      type="button"
                      className="breadcrumb-chip ancestor"
                      title={`Collapse back to: ${ancestor.title}`}
                      onClick={() => handleToggleExpandStep(ancestor.id)}
                    >
                      <span>{ancestor.title}</span>
                    </button>
                  </span>
                ))}

                {activeFrame && (
                  <span className="breadcrumb-segment current">
                    <ChevronRight
                      size={11}
                      className="breadcrumb-sep"
                      aria-hidden="true"
                    />
                    <span
                      className="breadcrumb-chip current-pill"
                      aria-current="location"
                      title={`Step ${activeFrame.displayStepNumber}: ${activeFrame.step.title}`}
                    >
                      Step {activeFrame.displayStepNumber}: {activeFrame.step.title}
                    </span>
                  </span>
                )}

                {emptyExpandedStepIds.length > 0 && (
                  <span className="breadcrumb-empty-warning" role="status">
                    <span
                      className="breadcrumb-empty-warning-label"
                      title={
                        emptyExpandedHiddenCategories.length > 0
                          ? `Expanded sub-steps hidden by category filters: ${emptyExpandedHiddenCategories
                              .map((c) => STEP_CATEGORY_MAP[c].label)
                              .join(', ')}`
                          : 'Expanded sub-steps are hidden by active category filters'
                      }
                    >
                      Hidden:{' '}
                      {emptyExpandedHiddenCategories.length > 0
                        ? emptyExpandedHiddenCategories
                            .map((c) => STEP_CATEGORY_MAP[c].short)
                            .join(', ')
                        : 'filtered out'}
                    </span>
                    {emptyExpandedHiddenCategories.length > 0 && (
                      <button
                        type="button"
                        className="breadcrumb-warning-action include"
                        title={`Include ${emptyExpandedHiddenCategories
                          .map((c) => STEP_CATEGORY_MAP[c].short)
                          .join(', ')}`}
                        onClick={() =>
                          handleEnableCategories(emptyExpandedHiddenCategories)
                        }
                      >
                        {emptyExpandedHiddenCategories.length === 1
                          ? `Include ${STEP_CATEGORY_MAP[emptyExpandedHiddenCategories[0]].short}`
                          : `Include ${emptyExpandedHiddenCategories.length} categories`}
                      </button>
                    )}
                    <button
                      type="button"
                      className="breadcrumb-warning-action"
                      title="Collapse empty expanded macro steps"
                      onClick={handleCollapseEmptyExpanded}
                    >
                      Collapse
                    </button>
                  </span>
                )}
              </nav>

              <div className="depth-controls-right">
                {!isHighLevelDepth && !isAllExpandedDepth && (
                  <span className="depth-live-count" aria-live="polite">
                    {projection.visibleSteps.length} steps
                  </span>
                )}
                <div
                  className="depth-mode-toggle"
                  role="group"
                  aria-label="Trace depth mode"
                >
                  <button
                    type="button"
                    className={isHighLevelDepth ? 'active' : ''}
                    aria-pressed={isHighLevelDepth}
                    title="Collapse all macro steps to the high-level flow"
                    onClick={handleCollapseAll}
                  >
                    High-Level
                  </button>
                  <button
                    type="button"
                    className={isAllExpandedDepth ? 'active' : ''}
                    aria-pressed={isAllExpandedDepth}
                    title={`Expand all ${expandableStepIds.length} macro steps (${allExpandedStepCount} steps)`}
                    onClick={handleExpandAll}
                  >
                    Expand All ({allExpandedStepCount} steps)
                  </button>
                </div>
              </div>
            </div>
          )}

          {!categoryBarOpen && projection.skippedSpans.length > 0 && (
            <div className="skipped-inline-banner" role="status">
              <span className="skipped-inline-copy">
                <Eye size={12} aria-hidden="true" />
                <span className="skipped-inline-text">
                  <strong>Skipped:</strong>{' '}
                  {skippedCategoryIds
                    .map((cat) => STEP_CATEGORY_MAP[cat]?.short ?? cat)
                    .join(', ')}{' '}
                  ({totalSkippedEvents}{' '}
                  {totalSkippedEvents === 1 ? 'event' : 'events'} hidden)
                </span>
              </span>
              <span className="skipped-inline-actions">
                <button
                  type="button"
                  className="skipped-inline-btn primary"
                  onClick={handleResetFilters}
                >
                  Include all
                </button>
                <button
                  type="button"
                  className="skipped-inline-btn"
                  onClick={() => setCategoryBarOpen(true)}
                >
                  Filters
                </button>
              </span>
            </div>
          )}

          <div className={`diagram-stage view-${view}`}>
            {view === 'cluster' ? (
              <div className="diagram-content cluster-content">
                <ClusterMapCanvas
                  {...canvasProps}
                  activeLanguage={activeLanguage}
                  zoom={zoom}
                  onZoomChange={setZoom}
                />
              </div>
            ) : (
              <div
                className="diagram-content"
                style={zoom !== 1 ? { transform: `scale(${zoom})` } : undefined}
              >
                {view === 'topology' ? (
                  <TopologyCanvas
                    {...canvasProps}
                    onToggleExpandStep={handleToggleExpandStep}
                  />
                ) : (
                  <SequenceCanvas
                    {...canvasProps}
                    onEnableCategories={handleEnableCategories}
                  />
                )}
              </div>
            )}
          </div>

          <div className="diagram-bottom">
            <div className="step-narration">
              <span className="step-counter" aria-live="polite">
                {String(activeProjectedStep?.displayNumber ?? 0).padStart(2, '0')}
                <small>/{String(projection.visibleSteps.length).padStart(2, '0')}</small>
              </span>
              <div className="step-narration-copy">
                <div className="step-title-line">
                  <h2>
                    {projection.visibleFrames.length === 0
                      ? 'No matching events — Reset filters'
                      : (activeStepObj?.title ?? activeRun.name)}
                  </h2>
                  {isLocalEvent && (
                    <span className="local-step-tag">LOCAL TRANSITION</span>
                  )}
                </div>
                <p>
                  {projection.visibleFrames.length === 0
                    ? totalSkippedEvents > 0
                      ? `${totalSkippedEvents} ${
                          totalSkippedEvents === 1 ? 'event' : 'events'
                        } hidden at current depth.`
                      : 'Reset category filters to restore the projected timeline.'
                    : (activeStepObj?.explanation ?? activeRun.summary)}
                </p>
              </div>
            </div>

            <div className="diagram-bottom-controls">
              {projection.visibleFrames.length === 0 && isCategoryFiltered && (
                <button
                  type="button"
                  className="step-nav-btn primary"
                  onClick={handleResetFilters}
                >
                  <RotateCcw size={13} aria-hidden="true" />
                  <span>Include all</span>
                </button>
              )}
              {activeFrame && activeFrame.ancestors.length > 0 && (
                <button
                  type="button"
                  className="step-nav-btn back-parent-btn"
                  title={`Collapse back to parent: ${
                    activeFrame.ancestors[activeFrame.ancestors.length - 1].title
                  }`}
                  onClick={() =>
                    handleToggleExpandStep(
                      activeFrame.ancestors[activeFrame.ancestors.length - 1].id,
                    )
                  }
                >
                  <ArrowLeft size={13} aria-hidden="true" />
                  <span>Back to Parent</span>
                </button>
              )}

              {activeProjectedStep?.hasChildren && !activeProjectedStep.isExpanded && (
                <button
                  type="button"
                  className="go-deeper-cta"
                  title={`Expand ${activeProjectedStep.step.title} (${activeProjectedStep.childCount} ${
                    activeProjectedStep.childCount === 1 ? 'sub-step' : 'sub-steps'
                  })`}
                  onClick={() => handleToggleExpandStep(activeProjectedStep.step.id)}
                >
                  <Layers size={13} aria-hidden="true" />
                  <span>
                    Go Deeper ({activeProjectedStep.childCount}{' '}
                    {activeProjectedStep.childCount === 1 ? 'sub-step' : 'sub-steps'})
                  </span>
                </button>
              )}

              <div className="canvas-step-nav" role="group" aria-label="Step navigation">
                <button
                  type="button"
                  className="step-nav-btn"
                  disabled={!activeFrame || activeFrame.visibleStepIndex <= 0}
                  onClick={handlePrevStep}
                  title="Previous step (Left Arrow)"
                >
                  <ChevronLeft size={14} />
                  <span>Prev Step</span>
                </button>
                <button
                  type="button"
                  className="step-nav-btn primary"
                  disabled={
                    !activeFrame ||
                    activeFrame.visibleStepIndex >= projection.visibleSteps.length - 1
                  }
                  onClick={handleNextStep}
                  title="Next step (Right Arrow)"
                >
                  <span>Next Step</span>
                  <ChevronRight size={14} />
                </button>
              </div>

              <div className="zoom-controls" aria-label="Canvas zoom">
                <button
                  type="button"
                  aria-label="Zoom out"
                  disabled={zoom <= minZoom}
                  onClick={() =>
                    setZoom((z) =>
                      Math.max(minZoom, Math.round((z - 0.15) * 100) / 100),
                    )
                  }
                >
                  <Minus size={12} />
                </button>
                <button
                  type="button"
                  className="zoom-reset-btn"
                  title="Reset zoom to 100%"
                  onClick={() => setZoom(1)}
                >
                  {Math.round(zoom * 100)}%
                </button>
                <button
                  type="button"
                  aria-label="Zoom in"
                  disabled={zoom >= maxZoom}
                  onClick={() =>
                    setZoom((z) =>
                      Math.min(maxZoom, Math.round((z + 0.15) * 100) / 100),
                    )
                  }
                >
                  <Plus size={12} />
                </button>
              </div>
            </div>
          </div>
        </section>

        {!diagramOnly && <InspectorPanel {...inspectorProps} />}

        <MessageTable
          run={activeRun}
          projection={projection}
          activeEventId={activeEventId}
          progress={progress}
          playing={playing}
          speed={speed}
          onReset={handleReset}
          onPrevEvent={handlePrevEvent}
          onTogglePlay={handleTogglePlay}
          onNextEvent={handleNextEvent}
          onSpeedChange={setSpeed}
          onSelectEvent={handleSelectEvent}
          onSelectComponent={handleSelectComponent}
          onEnableCategories={handleEnableCategories}
          onResetFilters={handleResetFilters}
        />
      </main>

      {diagramOnly && floatingInspectorOpen && (
        <InspectorPanel
          {...inspectorProps}
          floating
          onCloseFloating={() => setFloatingInspectorOpen(false)}
        />
      )}

      <footer className="page-footer">
        <span>
          PySpark on YARN · Apache Spark <code>dada2a6</code> &amp; Hadoop 3.4.1
          architecture telemetry
        </span>
        <span className="footer-right">
          <span>
            Active: <strong>{activeRun.name}</strong>
          </span>
          <span className="footer-separator">/</span>
          <button type="button" onClick={() => setModal('shortcuts')}>
            Press <kbd>?</kbd> for shortcuts
          </button>
        </span>
      </footer>

      <dialog
        ref={dialogRef}
        className="modal"
        onCancel={() => setModal(null)}
        onClick={(e) => {
          if (e.target === dialogRef.current) setModal(null);
        }}
      >
        <div className="modal-heading">
          <h2>
            {modal === 'search'
              ? 'Search Lifecycle Flows, Scenarios, Components & Steps'
              : 'Keyboard Shortcuts'}
          </h2>
          <button
            type="button"
            className="icon-button"
            aria-label="Close dialog"
            onClick={() => setModal(null)}
          >
            <X size={17} />
          </button>
        </div>

        {modal === 'search' ? (
          <>
            <div className="search-field">
              <Search size={17} aria-hidden="true" />
              <input
                ref={searchInputRef}
                type="search"
                value={searchQuery}
                placeholder="e.g. DAGScheduler, shuffle-reader, AQE, OOM, yarn-allocator, Py4J…"
                aria-label="Search traces, components, and steps"
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setSearchIndex(0);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    setSearchIndex((i) =>
                      Math.min(i + 1, Math.max(0, visibleSearchResults.length - 1)),
                    );
                  } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    setSearchIndex((i) => Math.max(0, i - 1));
                  } else if (e.key === 'Enter' && visibleSearchResults[searchIndex]) {
                    e.preventDefault();
                    e.stopPropagation();
                    handlePickSearchResult(visibleSearchResults[searchIndex]);
                  }
                }}
              />
              <kbd>ESC</kbd>
            </div>

            <div className="search-results" role="listbox" aria-label="Search results">
              {visibleSearchResults.length > 0 ? (
                visibleSearchResults.map((entry, idx) => (
                  <button
                    key={entry.key}
                    type="button"
                    role="option"
                    aria-selected={idx === searchIndex}
                    className={idx === searchIndex ? 'highlighted' : ''}
                    onMouseEnter={() => setSearchIndex(idx)}
                    onClick={() => handlePickSearchResult(entry)}
                  >
                    <span className="search-result-copy">
                      <strong>{entry.title}</strong>
                      <small>{entry.subtitle}</small>
                    </span>
                    <span className={`result-badge kind-${entry.kind}`}>
                      {entry.badge}
                    </span>
                    <ChevronRight size={14} aria-hidden="true" />
                  </button>
                ))
              ) : (
                <div className="empty-state">
                  <Search size={22} />
                  <h3>No matching flows, components, or steps</h3>
                  <p>
                    Try a component name (like BlockManager), a scenario (like speculation
                    or preemption), or a protocol (like Py4J or Netty).
                  </p>
                </div>
              )}
            </div>

            <div className="modal-footer">
              <span>
                {searchResults.length} matches
                {searchResults.length > visibleSearchResults.length
                  ? ` (showing top ${visibleSearchResults.length})`
                  : ''}
              </span>
              <span>
                <kbd>↑</kbd> <kbd>↓</kbd> navigate · <kbd>Enter</kbd> select
              </span>
            </div>
          </>
        ) : (
          <div className="shortcut-list">
            {[
              ['Quick search (flows, scenarios, components, steps)', 'Ctrl / ⌘ K or /'],
              ['Next / previous step (animates step on canvas)', '→ / ←'],
              ['Jump directly to Step 1–9', '1–9'],
              ['Play / pause full trace', 'Space'],
              ['Step forward one visible event', 'S'],
              ['Reset trace to Step 1', 'R'],
              ['Slower / faster playback speed', '[ / ]'],
              [`Cycle canvas view (Topology / All ${COMPONENT_LIST.length} Map / Sequence)`, 'V'],
              ['Toggle job, data & fault simulator', 'G'],
              ['Toggle step category filters', 'C'],
              ['Toggle left catalog sidebar', 'L'],
              ['Diagram-only focus mode', 'D'],
              ['Toggle dark / light blueprint theme', 'T'],
              ['Toggle fullscreen', 'F'],
              ['Show keyboard shortcuts', '?'],
              ['Close modal / simulator / filters / exit focus', 'Esc'],
            ].map(([label, shortcut]) => (
              <div key={label}>
                <span>{label}</span>
                <kbd>{shortcut}</kbd>
              </div>
            ))}
            <p className="fine-print">
              Shortcuts pause automatically while typing in inputs or using native
              select controls.
            </p>
          </div>
        )}
      </dialog>

      {notice && (
        <div className="notice" role="status">
          <span>{notice}</span>
          <button
            type="button"
            className="icon-button"
            aria-label="Dismiss notice"
            onClick={() => setNotice('')}
          >
            <X size={14} />
          </button>
        </div>
      )}
    </div>
  );
}
