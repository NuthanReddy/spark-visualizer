import { COMPONENTS } from '../data/components.ts';
import type { ComponentId, SparkLanguage, TraceLesson } from '../data/types.ts';
import { resolveValidSimulationConfig } from './simulator.ts';
import {
  DEFAULT_SIMULATION_CONFIG,
  FAULT_MAP,
  JOB_TYPE_MAP,
  SIMULATED_DATA_PROFILES,
  SIMULATED_FAULTS,
  adaptSimConfigForLanguage,
  validateSimulationConfig,
} from './simulatorTypes.ts';
import type {
  PythonUdfMode,
  SimulatedDataProfile,
  SimulatedFault,
  SimulatedJobType,
  SimulationConfig,
} from './simulatorTypes.ts';

export type CanvasView = 'topology' | 'cluster' | 'sequence';

export interface ExitSimulationTarget {
  traceId: string;
  language: SparkLanguage;
}

const ALL_SPARK_LANGUAGES: readonly SparkLanguage[] = [
  'pyspark',
  'scala',
  'java',
];

export type ParsedUrlRoute =
  | {
      kind: 'sim';
      simConfig: SimulationConfig;
      view: CanvasView;
      language: SparkLanguage;
    }
  | {
      kind: 'trace';
      traceId: string;
      view: CanvasView;
      language: SparkLanguage;
    };

const VALID_DATA_PROFILES = new Set<SimulatedDataProfile>(
  SIMULATED_DATA_PROFILES.map((p) => p.id),
);
const VALID_SIM_FAULTS = new Set<SimulatedFault>(
  SIMULATED_FAULTS.map((f) => f.id),
);
const VALID_DEPLOY_MODES = new Set<SimulationConfig['deployMode']>([
  'cluster',
  'client',
]);
const VALID_EXECUTOR_CORES = new Set<SimulationConfig['executorCores']>([1, 2, 4]);
const VALID_PARTITIONS = new Set<SimulationConfig['numPartitions']>([4, 8, 12, 24]);
const VALID_PYTHON_UDF_MODES = new Set<PythonUdfMode>(['pickled', 'arrow']);

function isValidJobType(value: string): value is SimulatedJobType {
  return Object.prototype.hasOwnProperty.call(JOB_TYPE_MAP, value);
}

function isValidFault(value: string): value is SimulatedFault {
  return VALID_SIM_FAULTS.has(value as SimulatedFault);
}

function parseStrictInt(raw: string | null): number | null {
  if (raw === null) return null;
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  return Number.parseInt(trimmed, 10);
}

function parseStrictBit(raw: string | null, fallback: boolean): boolean {
  if (raw === '1') return true;
  if (raw === '0') return false;
  return fallback;
}

export function isValidSparkLanguage(
  raw: string | null | undefined,
): raw is SparkLanguage {
  return raw === 'pyspark' || raw === 'scala' || raw === 'java';
}

export function normalizeSparkLanguage(
  raw: string | null | undefined,
  fallback: SparkLanguage = 'pyspark',
): SparkLanguage {
  if (typeof raw !== 'string') return fallback;
  const cleaned = raw.trim().toLowerCase();
  return isValidSparkLanguage(cleaned) ? cleaned : fallback;
}

export function getEffectiveTraceLanguages(
  trace: TraceLesson,
): readonly SparkLanguage[] {
  if (trace.languages && trace.languages.length > 0) {
    return trace.languages;
  }
  if (trace.componentId) {
    const compLangs = COMPONENTS[trace.componentId]?.languages;
    if (compLangs && compLangs.length > 0) {
      return compLangs;
    }
  }
  return ALL_SPARK_LANGUAGES;
}

export function inferTracePrimaryLanguage(trace: TraceLesson): SparkLanguage {
  const langs = getEffectiveTraceLanguages(trace);
  if (langs.includes('pyspark')) return 'pyspark';
  return langs[0];
}

export function resolveLanguageAfterTraceSelect(
  targetTrace: TraceLesson,
  currentLang: SparkLanguage,
): SparkLanguage {
  const effectiveLangs = getEffectiveTraceLanguages(targetTrace);
  return effectiveLangs.includes(currentLang)
    ? currentLang
    : inferTracePrimaryLanguage(targetTrace);
}

export function resolveLanguageVariantTraceId(
  traceId: string,
  targetLang: SparkLanguage,
  traces: readonly TraceLesson[],
): string {
  const current = traces.find((t) => t.id === traceId);
  if (!current?.languageVariantGroup) return traceId;

  const group = current.languageVariantGroup;
  const peers = traces.filter((t) => t.languageVariantGroup === group);

  const exactMatch = peers.find((t) => {
    const langs = getEffectiveTraceLanguages(t);
    return langs.length === 1 && langs[0] === targetLang;
  });
  if (exactMatch) return exactMatch.id;

  if (getEffectiveTraceLanguages(current).includes(targetLang)) {
    return current.id;
  }

  const compatible = peers.find((t) =>
    getEffectiveTraceLanguages(t).includes(targetLang),
  );
  return compatible ? compatible.id : traceId;
}

export function resolveExitSimulationTarget(
  underlyingTraceId: string,
  activeLanguage: SparkLanguage,
  traces: readonly TraceLesson[],
): ExitSimulationTarget {
  const resolvedId = resolveLanguageVariantTraceId(
    underlyingTraceId,
    activeLanguage,
    traces,
  );
  const resolvedTrace = traces.find((t) => t.id === resolvedId);
  if (!resolvedTrace) {
    return { traceId: resolvedId, language: activeLanguage };
  }
  return {
    traceId: resolvedTrace.id,
    language: resolveLanguageAfterTraceSelect(resolvedTrace, activeLanguage),
  };
}

export function parseSimHash(params: URLSearchParams): SimulationConfig | null {
  const simParam = params.get('sim')?.trim();
  if (!simParam) return null;

  const langRaw = params.get('lang')?.trim().toLowerCase() ?? null;
  const explicitLang: SparkLanguage | null =
    langRaw !== null && isValidSparkLanguage(langRaw) ? langRaw : null;

  // Short fallback form: #sim=<jobType> (e.g. #sim=broadcast-join, #sim=cached-rdd)
  if (simParam !== 'v1') {
    if (!isValidJobType(simParam)) return null;
    const jobMeta = JOB_TYPE_MAP[simParam];
    const defaultLang: SparkLanguage = jobMeta.languages.includes('pyspark')
      ? 'pyspark'
      : jobMeta.languages[0];
    const language: SparkLanguage = explicitLang ?? defaultLang;
    const shortCfg: SimulationConfig = {
      ...DEFAULT_SIMULATION_CONFIG,
      language,
      jobType: simParam,
      dataProfile: jobMeta.defaultDataProfile,
    };
    const repaired =
      explicitLang !== null
        ? adaptSimConfigForLanguage(shortCfg, explicitLang)
        : shortCfg;
    const resolved = resolveValidSimulationConfig(repaired);
    return validateSimulationConfig(resolved).valid ? resolved : null;
  }

  const jobRaw = params.get('job')?.trim() ?? null;
  if (jobRaw !== null && !isValidJobType(jobRaw)) {
    return null;
  }
  const jobType: SimulatedJobType = jobRaw ?? DEFAULT_SIMULATION_CONFIG.jobType;

  const dataRaw = params.get('data')?.trim() ?? null;
  const dataProfile: SimulatedDataProfile =
    dataRaw !== null && VALID_DATA_PROFILES.has(dataRaw as SimulatedDataProfile)
      ? (dataRaw as SimulatedDataProfile)
      : jobRaw !== null
        ? JOB_TYPE_MAP[jobType].defaultDataProfile
        : DEFAULT_SIMULATION_CONFIG.dataProfile;

  const modeRaw = params.get('mode')?.trim() ?? null;
  const deployMode: SimulationConfig['deployMode'] =
    modeRaw !== null &&
    VALID_DEPLOY_MODES.has(modeRaw as SimulationConfig['deployMode'])
      ? (modeRaw as SimulationConfig['deployMode'])
      : DEFAULT_SIMULATION_CONFIG.deployMode;

  const eParsed = parseStrictInt(params.get('e'));
  const numExecutors =
    eParsed !== null && eParsed >= 1 && eParsed <= 6
      ? eParsed
      : DEFAULT_SIMULATION_CONFIG.numExecutors;

  const cParsed = parseStrictInt(params.get('c'));
  const executorCores: SimulationConfig['executorCores'] =
    cParsed !== null &&
    VALID_EXECUTOR_CORES.has(cParsed as SimulationConfig['executorCores'])
      ? (cParsed as SimulationConfig['executorCores'])
      : DEFAULT_SIMULATION_CONFIG.executorCores;

  const pParsed = parseStrictInt(params.get('p'));
  const numPartitions: SimulationConfig['numPartitions'] =
    pParsed !== null &&
    VALID_PARTITIONS.has(pParsed as SimulationConfig['numPartitions'])
      ? (pParsed as SimulationConfig['numPartitions'])
      : DEFAULT_SIMULATION_CONFIG.numPartitions;

  const externalShuffleService = parseStrictBit(
    params.get('ess')?.trim() ?? null,
    DEFAULT_SIMULATION_CONFIG.externalShuffleService,
  );
  const dynamicAllocation = parseStrictBit(
    params.get('da')?.trim() ?? null,
    DEFAULT_SIMULATION_CONFIG.dynamicAllocation,
  );
  const aqeEnabled = parseStrictBit(
    params.get('aqe')?.trim() ?? null,
    DEFAULT_SIMULATION_CONFIG.aqeEnabled,
  );

  const udfRaw = params.get('udf')?.trim() ?? null;
  const pythonUdfMode: PythonUdfMode =
    udfRaw !== null && VALID_PYTHON_UDF_MODES.has(udfRaw as PythonUdfMode)
      ? (udfRaw as PythonUdfMode)
      : DEFAULT_SIMULATION_CONFIG.pythonUdfMode;

  const faultRaw = params.get('fault')?.trim() ?? null;
  const fault: SimulatedFault =
    faultRaw !== null && isValidFault(faultRaw)
      ? faultRaw
      : DEFAULT_SIMULATION_CONFIG.fault;

  const jobMeta = JOB_TYPE_MAP[jobType];
  const faultMeta = FAULT_MAP[fault];
  const defaultLang: SparkLanguage = !jobMeta.languages.includes('pyspark')
    ? jobMeta.languages[0]
    : faultMeta.requires?.jvmApp
      ? 'scala'
      : 'pyspark';
  const language: SparkLanguage = explicitLang ?? defaultLang;

  const candidate: SimulationConfig = {
    language,
    jobType,
    dataProfile,
    deployMode,
    numExecutors,
    executorCores,
    numPartitions,
    externalShuffleService,
    dynamicAllocation,
    aqeEnabled,
    pythonUdfMode,
    fault,
  };

  const repaired =
    explicitLang !== null
      ? adaptSimConfigForLanguage(candidate, explicitLang)
      : candidate;
  const resolved = resolveValidSimulationConfig(repaired);
  return validateSimulationConfig(resolved).valid ? resolved : null;
}

export function encodeSimHash(
  cfg: SimulationConfig,
  view: CanvasView,
  languageOverride?: SparkLanguage,
): string {
  const effectiveLang = languageOverride ?? cfg.language;
  const params = new URLSearchParams();
  params.set('sim', 'v1');
  params.set('job', cfg.jobType);
  params.set('data', cfg.dataProfile);
  params.set('mode', cfg.deployMode);
  params.set('e', String(cfg.numExecutors));
  params.set('c', String(cfg.executorCores));
  params.set('p', String(cfg.numPartitions));
  params.set('ess', cfg.externalShuffleService ? '1' : '0');
  params.set('da', cfg.dynamicAllocation ? '1' : '0');
  params.set('aqe', cfg.aqeEnabled ? '1' : '0');
  params.set('udf', cfg.pythonUdfMode);
  params.set('fault', cfg.fault);
  if (effectiveLang !== 'pyspark') {
    params.set('lang', effectiveLang);
  }
  if (view !== 'topology') {
    params.set('view', view);
  }
  return `#${params.toString()}`;
}

export function encodeCatalogHash(
  trace: TraceLesson,
  view: CanvasView,
  language: SparkLanguage,
): string {
  let base: string;
  if (trace.kind === 'component' && trace.componentId) {
    base = `component=${encodeURIComponent(trace.componentId)}`;
  } else if (trace.kind === 'scenario' && trace.scenarioNumber != null) {
    base = `scenario=${trace.scenarioNumber}`;
  } else {
    base = `flow=${encodeURIComponent(trace.id)}`;
  }
  const parts = [base];
  const inferredWithoutParam = inferTracePrimaryLanguage(trace);
  if (language !== 'pyspark' || inferredWithoutParam !== 'pyspark') {
    parts.push(`lang=${language}`);
  }
  if (view !== 'topology') {
    parts.push(`view=${view}`);
  }
  return `#${parts.join('&')}`;
}

function findFallbackTrace(
  traces: readonly TraceLesson[],
  traceById: Record<string, TraceLesson>,
): TraceLesson | null {
  return (
    traceById['flow-cluster-lifecycle'] ??
    traces.find((t) => t.kind === 'lifecycle') ??
    traces[0] ??
    null
  );
}

export function parseUrlHash(
  hash: string,
  traces: readonly TraceLesson[],
  traceById: Record<string, TraceLesson>,
  componentTraceById: Partial<Record<ComponentId, TraceLesson>>,
): ParsedUrlRoute {
  const cleaned = hash.replace(/^#/, '').trim();
  const fallbackTrace = findFallbackTrace(traces, traceById);
  const fallbackTraceId = fallbackTrace?.id ?? 'flow-cluster-lifecycle';

  if (!cleaned) {
    return {
      kind: 'trace',
      traceId: fallbackTraceId,
      view: 'topology',
      language: 'pyspark',
    };
  }

  const rawParams = new URLSearchParams(cleaned);
  const params = new URLSearchParams();
  for (const [k, v] of rawParams.entries()) {
    const key = k.trim().toLowerCase();
    const val = v.trim();
    if (key && val) params.set(key, val);
  }

  const viewParam = params.get('view')?.toLowerCase() ?? null;
  const view: CanvasView =
    viewParam === 'cluster' || viewParam === 'topology' || viewParam === 'sequence'
      ? viewParam
      : 'topology';

  const rawLang = params.get('lang');
  const loweredLang = rawLang !== null ? rawLang.toLowerCase() : null;
  const explicitValidLang: SparkLanguage | null = isValidSparkLanguage(loweredLang)
    ? loweredLang
    : null;

  if (params.has('sim')) {
    const simConfig = parseSimHash(params);
    if (simConfig) {
      return {
        kind: 'sim',
        simConfig,
        view,
        language: simConfig.language,
      };
    }
  }

  let matchedTrace: TraceLesson | null = null;

  const compParam = params.get('component');
  if (compParam) {
    const cid = compParam as ComponentId;
    matchedTrace =
      componentTraceById[cid] ??
      traceById[`comp-${cid}`] ??
      traces.find(
        (t) =>
          t.kind === 'component' &&
          t.componentId?.toLowerCase() === compParam.toLowerCase(),
      ) ??
      null;
  }

  if (!matchedTrace) {
    const scenarioParam = params.get('scenario');
    if (scenarioParam) {
      const num = Number.parseInt(scenarioParam, 10);
      if (!Number.isNaN(num) && num >= 1) {
        const scenarioTraces = traces.filter((t) => t.kind === 'scenario');
        matchedTrace =
          scenarioTraces.find(
            (t) =>
              t.scenarioNumber === num ||
              t.id === `scenario-${num}` ||
              t.id === `scenario-${String(num).padStart(2, '0')}`,
          ) ??
          scenarioTraces[num - 1] ??
          null;
      }
    }
  }

  if (!matchedTrace) {
    const flowParam = params.get('flow');
    if (flowParam) {
      matchedTrace =
        traceById[flowParam] ??
        traces.find((t) => t.id.toLowerCase() === flowParam.toLowerCase()) ??
        null;
    }
  }

  const targetTrace = matchedTrace ?? fallbackTrace;
  if (!targetTrace) {
    return {
      kind: 'trace',
      traceId: fallbackTraceId,
      view,
      language: normalizeSparkLanguage(rawLang, 'pyspark'),
    };
  }

  if (explicitValidLang !== null) {
    const resolvedTraceId = resolveLanguageVariantTraceId(
      targetTrace.id,
      explicitValidLang,
      traces,
    );
    return {
      kind: 'trace',
      traceId: resolvedTraceId,
      view,
      language: explicitValidLang,
    };
  }

  return {
    kind: 'trace',
    traceId: targetTrace.id,
    view,
    language: inferTracePrimaryLanguage(targetTrace),
  };
}

export function resolveInitialCatalogTraceId(
  route: ParsedUrlRoute,
  historyState: unknown,
  traceById: Readonly<Record<string, TraceLesson>>,
  defaultTraceId: string,
): string {
  if (route.kind === 'trace') {
    return route.traceId;
  }
  if (
    historyState !== null &&
    typeof historyState === 'object' &&
    'catalogTraceId' in historyState
  ) {
    const candidate = (historyState as { catalogTraceId?: unknown }).catalogTraceId;
    if (
      typeof candidate === 'string' &&
      Object.prototype.hasOwnProperty.call(traceById, candidate) &&
      Boolean(traceById[candidate])
    ) {
      return candidate;
    }
  }
  return defaultTraceId;
}
