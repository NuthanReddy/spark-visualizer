import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ALL_TRACES,
  COMPONENT_TRACE_BY_ID,
  SCENARIO_TRACES,
  TRACE_BY_ID,
} from '../src/data/traces.ts';
import type { ComponentId, SparkLanguage, TraceLesson } from '../src/data/types.ts';
import {
  DEFAULT_SIMULATION_CONFIG,
  FAULT_MAP,
  adaptSimConfigForLanguage,
  validateSimulationConfig,
  type SimulationConfig,
} from '../src/engine/simulatorTypes.ts';
import {
  encodeCatalogHash,
  encodeSimHash,
  getEffectiveTraceLanguages,
  inferTracePrimaryLanguage,
  isValidSparkLanguage,
  normalizeSparkLanguage,
  parseSimHash,
  parseUrlHash,
  resolveExitSimulationTarget,
  resolveInitialCatalogTraceId,
  resolveLanguageAfterTraceSelect,
  resolveLanguageVariantTraceId,
  type CanvasView,
  type ParsedUrlRoute,
} from '../src/engine/urlRoute.ts';

function expectTraceRoute(
  route: ParsedUrlRoute,
): Extract<ParsedUrlRoute, { kind: 'trace' }> {
  assert.equal(route.kind, 'trace');
  if (route.kind !== 'trace') {
    throw new Error(`Expected trace route, got ${route.kind}`);
  }
  return route;
}

function expectSimRoute(
  route: ParsedUrlRoute,
): Extract<ParsedUrlRoute, { kind: 'sim' }> {
  assert.equal(route.kind, 'sim');
  if (route.kind !== 'sim') {
    throw new Error(`Expected sim route, got ${route.kind}`);
  }
  return route;
}

function makeMockTrace(overrides: Partial<TraceLesson> & { id: string }): TraceLesson {
  return {
    kind: 'lifecycle',
    name: overrides.id,
    subtitle: 'Mock trace',
    summary: 'Mock summary',
    deployMode: 'cluster',
    layerTag: 'TEST',
    focusNodes: ['spark-submit'],
    steps: [],
    messages: [],
    ...overrides,
  };
}

function makeVariantFixture() {
  const pyLifecycle = makeMockTrace({
    id: 'flow-cluster-lifecycle',
    languages: ['pyspark'],
    languageVariantGroup: 'cluster-lifecycle',
  });
  const scalaLifecycle = makeMockTrace({
    id: 'flow-scala-cluster-lifecycle',
    languages: ['scala'],
    languageVariantGroup: 'cluster-lifecycle',
  });
  const javaLifecycle = makeMockTrace({
    id: 'flow-java-cluster-lifecycle',
    languages: ['java'],
    languageVariantGroup: 'cluster-lifecycle',
  });
  const pyUdf = makeMockTrace({
    id: 'flow-python-udf-boundary',
    languages: ['pyspark'],
    languageVariantGroup: 'udf-boundary',
  });
  const jvmUdf = makeMockTrace({
    id: 'flow-jvm-udf-encoder-boundary',
    languages: ['scala', 'java'],
    languageVariantGroup: 'udf-boundary',
  });
  const sharedFlow = makeMockTrace({
    id: 'flow-shuffle-aggregation',
    languages: ['pyspark', 'scala', 'java'],
  });
  const compJvm = makeMockTrace({
    id: 'comp-jvm-user-app',
    kind: 'component',
    componentId: 'jvm-user-app',
    languages: ['scala', 'java'],
  });
  const compCtx = makeMockTrace({
    id: 'comp-spark-context',
    kind: 'component',
    componentId: 'spark-context',
    languages: ['pyspark', 'scala', 'java'],
  });
  const scenario3 = makeMockTrace({
    id: 'scenario-03',
    kind: 'scenario',
    scenarioNumber: 3,
    languages: ['pyspark', 'scala', 'java'],
  });

  const traces: TraceLesson[] = [
    pyLifecycle,
    scalaLifecycle,
    javaLifecycle,
    pyUdf,
    jvmUdf,
    sharedFlow,
    compJvm,
    compCtx,
    scenario3,
  ];
  const traceById: Record<string, TraceLesson> = Object.fromEntries(
    traces.map((t) => [t.id, t]),
  );
  const componentTraceById: Partial<Record<ComponentId, TraceLesson>> = {
    'jvm-user-app': compJvm,
    'spark-context': compCtx,
  };
  return { traces, traceById, componentTraceById };
}

test('language guards, normalization, and primary language inference', () => {
  assert.equal(isValidSparkLanguage('pyspark'), true);
  assert.equal(isValidSparkLanguage('scala'), true);
  assert.equal(isValidSparkLanguage('java'), true);
  assert.equal(isValidSparkLanguage('ruby'), false);
  assert.equal(isValidSparkLanguage('Scala'), false);
  assert.equal(isValidSparkLanguage(null), false);
  assert.equal(isValidSparkLanguage(undefined), false);

  assert.equal(normalizeSparkLanguage(null), 'pyspark');
  assert.equal(normalizeSparkLanguage(undefined), 'pyspark');
  assert.equal(normalizeSparkLanguage('ruby'), 'pyspark');
  assert.equal(normalizeSparkLanguage(' SCALA '), 'scala');
  assert.equal(normalizeSparkLanguage('Java'), 'java');
  assert.equal(normalizeSparkLanguage('invalid', 'scala'), 'scala');

  assert.equal(
    inferTracePrimaryLanguage(makeMockTrace({ id: 'a' })),
    'pyspark',
  );
  assert.equal(
    inferTracePrimaryLanguage(makeMockTrace({ id: 'b', languages: ['java'] })),
    'java',
  );
  assert.equal(
    inferTracePrimaryLanguage(makeMockTrace({ id: 'c', languages: ['scala', 'java'] })),
    'scala',
  );
  assert.equal(
    inferTracePrimaryLanguage(
      makeMockTrace({ id: 'd', languages: ['pyspark', 'scala', 'java'] }),
    ),
    'pyspark',
  );
});

test('1. cold load of legacy hashes defaults to language: pyspark', () => {
  const empty = expectTraceRoute(
    parseUrlHash('', ALL_TRACES, TRACE_BY_ID, COMPONENT_TRACE_BY_ID),
  );
  assert.equal(empty.traceId, 'flow-cluster-lifecycle');
  assert.equal(empty.view, 'topology');
  assert.equal(empty.language, 'pyspark');

  const flow = parseUrlHash(
    '#flow=flow-cluster-lifecycle',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(flow, {
    kind: 'trace',
    traceId: 'flow-cluster-lifecycle',
    view: 'topology',
    language: 'pyspark',
  });

  const comp = parseUrlHash(
    '#component=spark-submit',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(comp, {
    kind: 'trace',
    traceId: 'comp-spark-submit',
    view: 'topology',
    language: 'pyspark',
  });

  const expectedScenario3 =
    SCENARIO_TRACES.find((t) => t.scenarioNumber === 3) ?? SCENARIO_TRACES[2];
  const scenario = parseUrlHash(
    '#scenario=3',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(scenario, {
    kind: 'trace',
    traceId: expectedScenario3.id,
    view: 'topology',
    language: 'pyspark',
  });

  const shortSim = expectSimRoute(
    parseUrlHash(
      '#sim=broadcast-join',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(shortSim.simConfig.jobType, 'broadcast-join');
  assert.equal(shortSim.view, 'topology');
  assert.equal(shortSim.language, 'pyspark');

  const fullSimHash = encodeSimHash(DEFAULT_SIMULATION_CONFIG, 'topology');
  assert.equal(fullSimHash.includes('lang='), false);
  const fullSim = expectSimRoute(
    parseUrlHash(
      fullSimHash,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.deepEqual(fullSim.simConfig, DEFAULT_SIMULATION_CONFIG);
  assert.equal(fullSim.view, 'topology');
  assert.equal(fullSim.language, 'pyspark');
});

test('2. explicit lang=scala and lang=java with view=cluster|sequence on all hash kinds', () => {
  const flowScala = parseUrlHash(
    '#flow=flow-shuffle-aggregation&lang=scala&view=cluster',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(flowScala, {
    kind: 'trace',
    traceId: 'flow-shuffle-aggregation',
    view: 'cluster',
    language: 'scala',
  });

  const compJava = parseUrlHash(
    '#component=spark-context&lang=java&view=sequence',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(compJava, {
    kind: 'trace',
    traceId: 'comp-spark-context',
    view: 'sequence',
    language: 'java',
  });

  const expectedScenario3 =
    SCENARIO_TRACES.find((t) => t.scenarioNumber === 3) ?? SCENARIO_TRACES[2];
  const scenarioScala = parseUrlHash(
    '#scenario=3&lang=scala&view=cluster',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(scenarioScala, {
    kind: 'trace',
    traceId: expectedScenario3.id,
    view: 'cluster',
    language: 'scala',
  });

  const simJava = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=wide-shuffle&data=uniform&mode=client&e=2&c=2&p=8&ess=1&da=0&aqe=1&udf=pickled&fault=none&lang=java&view=sequence',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(simJava.simConfig.deployMode, 'client');
  assert.equal(simJava.simConfig.externalShuffleService, true);
  assert.equal(simJava.view, 'sequence');
  assert.equal(simJava.language, 'java');
});

test('3. invalid lang=ruby falls back to trace primary language (never breaks JVM traces)', () => {
  const badFlow = expectTraceRoute(
    parseUrlHash(
      '#flow=flow-cluster-lifecycle&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badFlow.traceId, 'flow-cluster-lifecycle');
  assert.equal(badFlow.language, 'pyspark');

  const badScalaFlow = expectTraceRoute(
    parseUrlHash(
      '#flow=flow-scala-cluster-lifecycle&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badScalaFlow.traceId, 'flow-cluster-lifecycle');
  assert.equal(badScalaFlow.language, 'scala');

  const badJavaFlow = expectTraceRoute(
    parseUrlHash(
      '#flow=flow-java-cluster-lifecycle&lang=ruby&view=sequence',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badJavaFlow.traceId, 'flow-cluster-lifecycle');
  assert.equal(badJavaFlow.view, 'sequence');
  assert.equal(badJavaFlow.language, 'java');

  const badScenario21 = expectTraceRoute(
    parseUrlHash(
      '#scenario=21&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badScenario21.traceId, 'scenario-21');
  assert.equal(badScenario21.language, 'scala');

  const badJvmComp = expectTraceRoute(
    parseUrlHash(
      '#component=jvm-user-app&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badJvmComp.traceId, 'comp-jvm-user-app');
  assert.equal(badJvmComp.language, 'scala');

  const badComp = expectTraceRoute(
    parseUrlHash(
      '#component=spark-submit&lang=ruby&view=cluster',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badComp.traceId, 'comp-spark-submit');
  assert.equal(badComp.view, 'cluster');
  assert.equal(badComp.language, 'pyspark');

  const badSim = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=wide-shuffle&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(badSim.language, 'pyspark');
});

test('4. direct link to Scala/Java-specific trace without lang infers scala/java automatically', () => {
  const { traces, traceById, componentTraceById } = makeVariantFixture();

  const scalaDirect = parseUrlHash(
    '#flow=flow-scala-cluster-lifecycle',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(scalaDirect, {
    kind: 'trace',
    traceId: 'flow-scala-cluster-lifecycle',
    view: 'topology',
    language: 'scala',
  });

  const javaDirect = parseUrlHash(
    '#flow=flow-java-cluster-lifecycle&view=sequence',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(javaDirect, {
    kind: 'trace',
    traceId: 'flow-java-cluster-lifecycle',
    view: 'sequence',
    language: 'java',
  });

  const jvmUdfDirect = parseUrlHash(
    '#flow=flow-jvm-udf-encoder-boundary',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(jvmUdfDirect, {
    kind: 'trace',
    traceId: 'flow-jvm-udf-encoder-boundary',
    view: 'topology',
    language: 'scala',
  });

  // Real catalog component trace: jvm-user-app has languages: ['scala', 'java']
  const realJvmComp = parseUrlHash(
    '#component=jvm-user-app',
    ALL_TRACES,
    TRACE_BY_ID,
    COMPONENT_TRACE_BY_ID,
  );
  assert.deepEqual(realJvmComp, {
    kind: 'trace',
    traceId: 'comp-jvm-user-app',
    view: 'topology',
    language: 'scala',
  });
});

test('5. conflicting variant + explicit lang resolves deterministically to matching variant', () => {
  const { traces, traceById, componentTraceById } = makeVariantFixture();

  assert.equal(
    resolveLanguageVariantTraceId('flow-cluster-lifecycle', 'scala', traces),
    'flow-scala-cluster-lifecycle',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-cluster-lifecycle', 'java', traces),
    'flow-java-cluster-lifecycle',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-scala-cluster-lifecycle', 'pyspark', traces),
    'flow-cluster-lifecycle',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-python-udf-boundary', 'scala', traces),
    'flow-jvm-udf-encoder-boundary',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-python-udf-boundary', 'java', traces),
    'flow-jvm-udf-encoder-boundary',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-jvm-udf-encoder-boundary', 'java', traces),
    'flow-jvm-udf-encoder-boundary',
  );
  assert.equal(
    resolveLanguageVariantTraceId('flow-jvm-udf-encoder-boundary', 'pyspark', traces),
    'flow-python-udf-boundary',
  );
  assert.equal(
    resolveLanguageVariantTraceId('comp-spark-context', 'scala', traces),
    'comp-spark-context',
  );

  const conflictToScala = parseUrlHash(
    '#flow=flow-cluster-lifecycle&lang=scala',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(conflictToScala, {
    kind: 'trace',
    traceId: 'flow-scala-cluster-lifecycle',
    view: 'topology',
    language: 'scala',
  });

  const conflictToJava = parseUrlHash(
    '#flow=flow-cluster-lifecycle&lang=java&view=cluster',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(conflictToJava, {
    kind: 'trace',
    traceId: 'flow-java-cluster-lifecycle',
    view: 'cluster',
    language: 'java',
  });

  const conflictUdfToJava = parseUrlHash(
    '#flow=flow-python-udf-boundary&lang=java',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(conflictUdfToJava, {
    kind: 'trace',
    traceId: 'flow-jvm-udf-encoder-boundary',
    view: 'topology',
    language: 'java',
  });

  const compatibleJvmUdfJava = parseUrlHash(
    '#flow=flow-jvm-udf-encoder-boundary&lang=java',
    traces,
    traceById,
    componentTraceById,
  );
  assert.deepEqual(compatibleJvmUdfJava, {
    kind: 'trace',
    traceId: 'flow-jvm-udf-encoder-boundary',
    view: 'topology',
    language: 'java',
  });
});

test('6. round-trip encodeCatalogHash and encodeSimHash across all 3 languages and 3 views', () => {
  const languages: SparkLanguage[] = ['pyspark', 'scala', 'java'];
  const views: CanvasView[] = ['topology', 'cluster', 'sequence'];

  const sampleTraces: TraceLesson[] = [
    TRACE_BY_ID['flow-shuffle-aggregation'],
    TRACE_BY_ID['comp-spark-context'],
    SCENARIO_TRACES.find((t) => t.scenarioNumber === 3) ?? SCENARIO_TRACES[2],
  ];

  for (const trace of sampleTraces) {
    assert.ok(trace, 'expected sample trace');
    for (const lang of languages) {
      for (const view of views) {
        const encoded = encodeCatalogHash(trace, view, lang);
        if (lang === 'pyspark') {
          assert.equal(
            encoded.includes('lang='),
            false,
            `PySpark hash should omit lang: ${encoded}`,
          );
        } else {
          assert.equal(
            encoded.includes(`lang=${lang}`),
            true,
            `Non-PySpark hash should include lang=${lang}: ${encoded}`,
          );
        }
        if (view === 'topology') {
          assert.equal(
            encoded.includes('view='),
            false,
            `Topology hash should omit view: ${encoded}`,
          );
        } else {
          assert.equal(
            encoded.includes(`view=${view}`),
            true,
            `Non-topology hash should include view=${view}: ${encoded}`,
          );
        }

        const decoded = parseUrlHash(
          encoded,
          ALL_TRACES,
          TRACE_BY_ID,
          COMPONENT_TRACE_BY_ID,
        );
        assert.deepEqual(decoded, {
          kind: 'trace',
          traceId: trace.id,
          view,
          language: lang,
        });
      }
    }
  }

  const simConfig: SimulationConfig = {
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'client',
    numExecutors: 4,
    executorCores: 2,
    numPartitions: 12,
    externalShuffleService: true,
    dynamicAllocation: true,
    aqeEnabled: true,
  };

  for (const lang of languages) {
    const cfgForLang: SimulationConfig = { ...simConfig, language: lang };
    for (const view of views) {
      const encoded = encodeSimHash(cfgForLang, view);
      assert.equal(encoded, encodeSimHash(simConfig, view, lang));
      if (lang === 'pyspark') {
        assert.equal(encoded.includes('lang='), false);
      } else {
        assert.equal(encoded.includes(`lang=${lang}`), true);
      }
      const decoded = expectSimRoute(
        parseUrlHash(
          encoded,
          ALL_TRACES,
          TRACE_BY_ID,
          COMPONENT_TRACE_BY_ID,
        ),
      );
      assert.deepEqual(decoded.simConfig, cfgForLang);
      assert.equal(decoded.view, view);
      assert.equal(decoded.language, lang);
    }
  }

  // parseSimHash direct check
  const directSim = parseSimHash(new URLSearchParams('sim=broadcast-join'));
  assert.ok(directSim !== null);
  assert.equal(directSim?.jobType, 'broadcast-join');
});

test('7. same-trace lang change produces distinct hashes', () => {
  const sparkContextTrace = TRACE_BY_ID['comp-spark-context'];
  assert.ok(sparkContextTrace);

  const pyHash = encodeCatalogHash(sparkContextTrace, 'topology', 'pyspark');
  const scalaHash = encodeCatalogHash(sparkContextTrace, 'topology', 'scala');
  const javaHash = encodeCatalogHash(sparkContextTrace, 'topology', 'java');

  assert.equal(pyHash, '#component=spark-context');
  assert.equal(scalaHash, '#component=spark-context&lang=scala');
  assert.equal(javaHash, '#component=spark-context&lang=java');
  assert.notEqual(pyHash, scalaHash);
  assert.notEqual(scalaHash, javaHash);
  assert.notEqual(pyHash, javaHash);
});

test('8. getEffectiveTraceLanguages resolves trace and component languages uniformly', () => {
  const compPythonDriver = TRACE_BY_ID['comp-python-driver'];
  const compJvmUserApp = TRACE_BY_ID['comp-jvm-user-app'];
  const compDatasetEncoderUdf = TRACE_BY_ID['comp-dataset-encoder-udf'];
  const scenario21 = TRACE_BY_ID['scenario-21'];
  const flowClusterLifecycle = TRACE_BY_ID['flow-cluster-lifecycle'];
  const flowUdfBoundary = TRACE_BY_ID['flow-python-udf-boundary'];

  assert.ok(compPythonDriver);
  assert.ok(compJvmUserApp);
  assert.ok(compDatasetEncoderUdf);
  assert.ok(scenario21);
  assert.ok(flowClusterLifecycle);
  assert.ok(flowUdfBoundary);

  assert.deepEqual(getEffectiveTraceLanguages(compPythonDriver), ['pyspark']);
  assert.deepEqual(getEffectiveTraceLanguages(compJvmUserApp), ['scala', 'java']);
  assert.deepEqual(getEffectiveTraceLanguages(compDatasetEncoderUdf), [
    'pyspark',
    'scala',
    'java',
  ]);
  assert.deepEqual(getEffectiveTraceLanguages(scenario21), ['scala', 'java']);
  assert.deepEqual(getEffectiveTraceLanguages(flowClusterLifecycle), [
    'pyspark',
    'scala',
    'java',
  ]);
  assert.deepEqual(getEffectiveTraceLanguages(flowUdfBoundary), [
    'pyspark',
    'scala',
    'java',
  ]);

  // Fallback when neither trace.languages nor component.languages is specified
  assert.deepEqual(getEffectiveTraceLanguages(makeMockTrace({ id: 'generic' })), [
    'pyspark',
    'scala',
    'java',
  ]);

  assert.equal(inferTracePrimaryLanguage(compPythonDriver), 'pyspark');
  assert.equal(inferTracePrimaryLanguage(compJvmUserApp), 'scala');
  assert.equal(inferTracePrimaryLanguage(scenario21), 'scala');
  assert.equal(inferTracePrimaryLanguage(flowClusterLifecycle), 'pyspark');
  assert.equal(inferTracePrimaryLanguage(flowUdfBoundary), 'pyspark');
});

test('8b. legacy lifecycle flow aliases resolve cleanly to unified trace IDs + expected language', () => {
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-scala-cluster-lifecycle',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-cluster-lifecycle',
      view: 'topology',
      language: 'scala',
    },
  );
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-java-cluster-lifecycle',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-cluster-lifecycle',
      view: 'topology',
      language: 'java',
    },
  );
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-java-cluster-lifecycle&view=sequence',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-cluster-lifecycle',
      view: 'sequence',
      language: 'java',
    },
  );
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-jvm-udf-encoder-boundary',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-python-udf-boundary',
      view: 'topology',
      language: 'scala',
    },
  );
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-jvm-udf-encoder-boundary&lang=java&view=cluster',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-python-udf-boundary',
      view: 'cluster',
      language: 'java',
    },
  );
  assert.deepEqual(
    parseUrlHash(
      '#flow=flow-scala-cluster-lifecycle&lang=java',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
    {
      kind: 'trace',
      traceId: 'flow-cluster-lifecycle',
      view: 'topology',
      language: 'java',
    },
  );
});

test('9. lossless encodeCatalogHash -> parseUrlHash on language-restricted & unified traces', () => {
  const scenario21 = TRACE_BY_ID['scenario-21'];
  const compPythonDriver = TRACE_BY_ID['comp-python-driver'];
  const compJvmUserApp = TRACE_BY_ID['comp-jvm-user-app'];
  const flowClusterLifecycle = TRACE_BY_ID['flow-cluster-lifecycle'];
  const flowUdfBoundary = TRACE_BY_ID['flow-python-udf-boundary'];

  assert.ok(scenario21);
  assert.ok(compPythonDriver);
  assert.ok(compJvmUserApp);
  assert.ok(flowClusterLifecycle);
  assert.ok(flowUdfBoundary);

  // Explicit exact hash shapes
  assert.equal(
    encodeCatalogHash(scenario21, 'topology', 'pyspark'),
    '#scenario=21&lang=pyspark',
  );
  assert.equal(
    encodeCatalogHash(scenario21, 'topology', 'scala'),
    '#scenario=21&lang=scala',
  );
  assert.equal(
    encodeCatalogHash(scenario21, 'topology', 'java'),
    '#scenario=21&lang=java',
  );
  assert.equal(
    encodeCatalogHash(compPythonDriver, 'topology', 'pyspark'),
    '#component=python-driver',
  );
  assert.equal(
    encodeCatalogHash(compJvmUserApp, 'topology', 'pyspark'),
    '#component=jvm-user-app&lang=pyspark',
  );
  assert.equal(
    encodeCatalogHash(flowClusterLifecycle, 'topology', 'scala'),
    '#flow=flow-cluster-lifecycle&lang=scala',
  );
  assert.equal(
    encodeCatalogHash(flowClusterLifecycle, 'cluster', 'java'),
    '#flow=flow-cluster-lifecycle&lang=java&view=cluster',
  );
  assert.equal(
    encodeCatalogHash(flowUdfBoundary, 'sequence', 'java'),
    '#flow=flow-python-udf-boundary&lang=java&view=sequence',
  );

  // Lossless round-trip across ALL 3 languages and ALL 3 views
  const languages: SparkLanguage[] = ['pyspark', 'scala', 'java'];
  const views: CanvasView[] = ['topology', 'cluster', 'sequence'];
  for (const trace of [
    scenario21,
    compPythonDriver,
    compJvmUserApp,
    flowClusterLifecycle,
    flowUdfBoundary,
  ]) {
    for (const lang of languages) {
      for (const view of views) {
        const hash = encodeCatalogHash(trace, view, lang);
        const decoded = parseUrlHash(
          hash,
          ALL_TRACES,
          TRACE_BY_ID,
          COMPONENT_TRACE_BY_ID,
        );
        assert.deepEqual(decoded, {
          kind: 'trace',
          traceId: trace.id,
          view,
          language: lang,
        });
      }
    }
  }
});

test('10. resolveLanguageAfterTraceSelect reconciles explicit lesson clicks', () => {
  const compPythonDriver = TRACE_BY_ID['comp-python-driver'];
  const compSparkContext = TRACE_BY_ID['comp-spark-context'];
  const compJvmUserApp = TRACE_BY_ID['comp-jvm-user-app'];
  const scenario21 = TRACE_BY_ID['scenario-21'];
  const flowClusterLifecycle = TRACE_BY_ID['flow-cluster-lifecycle'];

  assert.ok(compPythonDriver);
  assert.ok(compSparkContext);
  assert.ok(compJvmUserApp);
  assert.ok(scenario21);
  assert.ok(flowClusterLifecycle);

  // Clicking Python Driver while in Java switches to PySpark (and omits lang in URL)
  const pyAfterJava = resolveLanguageAfterTraceSelect(compPythonDriver, 'java');
  assert.equal(pyAfterJava, 'pyspark');
  assert.equal(
    encodeCatalogHash(compPythonDriver, 'topology', pyAfterJava),
    '#component=python-driver',
  );

  // Clicking shared SparkContext from Java preserves Java
  assert.equal(resolveLanguageAfterTraceSelect(compSparkContext, 'java'), 'java');
  assert.equal(resolveLanguageAfterTraceSelect(compSparkContext, 'scala'), 'scala');

  // Clicking JVM User App from PySpark switches to Scala; from Java preserves Java
  assert.equal(resolveLanguageAfterTraceSelect(compJvmUserApp, 'pyspark'), 'scala');
  assert.equal(resolveLanguageAfterTraceSelect(compJvmUserApp, 'java'), 'java');

  // Clicking Scenario 21 from PySpark switches to Scala; from Java preserves Java
  assert.equal(resolveLanguageAfterTraceSelect(scenario21, 'pyspark'), 'scala');
  assert.equal(resolveLanguageAfterTraceSelect(scenario21, 'java'), 'java');

  // Unified lifecycle flow preserves active language (PySpark, Scala, or Java)
  assert.equal(resolveLanguageAfterTraceSelect(flowClusterLifecycle, 'pyspark'), 'pyspark');
  assert.equal(resolveLanguageAfterTraceSelect(flowClusterLifecycle, 'scala'), 'scala');
  assert.equal(resolveLanguageAfterTraceSelect(flowClusterLifecycle, 'java'), 'java');
});

test('11. resolveExitSimulationTarget resolves unified flow and language cleanly', () => {
  // Unified lifecycle flows keep their stable traceId while adapting activeLanguage in place
  assert.deepEqual(
    resolveExitSimulationTarget('flow-cluster-lifecycle', 'java', ALL_TRACES),
    { traceId: 'flow-cluster-lifecycle', language: 'java' },
  );
  assert.deepEqual(
    resolveExitSimulationTarget('flow-cluster-lifecycle', 'scala', ALL_TRACES),
    { traceId: 'flow-cluster-lifecycle', language: 'scala' },
  );
  assert.deepEqual(
    resolveExitSimulationTarget('flow-cluster-lifecycle', 'pyspark', ALL_TRACES),
    { traceId: 'flow-cluster-lifecycle', language: 'pyspark' },
  );

  // Unified UDF boundary keeps stable traceId
  assert.deepEqual(
    resolveExitSimulationTarget('flow-python-udf-boundary', 'java', ALL_TRACES),
    { traceId: 'flow-python-udf-boundary', language: 'java' },
  );

  // Shared component preserves activeLanguage
  assert.deepEqual(
    resolveExitSimulationTarget('comp-spark-context', 'java', ALL_TRACES),
    { traceId: 'comp-spark-context', language: 'java' },
  );

  // Language-restricted non-variant component reconciles incompatible language
  assert.deepEqual(
    resolveExitSimulationTarget('comp-python-driver', 'java', ALL_TRACES),
    { traceId: 'comp-python-driver', language: 'pyspark' },
  );
  assert.deepEqual(
    resolveExitSimulationTarget('scenario-21', 'pyspark', ALL_TRACES),
    { traceId: 'scenario-21', language: 'scala' },
  );
});

test('12. multi-language simulator URL hash parsing and round-trip', () => {
  // Full #sim=v1 with jvm-udf-dataset + lang=java round-trips cleanly
  const javaDatasetHash =
    '#sim=v1&job=jvm-udf-dataset&data=uniform&mode=cluster&e=3&c=2&p=12&ess=0&da=0&aqe=1&udf=pickled&fault=none&lang=java';
  const javaDatasetParsed = expectSimRoute(
    parseUrlHash(
      javaDatasetHash,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(javaDatasetParsed.simConfig.jobType, 'jvm-udf-dataset');
  assert.equal(javaDatasetParsed.simConfig.language, 'java');
  assert.equal(javaDatasetParsed.language, 'java');
  assert.equal(
    encodeSimHash(javaDatasetParsed.simConfig, javaDatasetParsed.view),
    javaDatasetHash,
  );
  const javaDatasetReParsed = expectSimRoute(
    parseUrlHash(
      encodeSimHash(javaDatasetParsed.simConfig, javaDatasetParsed.view),
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.deepEqual(javaDatasetReParsed, javaDatasetParsed);

  // Full #sim=v1 with wide-shuffle + lang=scala + fault=class-not-found-jar round-trips cleanly
  const scalaCnfParsed = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=wide-shuffle&lang=scala&fault=class-not-found-jar',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(scalaCnfParsed.simConfig.jobType, 'wide-shuffle');
  assert.equal(scalaCnfParsed.simConfig.language, 'scala');
  assert.equal(scalaCnfParsed.simConfig.fault, 'class-not-found-jar');
  assert.equal(scalaCnfParsed.language, 'scala');
  const scalaCnfEncoded = encodeSimHash(
    scalaCnfParsed.simConfig,
    scalaCnfParsed.view,
  );
  assert.equal(scalaCnfEncoded.includes('lang=scala'), true);
  assert.equal(scalaCnfEncoded.includes('fault=class-not-found-jar'), true);
  const scalaCnfReParsed = expectSimRoute(
    parseUrlHash(
      scalaCnfEncoded,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.deepEqual(scalaCnfReParsed, scalaCnfParsed);

  // Short #sim=jvm-udf-dataset&lang=java parses validly with language: 'java'
  const shortJavaParsed = expectSimRoute(
    parseUrlHash(
      '#sim=jvm-udf-dataset&lang=java',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(shortJavaParsed.simConfig.jobType, 'jvm-udf-dataset');
  assert.equal(shortJavaParsed.simConfig.language, 'java');
  assert.equal(shortJavaParsed.language, 'java');

  // Legacy #sim=v1 without lang defaults to language: 'pyspark' and omits lang on re-encode
  const legacyKernelOomParsed = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=wide-shuffle&fault=kernel-node-oom',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(legacyKernelOomParsed.simConfig.jobType, 'wide-shuffle');
  assert.equal(legacyKernelOomParsed.simConfig.fault, 'kernel-node-oom');
  assert.equal(legacyKernelOomParsed.simConfig.language, 'pyspark');
  assert.equal(legacyKernelOomParsed.language, 'pyspark');
  const legacyEncoded = encodeSimHash(
    legacyKernelOomParsed.simConfig,
    legacyKernelOomParsed.view,
  );
  assert.equal(legacyEncoded.includes('lang='), false);
  const legacyReParsed = expectSimRoute(
    parseUrlHash(
      legacyEncoded,
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.deepEqual(legacyReParsed, legacyKernelOomParsed);
});

test('13. #sim=v1 without lang infers JVM language for JVM-only jobs and faults', () => {
  const v1JvmDataset = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=jvm-udf-dataset',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  const shortJvmDataset = expectSimRoute(
    parseUrlHash(
      '#sim=jvm-udf-dataset',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );

  assert.equal(v1JvmDataset.language, 'scala');
  assert.equal(v1JvmDataset.simConfig.language, 'scala');
  assert.equal(v1JvmDataset.simConfig.jobType, 'jvm-udf-dataset');
  assert.deepEqual(v1JvmDataset, shortJvmDataset);

  const v1JvmFault = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=wide-shuffle&fault=class-not-found-jar',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(v1JvmFault.language, 'scala');
  assert.equal(v1JvmFault.simConfig.language, 'scala');
  assert.equal(v1JvmFault.simConfig.jobType, 'wide-shuffle');
  assert.equal(v1JvmFault.simConfig.fault, 'class-not-found-jar');

  const v1InvalidLangJvmJob = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=jvm-udf-dataset&lang=ruby',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(v1InvalidLangJvmJob.language, 'scala');
  assert.equal(v1InvalidLangJvmJob.simConfig.jobType, 'jvm-udf-dataset');
});

test('14. explicit lang in sim URL wins over conflicting fault or job via adaptSimConfigForLanguage', () => {
  const javaPyCrash = expectSimRoute(
    parseUrlHash(
      '#sim=v1&lang=java&fault=python-worker-crash',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  const expectedJavaPyCrash = adaptSimConfigForLanguage(
    {
      ...DEFAULT_SIMULATION_CONFIG,
      language: 'java',
      fault: 'python-worker-crash',
    },
    'java',
  );
  assert.equal(javaPyCrash.language, 'java');
  assert.equal(javaPyCrash.simConfig.language, 'java');
  assert.notEqual(javaPyCrash.simConfig.fault, 'python-worker-crash');
  assert.equal(FAULT_MAP[javaPyCrash.simConfig.fault].requires?.pysparkOnly ?? false, false);
  assert.equal(FAULT_MAP[javaPyCrash.simConfig.fault].requires?.pythonUdf ?? false, false);
  assert.equal(validateSimulationConfig(javaPyCrash.simConfig).valid, true);
  assert.deepEqual(javaPyCrash.simConfig, expectedJavaPyCrash);

  const javaPyVersion = expectSimRoute(
    parseUrlHash(
      '#sim=v1&lang=java&fault=python-version-mismatch',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(javaPyVersion.language, 'java');
  assert.equal(javaPyVersion.simConfig.language, 'java');
  assert.equal(javaPyVersion.simConfig.fault, 'none');

  const pyJvmFault = expectSimRoute(
    parseUrlHash(
      '#sim=v1&lang=pyspark&fault=class-not-found-jar',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(pyJvmFault.language, 'pyspark');
  assert.equal(pyJvmFault.simConfig.language, 'pyspark');
  assert.equal(pyJvmFault.simConfig.fault, 'none');

  const javaPyJob = expectSimRoute(
    parseUrlHash(
      '#sim=v1&lang=java&job=python-udf',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(javaPyJob.language, 'java');
  assert.equal(javaPyJob.simConfig.language, 'java');
  assert.equal(javaPyJob.simConfig.jobType, 'jvm-udf-dataset');

  const shortJavaPyJob = expectSimRoute(
    parseUrlHash(
      '#sim=python-udf&lang=java',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(shortJavaPyJob.language, 'java');
  assert.equal(shortJavaPyJob.simConfig.language, 'java');
  assert.equal(shortJavaPyJob.simConfig.jobType, 'jvm-udf-dataset');
});

test('15. resolveInitialCatalogTraceId restores history.state.catalogTraceId on sim reload', () => {
  const defaultTraceId = 'flow-cluster-lifecycle';
  const javaSimRoute = expectSimRoute(
    parseUrlHash(
      '#sim=v1&job=jvm-udf-dataset&lang=java',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );

  const restored = resolveInitialCatalogTraceId(
    javaSimRoute,
    { catalogTraceId: 'flow-python-udf-boundary' },
    TRACE_BY_ID,
    defaultTraceId,
  );
  assert.equal(restored, 'flow-python-udf-boundary');
  assert.deepEqual(
    resolveExitSimulationTarget(restored, javaSimRoute.language, ALL_TRACES),
    { traceId: 'flow-python-udf-boundary', language: 'java' },
  );

  assert.equal(
    resolveInitialCatalogTraceId(javaSimRoute, null, TRACE_BY_ID, defaultTraceId),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(javaSimRoute, undefined, TRACE_BY_ID, defaultTraceId),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(javaSimRoute, {}, TRACE_BY_ID, defaultTraceId),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(
      javaSimRoute,
      { catalogTraceId: 'non-existent-trace' },
      TRACE_BY_ID,
      defaultTraceId,
    ),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(
      javaSimRoute,
      { catalogTraceId: 'toString' },
      TRACE_BY_ID,
      defaultTraceId,
    ),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(
      javaSimRoute,
      { catalogTraceId: 42 },
      TRACE_BY_ID,
      defaultTraceId,
    ),
    defaultTraceId,
  );
  assert.equal(
    resolveInitialCatalogTraceId(
      javaSimRoute,
      'invalid-state',
      TRACE_BY_ID,
      defaultTraceId,
    ),
    defaultTraceId,
  );

  const traceRoute = expectTraceRoute(
    parseUrlHash(
      '#component=python-driver',
      ALL_TRACES,
      TRACE_BY_ID,
      COMPONENT_TRACE_BY_ID,
    ),
  );
  assert.equal(
    resolveInitialCatalogTraceId(
      traceRoute,
      { catalogTraceId: 'flow-python-udf-boundary' },
      TRACE_BY_ID,
      defaultTraceId,
    ),
    'comp-python-driver',
  );
});

test('16. catalog-return draft adaptation preserves compatible unapplied draft fields', () => {
  const javaDraft: SimulationConfig = {
    ...DEFAULT_SIMULATION_CONFIG,
    language: 'java',
    jobType: 'jvm-udf-dataset',
    numExecutors: 5,
    numPartitions: 24,
    executorCores: 4,
    externalShuffleService: true,
    aqeEnabled: false,
    fault: 'class-not-found-jar',
  };

  const exitTarget = resolveExitSimulationTarget(
    'comp-python-driver',
    'java',
    ALL_TRACES,
  );
  assert.deepEqual(exitTarget, {
    traceId: 'comp-python-driver',
    language: 'pyspark',
  });

  const adapted = adaptSimConfigForLanguage(javaDraft, exitTarget.language);
  assert.equal(adapted.language, 'pyspark');
  assert.equal(adapted.jobType, 'python-udf');
  assert.equal(adapted.fault, 'none');
  assert.equal(adapted.numExecutors, 5);
  assert.equal(adapted.numPartitions, 24);
  assert.equal(adapted.executorCores, 4);
  assert.equal(adapted.externalShuffleService, true);
  assert.equal(adapted.aqeEnabled, false);
  assert.equal(validateSimulationConfig(adapted).valid, true);
  assert.deepEqual(adaptSimConfigForLanguage(adapted, exitTarget.language), adapted);
});
