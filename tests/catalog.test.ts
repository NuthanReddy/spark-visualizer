import assert from 'node:assert/strict';
import test from 'node:test';
import { COMPONENTS, COMPONENT_LIST } from '../src/data/components.ts';
import {
  ALL_TRACES,
  COMPONENT_TRACES,
  COMPONENT_TRACE_BY_ID,
  LIFECYCLE_TRACES,
  SCENARIO_TRACES,
  TRACE_BY_ID,
} from '../src/data/traces.ts';
import { DRIVER_SUB_ZONES, HOST_ZONES, MASTER_CANVAS } from '../src/data/types.ts';
import { adaptTraceToRun, inferStepCategory } from '../src/engine/adaptTrace.ts';
import { computeClusterCardSize } from '../src/engine/mapGeometry.ts';
import { computePeerInteractions } from '../src/engine/peerInteractions.ts';
import { advanceTimeline, buildEdgeCurve, MESSAGE_DURATION_MS } from '../src/playback.ts';

test('catalog defines all 100 components with non-overlapping Master Canvas coordinates and balanced multi-language Venn coverage', () => {
  assert.equal(COMPONENT_LIST.length, 100);
  const ids = new Set(COMPONENT_LIST.map(c => c.id));
  assert.equal(ids.size, 100);

  const sharedComps = COMPONENT_LIST.filter(c => (c.languages?.length ?? 0) === 3).length;
  const pysparkComps = COMPONENT_LIST.filter(c => c.languages?.includes('pyspark')).length;
  const scalaComps = COMPONENT_LIST.filter(c => c.languages?.includes('scala')).length;
  const javaComps = COMPONENT_LIST.filter(c => c.languages?.includes('java')).length;
  assert.equal(sharedComps, 77);
  assert.equal(pysparkComps, 87);
  assert.equal(scalaComps, 89);
  assert.equal(javaComps, 88);
  assert.ok(scalaComps > pysparkComps && javaComps > pysparkComps);

  const subZoneByLabel = new Map(DRIVER_SUB_ZONES.map(s => [s.label, s.rect]));
  const { cardW, cardH } = computeClusterCardSize(COMPONENT_LIST);
  assert.equal(cardW, 166);
  assert.equal(cardH, 44);
  const halfW = cardW / 2;
  const halfH = cardH / 2;
  const HOST_HEADER_DIVIDER_OFFSET = 34;
  const SUB_ZONE_HEADER_CLEAR_Y = 22;

  for (const comp of COMPONENT_LIST) {
    const [x, y] = comp.clusterPos;
    const left = x - halfW;
    const right = x + halfW;
    const top = y - halfH;
    const bottom = y + halfH;
    const zone = HOST_ZONES[comp.hostZone].rect;
    assert.ok(left >= 0 && right <= MASTER_CANVAS.width, `${comp.id}: card x=[${left},${right}] out of canvas`);
    assert.ok(top >= 0 && bottom <= MASTER_CANVAS.height, `${comp.id}: card y=[${top},${bottom}] out of canvas`);
    assert.ok(
      left >= zone.x + 8 &&
        right <= zone.x + zone.w - 8 &&
        top > zone.y + HOST_HEADER_DIVIDER_OFFSET &&
        bottom <= zone.y + zone.h - 8,
      `${comp.id}: rendered card [${left},${top},${right},${bottom}] violates host zone ${comp.hostZone} bounds/header`,
    );
    if (comp.hostZone === 'driver-jvm') {
      assert.ok(comp.subZone, `${comp.id}: missing driver-jvm subZone`);
      const subRect = subZoneByLabel.get(comp.subZone);
      assert.ok(subRect, `${comp.id}: unknown driver-jvm subZone "${comp.subZone}"`);
      assert.ok(
        left >= subRect.x + 8 &&
          right <= subRect.x + subRect.w - 8 &&
          top > subRect.y + SUB_ZONE_HEADER_CLEAR_Y &&
          bottom <= subRect.y + subRect.h - 8,
        `${comp.id}: rendered card [${left},${top},${right},${bottom}] violates driver sub-zone ${comp.subZone} bounds/header`,
      );
    }
    assert.ok(comp.languages && comp.languages.length >= 1, `${comp.id}: missing languages`);
    assert.ok(comp.role.length > 10, `${comp.id}: missing role`);
    assert.ok(comp.keyDistinction.length > 10, `${comp.id}: missing keyDistinction`);
    assert.ok(comp.inputs.length >= 2, `${comp.id}: expected >=2 inputs`);
    assert.ok(comp.outputs.length >= 2, `${comp.id}: expected >=2 outputs`);
    assert.ok(comp.internalState.length >= 2, `${comp.id}: expected >=2 internalState items`);
    assert.ok(comp.failureClues.length >= 2, `${comp.id}: expected >=2 failureClues`);
    assert.ok(comp.sourceLinks.length >= 1, `${comp.id}: expected >=1 sourceLink`);
    for (const link of comp.sourceLinks) {
      assert.match(link.url, /^https:\/\//, `${comp.id}: invalid URL ${link.url}`);
    }
    assert.equal(comp.docFile, `${comp.id}.md`);
  }

  // Submission-language vs execution-capability boundaries (Plan v2.1)
  assert.deepEqual([...(COMPONENTS['jvm-user-app'].languages ?? [])], ['scala', 'java']);
  assert.deepEqual([...(COMPONENTS['dataset-encoder-udf'].languages ?? [])], ['pyspark', 'scala', 'java']);
  assert.deepEqual([...(COMPONENTS['python-driver'].languages ?? [])], ['pyspark']);
  assert.deepEqual([...(COMPONENTS['py4j-gateway'].languages ?? [])], ['pyspark']);

  // Pairwise non-overlap check using the actual rendered cardW x cardH (166 x 44) rectangles
  for (let i = 0; i < COMPONENT_LIST.length; i++) {
    for (let j = i + 1; j < COMPONENT_LIST.length; j++) {
      const a = COMPONENT_LIST[i];
      const b = COMPONENT_LIST[j];
      const leftA = a.clusterPos[0] - halfW;
      const rightA = a.clusterPos[0] + halfW;
      const topA = a.clusterPos[1] - halfH;
      const bottomA = a.clusterPos[1] + halfH;
      const leftB = b.clusterPos[0] - halfW;
      const rightB = b.clusterPos[0] + halfW;
      const topB = b.clusterPos[1] - halfH;
      const bottomB = b.clusterPos[1] + halfH;
      const intersects =
        leftA < rightB && rightA > leftB && topA < bottomB && bottomA > topB;
      assert.equal(
        intersects,
        false,
        `Rendered ${cardW}x${cardH} card overlap on Master Canvas between ${a.id} (${a.clusterPos}) and ${b.id} (${b.clusterPos})`,
      );
    }
  }
});

test('traces cover 11 lifecycle flows, 28 scenarios, and all 100 components', () => {
  assert.equal(LIFECYCLE_TRACES.length, 11);
  assert.equal(SCENARIO_TRACES.length, 28);
  assert.equal(COMPONENT_TRACES.length, 100);
  assert.equal(ALL_TRACES.length, 139);

  for (const flow of LIFECYCLE_TRACES) {
    if (flow.id === 'flow-typed-dataset-aggregator') {
      assert.deepEqual([...(flow.languages ?? [])], ['scala', 'java']);
    } else {
      assert.deepEqual(
        [...(flow.languages ?? [])],
        ['pyspark', 'scala', 'java'],
        `${flow.id}: expected unified lifecycle flow to support all 3 languages`,
      );
    }
    assert.equal(
      flow.languageVariantGroup,
      undefined,
      `${flow.id}: unified lifecycle flow should not carry obsolete languageVariantGroup`,
    );
  }

  const scenarioNums = SCENARIO_TRACES.map(t => t.scenarioNumber).sort((a, b) => (a ?? 0) - (b ?? 0));
  assert.deepEqual(
    scenarioNums,
    Array.from({ length: 28 }, (_, i) => i + 1),
  );

  for (const comp of COMPONENT_LIST) {
    assert.ok(COMPONENT_TRACE_BY_ID[comp.id], `Missing dedicated trace for component ${comp.id}`);
  }

  const seenIds = new Set<string>();
  for (const trace of ALL_TRACES) {
    assert.ok(!seenIds.has(trace.id), `Duplicate trace id: ${trace.id}`);
    seenIds.add(trace.id);
    assert.ok(trace.steps.length >= 3, `${trace.id}: expected >=3 steps`);
    assert.ok(trace.messages.length >= 4, `${trace.id}: expected >=4 messages`);

    const focusSet = new Set(trace.focusNodes);
    if (trace.componentId) {
      assert.ok(focusSet.has(trace.componentId), `${trace.id}: focusNodes missing ${trace.componentId}`);
    }
    for (const node of trace.focusNodes) {
      assert.ok(COMPONENTS[node], `${trace.id}: unknown focusNode ${node}`);
    }

    for (const msg of trace.messages) {
      assert.ok(COMPONENTS[msg.from], `${trace.id}/${msg.id}: unknown from ${msg.from}`);
      assert.ok(COMPONENTS[msg.to], `${trace.id}/${msg.id}: unknown to ${msg.to}`);
      assert.ok(focusSet.has(msg.from), `${trace.id}/${msg.id}: from=${msg.from} not in focusNodes`);
      assert.ok(focusSet.has(msg.to), `${trace.id}/${msg.id}: to=${msg.to} not in focusNodes`);
      assert.ok(
        msg.step >= 0 && msg.step < trace.steps.length,
        `${trace.id}/${msg.id}: step=${msg.step} out of range 0..${trace.steps.length - 1}`,
      );
      assert.ok(msg.protocol.length > 0 && msg.port.length > 0 && msg.label.length > 0);
      assert.ok(msg.fields.length >= 2, `${trace.id}/${msg.id}: expected >=2 wire fields`);
    }
  }
});

test('playback clock and curve sampler behave deterministically', () => {
  const start = advanceTimeline({ cursor: 0, progress: 0 }, MESSAGE_DURATION_MS / 2, 1, 4);
  assert.equal(start.cursor, 0);
  assert.ok(Math.abs(start.progress - 0.5) < 1e-6);

  const next = advanceTimeline(start, MESSAGE_DURATION_MS / 2, 2, 4);
  assert.equal(next.cursor, 1);
  assert.ok(Math.abs(next.progress - 0.5) < 1e-6);

  const end = advanceTimeline({ cursor: 3, progress: 0.8 }, MESSAGE_DURATION_MS, 1, 4);
  assert.deepEqual(end, { cursor: 3, progress: 1 });

  const curve = buildEdgeCurve([100, 100], [500, 300]);
  assert.deepEqual(curve.sample(0), [100, 100]);
  assert.deepEqual(curve.sample(1), [500, 300]);
  assert.equal(curve.trail(0.5, 10).length, 11);

  const loop = buildEdgeCurve([200, 200], [200, 200]);
  assert.ok(loop.path.includes('A '));
  assert.ok(loop.midpoint[1] < 200);
});

test('observability components, traces, categories, and peer interactions are wired cleanly', () => {
  assert.deepEqual(COMPONENTS['listener-bus'].clusterPos, [1868, 304]);
  assert.deepEqual(COMPONENTS['app-status-store'].clusterPos, [1868, 400]);
  assert.deepEqual(COMPONENTS['spark-ui'].clusterPos, [1868, 496]);
  assert.deepEqual(COMPONENTS['event-log'].clusterPos, [1868, 592]);
  assert.deepEqual(COMPONENTS['metrics-system'].clusterPos, [1868, 688]);
  assert.deepEqual(COMPONENTS['executor-metrics-poller'].clusterPos, [2024, 1032]);

  const msTrace = TRACE_BY_ID['comp-metrics-system'];
  const empTrace = TRACE_BY_ID['comp-executor-metrics-poller'];
  const obsFlow = TRACE_BY_ID['flow-observability-lifecycles'];
  assert.ok(msTrace && msTrace.steps.length === 5 && msTrace.messages.length === 6);
  assert.ok(empTrace && empTrace.steps.length === 5 && empTrace.messages.length === 6);
  assert.ok(
    obsFlow &&
      obsFlow.kind === 'lifecycle' &&
      obsFlow.deployMode === 'cluster' &&
      obsFlow.steps.length === 6 &&
      obsFlow.messages.length >= 10 &&
      obsFlow.messages.length <= 12,
  );

  assert.equal(
    inferStepCategory('spark-env', 'metrics-system', msTrace.messages[0]),
    'heartbeats-metrics',
  );
  assert.equal(
    inferStepCategory('executor', 'executor-metrics-poller', empTrace.messages[0]),
    'heartbeats-metrics',
  );

  const clientObsRun = adaptTraceToRun({ ...obsFlow, deployMode: 'client' });
  assert.equal(clientObsRun.instances['metrics-system'].hostZone, 'submit-host');
  assert.equal(clientObsRun.instances['executor-metrics-poller'].hostZone, 'worker-node-b');

  for (const comp of COMPONENT_LIST) {
    const peers = computePeerInteractions(comp.id);
    assert.ok(
      peers.receivesFrom.every(p => p.peerId !== comp.id),
      `${comp.id}: self-loop leaked into receivesFrom`,
    );
    assert.ok(
      peers.sendsTo.every(p => p.peerId !== comp.id),
      `${comp.id}: self-loop leaked into sendsTo`,
    );
    assert.ok(
      peers.receivesFrom.length + peers.sendsTo.length > 0,
      `${comp.id}: expected at least one peer interaction`,
    );
  }
  const msPeers = computePeerInteractions('metrics-system');
  assert.ok(msPeers.receivesFrom.some(p => p.peerId === 'executor-metrics-poller'));
  assert.ok(msPeers.sendsTo.some(p => p.peerId === 'spark-ui'));
});

test('computePeerInteractions filters and adapts peers by active SparkLanguage', () => {
  const scalaSubmitPeers = computePeerInteractions('spark-submit', 'scala');
  const scalaAll = [...scalaSubmitPeers.receivesFrom, ...scalaSubmitPeers.sendsTo];
  assert.ok(
    scalaAll.some(p => p.peerId === 'jvm-user-app'),
    'Expected spark-submit in Scala mode to include jvm-user-app peer',
  );
  assert.equal(
    scalaAll.some(p => p.peerId === 'python-driver'),
    false,
    'Expected spark-submit in Scala mode to exclude python-driver peer',
  );

  const javaSubmitPeers = computePeerInteractions('spark-submit', 'java');
  const javaAll = [...javaSubmitPeers.receivesFrom, ...javaSubmitPeers.sendsTo];
  assert.ok(
    javaAll.some(p => p.peerId === 'jvm-user-app'),
    'Expected spark-submit in Java mode to include jvm-user-app peer',
  );
  assert.equal(
    javaAll.some(p => p.peerId === 'python-driver'),
    false,
    'Expected spark-submit in Java mode to exclude python-driver peer',
  );

  const pySubmitPeers = computePeerInteractions('spark-submit', 'pyspark');
  const pyAll = [...pySubmitPeers.receivesFrom, ...pySubmitPeers.sendsTo];
  assert.ok(
    pyAll.some(p => p.peerId === 'python-driver'),
    'Expected spark-submit in PySpark mode to include python-driver peer',
  );
});
