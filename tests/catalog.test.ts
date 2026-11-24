import assert from 'node:assert/strict';
import test from 'node:test';
import { COMPONENTS, COMPONENT_LIST } from '../src/data/components.ts';
import {
  ALL_TRACES,
  COMPONENT_TRACES,
  COMPONENT_TRACE_BY_ID,
  LIFECYCLE_TRACES,
  SCENARIO_TRACES,
} from '../src/data/traces.ts';
import { HOST_ZONES, MASTER_CANVAS } from '../src/data/types.ts';
import { advanceTimeline, buildEdgeCurve, MESSAGE_DURATION_MS } from '../src/playback.ts';

test('catalog defines all 54 components with non-overlapping Master Canvas coordinates', () => {
  assert.equal(COMPONENT_LIST.length, 54);
  const ids = new Set(COMPONENT_LIST.map(c => c.id));
  assert.equal(ids.size, 54);

  for (const comp of COMPONENT_LIST) {
    const [x, y] = comp.clusterPos;
    const zone = HOST_ZONES[comp.hostZone].rect;
    assert.ok(x >= 0 && x <= MASTER_CANVAS.width, `${comp.id}: x=${x} out of canvas`);
    assert.ok(y >= 0 && y <= MASTER_CANVAS.height, `${comp.id}: y=${y} out of canvas`);
    assert.ok(
      x >= zone.x + 60 && x <= zone.x + zone.w - 60 && y >= zone.y + 40 && y <= zone.y + zone.h - 20,
      `${comp.id}: (${x}, ${y}) outside host zone ${comp.hostZone}`,
    );
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

  // Pairwise non-overlap check for 156x48 node pills
  for (let i = 0; i < COMPONENT_LIST.length; i++) {
    for (let j = i + 1; j < COMPONENT_LIST.length; j++) {
      const a = COMPONENT_LIST[i];
      const b = COMPONENT_LIST[j];
      const dx = Math.abs(a.clusterPos[0] - b.clusterPos[0]);
      const dy = Math.abs(a.clusterPos[1] - b.clusterPos[1]);
      assert.ok(
        dx >= 150 || dy >= 56,
        `Overlap on Master Canvas between ${a.id} (${a.clusterPos}) and ${b.id} (${b.clusterPos})`,
      );
    }
  }
});

test('traces cover 6 lifecycle flows, 20 scenarios, and all 54 components', () => {
  assert.equal(LIFECYCLE_TRACES.length, 6);
  assert.equal(SCENARIO_TRACES.length, 20);
  assert.equal(COMPONENT_TRACES.length, 54);
  assert.equal(ALL_TRACES.length, 80);

  const scenarioNums = SCENARIO_TRACES.map(t => t.scenarioNumber).sort((a, b) => (a ?? 0) - (b ?? 0));
  assert.deepEqual(
    scenarioNums,
    Array.from({ length: 20 }, (_, i) => i + 1),
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
