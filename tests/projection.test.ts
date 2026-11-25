import assert from 'node:assert/strict';
import test from 'node:test';
import { DEEP_STEP_ENRICHMENTS } from '../src/data/deepSteps.ts';
import {
  ALL_STEP_CATEGORIES,
  CATEGORY_PRESETS,
  CONTROL_ONLY_CATEGORIES,
  HIGH_LEVEL_CATEGORIES,
  STANDARD_CATEGORIES,
  STEP_CATEGORIES,
  detectCategoryPreset,
  type ExecutionRun,
  type RunEvent,
  type StepCategory,
} from '../src/data/runModel.ts';
import { ALL_TRACES, SCENARIO_TRACES } from '../src/data/traces.ts';
import { adaptTraceToRun, isLocalPort } from '../src/engine/adaptTrace.ts';
import {
  collectStepIdsToExpandForStep,
  computeDeepOnlyCategoryCounts,
  computeHiddenCategoriesForStep,
  computeRunWideCategoryCounts,
  getDirectChildren,
  getExpandableStepIds,
  getStepAncestors,
  projectRun,
  resolveEventAfterProjectionChange,
  resolveEventAfterToggleExpand,
  stepJumpEventId,
} from '../src/engine/projection.ts';

/**
 * Phase 1 Contract Gate Fixture (requested by GPT-6.2 Astra Phase 0 review):
 * Exercises:
 * 1. Multi-event step (Step 1 has 2 events: e1a, e1b)
 * 2. Hierarchical macro step with children (Step 2 macro summarizes 2a + 2b; expanding reveals children without replaying the macro summary)
 * 3. Category filtering out the currently selected event -> deterministic next-surviving fallback
 * 4. Empty filter projection -> null eventId, zero silent re-enabling, skipped span recorded
 * 5. Same-kind transfer between two distinct runtime instances (exec-1/bm -> exec-2/bm, both componentId='block-manager', peerTag='E1 -> E2')
 */
function buildContractFixture(): ExecutionRun {
  return {
    id: 'fixture-contract',
    sourceKind: 'simulation',
    traceKind: 'lifecycle',
    name: 'Contract Gate Fixture',
    subtitle: 'Hierarchy, projection filtering, and same-kind peer instances',
    summary: 'Proves Step/Event tree invariants before UI consumption.',
    deployMode: 'cluster',
    layerTag: 'Broadcast + YARN',
    instances: {
      'am-1': {
        id: 'am-1',
        componentId: 'application-master',
        label: 'ApplicationMaster (Attempt 1)',
        shortLabel: 'AM #1',
        hostZone: 'driver-jvm',
        hostLabel: 'driver-1',
        processId: 'AM JVM',
        attemptNumber: 1,
      },
      'driver-bm': {
        id: 'driver-bm',
        componentId: 'block-manager',
        label: 'Driver BlockManager',
        shortLabel: 'Driver BM',
        hostZone: 'driver-jvm',
        hostLabel: 'driver-1:41001',
        processId: 'Driver JVM',
      },
      'exec-1/bm': {
        id: 'exec-1/bm',
        componentId: 'block-manager',
        label: 'Executor 1 BlockManager',
        shortLabel: 'Exec-1 BM',
        hostZone: 'worker-node-a',
        hostLabel: 'worker-a:42001',
        processId: 'Executor 1 JVM',
        executorNumber: 1,
      },
      'exec-2/bm': {
        id: 'exec-2/bm',
        componentId: 'block-manager',
        label: 'Executor 2 BlockManager',
        shortLabel: 'Exec-2 BM',
        hostZone: 'worker-node-b',
        hostLabel: 'worker-b:42002',
        processId: 'Executor 2 JVM',
        executorNumber: 2,
      },
    },
    focusInstances: ['am-1', 'driver-bm', 'exec-1/bm', 'exec-2/bm'],
    steps: [
      {
        id: 's1',
        title: 'Submit & Register',
        explanation: 'Multi-event startup step.',
        category: 'yarn-lifecycle',
      },
      {
        id: 's2-macro',
        title: 'TorrentBroadcast Distribution',
        explanation: 'Macro summary of chunked broadcast distribution.',
        category: 'broadcast-torrent',
        isMacro: true,
      },
      {
        id: 's2a',
        parentId: 's2-macro',
        title: 'Driver Publishes Serialized Pieces',
        explanation: 'Driver stores all 4 MiB serialized broadcast pieces.',
        category: 'broadcast-torrent',
      },
      {
        id: 's2b',
        parentId: 's2-macro',
        title: 'Executor 2 Fetches Piece from Executor 1',
        explanation: 'Illustrative seeded peer-to-peer block fetch.',
        category: 'broadcast-torrent',
      },
      {
        id: 's3',
        title: 'Final Commit',
        explanation: 'Job commit finishes.',
        category: 'output-commit',
      },
    ],
    events: [
      {
        id: 'e1a',
        stepId: 's1',
        kind: 'wire',
        fromInstance: 'am-1',
        toInstance: 'driver-bm',
        fromComponent: 'application-master',
        toComponent: 'block-manager',
        channel: 'internal',
        protocol: 'In-JVM',
        port: 'in-JVM',
        label: 'Init Driver BM',
        info: 'Initialize driver block manager.',
        size: 'In-JVM',
        fields: [{ name: 'executorId', value: 'driver' }],
      },
      {
        id: 'e1b',
        stepId: 's1',
        kind: 'local',
        fromInstance: 'driver-bm',
        toInstance: 'driver-bm',
        fromComponent: 'block-manager',
        toComponent: 'block-manager',
        channel: 'internal',
        protocol: 'In-JVM',
        port: 'driver-1:41001',
        label: 'Bind BM Listener',
        info: 'Bind block transfer listener on driver.',
        size: 'Local',
        fields: [{ name: 'port', value: '41001' }],
      },
      {
        id: 'e2-summary',
        stepId: 's2-macro',
        kind: 'wire',
        fromInstance: 'driver-bm',
        toInstance: 'exec-1/bm',
        fromComponent: 'block-manager',
        toComponent: 'block-manager',
        channel: 'data',
        protocol: 'BlockTransfer · TorrentBroadcast',
        port: 'driver-1:41001',
        label: 'Broadcast Pieces',
        info: 'Collapsed macro summary of TorrentBroadcast.',
        size: '12 MiB (3 pieces)',
        fields: [{ name: 'pieces', value: '3 x 4 MiB' }],
        peerTag: 'Driver -> E1',
      },
      {
        id: 'e2a',
        stepId: 's2a',
        kind: 'wire',
        fromInstance: 'driver-bm',
        toInstance: 'exec-1/bm',
        fromComponent: 'block-manager',
        toComponent: 'block-manager',
        channel: 'data',
        protocol: 'BlockTransfer · OneForOneBlockFetcher',
        port: 'driver-1:41001',
        label: 'Fetch piece_0',
        info: 'Executor 1 fetches broadcast_0_piece0 from Driver.',
        size: '4 MiB',
        fields: [{ name: 'blockId', value: 'broadcast_0_piece0' }],
        peerTag: 'Driver -> E1',
      },
      {
        id: 'e2b',
        stepId: 's2b',
        kind: 'wire',
        fromInstance: 'exec-1/bm',
        toInstance: 'exec-2/bm',
        fromComponent: 'block-manager',
        toComponent: 'block-manager',
        channel: 'data',
        protocol: 'BlockTransfer · OneForOneBlockFetcher',
        port: 'worker-a:42001',
        label: 'P2P Fetch piece_0',
        info: 'Executor 2 fetches broadcast_0_piece0 from Executor 1.',
        size: '4 MiB',
        fields: [{ name: 'blockId', value: 'broadcast_0_piece0' }],
        peerTag: 'E1 -> E2',
      },
      {
        id: 'e3',
        stepId: 's3',
        kind: 'wire',
        fromInstance: 'exec-2/bm',
        toInstance: 'am-1',
        fromComponent: 'block-manager',
        toComponent: 'application-master',
        channel: 'rpc',
        protocol: 'Spark RPC · Netty',
        port: 'driver-1:41000',
        label: 'Commit Done',
        info: 'Final commit status.',
        size: '512 B',
        fields: [{ name: 'status', value: 'SUCCEEDED' }],
      },
    ],
  };
}

test('collapsed macro step hides descendants; expanding replaces summary without duplicate playback', () => {
  const run = buildContractFixture();
  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  assert.deepEqual(
    collapsed.visibleFrames.map(f => f.event.id),
    ['e1a', 'e1b', 'e2-summary', 'e3'],
  );
  assert.equal(collapsed.visibleSteps.length, 3);
  assert.equal(collapsed.visibleSteps[1].hasChildren, true);
  assert.equal(collapsed.visibleSteps[1].childCount, 2);

  // Step jump moves cleanly across multi-event Step 1 -> Step 2 -> Step 3
  assert.equal(stepJumpEventId('e1a', collapsed, 1), 'e2-summary');
  assert.equal(stepJumpEventId('e1b', collapsed, 1), 'e2-summary');
  assert.equal(stepJumpEventId('e2-summary', collapsed, 1), 'e3');
  assert.equal(stepJumpEventId('e2-summary', collapsed, -1), 'e1a');

  // Expand Step 2: summary disappears and e2a, e2b appear with ancestor s2-macro
  const expandedSet = new Set(['s2-macro']);
  const expanded = projectRun(run, expandedSet, ALL_STEP_CATEGORIES);
  assert.deepEqual(
    expanded.visibleFrames.map(f => f.event.id),
    ['e1a', 'e1b', 'e2a', 'e2b', 'e3'],
  );
  assert.equal(expanded.visibleFrames[3].event.peerTag, 'E1 -> E2');
  assert.equal(expanded.visibleFrames[3].event.fromInstance, 'exec-1/bm');
  assert.equal(expanded.visibleFrames[3].event.toInstance, 'exec-2/bm');
  assert.equal(expanded.visibleFrames[3].event.fromComponent, 'block-manager');
  assert.equal(expanded.visibleFrames[3].event.toComponent, 'block-manager');
  assert.deepEqual(
    expanded.visibleFrames[3].ancestors.map(a => a.id),
    ['s2-macro'],
  );

  // Expanding jumps cleanly to first descendant event; collapsing jumps back to macro summary
  assert.equal(
    resolveEventAfterToggleExpand('s2-macro', false, run, expanded, 'e2-summary'),
    'e2a',
  );
  assert.equal(
    resolveEventAfterToggleExpand('s2-macro', true, run, collapsed, 'e2b'),
    'e2-summary',
  );
});

test('category filter skips hidden events cleanly, preserves canonical survival order, and handles empty matches', () => {
  const run = buildContractFixture();
  const expandedSet = new Set(['s2-macro']);

  // Hide broadcast-torrent while user is standing on e2b
  const withoutBroadcast = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(c => c !== 'broadcast-torrent'),
  );
  const filtered = projectRun(run, expandedSet, withoutBroadcast);
  assert.deepEqual(
    filtered.visibleFrames.map(f => f.event.id),
    ['e1a', 'e1b', 'e3'],
  );
  assert.equal(filtered.skippedSpans.length, 1);
  assert.equal(filtered.skippedSpans[0].beforeFrameIndex, 2);
  assert.equal(filtered.skippedSpans[0].eventCount, 2);
  assert.deepEqual(filtered.skippedSpans[0].categories, ['broadcast-torrent']);

  // Currently selected e2b was filtered out -> advances deterministically to next surviving event e3
  assert.equal(resolveEventAfterProjectionChange('e2b', run, filtered), 'e3');
  // Surviving e1b stays exact
  assert.equal(resolveEventAfterProjectionChange('e1b', run, filtered), 'e1b');

  // If output-commit is also hidden while standing on e3 -> falls back backward to e1b
  const onlyYarn = new Set<StepCategory>(['yarn-lifecycle']);
  const filteredBack = projectRun(run, expandedSet, onlyYarn);
  assert.equal(resolveEventAfterProjectionChange('e3', run, filteredBack), 'e1b');

  // Empty filter -> 0 frames, null selected event, explicit skipped span covering all events
  const empty = projectRun(run, expandedSet, new Set<StepCategory>());
  assert.equal(empty.visibleFrames.length, 0);
  assert.equal(resolveEventAfterProjectionChange('e1a', run, empty), null);
  assert.equal(empty.skippedSpans.length, 1);
  assert.equal(empty.skippedSpans[0].eventCount, 5);
});

test('adaptTraceToRun maps customInstances, peerTag, and decoupled local kind cleanly', () => {
  // Every catalog trace adapts with valid instance bindings
  for (const trace of ALL_TRACES) {
    const run = adaptTraceToRun(trace);
    for (const ev of run.events) {
      assert.ok(run.instances[ev.fromInstance], `${trace.id}/${ev.id}: missing fromInstance ${ev.fromInstance}`);
      assert.ok(run.instances[ev.toInstance], `${trace.id}/${ev.id}: missing toInstance ${ev.toInstance}`);
    }
  }

  // Scenario 01: pre-submit verifyClusterResources failure is local despite error channel
  const s01 = SCENARIO_TRACES.find(t => t.id === 'scenario-01')!;
  const r01 = adaptTraceToRun(s01);
  const verifyEv = r01.events.find(e => e.id === 'scenario-01:s01-4')!;
  assert.equal(verifyEv.kind, 'local');
  assert.equal(verifyEv.channel, 'error');

  // Scenario 09: driver-bm, exec-1/bm, exec-2/bm replace bare block-manager with peer tags
  const s09 = SCENARIO_TRACES.find(t => t.id === 'scenario-09')!;
  const r09 = adaptTraceToRun(s09);
  assert.ok(r09.focusInstances.includes('driver-bm'));
  assert.ok(r09.focusInstances.includes('exec-1/bm'));
  assert.ok(r09.focusInstances.includes('exec-2/bm'));
  assert.ok(!r09.focusInstances.includes('block-manager'));
  const e1Fetch = r09.events.find(e => e.id === 'scenario-09:s09-5')!;
  assert.equal(e1Fetch.fromInstance, 'driver-bm');
  assert.equal(e1Fetch.toInstance, 'exec-1/bm');
  assert.equal(e1Fetch.peerTag, 'Driver -> E1');
  const e2Fetch = r09.events.find(e => e.id === 'scenario-09:s09-6')!;
  assert.equal(e2Fetch.fromInstance, 'exec-1/bm');
  assert.equal(e2Fetch.toInstance, 'exec-2/bm');
  assert.equal(e2Fetch.peerTag, 'E1 -> E2');

  // Scenario 18: driver-commit & worker-commit replace bare file-commit-protocol
  const s18 = SCENARIO_TRACES.find(t => t.id === 'scenario-18')!;
  const r18 = adaptTraceToRun(s18);
  assert.ok(r18.focusInstances.includes('driver-commit'));
  assert.ok(r18.focusInstances.includes('worker-commit'));
  assert.ok(!r18.focusInstances.includes('file-commit-protocol'));
  const commitTaskEv = r18.events.find(e => e.id === 'scenario-18:s18-2')!;
  assert.equal(commitTaskEv.toInstance, 'worker-commit');
  const commitJobEv = r18.events.find(e => e.id === 'scenario-18:s18-4')!;
  assert.equal(commitJobEv.toInstance, 'driver-commit');
  const abortJobEv = r18.events.find(e => e.id === 'scenario-18:s18-7')!;
  assert.equal(abortJobEv.toInstance, 'driver-commit');
});

function buildThreeLevelFixture(): ExecutionRun {
  return {
    id: 'fixture-3-level',
    sourceKind: 'simulation',
    traceKind: 'lifecycle',
    name: '3-Level Nested Fixture',
    subtitle: 'Level 1 -> Level 2 -> Level 3 hierarchy & breadcrumbs',
    summary: 'Validates multi-level expand/collapse and breadcrumb chains.',
    deployMode: 'cluster',
    layerTag: 'SQL + Shuffle',
    instances: {
      'dag-scheduler': {
        id: 'dag-scheduler',
        componentId: 'dag-scheduler',
        label: 'DAGScheduler',
        shortLabel: 'DAGSched',
        hostZone: 'driver-jvm',
        hostLabel: 'driver-1',
        processId: 'Driver JVM',
      },
      'shuffle-writer': {
        id: 'shuffle-writer',
        componentId: 'shuffle-writer',
        label: 'SortShuffleWriter',
        shortLabel: 'ShufWrite',
        hostZone: 'worker-node-a',
        hostLabel: 'worker-a',
        processId: 'Executor 1 JVM',
      },
      'shuffle-reader': {
        id: 'shuffle-reader',
        componentId: 'shuffle-reader',
        label: 'BlockStoreShuffleReader',
        shortLabel: 'ShufRead',
        hostZone: 'worker-node-b',
        hostLabel: 'worker-b',
        processId: 'Executor 2 JVM',
      },
    },
    focusInstances: ['dag-scheduler', 'shuffle-writer', 'shuffle-reader'],
    steps: [
      {
        id: 's0',
        title: 'Submit Job',
        explanation: 'Initial job submission.',
        category: 'yarn-lifecycle',
      },
      {
        id: 'l1',
        title: 'Execute Shuffle Stage',
        explanation: 'Level-1 macro step summarizing stage planning and shuffle transfer.',
        category: 'sql-planning-aqe',
        isMacro: true,
      },
      {
        id: 'l2-macro',
        parentId: 'l1',
        title: 'Shuffle Block Transfer',
        explanation: 'Level-2 macro step summarizing map write and reduce fetch.',
        category: 'shuffle-internals',
        isMacro: true,
      },
      {
        id: 'l3a',
        parentId: 'l2-macro',
        title: 'Write Shuffle Data & Index',
        explanation: 'Level-3 leaf writing .data and .index files.',
        category: 'shuffle-internals',
      },
      {
        id: 'l3b',
        parentId: 'l2-macro',
        title: 'Fetch Shuffle Blocks',
        explanation: 'Level-3 leaf fetching blocks over Netty.',
        category: 'shuffle-internals',
      },
      {
        id: 'l2-leaf',
        parentId: 'l1',
        title: 'AQE Coalesce Partitions',
        explanation: 'Level-2 leaf re-optimizing post-shuffle partitions.',
        category: 'sql-planning-aqe',
      },
      {
        id: 's-end',
        title: 'Commit Output',
        explanation: 'Final job commit.',
        category: 'output-commit',
      },
    ],
    events: [
      {
        id: 'e0',
        stepId: 's0',
        kind: 'local',
        fromInstance: 'dag-scheduler',
        toInstance: 'dag-scheduler',
        fromComponent: 'dag-scheduler',
        toComponent: 'dag-scheduler',
        channel: 'internal',
        protocol: 'In-JVM',
        port: 'in-JVM',
        label: 'Submit Stage',
        info: 'Submit stage to scheduler.',
        size: 'Local',
        fields: [{ name: 'stageId', value: '0' }],
      },
      {
        id: 'e-l1-summary',
        stepId: 'l1',
        kind: 'wire',
        fromInstance: 'dag-scheduler',
        toInstance: 'shuffle-reader',
        fromComponent: 'dag-scheduler',
        toComponent: 'shuffle-reader',
        channel: 'rpc',
        protocol: 'Spark RPC',
        port: 'driver-1:41000',
        label: 'Stage Shuffle Summary',
        info: 'Collapsed Level-1 macro summary.',
        size: '64 MiB',
        fields: [{ name: 'shuffleId', value: '0' }],
      },
      {
        id: 'e-l2-summary',
        stepId: 'l2-macro',
        kind: 'wire',
        fromInstance: 'shuffle-writer',
        toInstance: 'shuffle-reader',
        fromComponent: 'shuffle-writer',
        toComponent: 'shuffle-reader',
        channel: 'data',
        protocol: 'BlockTransfer',
        port: 'worker-a:7337',
        label: 'Shuffle Exchange Summary',
        info: 'Collapsed Level-2 macro summary.',
        size: '64 MiB',
        fields: [{ name: 'blocks', value: '8' }],
      },
      {
        id: 'e-l3a',
        stepId: 'l3a',
        kind: 'local',
        fromInstance: 'shuffle-writer',
        toInstance: 'shuffle-writer',
        fromComponent: 'shuffle-writer',
        toComponent: 'shuffle-writer',
        channel: 'internal',
        protocol: 'Local Disk',
        port: 'local',
        label: 'Commit .data/.index',
        info: 'Level-3 map-side shuffle commit.',
        size: '64 MiB',
        fields: [{ name: 'file', value: 'shuffle_0_0_0.data' }],
      },
      {
        id: 'e-l3b',
        stepId: 'l3b',
        kind: 'wire',
        fromInstance: 'shuffle-writer',
        toInstance: 'shuffle-reader',
        fromComponent: 'shuffle-writer',
        toComponent: 'shuffle-reader',
        channel: 'data',
        protocol: 'ChunkFetchRequest',
        port: 'worker-a:7337',
        label: 'Stream Shuffle Chunks',
        info: 'Level-3 reduce-side Netty fetch.',
        size: '64 MiB',
        fields: [{ name: 'chunks', value: '8' }],
      },
      {
        id: 'e-l2-leaf',
        stepId: 'l2-leaf',
        kind: 'local',
        fromInstance: 'dag-scheduler',
        toInstance: 'dag-scheduler',
        fromComponent: 'dag-scheduler',
        toComponent: 'dag-scheduler',
        channel: 'internal',
        protocol: 'In-JVM',
        port: 'in-JVM',
        label: 'Coalesce Partitions',
        info: 'AQE coalesces 200 -> 4 partitions.',
        size: 'Local',
        fields: [{ name: 'partitions', value: '4' }],
      },
      {
        id: 'e-end',
        stepId: 's-end',
        kind: 'wire',
        fromInstance: 'shuffle-reader',
        toInstance: 'dag-scheduler',
        fromComponent: 'shuffle-reader',
        toComponent: 'dag-scheduler',
        channel: 'rpc',
        protocol: 'Spark RPC',
        port: 'driver-1:41000',
        label: 'Commit Job',
        info: 'Final commit finished.',
        size: '256 B',
        fields: [{ name: 'status', value: 'COMMITTED' }],
      },
    ],
  };
}

test('3-level nested step tree projects cleanly across expand/collapse transitions and breadcrumbs', () => {
  const run = buildThreeLevelFixture();

  // Hierarchy helpers
  assert.deepEqual(getExpandableStepIds(run), ['l1', 'l2-macro']);
  assert.deepEqual(
    getDirectChildren(run, 'l1').map(s => s.id),
    ['l2-macro', 'l2-leaf'],
  );
  assert.deepEqual(
    getDirectChildren(run, 'l2-macro').map(s => s.id),
    ['l3a', 'l3b'],
  );
  assert.deepEqual(getDirectChildren(run, 'l3a'), []);
  assert.deepEqual(getStepAncestors(run, 'l1'), []);
  assert.deepEqual(
    getStepAncestors(run, 'l2-macro').map(s => s.id),
    ['l1'],
  );
  assert.deepEqual(
    getStepAncestors(run, 'l3b').map(s => s.id),
    ['l1', 'l2-macro'],
  );
  assert.deepEqual(collectStepIdsToExpandForStep(run, 'l1'), []);
  assert.deepEqual(collectStepIdsToExpandForStep(run, 'l2-macro'), ['l1']);
  assert.deepEqual(collectStepIdsToExpandForStep(run, 'l3b'), ['l1', 'l2-macro']);

  // 1. Collapsed: only top-level frames visible, L1 shows summary
  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  assert.deepEqual(
    collapsed.visibleFrames.map(f => f.event.id),
    ['e0', 'e-l1-summary', 'e-end'],
  );
  assert.equal(collapsed.visibleFrames[1].isSummaryFrame, true);
  assert.deepEqual(collapsed.visibleFrames[1].ancestors, []);
  assert.equal(collapsed.visibleSteps[1].depth, 0);
  assert.equal(collapsed.visibleSteps[1].hasChildren, true);
  assert.equal(collapsed.visibleSteps[1].childCount, 2);
  assert.deepEqual(collapsed.emptyExpandedStepIds, []);

  // 2. Expand L1 (L2 stays collapsed): L1 summary skipped, L2 summary shown, L3 hidden
  const l1Expanded = projectRun(run, new Set(['l1']), ALL_STEP_CATEGORIES);
  assert.deepEqual(
    l1Expanded.visibleFrames.map(f => f.event.id),
    ['e0', 'e-l2-summary', 'e-l2-leaf', 'e-end'],
  );
  assert.equal(l1Expanded.visibleFrames[1].isSummaryFrame, true);
  assert.deepEqual(
    l1Expanded.visibleFrames[1].ancestors.map(a => a.id),
    ['l1'],
  );
  assert.equal(l1Expanded.visibleSteps[1].depth, 1);
  assert.equal(l1Expanded.visibleSteps[1].hasChildren, true);
  assert.equal(l1Expanded.visibleSteps[1].isExpanded, false);
  assert.equal(l1Expanded.visibleSteps[1].childCount, 2);
  assert.deepEqual(
    l1Expanded.visibleFrames[2].ancestors.map(a => a.id),
    ['l1'],
  );
  assert.equal(
    resolveEventAfterToggleExpand('l1', false, run, l1Expanded, 'e-l1-summary'),
    'e-l2-summary',
  );

  // 3. Expand L2 as well: both L1 and L2 summaries skipped, L3 frames show [l1, l2-macro] breadcrumbs
  const l1AndL2Expanded = projectRun(run, new Set(['l1', 'l2-macro']), ALL_STEP_CATEGORIES);
  assert.deepEqual(
    l1AndL2Expanded.visibleFrames.map(f => f.event.id),
    ['e0', 'e-l3a', 'e-l3b', 'e-l2-leaf', 'e-end'],
  );
  assert.equal(l1AndL2Expanded.visibleFrames[1].isSummaryFrame, false);
  assert.deepEqual(
    l1AndL2Expanded.visibleFrames[1].ancestors.map(a => a.id),
    ['l1', 'l2-macro'],
  );
  assert.deepEqual(
    l1AndL2Expanded.visibleFrames[2].ancestors.map(a => a.id),
    ['l1', 'l2-macro'],
  );
  assert.equal(l1AndL2Expanded.visibleSteps[1].depth, 2);
  assert.equal(l1AndL2Expanded.visibleSteps[2].depth, 2);
  assert.equal(l1AndL2Expanded.visibleSteps[3].depth, 1);
  assert.equal(
    resolveEventAfterToggleExpand('l2-macro', false, run, l1AndL2Expanded, 'e-l2-summary'),
    'e-l3a',
  );

  // 4. Collapse L2 while standing on L3 grandchild -> lands on L2 summary frame
  assert.equal(
    resolveEventAfterToggleExpand('l2-macro', true, run, l1Expanded, 'e-l3b'),
    'e-l2-summary',
  );

  // 5. Collapse L1 directly while standing on L3 grandchild (even if L2 remains in expandedSet)
  const l1CollapsedWithStaleL2 = projectRun(run, new Set(['l2-macro']), ALL_STEP_CATEGORIES);
  assert.deepEqual(
    l1CollapsedWithStaleL2.visibleFrames.map(f => f.event.id),
    ['e0', 'e-l1-summary', 'e-end'],
  );
  assert.deepEqual(l1CollapsedWithStaleL2.emptyExpandedStepIds, []);
  assert.equal(
    resolveEventAfterToggleExpand('l1', true, run, l1CollapsedWithStaleL2, 'e-l3b'),
    'e-l1-summary',
  );
  assert.equal(
    resolveEventAfterToggleExpand('l1', true, run, collapsed, 'e-l3b'),
    'e-l1-summary',
  );
});

test('expanded parent with all children filtered out populates emptyExpandedStepIds and skippedSpans', () => {
  const run = buildContractFixture();
  const expandedSet = new Set(['s2-macro']);
  const withoutBroadcast = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(c => c !== 'broadcast-torrent'),
  );

  const filtered = projectRun(run, expandedSet, withoutBroadcast);
  assert.deepEqual(filtered.emptyExpandedStepIds, ['s2-macro']);
  assert.equal(filtered.skippedSpans.length, 1);
  assert.deepEqual(filtered.skippedSpans[0].categories, ['broadcast-torrent']);
  assert.deepEqual(filtered.skippedSpans[0].stepIds, ['s2a', 's2b']);
  assert.equal(filtered.skippedSpans[0].eventCount, 2);
  assert.equal(resolveEventAfterProjectionChange('e2a', run, filtered), 'e3');
  assert.equal(
    resolveEventAfterToggleExpand('s2-macro', false, run, filtered, 'e2-summary'),
    'e3',
  );

  // All categories disabled -> emptyExpandedStepIds still flags expanded parent and resolves to null
  const empty = projectRun(run, expandedSet, new Set<StepCategory>());
  assert.deepEqual(empty.emptyExpandedStepIds, ['s2-macro']);
  assert.equal(resolveEventAfterProjectionChange('e2a', run, empty), null);

  // 3-level partial filter: disabling shuffle-internals empties L2 but leaves L1 populated via l2-leaf
  const nested = buildThreeLevelFixture();
  const withoutShuffle = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(c => c !== 'shuffle-internals'),
  );
  const partialNested = projectRun(nested, new Set(['l1', 'l2-macro']), withoutShuffle);
  assert.deepEqual(partialNested.emptyExpandedStepIds, ['l2-macro']);
  assert.equal(partialNested.skippedSpans.length, 1);
  assert.deepEqual(partialNested.skippedSpans[0].categories, ['shuffle-internals']);
  assert.equal(resolveEventAfterProjectionChange('e-l3a', nested, partialNested), 'e-l2-leaf');

  // Disabling both shuffle-internals and sql-planning-aqe empties both L1 and L2
  const withoutStageCats = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(
      c => c !== 'shuffle-internals' && c !== 'sql-planning-aqe',
    ),
  );
  const emptyNested = projectRun(nested, new Set(['l1', 'l2-macro']), withoutStageCats);
  assert.deepEqual(emptyNested.emptyExpandedStepIds, ['l1', 'l2-macro']);
  assert.equal(resolveEventAfterProjectionChange('e-l3a', nested, emptyNested), 'e-end');
});

test('all enriched catalog traces adapt cleanly with valid trees, instances, and zero duplicate summaries', () => {
  const enrichedIds = Object.keys(DEEP_STEP_ENRICHMENTS);
  assert.ok(enrichedIds.length > 0, 'Expected at least one enriched trace in DEEP_STEP_ENRICHMENTS');

  const traceById = new Map(ALL_TRACES.map(t => [t.id, t]));

  for (const traceId of enrichedIds) {
    const enrichment = DEEP_STEP_ENRICHMENTS[traceId];
    assert.equal(enrichment.traceId, traceId, `${traceId}: mismatched enrichment.traceId`);

    const trace = traceById.get(traceId);
    assert.ok(trace, `Enrichment references unknown traceId ${traceId}`);
    if (!trace) continue;

    for (const rawKey of Object.keys(enrichment.expansions)) {
      const idx = Number(rawKey);
      assert.ok(
        Number.isInteger(idx) && idx >= 0 && idx < trace.steps.length,
        `${traceId}: invalid expansion step index ${rawKey} (0..${trace.steps.length - 1})`,
      );
    }

    const run = adaptTraceToRun(trace);
    const stepById = new Map(run.steps.map(s => [s.id, s]));
    assert.equal(stepById.size, run.steps.length, `${traceId}: duplicate step IDs`);

    const eventIds = new Set(run.events.map(e => e.id));
    assert.equal(eventIds.size, run.events.length, `${traceId}: duplicate event IDs`);

    // Validate no orphan parentIds and no cycles
    for (const step of run.steps) {
      if (step.parentId !== undefined) {
        assert.ok(
          stepById.has(step.parentId),
          `${traceId}/${step.id}: orphan parentId ${step.parentId}`,
        );
      }
      const seen = new Set<string>([step.id]);
      let cur = step.parentId ? stepById.get(step.parentId) : undefined;
      while (cur) {
        assert.ok(!seen.has(cur.id), `${traceId}/${step.id}: cycle detected at ${cur.id}`);
        seen.add(cur.id);
        cur = cur.parentId ? stepById.get(cur.parentId) : undefined;
      }
    }

    // Validate every event references existing step and runtime instances
    for (const ev of run.events) {
      assert.ok(stepById.has(ev.stepId), `${traceId}/${ev.id}: missing stepId ${ev.stepId}`);
      assert.ok(
        run.instances[ev.fromInstance],
        `${traceId}/${ev.id}: missing fromInstance ${ev.fromInstance}`,
      );
      assert.ok(
        run.instances[ev.toInstance],
        `${traceId}/${ev.id}: missing toInstance ${ev.toInstance}`,
      );
    }

    // Collapsed projection preserves base events cleanly
    const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
    assert.equal(
      collapsed.visibleFrames.length,
      trace.messages.length,
      `${traceId}: collapsed view should match base trace message count`,
    );
    assert.deepEqual(collapsed.emptyExpandedStepIds, []);

    // Fully expanding all expandable steps omits all macro summaries and yields >= base events
    const expandableIds = getExpandableStepIds(run);
    assert.ok(expandableIds.length > 0, `${traceId}: expected at least one expandable step`);
    const expandableSet = new Set(expandableIds);

    const expanded = projectRun(run, expandableSet, ALL_STEP_CATEGORIES);
    assert.deepEqual(expanded.emptyExpandedStepIds, []);
    assert.ok(
      expanded.visibleFrames.length >= trace.messages.length,
      `${traceId}: expanded frame count (${expanded.visibleFrames.length}) < base (${trace.messages.length})`,
    );

    const seenFrameEventIds = new Set<string>();
    for (const frame of expanded.visibleFrames) {
      assert.ok(
        !expandableSet.has(frame.step.id),
        `${traceId}: expanded view should not replay macro summary step ${frame.step.id}`,
      );
      assert.ok(
        !seenFrameEventIds.has(frame.event.id),
        `${traceId}: duplicate frame event ${frame.event.id}`,
      );
      seenFrameEventIds.add(frame.event.id);
      assert.ok(run.instances[frame.event.fromInstance]);
      assert.ok(run.instances[frame.event.toInstance]);
    }
  }
});

test('local port normalization classifies all qualified in-JVM/OS/local-dir events as local across all 83 traces and 7 enriched runs', () => {
  // Direct unit checks for qualified local ports vs real network/socket endpoints
  for (const localPort of [
    'in-JVM',
    'driver-1 in-JVM',
    'worker-a in-JVM',
    'driver-1 OS',
    'worker-a local-dir',
    'worker-a local disk',
    'local-disk',
    'in-process',
    'memory',
    'local',
  ]) {
    assert.equal(isLocalPort(localPort), true, `Expected ${JSON.stringify(localPort)} to be local`);
  }
  for (const wirePort of [
    '127.0.0.1:43122',
    'rm01:8030',
    'driver-1:41000',
    'worker-a:42001',
    'nn01:8020',
    'dn01:9866',
    'worker-a:dynamic',
    'localhost:41000',
  ]) {
    assert.equal(isLocalPort(wirePort), false, `Expected ${JSON.stringify(wirePort)} to stay wire`);
  }

  assert.equal(ALL_TRACES.length, 83);
  assert.equal(Object.keys(DEEP_STEP_ENRICHMENTS).length, 7);

  const qualifiedLocalRe =
    /(?:^|\s)(?:in-jvm|in-process|memory|local|local-dir|local-disk|os)(?:\s|$)/i;

  for (const trace of ALL_TRACES) {
    const run = adaptTraceToRun(trace);
    const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
    for (const frame of collapsed.visibleFrames) {
      if (qualifiedLocalRe.test(frame.event.port.trim())) {
        assert.equal(
          frame.event.kind,
          'local',
          `${trace.id} (collapsed) ${frame.event.id}: port ${JSON.stringify(frame.event.port)} misclassified as ${frame.event.kind}`,
        );
      }
    }

    if (DEEP_STEP_ENRICHMENTS[trace.id]) {
      const expanded = projectRun(run, new Set(getExpandableStepIds(run)), ALL_STEP_CATEGORIES);
      for (const frame of expanded.visibleFrames) {
        if (qualifiedLocalRe.test(frame.event.port.trim())) {
          assert.equal(
            frame.event.kind,
            'local',
            `${trace.id} (expanded) ${frame.event.id}: port ${JSON.stringify(frame.event.port)} misclassified as ${frame.event.kind}`,
          );
        }
      }
    }
  }

  // Real socket endpoints remain wire
  const fsaRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-shuffle-aggregation')!);
  assert.equal(fsaRun.events.find(e => e.id === 'flow-shuffle-aggregation:sa-1')?.kind, 'wire');
  assert.equal(fsaRun.events.find(e => e.id === 'flow-shuffle-aggregation:sa-6')?.kind, 'wire');
  assert.equal(fsaRun.events.find(e => e.id === 'flow-shuffle-aggregation:sa-9')?.kind, 'wire');
  const fclRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-cluster-lifecycle')!);
  assert.equal(fclRun.events.find(e => e.id === 'flow-cluster-lifecycle:lc-12')?.kind, 'wire');
});

test('flow-shuffle-aggregation Step 0 preserves driver-python-bridge frames both collapsed and expanded', () => {
  const trace = ALL_TRACES.find(t => t.id === 'flow-shuffle-aggregation');
  assert.ok(trace, 'Expected flow-shuffle-aggregation trace');
  const run = adaptTraceToRun(trace);

  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  const collapsedStep0PyBridge = collapsed.visibleFrames.filter(
    f =>
      f.step.id === 'flow-shuffle-aggregation:step-0' &&
      f.effectiveCategory === 'driver-python-bridge',
  );
  assert.ok(
    collapsedStep0PyBridge.length >= 2,
    `Expected >= 2 collapsed driver-python-bridge frames in Step 0, got ${collapsedStep0PyBridge.length}`,
  );
  assert.deepEqual(
    collapsedStep0PyBridge.map(f => f.event.id),
    ['flow-shuffle-aggregation:sa-1', 'flow-shuffle-aggregation:sa-1b'],
  );

  const expandedStep0 = projectRun(
    run,
    new Set(['flow-shuffle-aggregation:step-0']),
    ALL_STEP_CATEGORIES,
  );
  const expandedStep0PyBridge = expandedStep0.visibleFrames.filter(
    f =>
      f.ancestors.some(a => a.id === 'flow-shuffle-aggregation:step-0') &&
      f.effectiveCategory === 'driver-python-bridge',
  );
  assert.ok(
    expandedStep0PyBridge.length >= 2,
    `Expected >= 2 expanded driver-python-bridge frames in Step 0, got ${expandedStep0PyBridge.length}`,
  );
  assert.deepEqual(
    expandedStep0PyBridge.map(f => f.event.id),
    ['fsa-d-0a-1', 'fsa-d-0a-2'],
  );

  const allExpandedPyBridge = projectRun(
    run,
    new Set(getExpandableStepIds(run)),
    new Set<StepCategory>(['driver-python-bridge']),
  );
  assert.ok(
    allExpandedPyBridge.visibleFrames.length >= 2,
    `Expected >= 2 all-expanded driver-python-bridge frames, got ${allExpandedPyBridge.visibleFrames.length}`,
  );
});

test('flow-fetch-failed-recovery has no duplicate mapId=89 commit/resubmit when expanded and recomputes on worker-node-b', () => {
  const trace = ALL_TRACES.find(t => t.id === 'flow-fetch-failed-recovery');
  assert.ok(trace, 'Expected flow-fetch-failed-recovery trace');
  const run = adaptTraceToRun(trace);

  // customInstances preserved without clobbering or duplication when extraInstances share IDs
  assert.equal(run.instances['exec-2/shuffle-writer']?.hostZone, 'worker-node-b');
  assert.equal(run.instances['exec-2/shuffle-block-resolver']?.hostZone, 'worker-node-b');
  assert.equal(run.instances['exec-2/block-transfer-service']?.hostZone, 'worker-node-b');
  assert.equal(run.focusInstances.length, new Set(run.focusInstances).size);

  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  const expanded = projectRun(run, new Set(getExpandableStepIds(run)), ALL_STEP_CATEGORIES);

  // Exactly one DAG -> TaskScheduler resubmission and one shuffle-writer -> resolver mapId=89 commit
  const expandedResubmits = expanded.visibleFrames.filter(
    f => f.event.fromComponent === 'dag-scheduler' && f.event.toComponent === 'task-scheduler',
  );
  assert.equal(
    expandedResubmits.length,
    1,
    `Expected exactly 1 mapId=89 stage resubmit when expanded, got ${expandedResubmits.map(f => f.event.id).join(', ')}`,
  );

  const expandedMap89Commits = expanded.visibleFrames.filter(
    f =>
      f.event.fromComponent === 'shuffle-writer' &&
      f.event.toComponent === 'shuffle-block-resolver',
  );
  assert.equal(
    expandedMap89Commits.length,
    1,
    `Expected exactly 1 mapId=89 shuffle commit when expanded, got ${expandedMap89Commits.map(f => f.event.id).join(', ')}`,
  );

  // Every recomputed map event runs on worker-node-b (never dead worker-node-a), both collapsed and expanded
  for (const [label, proj] of [
    ['collapsed', collapsed],
    ['expanded', expanded],
  ] as const) {
    const writerFrames = proj.visibleFrames.filter(
      f => f.event.fromComponent === 'shuffle-writer' || f.event.toComponent === 'shuffle-writer',
    );
    assert.ok(writerFrames.length > 0, `${label}: expected recompute shuffle-writer frames`);
    for (const frame of writerFrames) {
      const writerInstId =
        frame.event.fromComponent === 'shuffle-writer'
          ? frame.event.fromInstance
          : frame.event.toInstance;
      assert.equal(
        run.instances[writerInstId]?.hostZone,
        'worker-node-b',
        `${label} ${frame.event.id}: recomputed map instance ${writerInstId} should be on worker-node-b`,
      );
    }
  }
});

test('semantic category filtering preserves Catalyst ShuffleExchangeExec planning, isolates TorrentBroadcast, and maps dynamic allocation to yarn-lifecycle', () => {
  // 1. flow-shuffle-aggregation: sql-planning-aqe retains Catalyst planning mentioning ShuffleExchangeExec
  const fsaRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-shuffle-aggregation')!);
  const fsaSqlCollapsed = projectRun(
    fsaRun,
    new Set(),
    new Set<StepCategory>(['sql-planning-aqe']),
  );
  assert.deepEqual(
    fsaSqlCollapsed.visibleFrames.map(f => f.event.id),
    [
      'flow-shuffle-aggregation:sa-2',
      'flow-shuffle-aggregation:sa-2b',
      'flow-shuffle-aggregation:sa-3',
      'flow-shuffle-aggregation:sa-3b',
      'flow-shuffle-aggregation:sa-6c',
    ],
  );

  const fsaShuffleCollapsed = projectRun(
    fsaRun,
    new Set(),
    new Set<StepCategory>(['shuffle-internals']),
  );
  assert.ok(
    !fsaShuffleCollapsed.visibleFrames.some(f => f.event.id === 'flow-shuffle-aggregation:sa-2'),
    'Catalyst ShuffleExchangeExec planning must not be lost to shuffle-internals',
  );
  assert.ok(
    fsaShuffleCollapsed.visibleFrames.some(f => f.event.id === 'flow-shuffle-aggregation:sa-6a'),
  );
  assert.ok(
    fsaShuffleCollapsed.visibleFrames.some(f => f.event.id === 'flow-shuffle-aggregation:sa-9'),
  );

  const fsaSqlExpanded = projectRun(
    fsaRun,
    new Set(getExpandableStepIds(fsaRun)),
    new Set<StepCategory>(['sql-planning-aqe']),
  );
  assert.deepEqual(
    fsaSqlExpanded.visibleFrames.map(f => f.event.id),
    [
      'fsa-d-0b-1',
      'fsa-d-0c-1',
      'fsa-d-0d-1',
      'fsa-d-0e-1',
      'fsa-d-0e-2',
      'flow-shuffle-aggregation:sa-3',
      'flow-shuffle-aggregation:sa-3b',
      'flow-shuffle-aggregation:sa-6c',
    ],
  );

  // 2. scenario-09: filtering to broadcast-torrent isolates all TorrentBroadcast pieces cleanly
  const s09Run = adaptTraceToRun(SCENARIO_TRACES.find(t => t.id === 'scenario-09')!);
  const s09BroadcastExpanded = projectRun(
    s09Run,
    new Set(getExpandableStepIds(s09Run)),
    new Set<StepCategory>(['broadcast-torrent']),
  );
  assert.deepEqual(
    s09BroadcastExpanded.visibleFrames.map(f => f.event.id),
    [
      'scenario-09:s09-1',
      'scenario-09:s09-2',
      's09-d-2a-1',
      's09-d-2a-2',
      's09-d-2b-1',
      's09-d-3a-1',
      's09-d-3a-2',
      's09-d-3b-1',
      's09-d-3b-2',
      's09-d-3b-3',
      'scenario-09:s09-7',
    ],
  );
  const s09ShuffleOnly = projectRun(
    s09Run,
    new Set(getExpandableStepIds(s09Run)),
    new Set<StepCategory>(['shuffle-internals']),
  );
  assert.equal(s09ShuffleOnly.visibleFrames.length, 0);

  // 3. Genuine FetchFailed stays shuffle-internals while stage resubmit is sql-planning-aqe
  const fffRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-fetch-failed-recovery')!);
  const fffShuffleCollapsed = projectRun(
    fffRun,
    new Set(),
    new Set<StepCategory>(['shuffle-internals']),
  );
  assert.deepEqual(
    fffShuffleCollapsed.visibleFrames.map(f => f.event.id),
    [
      'flow-fetch-failed-recovery:ff-1',
      'flow-fetch-failed-recovery:ff-2',
      'flow-fetch-failed-recovery:ff-3',
      'flow-fetch-failed-recovery:ff-4',
      'flow-fetch-failed-recovery:ff-5',
      'flow-fetch-failed-recovery:ff-6',
      'flow-fetch-failed-recovery:ff-8',
      'flow-fetch-failed-recovery:ff-8b',
      'flow-fetch-failed-recovery:ff-9',
      'flow-fetch-failed-recovery:ff-10',
      'flow-fetch-failed-recovery:ff-11',
      'flow-fetch-failed-recovery:ff-11b',
      'flow-fetch-failed-recovery:ff-11c',
    ],
  );

  // 4. executor-allocation-manager / executor-monitor default to yarn-lifecycle
  const fdaRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-dynamic-allocation')!);
  const fdaYarn = projectRun(fdaRun, new Set(), new Set<StepCategory>(['yarn-lifecycle']));
  const fdaYarnIds = fdaYarn.visibleFrames.map(f => f.event.id);
  assert.ok(fdaYarnIds.includes('flow-dynamic-allocation:da-2'));
  assert.ok(fdaYarnIds.includes('flow-dynamic-allocation:da-8'));
  assert.ok(fdaYarnIds.includes('flow-dynamic-allocation:da-9'));

  const s06Run = adaptTraceToRun(SCENARIO_TRACES.find(t => t.id === 'scenario-06')!);
  const s06Yarn = projectRun(s06Run, new Set(), new Set<StepCategory>(['yarn-lifecycle']));
  assert.deepEqual(
    s06Yarn.visibleFrames.map(f => f.event.id),
    [
      'scenario-06:s06-1',
      'scenario-06:s06-2',
      'scenario-06:s06-3',
      'scenario-06:s06-4',
      'scenario-06:s06-5',
      'scenario-06:s06-6',
    ],
  );
});

test('detectCategoryPreset matches all built-in presets and custom sets accurately', () => {
  assert.equal(detectCategoryPreset(ALL_STEP_CATEGORIES), 'all');
  assert.equal(detectCategoryPreset(new Set(ALL_STEP_CATEGORIES)), 'all');

  assert.equal(detectCategoryPreset(STANDARD_CATEGORIES), 'standard');
  assert.equal(detectCategoryPreset(new Set(STANDARD_CATEGORIES)), 'standard');

  assert.equal(detectCategoryPreset(HIGH_LEVEL_CATEGORIES), 'high-level');
  assert.equal(detectCategoryPreset(new Set(HIGH_LEVEL_CATEGORIES)), 'high-level');

  assert.equal(detectCategoryPreset(CONTROL_ONLY_CATEGORIES), 'control-only');
  assert.equal(detectCategoryPreset(new Set(CONTROL_ONLY_CATEGORIES)), 'control-only');

  for (const preset of CATEGORY_PRESETS) {
    assert.equal(detectCategoryPreset(new Set(preset.categories)), preset.id);
  }

  // Custom combinations (empty, single, superset, subset, and same-size mismatch)
  assert.equal(detectCategoryPreset(new Set<StepCategory>()), 'custom');
  assert.equal(detectCategoryPreset(new Set<StepCategory>(['broadcast-torrent'])), 'custom');
  assert.equal(
    detectCategoryPreset(new Set<StepCategory>(['yarn-lifecycle', 'output-commit'])),
    'custom',
  );
  assert.equal(
    detectCategoryPreset(
      new Set<StepCategory>([...CONTROL_ONLY_CATEGORIES, 'output-commit']),
    ),
    'custom',
  );
  assert.equal(
    detectCategoryPreset(
      new Set<StepCategory>([...ALL_STEP_CATEGORIES].filter(c => c !== 'broadcast-torrent')),
    ),
    'custom',
  );
});

test('scenario-09 TorrentBroadcast skip & re-include manages emptyExpandedStepIds, hidden categories, skippedSpans, and restores piece steps cleanly', () => {
  const s09Trace = SCENARIO_TRACES.find(t => t.id === 'scenario-09');
  assert.ok(s09Trace, 'Expected scenario-09 trace');
  const s09Run = adaptTraceToRun(s09Trace);

  const expandedSet = new Set(['scenario-09:step-2', 'scenario-09:step-3']);
  assert.deepEqual(getExpandableStepIds(s09Run), ['scenario-09:step-2', 'scenario-09:step-3']);

  const initial = projectRun(s09Run, expandedSet, ALL_STEP_CATEGORIES);
  assert.equal(initial.visibleFrames.length, 11);
  assert.deepEqual(initial.emptyExpandedStepIds, []);
  assert.deepEqual(initial.skippedSpans, []);

  // Disable broadcast-torrent: all TorrentBroadcast piece events are skipped cleanly
  const withoutBroadcast = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(c => c !== 'broadcast-torrent'),
  );
  const skipped = projectRun(s09Run, expandedSet, withoutBroadcast);

  assert.equal(skipped.visibleFrames.length, 0);
  assert.equal(skipped.visibleSteps.length, 0);
  assert.deepEqual(skipped.emptyExpandedStepIds, ['scenario-09:step-2', 'scenario-09:step-3']);
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:step-2', withoutBroadcast),
    ['broadcast-torrent'],
  );
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:step-3', withoutBroadcast),
    ['broadcast-torrent'],
  );
  // Leaf and unknown step IDs return empty hidden categories
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:step-2a', withoutBroadcast),
    [],
  );
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:missing', withoutBroadcast),
    [],
  );

  assert.equal(skipped.skippedSpans.length, 1);
  assert.deepEqual(skipped.skippedSpans[0].categories, ['broadcast-torrent']);
  assert.equal(skipped.skippedSpans[0].eventCount, 11);
  assert.equal(skipped.skippedSpans[0].beforeFrameIndex, -1);
  assert.ok(skipped.skippedSpans[0].stepIds.includes('scenario-09:step-2a'));
  assert.ok(skipped.skippedSpans[0].stepIds.includes('scenario-09:step-2b'));
  assert.ok(skipped.skippedSpans[0].stepIds.includes('scenario-09:step-3a'));
  assert.ok(skipped.skippedSpans[0].stepIds.includes('scenario-09:step-3b'));
  assert.equal(resolveEventAfterProjectionChange('s09-d-3b-2', s09Run, skipped), null);

  // Re-enable broadcast-torrent: restores all 4 piece-distribution steps without duplicate summaries
  const reEnabled = new Set<StepCategory>([...withoutBroadcast, 'broadcast-torrent']);
  const restored = projectRun(s09Run, expandedSet, reEnabled);

  assert.deepEqual(restored.emptyExpandedStepIds, []);
  assert.deepEqual(restored.skippedSpans, []);
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:step-2', reEnabled),
    [],
  );
  assert.deepEqual(
    computeHiddenCategoriesForStep(s09Run, 'scenario-09:step-3', reEnabled),
    [],
  );
  assert.deepEqual(
    restored.visibleFrames.map(f => f.event.id),
    [
      'scenario-09:s09-1',
      'scenario-09:s09-2',
      's09-d-2a-1',
      's09-d-2a-2',
      's09-d-2b-1',
      's09-d-3a-1',
      's09-d-3a-2',
      's09-d-3b-1',
      's09-d-3b-2',
      's09-d-3b-3',
      'scenario-09:s09-7',
    ],
  );

  const pieceSteps = restored.visibleSteps.filter(ps => ps.depth === 1);
  assert.deepEqual(
    pieceSteps.map(ps => ps.step.id),
    [
      'scenario-09:step-2a',
      'scenario-09:step-2b',
      'scenario-09:step-3a',
      'scenario-09:step-3b',
    ],
  );

  // Zero duplicate macro summary events replayed
  const restoredEventIds = new Set(restored.visibleFrames.map(f => f.event.id));
  for (const summaryId of [
    'scenario-09:s09-3',
    'scenario-09:s09-4',
    'scenario-09:s09-5',
    'scenario-09:s09-6',
  ]) {
    assert.ok(!restoredEventIds.has(summaryId), `Summary event ${summaryId} must not replay`);
  }
  assert.equal(restoredEventIds.size, restored.visibleFrames.length);
  assert.equal(
    resolveEventAfterProjectionChange(null, s09Run, restored),
    'scenario-09:s09-1',
  );
});

test('partial-child filter on expanded flow-cluster-lifecycle skips staging/heartbeats, keeps surviving ancestors, and advances cleanly', () => {
  const fclTrace = ALL_TRACES.find(t => t.id === 'flow-cluster-lifecycle');
  assert.ok(fclTrace, 'Expected flow-cluster-lifecycle trace');
  const fclRun = adaptTraceToRun(fclTrace);

  // Expand all 7 top-level macro steps
  const top7StepIds = fclRun.steps.filter(s => !s.parentId).map(s => s.id);
  assert.equal(top7StepIds.length, 7);
  const expandedTop7 = new Set(top7StepIds);

  const allInternals = projectRun(fclRun, expandedTop7, ALL_STEP_CATEGORIES);
  assert.equal(allInternals.visibleFrames.length, 46);
  assert.equal(allInternals.skippedSpans.length, 0);
  assert.deepEqual(allInternals.emptyExpandedStepIds, []);

  // Switch preset from All Internals -> High-Level
  const highLevel = projectRun(fclRun, expandedTop7, HIGH_LEVEL_CATEGORIES);

  // 3 staging-localization + 3 heartbeats-metrics events skipped; 40 frames survive
  assert.equal(highLevel.visibleFrames.length, 40);
  assert.deepEqual(highLevel.emptyExpandedStepIds, []);

  // Only yarn-lifecycle and driver-python-bridge frames survive
  const survivingCategories = new Set(highLevel.visibleFrames.map(f => f.effectiveCategory));
  assert.deepEqual([...survivingCategories].sort(), [
    'driver-python-bridge',
    'yarn-lifecycle',
  ]);
  const yarnFrames = highLevel.visibleFrames.filter(
    f => f.effectiveCategory === 'yarn-lifecycle',
  );
  const pyBridgeFrames = highLevel.visibleFrames.filter(
    f => f.effectiveCategory === 'driver-python-bridge',
  );
  assert.equal(yarnFrames.length, 27);
  assert.equal(pyBridgeFrames.length, 13);

  // Every surviving child sub-step still renders with its parent in ancestors
  for (const frame of highLevel.visibleFrames) {
    assert.equal(frame.ancestors.length, 1);
    assert.ok(expandedTop7.has(frame.ancestors[0].id));
  }
  const fcl04bFrame = highLevel.visibleFrames.find(f => f.event.id === 'fcl-0-4b');
  assert.ok(fcl04bFrame, 'Expected surviving fcl-0-4b frame');
  assert.deepEqual(
    fcl04bFrame.ancestors.map(a => a.id),
    ['flow-cluster-lifecycle:step-0'],
  );
  const fcl14aFrame = highLevel.visibleFrames.find(f => f.event.id === 'fcl-1-4a');
  assert.ok(fcl14aFrame, 'Expected surviving fcl-1-4a frame');
  assert.deepEqual(
    fcl14aFrame.ancestors.map(a => a.id),
    ['flow-cluster-lifecycle:step-1'],
  );

  // Exact SkippedSpans emitted for staging-localization and heartbeats-metrics
  assert.deepEqual(
    highLevel.skippedSpans.map(s => ({
      beforeFrameIndex: s.beforeFrameIndex,
      categories: s.categories,
      eventCount: s.eventCount,
      stepIds: s.stepIds,
    })),
    [
      {
        beforeFrameIndex: 4,
        categories: ['staging-localization'],
        eventCount: 1,
        stepIds: ['flow-cluster-lifecycle:step-0.4'],
      },
      {
        beforeFrameIndex: 7,
        categories: ['staging-localization'],
        eventCount: 1,
        stepIds: ['flow-cluster-lifecycle:step-1.3'],
      },
      {
        beforeFrameIndex: 20,
        categories: ['heartbeats-metrics'],
        eventCount: 2,
        stepIds: ['flow-cluster-lifecycle:step-3.3'],
      },
      {
        beforeFrameIndex: 35,
        categories: ['heartbeats-metrics'],
        eventCount: 1,
        stepIds: ['flow-cluster-lifecycle:step-5.4'],
      },
      {
        beforeFrameIndex: -1,
        categories: ['staging-localization'],
        eventCount: 1,
        stepIds: ['flow-cluster-lifecycle:step-6.4'],
      },
    ],
  );
  assert.equal(
    highLevel.skippedSpans.reduce((sum, s) => sum + s.eventCount, 0),
    6,
  );

  // Standing on skipped staging-localization events resolves deterministically
  assert.equal(
    resolveEventAfterProjectionChange('fcl-0-4a', fclRun, highLevel),
    'fcl-0-4b',
  );
  assert.equal(
    resolveEventAfterProjectionChange('fcl-1-3a', fclRun, highLevel),
    'fcl-1-4a',
  );
  assert.equal(
    resolveEventAfterProjectionChange('fcl-6-4b', fclRun, highLevel),
    'fcl-6-4a',
  );
  // Standing on skipped heartbeats-metrics event advances to next surviving frame
  assert.equal(
    resolveEventAfterProjectionChange('fcl-3-3a', fclRun, highLevel),
    'fcl-3-4a',
  );
  // Standing on surviving frame stays anchored
  assert.equal(
    resolveEventAfterProjectionChange('fcl-2-2a', fclRun, highLevel),
    'fcl-2-2a',
  );

  // Partial vs empty parents: enabling only driver-python-bridge leaves steps 2, 3, 6 partial
  // and flags only steps 0, 1, 4, 5 (which have 0 surviving descendants) in emptyExpandedStepIds
  const onlyPyBridge = new Set<StepCategory>(['driver-python-bridge']);
  const pyOnlyProj = projectRun(fclRun, expandedTop7, onlyPyBridge);
  assert.deepEqual(pyOnlyProj.emptyExpandedStepIds, [
    'flow-cluster-lifecycle:step-0',
    'flow-cluster-lifecycle:step-1',
    'flow-cluster-lifecycle:step-4',
    'flow-cluster-lifecycle:step-5',
  ]);
  assert.deepEqual(
    computeHiddenCategoriesForStep(fclRun, 'flow-cluster-lifecycle:step-0', onlyPyBridge),
    ['staging-localization', 'yarn-lifecycle'],
  );
  assert.deepEqual(
    computeHiddenCategoriesForStep(fclRun, 'flow-cluster-lifecycle:step-5', onlyPyBridge),
    ['heartbeats-metrics', 'yarn-lifecycle'],
  );
  // Step 6 has categoryOverride staging-localization on fcl-6-4b plus yarn-lifecycle
  assert.deepEqual(
    computeHiddenCategoriesForStep(fclRun, 'flow-cluster-lifecycle:step-6', onlyPyBridge),
    ['staging-localization', 'yarn-lifecycle'],
  );
});

test('computeRunWideCategoryCounts detects staging-localization and heartbeats-metrics inside collapsed flow-cluster-lifecycle', () => {
  const fclTrace = ALL_TRACES.find(t => t.id === 'flow-cluster-lifecycle');
  assert.ok(fclTrace, 'Expected flow-cluster-lifecycle trace');
  const fclRun = adaptTraceToRun(fclTrace);

  // While all macro steps are collapsed, heartbeats-metrics has 0 in-scope events
  const collapsed = projectRun(fclRun, new Set(), ALL_STEP_CATEGORIES);
  assert.equal(collapsed.categoryCounts['heartbeats-metrics'], 0);
  assert.equal(collapsed.categoryCounts['staging-localization'], 2);

  const runWide = computeRunWideCategoryCounts(fclRun);
  assert.equal(runWide['heartbeats-metrics'], 3);
  assert.equal(runWide['staging-localization'], 5);
  assert.equal(runWide['yarn-lifecycle'], 46);
  assert.equal(runWide['driver-python-bridge'], 16);
  assert.equal(runWide['sql-planning-aqe'], 0);
  assert.equal(runWide['broadcast-torrent'], 0);
  assert.equal(runWide['shuffle-internals'], 0);
  assert.equal(runWide['python-worker-ipc'], 0);
  assert.equal(runWide['output-commit'], 0);

  // Total run-wide count matches canonical run events and dominates every expansion scope
  const totalRunWide = Object.values(runWide).reduce((sum, n) => sum + n, 0);
  assert.equal(totalRunWide, fclRun.events.length);
  assert.equal(totalRunWide, collapsed.totalCanonicalEvents);

  const fullyExpanded = projectRun(
    fclRun,
    new Set(getExpandableStepIds(fclRun)),
    ALL_STEP_CATEGORIES,
  );
  for (const cat of STEP_CATEGORIES) {
    assert.ok(
      runWide[cat.id] >= collapsed.categoryCounts[cat.id],
      `runWide[${cat.id}] should be >= collapsed count`,
    );
    assert.ok(
      runWide[cat.id] >= fullyExpanded.categoryCounts[cat.id],
      `runWide[${cat.id}] should be >= expanded count`,
    );
  }
});

test('zero-match empty state yields 0 frames, null event resolution, and skippedSpans covering all active-expansion events', () => {
  const fclRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-cluster-lifecycle')!);
  const none = new Set<StepCategory>();

  // 1. Collapsed flow-cluster-lifecycle with all categories disabled
  const allCollapsed = projectRun(fclRun, new Set(), ALL_STEP_CATEGORIES);
  const emptyCollapsed = projectRun(fclRun, new Set(), none);

  assert.equal(emptyCollapsed.visibleFrames.length, 0);
  assert.equal(emptyCollapsed.visibleSteps.length, 0);
  assert.equal(emptyCollapsed.frameIndexByEventId.size, 0);
  assert.deepEqual(emptyCollapsed.emptyExpandedStepIds, []);
  assert.equal(
    resolveEventAfterProjectionChange(
      allCollapsed.visibleFrames[0].event.id,
      fclRun,
      emptyCollapsed,
    ),
    null,
  );
  assert.equal(resolveEventAfterProjectionChange(null, fclRun, emptyCollapsed), null);
  assert.equal(stepJumpEventId(null, emptyCollapsed, 1), null);
  assert.equal(emptyCollapsed.skippedSpans.length, 1);
  assert.equal(emptyCollapsed.skippedSpans[0].beforeFrameIndex, -1);
  assert.equal(
    emptyCollapsed.skippedSpans[0].eventCount,
    allCollapsed.visibleFrames.length,
  );

  // 2. Fully expanded flow-cluster-lifecycle with all categories disabled
  const allExpandableIds = getExpandableStepIds(fclRun);
  const expandedSet = new Set(allExpandableIds);
  const allExpanded = projectRun(fclRun, expandedSet, ALL_STEP_CATEGORIES);
  const emptyExpanded = projectRun(fclRun, expandedSet, none);

  assert.equal(emptyExpanded.visibleFrames.length, 0);
  assert.equal(emptyExpanded.visibleSteps.length, 0);
  assert.equal(
    resolveEventAfterProjectionChange(
      allExpanded.visibleFrames[0].event.id,
      fclRun,
      emptyExpanded,
    ),
    null,
  );
  assert.equal(emptyExpanded.skippedSpans.length, 1);
  assert.equal(emptyExpanded.skippedSpans[0].beforeFrameIndex, -1);
  assert.equal(
    emptyExpanded.skippedSpans[0].eventCount,
    allExpanded.visibleFrames.length,
  );
  assert.equal(
    emptyExpanded.skippedSpans[0].eventCount,
    Object.values(emptyExpanded.categoryCounts).reduce((sum, n) => sum + n, 0),
  );
  assert.deepEqual(emptyExpanded.emptyExpandedStepIds, allExpandableIds);
  for (const stepId of allExpandableIds) {
    const hiddenCats = computeHiddenCategoriesForStep(fclRun, stepId, none);
    assert.ok(hiddenCats.length > 0, `Expected disabled categories for ${stepId}`);
    assert.deepEqual(hiddenCats, [...hiddenCats].sort());
  }
});

test('computeDeepOnlyCategoryCounts counts only leaf events under collapsed branches without double-counting summaries', () => {
  const run = buildThreeLevelFixture();

  // 1. All collapsed: Shuffle current count is 0; deep leaf count is 2 (l3a, l3b), NEVER 3
  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  const deepCollapsed = computeDeepOnlyCategoryCounts(run, new Set());
  assert.equal(collapsed.categoryCounts['shuffle-internals'], 0);
  assert.equal(deepCollapsed['shuffle-internals'], 2);
  assert.equal(deepCollapsed['sql-planning-aqe'], 1);
  assert.equal(deepCollapsed['yarn-lifecycle'], 0);
  assert.equal(deepCollapsed['output-commit'], 0);
  // Sanity contrast: run-wide includes l2-macro summary + l3a + l3b (= 3)
  assert.equal(computeRunWideCategoryCounts(run)['shuffle-internals'], 3);

  // 2. L1 expanded, L2 collapsed: current Shuffle count is 1 (L2 summary), deep leaf count is 2 (l3a, l3b)
  const l1Expanded = projectRun(run, new Set(['l1']), ALL_STEP_CATEGORIES);
  const deepL1Expanded = computeDeepOnlyCategoryCounts(run, new Set(['l1']));
  assert.equal(l1Expanded.categoryCounts['shuffle-internals'], 1);
  assert.equal(deepL1Expanded['shuffle-internals'], 2);
  assert.equal(deepL1Expanded['sql-planning-aqe'], 0);

  // 3. All expanded: current Shuffle count is 2 (l3a, l3b), deep leaf count is 0 across every category
  const allExpanded = projectRun(run, new Set(['l1', 'l2-macro']), ALL_STEP_CATEGORIES);
  const deepAllExpanded = computeDeepOnlyCategoryCounts(run, new Set(['l1', 'l2-macro']));
  assert.equal(allExpanded.categoryCounts['shuffle-internals'], 2);
  assert.equal(deepAllExpanded['shuffle-internals'], 0);
  for (const cat of STEP_CATEGORIES) {
    assert.equal(deepAllExpanded[cat.id], 0, `Expected 0 deep count for ${cat.id} when fully expanded`);
  }

  // 4. Stale nested expansion ID (l2-macro expanded while l1 is collapsed) still reports 2 deep shuffle leaves
  const deepStale = computeDeepOnlyCategoryCounts(run, new Set(['l2-macro']));
  assert.equal(deepStale['shuffle-internals'], 2);
  assert.equal(deepStale['sql-planning-aqe'], 1);
});

test('computeHiddenCategoriesForStep is depth-aware and excludes collapsed grandchild categories', () => {
  const base = buildThreeLevelFixture();
  const run: ExecutionRun = {
    ...base,
    id: 'fixture-3-level-hidden-depth',
    steps: [
      {
        id: 'L1',
        title: 'Level-1 Macro',
        explanation: 'Top-level macro.',
        category: 'sql-planning-aqe',
        isMacro: true,
      },
      {
        id: 'L2',
        parentId: 'L1',
        title: 'Level-2 Sub-Macro',
        explanation: 'Intermediate sub-macro with yarn-lifecycle summary.',
        category: 'yarn-lifecycle',
        isMacro: true,
      },
      {
        id: 'L3',
        parentId: 'L2',
        title: 'Level-3 Grandchild',
        explanation: 'Leaf grandchild with heartbeats-metrics event.',
        category: 'heartbeats-metrics',
      },
    ],
    events: [
      {
        ...base.events[1],
        id: 'ev-l1',
        stepId: 'L1',
      },
      {
        ...base.events[2],
        id: 'ev-l2',
        stepId: 'L2',
      },
      {
        ...base.events[3],
        id: 'ev-l3',
        stepId: 'L3',
      },
    ],
  };

  const enabled = new Set<StepCategory>(
    [...ALL_STEP_CATEGORIES].filter(
      c => c !== 'yarn-lifecycle' && c !== 'heartbeats-metrics',
    ),
  );

  // L1 expanded, L2 collapsed: only L2's yarn-lifecycle is active at this depth, NOT L3's heartbeats-metrics
  const l1OnlyExpanded = new Set(['L1']);
  const projL1 = projectRun(run, l1OnlyExpanded, enabled);
  assert.deepEqual(projL1.emptyExpandedStepIds, ['L1']);
  assert.deepEqual(
    computeHiddenCategoriesForStep(run, 'L1', enabled, l1OnlyExpanded),
    ['yarn-lifecycle'],
  );

  // L1 and L2 both expanded: L2 summary is skipped, so only L3's heartbeats-metrics is surfaced
  const l1AndL2Expanded = new Set(['L1', 'L2']);
  const projL1L2 = projectRun(run, l1AndL2Expanded, enabled);
  assert.deepEqual(projL1L2.emptyExpandedStepIds, ['L1', 'L2']);
  assert.deepEqual(
    computeHiddenCategoriesForStep(run, 'L1', enabled, l1AndL2Expanded),
    ['heartbeats-metrics'],
  );
  assert.deepEqual(
    computeHiddenCategoriesForStep(run, 'L2', enabled, l1AndL2Expanded),
    ['heartbeats-metrics'],
  );

  // Backwards compatibility: omitting expandedStepIds inspects all descendants
  assert.deepEqual(
    computeHiddenCategoriesForStep(run, 'L1', enabled),
    ['heartbeats-metrics', 'yarn-lifecycle'],
  );
});

test('all 83 curated traces enforce same-hostZone invariant for local/in-JVM/loopback hops and validate observability + scenario-03 + locality fixes', () => {
  assert.equal(ALL_TRACES.length, 83);

  const sameHostPortRe =
    /127\.0\.0\.1|\blocalhost\b|loopback|in-?jvm|\bjvm\b|os proc|unix socket|\blocal\b/i;
  const sameHostProtocolRe =
    /127\.0\.0\.1|\blocalhost\b|loopback|in-?jvm|\bjvm\b|os proc|unix socket|\blocal\b|in-?memory|local[- ]disk|\bpipe\b/i;

  const violations: string[] = [];
  const checkEvent = (traceId: string, mode: string, run: ExecutionRun, ev: RunEvent) => {
    const fromInst = run.instances[ev.fromInstance];
    const toInst = run.instances[ev.toInstance];
    assert.ok(fromInst, `${traceId} (${mode})/${ev.id}: missing fromInstance ${ev.fromInstance}`);
    assert.ok(toInst, `${traceId} (${mode})/${ev.id}: missing toInstance ${ev.toInstance}`);
    const sameHostShortCircuitRe = /short-circuit|BlockReaderLocal/i;
    const requiresSameHost =
      ev.kind === 'local' ||
      isLocalPort(ev.port) ||
      sameHostPortRe.test(ev.port) ||
      sameHostProtocolRe.test(ev.protocol) ||
      sameHostShortCircuitRe.test(`${ev.protocol} ${ev.command ?? ''}`) ||
      ev.channel === 'internal';
    if (requiresSameHost && fromInst.hostZone !== toInst.hostZone) {
      violations.push(
        `${mode}:${ev.id} (${ev.fromInstance}@${fromInst.hostZone} -> ${ev.toInstance}@${toInst.hostZone}, port=${ev.port}, protocol=${ev.protocol})`,
      );
    }
  };

  for (const trace of ALL_TRACES) {
    const run = adaptTraceToRun(trace);
    for (const ev of run.events) {
      checkEvent(trace.id, 'raw', run, ev);
    }
    const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
    for (const frame of collapsed.visibleFrames) {
      checkEvent(trace.id, 'collapsed', run, frame.event);
    }
    const expanded = projectRun(run, new Set(getExpandableStepIds(run)), ALL_STEP_CATEGORIES);
    for (const frame of expanded.visibleFrames) {
      checkEvent(trace.id, 'expanded', run, frame.event);
    }
  }

  assert.deepEqual(
    violations,
    [],
    `Unexpected cross-host local/in-JVM/loopback events: ${violations.join(', ')}`,
  );

  // Worker-side in-JVM hops keep driver-side singletons in driver-jvm and worker instances in worker-node-a
  const envRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-spark-env')!);
  assert.equal(envRun.instances['spark-env']?.hostZone, 'driver-jvm');
  assert.equal(envRun.instances['exec-spark-env']?.hostZone, 'worker-node-a');
  const envCreateExec = envRun.events.find(e => e.id === 'comp-spark-env:c-env-4')!;
  assert.equal(envCreateExec.fromInstance, 'executor-backend');
  assert.equal(envCreateExec.toInstance, 'exec-spark-env');
  assert.equal(envRun.instances[envCreateExec.fromInstance]?.hostZone, 'worker-node-a');
  assert.equal(envRun.instances[envCreateExec.toInstance]?.hostZone, 'worker-node-a');

  const ebRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-executor-backend')!);
  assert.equal(ebRun.instances['exec-spark-env']?.hostZone, 'worker-node-a');
  const ebSetup = ebRun.events.find(e => e.id === 'comp-executor-backend:c-eb-3')!;
  assert.equal(ebSetup.fromInstance, 'executor-backend');
  assert.equal(ebSetup.toInstance, 'exec-spark-env');
  assert.equal(ebRun.instances[ebSetup.fromInstance]?.hostZone, 'worker-node-a');
  assert.equal(ebRun.instances[ebSetup.toInstance]?.hostZone, 'worker-node-a');

  const rpcRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-rpc-env')!);
  assert.equal(rpcRun.instances['rpc-env']?.hostZone, 'driver-jvm');
  assert.equal(rpcRun.instances['exec-rpc-env']?.hostZone, 'worker-node-a');
  const rpcReverse = rpcRun.events.find(e => e.id === 'comp-rpc-env:c-rpc-3')!;
  assert.equal(rpcReverse.fromInstance, 'driver-scheduler-backend');
  assert.equal(rpcReverse.toInstance, 'exec-rpc-env');
  const rpcInbox = rpcRun.events.find(e => e.id === 'comp-rpc-env:c-rpc-4')!;
  assert.equal(rpcInbox.fromInstance, 'exec-rpc-env');
  assert.equal(rpcInbox.toInstance, 'executor-backend');
  assert.equal(rpcRun.instances[rpcInbox.fromInstance]?.hostZone, 'worker-node-a');
  assert.equal(rpcRun.instances[rpcInbox.toInstance]?.hostZone, 'worker-node-a');

  // Scenario 12: Attempt 0 Python loopback + TaskRunner failure share worker-node-b; retries run on worker-node-a
  const s12Run = adaptTraceToRun(SCENARIO_TRACES.find(t => t.id === 'scenario-12')!);
  const s12PyErr = s12Run.events.find(e => e.id === 'scenario-12:s12-1')!;
  assert.equal(s12PyErr.fromInstance, 'python-worker');
  assert.equal(s12PyErr.toInstance, 'exec-2/task-attempt');
  assert.equal(s12Run.instances[s12PyErr.fromInstance]?.hostZone, 'worker-node-b');
  assert.equal(s12Run.instances[s12PyErr.toInstance]?.hostZone, 'worker-node-b');
  const s12Fail = s12Run.events.find(e => e.id === 'scenario-12:s12-2')!;
  assert.equal(s12Fail.fromInstance, 'exec-2/task-attempt');
  assert.equal(s12Fail.toInstance, 'exec-2/executor-backend');
  const s12Status = s12Run.events.find(e => e.id === 'scenario-12:s12-3')!;
  assert.equal(s12Status.fromInstance, 'exec-2/executor-backend');
  assert.equal(s12Status.toInstance, 'task-set-manager');
  const s12Retry = s12Run.events.find(e => e.id === 'scenario-12:s12-4')!;
  assert.equal(s12Retry.toInstance, 'exec-1/task-attempt');
  assert.equal(s12Run.instances['exec-1/task-attempt']?.hostZone, 'worker-node-a');

  // comp-rdd-cache: driver Py4J persist stays in driver-jvm; executor getOrCompute stays in worker-node-a
  const rcRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-rdd-cache')!);
  const rcPersist = rcRun.events.find(e => e.id === 'comp-rdd-cache:rc-1')!;
  assert.equal(rcPersist.fromInstance, 'python-driver');
  assert.equal(rcPersist.toInstance, 'driver-rdd-cache');
  assert.equal(rcRun.instances['driver-rdd-cache']?.hostZone, 'driver-jvm');
  assert.equal(rcRun.instances['rdd-cache']?.hostZone, 'worker-node-a');
  const rcCompute = rcRun.events.find(e => e.id === 'comp-rdd-cache:rc-3')!;
  assert.equal(rcCompute.fromInstance, 'task-attempt');
  assert.equal(rcCompute.toInstance, 'rdd-cache');

  // comp-history-server: read-only SparkUI(sc=None) mounts in-JVM inside HistoryServer (storage-history)
  const hsRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-history-server')!);
  const hsMount = hsRun.events.find(e => e.id === 'comp-history-server:hs-6')!;
  assert.equal(hsMount.fromInstance, 'history-server');
  assert.equal(hsMount.toInstance, 'history-spark-ui');
  assert.equal(hsRun.instances['history-spark-ui']?.hostZone, 'storage-history');

  // Observability traces: driver MetricsSystem stays in driver-jvm; executor-side components share one worker hostZone
  const obsRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'flow-observability-lifecycles')!);
  assert.equal(obsRun.instances['metrics-system']?.hostZone, 'driver-jvm');
  assert.equal(obsRun.instances['exec-2']?.hostZone, 'worker-node-b');
  assert.equal(obsRun.instances['executor-metrics-poller']?.hostZone, 'worker-node-b');
  assert.equal(obsRun.instances['exec-metrics-system']?.hostZone, 'worker-node-b');
  const obsHb = obsRun.events.find(e => e.id === 'flow-observability-lifecycles:obs-5')!;
  assert.equal(obsHb.kind, 'wire');
  assert.equal(obsHb.port, 'driver-1:41000');

  const msRun = adaptTraceToRun(ALL_TRACES.find(t => t.id === 'comp-metrics-system')!);
  assert.equal(msRun.instances['metrics-system']?.hostZone, 'driver-jvm');
  assert.equal(msRun.instances['executor']?.hostZone, 'worker-node-a');
  assert.equal(msRun.instances['exec-metrics-system']?.hostZone, 'worker-node-a');

  const empTrace = ALL_TRACES.find(t => t.id === 'comp-executor-metrics-poller')!;
  const empRun = adaptTraceToRun(empTrace);
  assert.equal(empRun.instances['executor']?.hostZone, 'worker-node-a');
  assert.equal(empRun.instances['task-attempt']?.hostZone, 'worker-node-a');
  assert.equal(empRun.instances['exec-poller']?.hostZone, 'worker-node-a');
  assert.equal(empRun.instances['exec-metrics-system']?.hostZone, 'worker-node-a');
  const empHb = empRun.events.find(e => e.id === 'comp-executor-metrics-poller:emp-5')!;
  assert.equal(empHb.kind, 'wire');
  assert.equal(empHb.port, 'driver-1:41000');
  const empEventLogMsg = empTrace.messages.find(m => m.id === 'emp-6')!;
  assert.match(empEventLogMsg.info, /spark\.eventLog\.logStageExecutorMetrics=true/);

  // Scenario 03: client-mode split-network firewall failure faithful to ApplicationMaster.scala:564-581
  const s03Trace = SCENARIO_TRACES.find(t => t.id === 'scenario-03')!;
  const s03Run = adaptTraceToRun(s03Trace);
  assert.equal(s03Run.instances['yarn-scheduler-backend']?.hostZone, 'submit-host');
  assert.equal(s03Run.instances['driver-scheduler-backend']?.hostZone, 'submit-host');
  assert.equal(s03Run.instances['application-master']?.hostZone, 'driver-jvm');
  const s03Req = s03Run.events.find(e => e.id === 'scenario-03:s03-1')!;
  assert.equal(s03Req.kind, 'wire');
  assert.equal(isLocalPort(s03Req.port), false);
  assert.equal(s03Req.port, 'edge-01.dmz:41000');
  const s03Alloc = s03Run.events.find(e => e.id === 'scenario-03:s03-1b')!;
  assert.equal(s03Alloc.kind, 'local');
  const s03Fail = s03Run.events.find(e => e.id === 'scenario-03:s03-5')!;
  assert.equal(s03Fail.kind, 'wire');
  assert.equal(s03Fail.port, 'edge-01.dmz:41000');
});

