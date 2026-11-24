import assert from 'node:assert/strict';
import test from 'node:test';
import { COMPONENTS } from '../src/data/components.ts';
import {
  ALL_STEP_CATEGORIES,
  CATEGORY_PRESETS,
  type ExecutionRun,
} from '../src/data/runModel.ts';
import { isLocalPort } from '../src/engine/adaptTrace.ts';
import { getExpandableStepIds, projectRun } from '../src/engine/projection.ts';
import { buildSimulatedRun, resolveValidSimulationConfig } from '../src/engine/simulator.ts';
import {
  DEFAULT_SIMULATION_CONFIG,
  SIMULATED_FAULTS,
  SIMULATED_JOBS,
  SIMULATION_PRESETS,
  validateSimulationConfig,
  type SimulationConfig,
} from '../src/engine/simulatorTypes.ts';

function assertValidSimulatedRun(run: ExecutionRun, label: string) {
  assert.equal(run.sourceKind, 'simulation', label);
  assert.ok(run.steps.length >= 2, `${label}: expected >=2 steps`);
  assert.ok(run.events.length >= 2, `${label}: expected >=2 events`);
  assert.ok(run.telemetry, `${label}: expected telemetry`);

  const stepIds = new Set<string>();
  for (const step of run.steps) {
    assert.ok(!stepIds.has(step.id), `${label}: duplicate step id ${step.id}`);
    stepIds.add(step.id);
  }
  for (const step of run.steps) {
    if (step.parentId) {
      assert.ok(stepIds.has(step.parentId), `${label}: orphan parentId ${step.parentId}`);
    }
  }

  const eventIds = new Set<string>();
  for (const ev of run.events) {
    assert.ok(!eventIds.has(ev.id), `${label}: duplicate event id ${ev.id}`);
    eventIds.add(ev.id);
    assert.ok(stepIds.has(ev.stepId), `${label}: unknown stepId ${ev.stepId}`);
    assert.ok(run.instances[ev.fromInstance], `${label}: missing fromInstance ${ev.fromInstance}`);
    assert.ok(run.instances[ev.toInstance], `${label}: missing toInstance ${ev.toInstance}`);
    assert.ok(COMPONENTS[ev.fromComponent], `${label}: invalid fromComponent ${ev.fromComponent}`);
    assert.ok(COMPONENTS[ev.toComponent], `${label}: invalid toComponent ${ev.toComponent}`);
    assert.ok(ev.label.length <= 28, `${label}: label too long "${ev.label}"`);
    assert.ok(!/[^\x00-\x7f]/.test(`${ev.label} ${ev.info} ${ev.command ?? ''}`), `${label}: non-ASCII`);
    if (isLocalPort(ev.port)) {
      assert.equal(ev.kind, 'local', `${label}/${ev.id}: local port "${ev.port}" marked wire`);
    }
  }

  // Projection works collapsed, fully expanded, and under every preset
  const collapsed = projectRun(run, new Set(), ALL_STEP_CATEGORIES);
  assert.ok(collapsed.visibleFrames.length > 0, `${label}: empty collapsed projection`);

  const allExpandedIds = new Set(getExpandableStepIds(run));
  const expanded = projectRun(run, allExpandedIds, ALL_STEP_CATEGORIES);
  assert.ok(expanded.visibleFrames.length >= collapsed.visibleFrames.length, `${label}: expanded shrank`);

  for (const preset of CATEGORY_PRESETS) {
    const projected = projectRun(run, allExpandedIds, preset.categories);
    assert.equal(projected.runId, run.id);
  }
}

test('all 8 simulation presets build valid hierarchical runs', () => {
  assert.equal(SIMULATION_PRESETS.length, 8);
  for (const preset of SIMULATION_PRESETS) {
    const check = validateSimulationConfig(preset.config);
    assert.equal(check.valid, true, `Preset ${preset.id} should be valid: ${check.blockers.map(b => b.reason)}`);
    const run = buildSimulatedRun(preset.config);
    assertValidSimulatedRun(run, preset.id);
  }
});

test('compatibility validation blocks invalid job/fault/mode pairs and resolveValidSimulationConfig repairs them', () => {
  const invalidPairs: SimulationConfig[] = [
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'narrow-filter', fault: 'shuffle-fetch-failed' },
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', numExecutors: 1, fault: 'shuffle-fetch-failed' },
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'collect-action', fault: 'commit-job-failure' },
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'wide-shuffle', fault: 'max-result-size' },
    { ...DEFAULT_SIMULATION_CONFIG, deployMode: 'client', fault: 'am-sc-timeout' },
    { ...DEFAULT_SIMULATION_CONFIG, deployMode: 'client', fault: 'am-crash-retry' },
    { ...DEFAULT_SIMULATION_CONFIG, deployMode: 'cluster', fault: 'client-disconnect' },
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'cached-rdd', fault: 'speculation-race' },
    { ...DEFAULT_SIMULATION_CONFIG, jobType: 'narrow-filter', numExecutors: 1, fault: 'speculation-race' },
  ];
  for (const bad of invalidPairs) {
    const v = validateSimulationConfig(bad);
    assert.equal(v.valid, false, `Expected blocker for ${bad.jobType}/${bad.deployMode}/${bad.fault}/e=${bad.numExecutors}`);
    assert.throws(() => buildSimulatedRun(bad));
    const fixed = resolveValidSimulationConfig(bad);
    assert.equal(validateSimulationConfig(fixed).valid, true);
    assertValidSimulatedRun(buildSimulatedRun(fixed), `fixed-${bad.fault}`);
  }
});

test('dynamic executor count (1..6), client placement, TorrentBroadcast P2P, ESS, and AQE behave accurately', () => {
  // 1. Broadcast Join with 1 executor has ZERO E1->E2 P2P hops; 3 executors HAS E1->E2
  const bcast1 = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'broadcast-join',
    dataProfile: 'small-dim',
    numExecutors: 1,
  });
  assertValidSimulatedRun(bcast1, 'bcast-1');
  assert.equal(bcast1.events.some(e => e.peerTag === 'E1 -> E2'), false);
  assert.ok(validateSimulationConfig({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', numExecutors: 1 }).notes.length > 0);

  const bcast3 = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'broadcast-join',
    dataProfile: 'small-dim',
    numExecutors: 3,
  });
  assertValidSimulatedRun(bcast3, 'bcast-3');
  assert.equal(bcast3.events.some(e => e.peerTag === 'E1 -> E2'), true);
  assert.ok(bcast3.instances['exec-1'] && bcast3.instances['exec-2'] && bcast3.instances['exec-3']);

  // 2. 6 executors creates exec-1..exec-6 alternating worker-node-a and worker-node-b
  const six = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'narrow-filter',
    numExecutors: 6,
    executorCores: 2,
    numPartitions: 24,
  });
  assertValidSimulatedRun(six, 'narrow-6');
  assert.equal(six.telemetry?.totalSlots, 12);
  assert.equal(six.telemetry?.idealWavesPerStage, 2);
  assert.equal(six.instances['exec-5']?.hostZone, 'worker-node-a');
  assert.equal(six.instances['exec-6']?.hostZone, 'worker-node-b');

  // 3. Client mode places driver instances in submit-host
  const clientRun = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'client',
    numExecutors: 2,
  });
  assertValidSimulatedRun(clientRun, 'client-mode');
  assert.equal(clientRun.instances['spark-context']?.hostZone, 'submit-host');
  assert.equal(clientRun.instances['application-master']?.hostZone, 'driver-jvm');

  // 4. ESS ON fetches from external-shuffle-service (:7337); ESS OFF fetches from exec-1/bm (:42001)
  const essOn = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    externalShuffleService: true,
  });
  assertValidSimulatedRun(essOn, 'ess-on');
  assert.equal(essOn.events.some(e => e.toInstance === 'ess-a' && e.port.includes('7337')), true);

  const essOff = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    externalShuffleService: false,
  });
  assertValidSimulatedRun(essOff, 'ess-off');
  assert.equal(essOff.events.some(e => e.toInstance === 'exec-1/bm' && e.port.includes('42001')), true);

  // 5. AQE Skew Split emits PartialReducerPartitionSpec ONLY when aqeEnabled && skewed
  const skewSplit = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'sort-merge-skew',
    dataProfile: 'skewed',
    aqeEnabled: true,
  });
  assertValidSimulatedRun(skewSplit, 'skew-aqe-on');
  assert.equal(skewSplit.steps.some(s => s.title.includes('PartialReducerPartitionSpec')), true);

  const skewOff = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'sort-merge-skew',
    dataProfile: 'skewed',
    aqeEnabled: false,
  });
  assertValidSimulatedRun(skewOff, 'skew-aqe-off');
  assert.equal(skewOff.steps.some(s => s.title.includes('PartialReducerPartitionSpec')), false);
});

test('all 7 job types and all 13 fault modes produce valid runs when paired cleanly', () => {
  for (const job of SIMULATED_JOBS) {
    const cfg: SimulationConfig = {
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: job.id,
      dataProfile: job.defaultDataProfile,
      dynamicAllocation: job.id === 'wide-shuffle',
    };
    assertValidSimulatedRun(buildSimulatedRun(cfg), `job-${job.id}`);
  }
  for (const fault of SIMULATED_FAULTS) {
    const raw: SimulationConfig = {
      ...DEFAULT_SIMULATION_CONFIG,
      fault: fault.id,
      deployMode: fault.id === 'client-disconnect' ? 'client' : 'cluster',
    };
    const valid = resolveValidSimulationConfig(raw);
    assertValidSimulatedRun(buildSimulatedRun(valid), `fault-${fault.id}`);
  }
});

test('live AM and live executor tracking prevents dead instances from acting or receiving StopExecutor', () => {
  // 1. am-crash-retry: dead AM ('application-master') NEVER emits events after am-attempt-2 registers
  const amRetry = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    deployMode: 'cluster',
    numExecutors: 2,
    fault: 'am-crash-retry',
    dynamicAllocation: true,
  });
  assertValidSimulatedRun(amRetry, 'am-crash-retry-live-am');
  const am2RegIdx = amRetry.events.findIndex(
    e => e.fromInstance === 'am-attempt-2' && e.toInstance === 'resource-manager',
  );
  assert.ok(am2RegIdx >= 0, 'Expected am-attempt-2 registration event');
  const afterAm2 = amRetry.events.slice(am2RegIdx + 1);
  assert.equal(
    afterAm2.some(e => e.fromInstance === 'application-master' || e.toInstance === 'application-master'),
    false,
    'Dead AM Attempt 1 must never emit or receive events after am-attempt-2 registers',
  );
  const finalUnreg = afterAm2.find(e => e.label.includes('unregisterAM(SUCCEEDED)'));
  assert.ok(finalUnreg, 'Expected final unregisterAM(SUCCEEDED)');
  assert.equal(finalUnreg.fromInstance, 'am-attempt-2');

  // Verify fresh Attempt-2 executors (container_e02_*) replace Attempt-1 containers across all 7 job types
  for (const jobType of [
    'wide-shuffle',
    'sort-merge-skew',
    'broadcast-join',
    'narrow-filter',
    'python-udf',
    'cached-rdd',
    'collect-action',
  ] as const) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType,
      dataProfile:
        jobType === 'broadcast-join'
          ? 'small-dim'
          : jobType === 'sort-merge-skew'
            ? 'skewed'
            : 'uniform',
      deployMode: 'cluster',
      numExecutors: 2,
      fault: 'am-crash-retry',
      dynamicAllocation: jobType === 'wide-shuffle',
    });
    assertValidSimulatedRun(run, `am-crash-retry-${jobType}`);
    assert.equal(run.instances['exec-a2-1']?.containerId, 'container_e02_000002');
    assert.equal(run.instances['exec-a2-2']?.containerId, 'container_e02_000003');
    assert.equal(run.instances['exec-a2-1/bm']?.containerId, 'container_e02_000002');
    assert.equal(run.instances['exec-a2-1']?.attemptNumber, 2);

    const regIdx = run.events.findIndex(
      e => e.fromInstance === 'am-attempt-2' && e.toInstance === 'resource-manager',
    );
    assert.ok(regIdx >= 0, `${jobType}: expected am-attempt-2 registration`);
    const postAm2Events = run.events.slice(regIdx + 1);
    assert.equal(
      postAm2Events.some(e => {
        const fromCid = run.instances[e.fromInstance]?.containerId ?? '';
        const toCid = run.instances[e.toInstance]?.containerId ?? '';
        return (
          fromCid.startsWith('container_e01_') ||
          toCid.startsWith('container_e01_') ||
          `${e.info} ${e.command ?? ''}`.includes('container_e01_')
        );
      }),
      false,
      `${jobType}: no container_e01_* executor/BM may receive workload or StopExecutor after Attempt 2 registers`,
    );
    const stopEvs = postAm2Events.filter(e => e.label.startsWith('StopExecutor'));
    assert.ok(stopEvs.length >= 2, `${jobType}: expected Attempt-2 StopExecutor events`);
    for (const stopEv of stopEvs) {
      const targetInst = run.instances[stopEv.toInstance];
      assert.ok(
        targetInst?.containerId?.startsWith('container_e02_'),
        `${jobType}: StopExecutor target ${stopEv.toInstance} must use container_e02_*, got ${targetInst?.containerId}`,
      );
    }
  }

  // 2. oom-pmem, yarn-preemption, heartbeat-timeout: dead exec-1 NEVER receives StopExecutor
  for (const lossFault of ['oom-pmem', 'yarn-preemption', 'heartbeat-timeout'] as const) {
    for (const numExecutors of [1, 3]) {
      const run = buildSimulatedRun({
        ...DEFAULT_SIMULATION_CONFIG,
        jobType: 'python-udf',
        numExecutors,
        fault: lossFault,
      });
      assertValidSimulatedRun(run, `${lossFault}-e${numExecutors}`);
      const stopEvents = run.events.filter(e => e.label.startsWith('StopExecutor'));
      assert.ok(stopEvents.length >= 1, `${lossFault}/e${numExecutors}: expected StopExecutor for live executors`);
      assert.equal(
        stopEvents.some(e => e.toInstance === 'exec-1'),
        false,
        `${lossFault}/e${numExecutors}: dead exec-1 must NEVER receive StopExecutor`,
      );
      assert.equal(
        run.events.some(e => e.fromInstance === 'exec-repl' && e.label.includes('RegisterExecutor')),
        true,
        `${lossFault}/e${numExecutors}: expected replacement exec-repl registration`,
      );
      assert.equal(
        stopEvents.some(e => e.toInstance === 'exec-repl'),
        true,
        `${lossFault}/e${numExecutors}: expected replacement exec-repl to receive StopExecutor`,
      );
    }
  }

  // 3. shuffle-fetch-failed (ESS OFF): exec-1 is lost, recompute runs on surviving exec-2, shutdown stops only live execs
  const ffOff = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 3,
    externalShuffleService: false,
    fault: 'shuffle-fetch-failed',
  });
  assertValidSimulatedRun(ffOff, 'ff-ess-off-live');
  const recomputeEv = ffOff.events.find(e => e.label.includes('Recompute mapId='));
  assert.ok(recomputeEv, 'Expected recompute event');
  assert.equal(recomputeEv.fromInstance, 'exec-2');
  const ffStops = ffOff.events.filter(e => e.label.startsWith('StopExecutor'));
  assert.equal(ffStops.some(e => e.toInstance === 'exec-1'), false, 'Dead exec-1 must not receive StopExecutor');
  assert.equal(ffStops.some(e => e.toInstance === 'exec-2'), true);

  // 4. shuffle-fetch-failed (ESS ON) with numExecutors=4 retires BOTH exec-1 AND exec-3 on lost worker-a
  for (const [numExecutors, expectedRetired, expectedStopped] of [
    [4, ['exec-1', 'exec-3'], ['exec-2', 'exec-4']],
    [6, ['exec-1', 'exec-3', 'exec-5'], ['exec-2', 'exec-4', 'exec-6']],
  ] as const) {
    const ffEss = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'wide-shuffle',
      numExecutors,
      externalShuffleService: true,
      fault: 'shuffle-fetch-failed',
    });
    assertValidSimulatedRun(ffEss, `ff-ess-on-e${numExecutors}`);
    for (const retiredId of expectedRetired) {
      assert.equal(ffEss.instances[retiredId]?.hostZone, 'worker-node-a');
    }
    const retireEv = ffEss.events.find(e => e.label.startsWith('Retire worker-a'));
    assert.ok(retireEv, `e=${numExecutors}: expected explicit worker-a host retirement event`);
    for (const retiredId of expectedRetired) {
      assert.ok(
        retireEv.info.includes(retiredId),
        `e=${numExecutors}: expected ${retiredId} in worker-a retirement event info`,
      );
    }
    const essStops = ffEss.events.filter(e => e.label.startsWith('StopExecutor'));
    const stoppedIds = essStops.map(e => e.toInstance);
    assert.deepEqual(
      stoppedIds,
      [...expectedStopped],
      `e=${numExecutors}: final StopExecutor must target only surviving worker-b executors`,
    );
    for (const retiredId of expectedRetired) {
      assert.equal(
        stoppedIds.includes(retiredId),
        false,
        `e=${numExecutors}: retired worker-a executor ${retiredId} must NEVER receive StopExecutor`,
      );
    }
    assert.equal(
      essStops.some(e => ffEss.instances[e.toInstance]?.hostZone === 'worker-node-a'),
      false,
      `e=${numExecutors}: no executor on lost worker-a may receive StopExecutor`,
    );
  }
});

test('lazy instance creation, exact YARN protocol codes, and AM unregister guard', () => {
  // 1. oversized-request creates <= 4 instances (no phantom driver/executor instances)
  const oversize = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    fault: 'oversized-request',
  });
  assertValidSimulatedRun(oversize, 'oversized-request-lazy');
  assert.ok(
    Object.keys(oversize.instances).length <= 4,
    `Expected <= 4 instances on oversized-request, got ${Object.keys(oversize.instances).length}`,
  );
  assert.equal(oversize.instances['exec-1'], undefined);

  // 2. am-sc-timeout emits ZERO unregisterAM events and zero am->rm RPCs
  const timeout = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'cluster',
    fault: 'am-sc-timeout',
  });
  assertValidSimulatedRun(timeout, 'am-sc-timeout-no-unregister');
  assert.equal(
    timeout.events.some(
      e =>
        e.label.toLowerCase().includes('unregister') ||
        (e.command ?? '').toLowerCase().includes('unregister') ||
        (e.fromInstance === 'application-master' && e.toInstance === 'resource-manager'),
    ),
    false,
    'am-sc-timeout must NEVER emit unregisterAM or am->rm RPC (registered==false)',
  );
  assert.equal(
    timeout.events.some(
      e => e.fromInstance === 'am-nm' && e.toInstance === 'resource-manager' && (e.command ?? '').includes('exitStatus=13'),
    ),
    true,
    'am-sc-timeout should report ContainerStatus exitStatus=13 from am-nm to RM',
  );

  // 3. oom-pmem uses -104 (KILLED_EXCEEDED_PMEM), NOT 137 as YARN ContainerExitStatus
  const pmem = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'python-udf',
    fault: 'oom-pmem',
  });
  assertValidSimulatedRun(pmem, 'oom-pmem-code');
  const containerStatusEv = pmem.events.find(e => (e.command ?? '').includes('ContainerStatus('));
  assert.ok(containerStatusEv, 'Expected ContainerStatus event for oom-pmem');
  assert.ok(
    (containerStatusEv.command ?? '').includes('exitStatus=-104'),
    `Expected exitStatus=-104 in ContainerStatus, got: ${containerStatusEv.command}`,
  );
  assert.equal(
    (containerStatusEv.command ?? '').includes('exitStatus=137'),
    false,
    'oom-pmem must NOT report 137 as YARN ContainerExitStatus',
  );
  const removeExecEv = pmem.events.find(e => (e.command ?? '').includes('RemoveExecutor('));
  assert.ok(removeExecEv && (removeExecEv.command ?? '').includes('ExecutorExited(-104'));
});

test('one-executor accuracy, valid partition indices, task-exception timing, and FileOutputCommitter algo 1', () => {
  // 1. wide-shuffle with numExecutors === 1 uses local shuffle read on worker-a (never remote worker-b)
  const shuffle1 = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 1,
    dynamicAllocation: false,
    fault: 'none',
  });
  assertValidSimulatedRun(shuffle1, 'wide-shuffle-1exec');
  const localReadEv = shuffle1.events.find(e => e.fromComponent === 'shuffle-reader' && e.toComponent === 'shuffle-block-resolver');
  assert.ok(localReadEv, 'Expected local shuffle read event on worker-a for 1 executor');
  assert.equal(localReadEv.kind, 'local');
  assert.equal(
    shuffle1.events.some(e => e.port.includes('worker-b')),
    false,
    'Single-executor wide-shuffle must not reference non-existent worker-b',
  );
  assert.equal(shuffle1.instances['file-format-writer']?.hostZone, 'worker-node-a');
  assert.equal(shuffle1.instances['file-commit-protocol']?.hostZone, 'worker-node-a');

  // 2. broadcast-join and python-udf with numExecutors === 1 bind probe/UDF/commit to worker-a
  const bcast1 = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'broadcast-join',
    dataProfile: 'small-dim',
    numExecutors: 1,
  });
  assert.equal(bcast1.instances['broadcast-hash-join']?.hostZone, 'worker-node-a');
  assert.equal(
    bcast1.events.some(e => e.toComponent === 'broadcast-hash-join' && e.port === 'worker-a in-JVM'),
    true,
  );
  assert.equal(bcast1.events.some(e => `${e.label} ${e.info}`.includes('3 x 4')), false);

  const udf1 = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'python-udf',
    numExecutors: 1,
  });
  assert.equal(udf1.instances['python-udf-operator']?.hostZone, 'worker-node-a');
  assert.equal(udf1.instances['python-worker']?.hostZone, 'worker-node-a');
  assert.equal(udf1.instances['file-format-writer']?.hostZone, 'worker-node-a');

  // 3. task-exception uses valid partition index (< numPartitions) and injects BEFORE completion
  for (const numPartitions of [4, 8, 12, 24] as const) {
    const taskFail = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'narrow-filter',
      numPartitions,
      fault: 'task-exception',
    });
    assertValidSimulatedRun(taskFail, `task-exception-p${numPartitions}`);
    const expectedPart = Math.min(numPartitions - 1, Math.floor(numPartitions / 2));
    assert.ok(expectedPart >= 0 && expectedPart < numPartitions);
    assert.equal(
      taskFail.events.some(e => e.label === `Task p=${expectedPart} FAIL (4/4)`),
      true,
      `Expected valid partition ${expectedPart} for numPartitions=${numPartitions}`,
    );
    assert.equal(
      taskFail.events.some(e => (e.senderState ?? '').includes('All narrow tasks finished')),
      false,
      'task-exception must abort BEFORE marking stage tasks finished',
    );
  }

  // 4. speculation-race uses valid partition index and distinct hosts (exec-1 on worker-a vs exec-2 on worker-b)
  const spec = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'narrow-filter',
    numExecutors: 2,
    numPartitions: 4,
    fault: 'speculation-race',
  });
  assertValidSimulatedRun(spec, 'speculation-p4');
  const winEv = spec.events.find(e => e.label === 'canCommit(p=2,a=1) OK');
  const loseEv = spec.events.find(e => e.label === 'canCommit(p=2,a=0) NO');
  assert.ok(winEv && loseEv, 'Expected valid targetPart=2 for numPartitions=4');
  assert.equal(winEv.fromInstance, 'exec-2');
  assert.equal(loseEv.fromInstance, 'exec-1');

  // 5. FileOutputCommitter algorithm 1 consistently used in happy path and commit-job-failure
  const commitOk = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'narrow-filter', fault: 'none' });
  const commitStep = commitOk.steps.find(s => s.id === 'sim-step-commit');
  assert.ok(commitStep && commitStep.title.includes('algo 1'));
  assert.equal(commitOk.events.some(e => e.info.includes('algorithm 2')), false);
});

test('collect() maxResultSize math, AQE OFF omission, dynamic allocation ordering, and MEMORY_ONLY cache', () => {
  // 1. collect-action + max-result-size math matches numPartitions * perPartMiB > 512 across [4, 8, 12, 24]
  for (const numPartitions of [4, 8, 12, 24] as const) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'collect-action',
      dataProfile: 'large-result',
      numPartitions,
      fault: 'max-result-size',
    });
    assertValidSimulatedRun(run, `collect-max-result-p${numPartitions}`);
    const perPartMiB = Math.ceil(600 / numPartitions);
    const totalMiB = perPartMiB * numPartitions;
    const crossingTotalMiB = (Math.floor(512 / perPartMiB) + 1) * perPartMiB;
    assert.ok(totalMiB >= 600 && totalMiB > 512);
    assert.ok(crossingTotalMiB > 512);
    assert.equal(
      run.events.some(e => e.label === `IndirectTaskResult(${perPartMiB}M)`),
      true,
      `p=${numPartitions}: expected IndirectTaskResult(${perPartMiB}M)`,
    );
    assert.equal(
      run.events.some(e => e.label === `Abort: ${crossingTotalMiB}MiB > 512MiB`),
      true,
      `p=${numPartitions}: expected crossing abort ${crossingTotalMiB}MiB > 512MiB`,
    );
    assert.equal(
      run.events.some(e => e.label.includes('getRemoteBytes')),
      false,
      `p=${numPartitions}: must abort BEFORE fetching crossing TaskResultBlock`,
    );
  }

  // Healthy large-result collect() uses 1 GiB limit and succeeds honestly
  const collectOk = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'collect-action',
    dataProfile: 'large-result',
    numPartitions: 12,
    fault: 'none',
  });
  assertValidSimulatedRun(collectOk, 'collect-large-ok');
  assert.equal(collectOk.events.some(e => e.label === 'getRemoteBytes(taskres)'), true);
  assert.equal(collectOk.steps.some(s => s.title.includes('1 GiB')), true);

  // 2. sort-merge-skew + aqeEnabled: false emits ZERO events touching adaptive-query-execution
  for (const dataProfile of ['skewed', 'uniform'] as const) {
    const noAqe = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'sort-merge-skew',
      dataProfile,
      aqeEnabled: false,
    });
    assertValidSimulatedRun(noAqe, `smj-no-aqe-${dataProfile}`);
    assert.equal(
      noAqe.events.some(
        e => e.fromComponent === 'adaptive-query-execution' || e.toComponent === 'adaptive-query-execution',
      ),
      false,
      `sort-merge-skew aqeEnabled=false (${dataProfile}) must emit ZERO adaptive-query-execution events`,
    );
    if (dataProfile === 'uniform') {
      assert.equal(
        noAqe.steps.some(s => s.title.includes('640 MiB') || s.explanation.includes('640 MiB')),
        false,
        'uniform profile must not claim 640 MiB skew',
      );
    }
  }

  // 3. dynamicAllocation: true emits backlog scale-up AFTER workload submission, NEVER before
  const dynRun = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 2,
    executorCores: 2,
    numPartitions: 12,
    dynamicAllocation: true,
  });
  assertValidSimulatedRun(dynRun, 'dynalloc-order');
  const submitStepIdx = dynRun.steps.findIndex(s => s.id === 'sim-step-map-submit');
  const dynStepIdx = dynRun.steps.findIndex(s => s.id === 'sim-step-dynalloc');
  assert.ok(submitStepIdx >= 0 && dynStepIdx >= 0, 'Expected workload submit and dynamic allocation steps');
  assert.ok(dynStepIdx > submitStepIdx, 'Dynamic allocation backlog scale-up MUST happen AFTER workload submission');
  const dynStops = dynRun.events.filter(e => e.label.startsWith('StopExecutor'));
  assert.equal(dynStops.length, 6, 'Shutdown should stop all 6 live executors after dynamic scale-up');

  // 3b. Dynamic allocation exponential increment ramp: 1 -> 2 (+1) -> 4 (+2) -> 6 (+2, capped at max 6)
  const expRampRun = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 1,
    executorCores: 1,
    numPartitions: 24,
    dynamicAllocation: true,
  });
  assertValidSimulatedRun(expRampRun, 'dynalloc-exp-ramp-1-2-4-6');
  const rampEvents = expRampRun.events.filter(e => e.stepId === 'sim-step-dynalloc');
  const eamRequests = rampEvents.filter(e => e.label.startsWith('requestTotalExecs('));
  const amRequests = rampEvents.filter(e => e.label.startsWith('RequestExecutors('));
  assert.deepEqual(
    eamRequests.map(e => e.label),
    ['requestTotalExecs(2)', 'requestTotalExecs(4)', 'requestTotalExecs(6)'],
    'ExecutorAllocationManager must ramp targets exponentially: 2 -> 4 -> 6',
  );
  assert.deepEqual(
    amRequests.map(e => e.label),
    ['RequestExecutors(2)', 'RequestExecutors(4)', 'RequestExecutors(6)'],
    'YarnSchedulerBackend RequestExecutors must ramp 2 -> 4 -> 6',
  );
  assert.ok(
    eamRequests[0].info.includes('schedulerBacklogTimeout'),
    'First ramp round must fire on schedulerBacklogTimeout',
  );
  assert.ok(
    eamRequests[1].info.includes('sustainedSchedulerBacklogTimeout') &&
      eamRequests[2].info.includes('sustainedSchedulerBacklogTimeout'),
    'Subsequent ramp rounds must fire on sustainedSchedulerBacklogTimeout',
  );
  const rampStep = expRampRun.steps.find(s => s.id === 'sim-step-dynalloc');
  assert.ok(rampStep?.keyState?.includes('1 -> 2 (+1) -> 4 (+2) -> 6 (+2, capped at max 6)'));
  assert.equal(
    expRampRun.events.filter(e => e.label.startsWith('StopExecutor')).length,
    6,
    'Shutdown should stop all 6 live executors after exponential ramp',
  );

  // 4. cached-rdd uses MEMORY_ONLY and emits BOTH hit p=0 and evicted-partition RDD.getOrCompute recompute
  const cacheRun = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'cached-rdd',
    numPartitions: 12,
  });
  assertValidSimulatedRun(cacheRun, 'cached-rdd-mem-only');
  assert.equal(cacheRun.events.some(e => (e.command ?? '').includes('MEMORY_AND_DISK')), false);
  assert.equal(cacheRun.events.some(e => e.label === 'Action 2: hit rdd_17_0'), true);
  assert.equal(cacheRun.events.some(e => e.label === 'Miss evicted rdd_17_6'), true);
  assert.equal(
    cacheRun.events.some(e => e.label === 'Recompute p=6 lineage' && (e.command ?? '').includes('RDD.getOrCompute')),
    true,
  );
});
