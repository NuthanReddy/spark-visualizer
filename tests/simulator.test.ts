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
import {
  buildSimulatedRun,
  resolveValidSimulationConfig,
  SimRunBuilder,
} from '../src/engine/simulator.ts';
import {
  DEFAULT_SIMULATION_CONFIG,
  FAULT_CATEGORIES,
  FAULT_MAP,
  FAULTS_BY_CATEGORY,
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
    const fromInst = run.instances[ev.fromInstance];
    const toInst = run.instances[ev.toInstance];
    assert.ok(fromInst, `${label}: missing fromInstance ${ev.fromInstance}`);
    assert.ok(toInst, `${label}: missing toInstance ${ev.toInstance}`);
    assert.ok(COMPONENTS[ev.fromComponent], `${label}: invalid fromComponent ${ev.fromComponent}`);
    assert.ok(COMPONENTS[ev.toComponent], `${label}: invalid toComponent ${ev.toComponent}`);
    assert.ok(ev.label.length <= 28, `${label}: label too long "${ev.label}"`);
    assert.ok(!/[^\x00-\x7f]/.test(`${ev.label} ${ev.info} ${ev.command ?? ''}`), `${label}: non-ASCII`);
    if (isLocalPort(ev.port)) {
      assert.equal(ev.kind, 'local', `${label}/${ev.id}: local port "${ev.port}" marked wire`);
    }
    if (ev.kind === 'local' || isLocalPort(ev.port)) {
      assert.equal(
        fromInst.hostZone,
        toInst.hostZone,
        `${label}/${ev.id}: cross-host local event ${ev.fromInstance}(${fromInst.hostZone}) -> ${ev.toInstance}(${toInst.hostZone}) port="${ev.port}"`,
      );
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

test('all 12 simulation presets build valid hierarchical runs', () => {
  assert.equal(SIMULATION_PRESETS.length, 12);
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

test('all 7 job types and all 53 fault modes produce valid runs matching severity and invariants', () => {
  assert.equal(SIMULATED_FAULTS.length, 53);

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
    };
    const valid = resolveValidSimulationConfig(raw);
    assert.equal(validateSimulationConfig(valid).valid, true, `fault-${fault.id}: auto-resolve must be valid`);
    const run = buildSimulatedRun(valid);
    assertValidSimulatedRun(run, `fault-${fault.id}`);
    assert.ok(run.events.length >= 4, `fault-${fault.id}: expected >= 4 events, got ${run.events.length}`);

    const hasSucceededUnreg = run.events.some(e => e.label === 'unregisterAM(SUCCEEDED)');
    if (fault.severity === 'terminal') {
      assert.equal(
        hasSucceededUnreg,
        false,
        `fault-${fault.id}: terminal fault must NOT emit unregisterAM(SUCCEEDED)`,
      );
      assert.equal(
        run.events.some(e => e.isError === true),
        true,
        `fault-${fault.id}: terminal fault must emit at least one error event`,
      );
    } else {
      assert.equal(
        hasSucceededUnreg,
        true,
        `fault-${fault.id} (${fault.severity}): non-terminal fault must complete with unregisterAM(SUCCEEDED)`,
      );
    }
  }
});

test('all 9 fault categories are non-empty and partition all 53 simulated faults', () => {
  assert.equal(FAULT_CATEGORIES.length, 9);
  const seen = new Set<string>();
  for (const cat of FAULT_CATEGORIES) {
    const list = FAULTS_BY_CATEGORY[cat.id];
    assert.ok(list && list.length > 0, `Category ${cat.id} must be non-empty`);
    for (const f of list) {
      assert.equal(f.category, cat.id);
      assert.ok(!seen.has(f.id), `Duplicate fault ${f.id} across categories`);
      seen.add(f.id);
    }
  }
  assert.equal(seen.size, SIMULATED_FAULTS.length);
  assert.equal(seen.size, 53);
});

test('SIGKILL-137 and key non-137 distinction faults carry exact diagnostic semantics', () => {
  const runCorpus = (faultId: SimulationConfig['fault']) => {
    const cfg = resolveValidSimulationConfig({ ...DEFAULT_SIMULATION_CONFIG, fault: faultId });
    const run = buildSimulatedRun(cfg);
    assertValidSimulatedRun(run, `distinction-${faultId}`);
    const text = [
      ...run.steps.map(s => `${s.title} ${s.explanation} ${s.keyState ?? ''} ${s.command ?? ''}`),
      ...run.events.map(e => `${e.label} ${e.info} ${e.command ?? ''} ${e.fields.map(f => `${f.name}=${f.value}`).join(' ')}`),
    ].join('\n');
    return { run, text };
  };

  // 1. oom-pmem: -104 & KILLED_EXCEEDED_PMEM
  const pmem = runCorpus('oom-pmem');
  assert.ok(pmem.text.includes('-104') && pmem.text.includes('KILLED_EXCEEDED_PMEM'));

  // 2. oom-vmem: -103 & KILLED_EXCEEDED_VMEM
  const vmem = runCorpus('oom-vmem');
  assert.ok(vmem.text.includes('-103') && vmem.text.includes('KILLED_EXCEEDED_VMEM'));

  // 3. kernel-node-oom: 137 & Linux kernel OOM killer
  const kernel = runCorpus('kernel-node-oom');
  assert.ok(kernel.text.includes('137') && kernel.text.includes('Linux kernel OOM killer'));

  // 4. yarn-preemption: -102 / PREEMPTED and exitCausedByApp=false
  const preempt = runCorpus('yarn-preemption');
  assert.ok(
    preempt.text.includes('-102') &&
      preempt.text.includes('PREEMPTED') &&
      preempt.text.includes('exitCausedByApp=false'),
  );

  // 5. jvm-heap-oom: exit 52 (SparkExitCode.OOM) and NOT 137 as exit code
  const heap = runCorpus('jvm-heap-oom');
  assert.ok(heap.text.includes('52') && heap.text.includes('SparkExitCode.OOM'));
  assert.equal(heap.text.includes('exitStatus=137'), false);
  assert.equal(heap.text.includes('exit 137'), false);
  assert.equal(heap.text.includes('exit code 137'), false);

  // 6. node-crash-power-loss: no exit status / node lost & dead worker-a execs not stopped
  const nodeCrash = runCorpus('node-crash-power-loss');
  assert.ok(
    nodeCrash.text.toLowerCase().includes('no exit code') &&
      nodeCrash.text.includes('LOST'),
  );
  const crashStops = nodeCrash.run.events.filter(e => e.label.startsWith('StopExecutor'));
  assert.ok(crashStops.length >= 1);
  assert.equal(
    crashStops.some(e => nodeCrash.run.instances[e.toInstance]?.hostZone === 'worker-node-a'),
    false,
    'Dead worker-a executors must never receive StopExecutor after node crash',
  );

  // 7. yarn-app-kill: ends in KILLED (terminal)
  const appKill = runCorpus('yarn-app-kill');
  assert.ok(appKill.text.includes('FinalApplicationStatus.KILLED') && appKill.text.includes('-106'));
  assert.equal(appKill.run.events.some(e => e.label === 'unregisterAM(SUCCEEDED)'), false);

  // 8. sigterm-escalated-sigkill: SIGTERM -> SIGKILL (137)
  const sigterm = runCorpus('sigterm-escalated-sigkill');
  assert.ok(
    sigterm.text.includes('SIGTERM') &&
      sigterm.text.includes('SIGKILL') &&
      sigterm.text.includes('137'),
  );
});

test('client-mode placement and 1-executor mode hold across all compatible faults with zero cross-host local ports', () => {
  for (const fault of SIMULATED_FAULTS) {
    if (!fault.requires?.clusterMode) {
      const clientCfg = resolveValidSimulationConfig({
        ...DEFAULT_SIMULATION_CONFIG,
        deployMode: 'client',
        fault: fault.id,
      });
      assert.equal(clientCfg.deployMode, 'client', `${fault.id} should remain in client mode`);
      const clientRun = buildSimulatedRun(clientCfg);
      assertValidSimulatedRun(clientRun, `client-${fault.id}`);
      assert.ok(clientRun.events.length >= 4, `client-${fault.id}: expected >= 4 events`);
      if (fault.id === 'listener-bus-overflow') {
        assert.equal(clientRun.instances['metrics-system']?.hostZone, 'submit-host');
      }
      if (fault.id === 'rpc-ask-timeout') {
        assert.equal(clientRun.instances['rpc-env']?.hostZone, 'submit-host');
      }
    }

    if ((fault.requires?.minExecutors ?? 1) <= 1) {
      const singleCfg = resolveValidSimulationConfig({
        ...DEFAULT_SIMULATION_CONFIG,
        numExecutors: 1,
        fault: fault.id,
      });
      assert.equal(singleCfg.numExecutors, 1, `${fault.id} should remain at 1 executor`);
      const singleRun = buildSimulatedRun(singleCfg);
      assertValidSimulatedRun(singleRun, `e1-${fault.id}`);
      assert.ok(singleRun.events.length >= 4, `e1-${fault.id}: expected >= 4 events`);
    }
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

function collectRunText(run: ExecutionRun): string {
  return [
    ...run.steps.map(s => `${s.title} ${s.explanation} ${s.keyState ?? ''} ${s.command ?? ''}`),
    ...run.events.map(
      e =>
        `${e.label} ${e.info} ${e.command ?? ''} ${e.senderState ?? ''} ${e.receiverState ?? ''} ${e.fields.map(f => `${f.name}=${f.value}`).join(' ')}`,
    ),
  ].join('\n');
}

function assertAmRegistrationInvariant(run: ExecutionRun, label: string) {
  const registeredAms = new Set<string>();
  for (const ev of run.events) {
    const isReg =
      ev.fromComponent === 'application-master' &&
      ev.toComponent === 'resource-manager' &&
      ev.port === 'rm01:8030' &&
      /register/i.test(`${ev.label} ${ev.command ?? ''}`) &&
      !/unregister/i.test(`${ev.label} ${ev.command ?? ''}`);
    if (isReg) {
      registeredAms.add(ev.fromInstance);
      continue;
    }
    if (
      ev.fromComponent === 'node-manager' &&
      ev.toComponent === 'resource-manager' &&
      ev.port === 'rm01:8031' &&
      (ev.command ?? '').includes('container_e01_000001')
    ) {
      registeredAms.delete('application-master');
      continue;
    }
    const isUnreg =
      /(?:^|\b)(?:amclient\.)?unregister(?:am|applicationmaster)?\s*\(/i.test(ev.label) ||
      /(?:^|\b)(?:amclient\.)?unregister(?:am|applicationmaster)?\s*\(/i.test(ev.command ?? '') ||
      (ev.fromComponent === 'application-master' &&
        ev.toComponent === 'resource-manager' &&
        /unregister/i.test(`${ev.label} ${ev.command ?? ''}`));
    if (isUnreg) {
      assert.equal(
        registeredAms.has(ev.fromInstance),
        true,
        `${label}: unregister event "${ev.label}" from ${ev.fromInstance} emitted without preceding registerAM in same attempt`,
      );
    }
  }
}

function assertUniqueExecutorContainerIds(run: ExecutionRun, label: string) {
  const seenByAttempt = new Map<string, string>();
  for (const inst of Object.values(run.instances)) {
    if (inst.componentId !== 'executor-backend' || !inst.containerId) continue;
    const key = inst.containerId;
    assert.equal(
      seenByAttempt.has(key),
      false,
      `${label}: duplicate containerId ${key} shared by ${seenByAttempt.get(key)} and ${inst.id}`,
    );
    seenByAttempt.set(key, inst.id);
  }
}

test('Astra regression: AM registration invariant & unique container IDs hold across all 53 faults in cluster & client modes', () => {
  for (const fault of SIMULATED_FAULTS) {
    for (const deployMode of ['cluster', 'client'] as const) {
      if (deployMode === 'cluster' && fault.requires?.clientMode) continue;
      if (deployMode === 'client' && fault.requires?.clusterMode) continue;
      const cfg = resolveValidSimulationConfig({
        ...DEFAULT_SIMULATION_CONFIG,
        deployMode,
        fault: fault.id,
      });
      assert.equal(cfg.deployMode, deployMode, `${fault.id} must stay in ${deployMode} mode`);
      const run = buildSimulatedRun(cfg);
      assertValidSimulatedRun(run, `am-inv-${deployMode}-${fault.id}`);
      assertAmRegistrationInvariant(run, `am-inv-${deployMode}-${fault.id}`);
      assertUniqueExecutorContainerIds(run, `cid-inv-${deployMode}-${fault.id}`);

      // Client-mode Spark Netty RPC must never target a fabricated driver-1:dynamic listening AM port
      if (deployMode === 'client') {
        assert.equal(
          run.events.some(e => e.protocol.includes('Spark Netty RPC') && e.port === 'driver-1:dynamic'),
          false,
          `client-${fault.id}: Spark Netty RPC must use AM outbound channel, never driver-1:dynamic`,
        );
      }

      // Terminal fault AM disposition invariant by deploy mode:
      // - Cluster mode terminal faults NEVER emit unregisterAM(SUCCEEDED)
      // - Client mode AM/RM-terminated & pre-AM faults NEVER emit unregisterAM(SUCCEEDED)
      // - Client mode SparkContext.stop() teardown on Python/init exceptions emits orders.py exits 1
      //   AND Shutdown(exit=0) -> AM -> unregisterAM(SUCCEEDED)
      if (fault.severity === 'terminal') {
        const hasSucceededUnreg = run.events.some(e => e.label === 'unregisterAM(SUCCEEDED)');
        if (deployMode === 'cluster') {
          assert.equal(
            hasSucceededUnreg,
            false,
            `cluster-${fault.id}: cluster terminal fault must NEVER emit unregisterAM(SUCCEEDED)`,
          );
        } else {
          const amOrPreAmClientFaults = new Set([
            'oversized-request',
            'am-invalid-java-opts',
            'port-bind-exhausted',
            'client-disconnect',
            'yarn-app-kill',
            'max-executor-failures',
            'all-nodes-excluded',
          ]);
          if (amOrPreAmClientFaults.has(fault.id)) {
            assert.equal(
              hasSucceededUnreg,
              false,
              `client-${fault.id}: AM/RM/pre-AM terminal fault must NEVER emit unregisterAM(SUCCEEDED)`,
            );
          } else {
            assert.equal(
              run.events.some(e => e.label === 'orders.py exits 1' && e.isError === true),
              true,
              `client-${fault.id}: client SparkContext.stop() teardown must record orders.py exits 1`,
            );
            assert.equal(
              run.events.some(e => e.label === 'Shutdown(exit=0) -> AM'),
              true,
              `client-${fault.id}: default SparkContext.stop() must send Shutdown(exit=0) -> AM`,
            );
            assert.equal(
              hasSucceededUnreg,
              true,
              `client-${fault.id}: client AM must finish SUCCEEDED on default SparkContext.stop() -> Shutdown(0)`,
            );
          }
        }
      }
    }
  }

  // Direct SimRunBuilder unit test: registeredAmIds tracks AM registration per RuntimeInstance ID
  const builder = new SimRunBuilder({ ...DEFAULT_SIMULATION_CONFIG, deployMode: 'cluster' });
  const am1 = builder.std('application-master');
  const rm = builder.std('resource-manager');
  const amNm = builder.amNmInstance();
  const am2 = builder.ensureInstance('am-attempt-2', {
    componentId: 'application-master',
    label: 'ApplicationMaster (Attempt 2)',
    shortLabel: 'AM Attempt 2',
    hostZone: 'driver-jvm',
    hostLabel: 'driver-1 (Attempt 2)',
    processId: 'AM JVM Attempt 2',
    containerId: 'container_e02_000001',
    attemptNumber: 2,
  });
  const testStep = builder.addStep({
    id: 'test-step-am-guard',
    title: 'Test AM registration guard',
    explanation: 'Verifies per-instance registeredAmIds enforcement',
    category: 'yarn-lifecycle',
    keyState: 'test',
    command: 'test',
    detailsPassed: [],
  });
  assert.equal(builder.isAmRegistered(am1), false);
  assert.throws(
    () =>
      builder.addEvent({
        stepId: testStep,
        from: am1,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
        port: 'rm01:8030',
        label: 'unregisterAM(SUCCEEDED)',
        info: 'illegal unregister before register',
        size: '920 B',
        command: 'amClient.unregister(FinalApplicationStatus.SUCCEEDED, "")',
        fields: [],
        senderState: 'test',
        receiverState: 'test',
      }),
    /unregister forbidden/,
  );
  builder.addEvent({
    stepId: testStep,
    from: am1,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: 'registerAM(driver:41000)',
    info: 'register attempt 1',
    size: '1.6 KB',
    command: 'amClient.register("driver-1", 41000, conf, "http://driver-1:4040")',
    fields: [],
    senderState: 'test',
    receiverState: 'test',
  });
  assert.equal(builder.isAmRegistered(am1), true);
  assert.equal(builder.isAmRegistered(am2), false);
  assert.throws(
    () =>
      builder.addEvent({
        stepId: testStep,
        from: am2,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
        port: 'rm01:8030',
        label: 'unregisterAM(SUCCEEDED)',
        info: 'illegal attempt-2 unregister before attempt-2 register',
        size: '920 B',
        command: 'amClient.unregister(FinalApplicationStatus.SUCCEEDED, "")',
        fields: [],
        senderState: 'test',
        receiverState: 'test',
      }),
    /unregister forbidden/,
  );
  builder.addEvent({
    stepId: testStep,
    from: amNm,
    to: rm,
    channel: 'error',
    protocol: 'Hadoop RPC (ResourceTracker :8031)',
    port: 'rm01:8031',
    label: 'AM Cont #1 exit (137)',
    info: 'attempt 1 died',
    size: 'ContainerStatus',
    command: 'nodeHeartbeat -> ContainerStatus(container_e01_000001, COMPLETE, exitStatus=137)',
    fields: [],
    senderState: 'test',
    receiverState: 'test',
    isError: true,
  });
  assert.equal(builder.isAmRegistered(am1), false);
  assert.throws(
    () =>
      builder.addEvent({
        stepId: testStep,
        from: am1,
        to: rm,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
        port: 'rm01:8030',
        label: 'unregisterAM(FAILED)',
        info: 'dead attempt 1 cannot unregister',
        size: '920 B',
        command: 'amClient.unregister(FinalApplicationStatus.FAILED, "")',
        fields: [],
        senderState: 'test',
        receiverState: 'test',
      }),
    /unregister forbidden/,
  );

  // Specific duplicate-container regression: 1 initial executor + DA + disk-dir-create-fail
  const daDiskFail = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 1,
    executorCores: 1,
    numPartitions: 24,
    dynamicAllocation: true,
    fault: 'disk-dir-create-fail',
  });
  assertValidSimulatedRun(daDiskFail, 'da-1exec-disk-dir-fail');
  assertUniqueExecutorContainerIds(daDiskFail, 'da-1exec-disk-dir-fail');
  const execInstances = Object.values(daDiskFail.instances).filter(
    i => i.componentId === 'executor-backend',
  );
  assert.ok(execInstances.length >= 6, 'Expected initial, replacement, and DA ramped executors');
  const cids = execInstances.map(i => i.containerId);
  assert.equal(new Set(cids).size, cids.length, `Duplicate container IDs found: ${cids.join(', ')}`);
});

test('Astra regression: disk-dir-create-fail timing, exitCausedByApp=true, and live shuffle producer', () => {
  for (const [numExecutors, dynamicAllocation, expectedLiveExecNum] of [
    [3, false, 4],
    [1, true, 2],
  ] as const) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'wide-shuffle',
      numExecutors,
      executorCores: 1,
      numPartitions: 12,
      externalShuffleService: false,
      dynamicAllocation,
      fault: 'disk-dir-create-fail',
    });
    assertValidSimulatedRun(run, `disk-dir-e${numExecutors}-da${dynamicAllocation}`);
    assertUniqueExecutorContainerIds(run, `disk-dir-e${numExecutors}-da${dynamicAllocation}`);

    // Exec-1 must NEVER register its Executor, register its BlockManager, or send Heartbeats
    assert.equal(
      run.events.some(e => e.fromInstance === 'exec-1' && e.label.includes('RegisterExecutor')),
      false,
      'Dead Exec-1 must fail before RegisterExecutor',
    );
    assert.equal(
      run.events.some(e => e.fromInstance === 'exec-1/bm' && e.label.includes('RegisterBM')),
      false,
      'Dead Exec-1 must fail before RegisterBM',
    );
    assert.equal(
      run.events.some(e => e.fromInstance === 'exec-1' && e.label.includes('Heartbeat')),
      false,
      'Dead Exec-1 must fail before sending any Heartbeat',
    );

    // Attribution must be exitCausedByApp=true (NOT false)
    const dirEvents = run.events.filter(e => e.stepId === 'sim-step-disk-dir-fail');
    const removeEv = dirEvents.find(e => (e.command ?? '').includes('RemoveExecutor'));
    assert.ok(removeEv, 'Expected RemoveExecutor event for exit 53');
    assert.ok(
      (removeEv.command ?? '').includes('exitCausedByApp=true'),
      `Expected exitCausedByApp=true for exit 53, got: ${removeEv.command}`,
    );
    assert.equal(
      (removeEv.command ?? '').includes('exitCausedByApp=false'),
      false,
      'Unexpected exit 53 must NOT be attributed as exitCausedByApp=false',
    );

    // Shuffle fetch must reference LIVE replacement executor, NEVER dead "1"
    const fetchEv = run.events.find(e => e.label === 'FetchShuffleBlocks(0)');
    assert.ok(fetchEv, 'Expected FetchShuffleBlocks(0) event');
    assert.equal(fetchEv.toInstance, 'exec-repl/bm', 'Shuffle fetch must target live exec-repl/bm');
    assert.ok(
      (fetchEv.command ?? '').includes(`"${expectedLiveExecNum}"`),
      `Shuffle fetch must reference live replacement exec "${expectedLiveExecNum}", got: ${fetchEv.command}`,
    );
    assert.equal(
      (fetchEv.command ?? '').includes('FetchShuffleBlocks(appId, "1"'),
      false,
      'Shuffle fetch must NEVER hardcode dead executor "1"',
    );
  }
});

test('Astra regression: am-crash-retry NM->RM :8031 exit 137, zero shutdown hook, and Attempt-2 bindings', () => {
  const run = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    deployMode: 'cluster',
    numExecutors: 2,
    fault: 'am-crash-retry',
  });
  assertValidSimulatedRun(run, 'am-crash-retry-astra');
  assertAmRegistrationInvariant(run, 'am-crash-retry-astra');

  const nmExit137 = run.events.find(
    e =>
      e.fromComponent === 'node-manager' &&
      e.toComponent === 'resource-manager' &&
      e.port === 'rm01:8031' &&
      (e.command ?? '').includes('exitStatus=137'),
  );
  assert.ok(nmExit137, 'AM Attempt 1 SIGKILL exit 137 must be reported by node-manager -> resource-manager on :8031');

  const bogusDeadAmReport = run.events.find(
    e =>
      e.fromInstance === 'application-master' &&
      e.toInstance === 'resource-manager' &&
      e.port === 'rm01:8030' &&
      (e.isError || (e.command ?? '').includes('137')),
  );
  assert.equal(bogusDeadAmReport, undefined, 'Dead AM must NEVER report its own SIGKILL on :8030');

  const crashStep = run.steps.find(s => s.id === 'sim-step-am-crash');
  assert.ok(crashStep && crashStep.explanation.includes('ZERO JVM shutdown hooks'));
  assert.equal(
    run.instances['yarn-allocator-a2']?.attemptNumber,
    2,
    'Attempt 2 must bind its own Attempt-2 yarn-allocator',
  );

  // Item 8: Attempt 2 must launch via resource-manager -> am-nm (ContainerManagementProtocol.startContainers),
  // then local am-nm -> am-attempt-2, without claiming RM preserves .sparkStaging
  const rmToAmNmIdx = run.events.findIndex(
    e =>
      e.stepId === 'sim-step-am-crash' &&
      e.fromInstance === 'resource-manager' &&
      e.toInstance === 'am-nm' &&
      e.protocol.includes('ContainerManagementProtocol') &&
      (e.command ?? '').includes('startContainers'),
  );
  const amNmToAm2Idx = run.events.findIndex(
    e =>
      e.stepId === 'sim-step-am-crash' &&
      e.fromInstance === 'am-nm' &&
      e.toInstance === 'am-attempt-2' &&
      e.kind === 'local',
  );
  const am2RegIdx = run.events.findIndex(
    e =>
      e.stepId === 'sim-step-am-crash' &&
      e.fromInstance === 'am-attempt-2' &&
      e.toInstance === 'resource-manager' &&
      e.port === 'rm01:8030',
  );
  assert.ok(
    rmToAmNmIdx >= 0 && amNmToAm2Idx > rmToAmNmIdx && am2RegIdx > amNmToAm2Idx,
    `Expected RM->am-nm (${rmToAmNmIdx}) < am-nm->am-attempt-2 (${amNmToAm2Idx}) < am-attempt-2 register (${am2RegIdx})`,
  );
  const crashText = collectRunText(run);
  assert.ok(crashText.includes('ShutdownHookManager'), 'am-crash-retry must explain .sparkStaging stays intact because SIGKILLed Attempt 1 AM never ran ShutdownHookManager');
  assert.equal(crashText.includes('RM preserves .sparkStaging'), false, 'am-crash-retry must NOT claim RM preserves .sparkStaging');
  assert.equal(crashText.includes('RM skips staging'), false, 'am-crash-retry must NOT claim RM skips staging deletion');
  assert.equal(crashText.includes('RM keeps staging'), false, 'am-crash-retry must NOT claim RM keeps staging dir');
});

test('Astra regression: task-result-lost (TaskState.FINISHED) & task-spill-oom happen BEFORE action completion', () => {
  // 1. collect-action + task-result-lost
  const trl = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'collect-action',
    dataProfile: 'large-result',
    numExecutors: 3,
    fault: 'task-result-lost',
  });
  assertValidSimulatedRun(trl, 'collect-task-result-lost');
  const indirectIdx = trl.events.findIndex(e => e.label.startsWith('IndirectTaskResult'));
  const lostIdx = trl.events.findIndex(e => e.label === 'getRemoteBytes -> LOST');
  const retryIdx = trl.events.findIndex(e => e.label === 'TaskResultLost -> retry');
  const launchRetryIdx = trl.events.findIndex(e => e.label === 'LaunchTask (attempt 1)');
  const deliveredIdx = trl.events.findIndex(e => (e.senderState ?? '').includes('Result delivered'));
  assert.ok(indirectIdx >= 0 && lostIdx >= 0 && retryIdx >= 0 && launchRetryIdx >= 0 && deliveredIdx >= 0);
  assert.ok(
    indirectIdx < lostIdx && lostIdx < retryIdx && retryIdx < launchRetryIdx && launchRetryIdx < deliveredIdx,
    `Expected IndirectTaskResult(${indirectIdx}) < LOST(${lostIdx}) < retry(${retryIdx}) < launchRetry(${launchRetryIdx}) < delivered(${deliveredIdx})`,
  );
  const trlText = collectRunText(trl);
  assert.ok(trlText.includes('MEMORY_AND_DISK_SER'), 'TaskResultLost must note StorageLevel.MEMORY_AND_DISK_SER');
  assert.ok(trlText.includes('TaskState.FINISHED, TaskResultLost'), 'TaskResultLost must use TaskState.FINISHED, TaskResultLost');
  assert.equal(trlText.includes('TaskState.FAILED, TaskResultLost'), false, 'TaskResultLost must NOT use TaskState.FAILED');
  assert.equal(trlText.includes('evicted under memory pressure'), false, 'TaskResultLost must not claim plain memory eviction');

  // 2. cached-rdd + task-result-lost: loss + TaskState.FINISHED, TaskResultLost + retry ALL occur BEFORE Action 2 completion
  const cachedTrl = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'cached-rdd',
    numExecutors: 2,
    numPartitions: 12,
    fault: 'task-result-lost',
  });
  assertValidSimulatedRun(cachedTrl, 'cached-rdd-task-result-lost');
  const cIndirectIdx = cachedTrl.events.findIndex(e => e.label.startsWith('IndirectTaskResult'));
  const cLostIdx = cachedTrl.events.findIndex(e => e.label === 'getRemoteBytes -> LOST');
  const cRetryIdx = cachedTrl.events.findIndex(e => e.label === 'TaskResultLost -> retry');
  const cLaunchIdx = cachedTrl.events.findIndex(e => e.label === 'LaunchTask (attempt 1)');
  const cFetchIdx = cachedTrl.events.findIndex(e => e.label === 'getRemoteBytes(taskres)');
  const cActionDoneIdx = cachedTrl.events.findIndex(e => e.label === 'Action 2 completed');
  assert.ok(
    cIndirectIdx >= 0 &&
      cLostIdx > cIndirectIdx &&
      cRetryIdx > cLostIdx &&
      cLaunchIdx > cRetryIdx &&
      cFetchIdx > cLaunchIdx &&
      cActionDoneIdx > cFetchIdx,
    `cached-rdd task-result-lost ordering violated: indirect=${cIndirectIdx} lost=${cLostIdx} retry=${cRetryIdx} launch=${cLaunchIdx} fetch=${cFetchIdx} done=${cActionDoneIdx}`,
  );
  assert.equal(
    cachedTrl.events
      .slice(0, cActionDoneIdx)
      .some(e => `${e.label} ${e.senderState ?? ''} ${e.receiverState ?? ''}`.includes('Action 2 completed')),
    false,
    'cached-rdd + task-result-lost must NEVER report Action 2 completed before TaskResultLost retry finishes',
  );
  const cachedTrlText = collectRunText(cachedTrl);
  assert.ok(cachedTrlText.includes('TaskState.FINISHED, TaskResultLost'));
  assert.equal(cachedTrlText.includes('TaskState.FAILED, TaskResultLost'), false);

  // 3. All file-write jobs with task-result-lost: loss + retry occur BEFORE commitJob()
  for (const jobType of [
    'narrow-filter',
    'wide-shuffle',
    'sort-merge-skew',
    'broadcast-join',
    'python-udf',
  ] as const) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType,
      dataProfile: jobType === 'broadcast-join' ? 'small-dim' : 'uniform',
      fault: 'task-result-lost',
    });
    assertValidSimulatedRun(run, `${jobType}-task-result-lost`);
    const lIdx = run.events.findIndex(e => e.label === 'getRemoteBytes -> LOST');
    const rIdx = run.events.findIndex(e => e.label === 'TaskResultLost -> retry');
    const fIdx = run.events.findIndex(e => e.label === 'getRemoteBytes(taskres)');
    const cIdx = run.events.findIndex(e => e.label === 'commitJob() + _SUCCESS');
    assert.ok(
      lIdx >= 0 && rIdx > lIdx && fIdx > rIdx && cIdx > fIdx,
      `${jobType}: task-result-lost (${lIdx}/${rIdx}/${fIdx}) must complete BEFORE commitJob (${cIdx})`,
    );
    assert.ok(collectRunText(run).includes('TaskState.FINISHED, TaskResultLost'));
  }

  // 3b. narrow-filter + DA + 2 initial execs + 1 core + 24 partitions + task-result-lost:
  // ResultStage 0 must stay pending until TaskResultLost retry completes!
  const narrowDaTrl = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'narrow-filter',
    dataProfile: 'uniform',
    numExecutors: 2,
    executorCores: 1,
    numPartitions: 24,
    dynamicAllocation: true,
    fault: 'task-result-lost',
  });
  assertValidSimulatedRun(narrowDaTrl, 'narrow-da-task-result-lost');
  const ndLostStepIdx = narrowDaTrl.steps.findIndex(s => s.id === 'sim-step-task-result-lost');
  assert.ok(ndLostStepIdx >= 0, 'Expected sim-step-task-result-lost step');
  assert.equal(
    narrowDaTrl.steps
      .slice(0, ndLostStepIdx)
      .some(s => `${s.title} ${s.keyState ?? ''}`.includes('ResultStage 0 completed')),
    false,
    'narrow-filter + DA + task-result-lost must NOT claim ResultStage 0 completed before TaskResultLost retry',
  );

  // 3c. Custom AM runtime ID ("attempt2-on-driver-1") resolved by containerId on COMPLETE(137)
  const customBuilder = new SimRunBuilder(DEFAULT_SIMULATION_CONFIG);
  const customRm = customBuilder.std('resource-manager');
  const customAmNm = customBuilder.amNmInstance();
  const customAm = customBuilder.ensureInstance('attempt2-on-driver-1', {
    componentId: 'application-master',
    label: 'ApplicationMaster (Custom Attempt 2)',
    shortLabel: 'AM Custom',
    hostZone: 'driver-jvm',
    hostLabel: 'driver-1',
    processId: 'AM JVM Custom',
    containerId: 'container_e02_000001',
    attemptNumber: 2,
  });
  const sCustom = customBuilder.addStep({
    id: 's-custom-am',
    title: 'Custom AM registration lifecycle',
    explanation: 'Verifies containerId-based AM deregistration on COMPLETE exit.',
    category: 'yarn-lifecycle',
  });
  customBuilder.addEvent({
    stepId: sCustom,
    from: customAm,
    to: customRm,
    channel: 'yarn',
    protocol: 'ApplicationMasterProtocol',
    port: 'rm01:8030',
    label: 'registerApplicationMaster',
    info: 'Register custom AM instance.',
    size: '1 KB',
    command: 'amClient.register("driver-1", 41000)',
    fields: [],
  });
  assert.equal(customBuilder.isAmRegistered(customAm), true);
  // Non-terminal RUNNING heartbeat for AM paired with ANOTHER container's COMPLETE must NOT clear AM registration
  customBuilder.addEvent({
    stepId: sCustom,
    from: customAmNm,
    to: customRm,
    channel: 'yarn',
    protocol: 'ResourceTracker',
    port: 'rm01:8031',
    label: 'AM RUNNING + Exec COMPLETE',
    info: 'Heartbeat reports container_e02_000001 RUNNING while container_e02_000002 COMPLETE (exit 1).',
    size: 'ContainerStatus',
    command: 'nodeHeartbeat -> ContainerStatus(container_e02_000001, RUNNING), ContainerStatus(container_e02_000002, COMPLETE, exitStatus=1)',
    fields: [],
  });
  assert.equal(customBuilder.isAmRegistered(customAm), true);
  // Terminal COMPLETE(137) removes exact custom AM ID and blocks unregister
  customBuilder.addEvent({
    stepId: sCustom,
    from: customAmNm,
    to: customRm,
    channel: 'error',
    protocol: 'ResourceTracker',
    port: 'rm01:8031',
    label: 'AM Cont COMPLETE(137)',
    info: 'Heartbeat reports container_e02_000001 COMPLETE (exit 137).',
    size: 'ContainerStatus',
    command: 'nodeHeartbeat -> ContainerStatus(container_e02_000001, COMPLETE, exitStatus=137)',
    fields: [],
    isError: true,
  });
  assert.equal(customBuilder.isAmRegistered(customAm), false);
  assert.throws(
    () =>
      customBuilder.addEvent({
        stepId: sCustom,
        from: customAm,
        to: customRm,
        channel: 'yarn',
        protocol: 'ApplicationMasterProtocol',
        port: 'rm01:8030',
        label: 'unregisterAM(FAILED)',
        info: 'Must fail because custom AM already died.',
        size: '1 KB',
        command: 'amClient.unregisterApplicationMaster(FAILED)',
        fields: [],
      }),
    /AM unregister forbidden/,
  );

  // 4. collect-action + task-spill-oom
  const spill = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'collect-action',
    dataProfile: 'large-result',
    numExecutors: 3,
    fault: 'task-spill-oom',
  });
  assertValidSimulatedRun(spill, 'collect-task-spill-oom');
  const spillIdx = spill.events.findIndex(e => e.label === 'SPILL_OUT_OF_MEMORY');
  const spillDeliveredIdx = spill.events.findIndex(e => (e.senderState ?? '').includes('Result delivered'));
  assert.ok(spillIdx >= 0 && spillDeliveredIdx >= 0 && spillIdx < spillDeliveredIdx,
    `task-spill-oom (${spillIdx}) must occur BEFORE Result delivered (${spillDeliveredIdx})`);
});

test('Astra regression: terminal faults emit centralized AM disposition & exact exit codes', () => {
  // task-exception, max-result-size, commit-job-failure:
  // - cluster mode: SparkUserAppException(exitCode=1) -> unregisterAM(FAILED) (NOT exit 15, NEVER SUCCEEDED)
  // - client mode: orders.py exits 1, while default SparkContext.stop() sends Shutdown(0) -> unregisterAM(SUCCEEDED)
  for (const [faultId, jobType, dataProfile] of [
    ['task-exception', 'narrow-filter', 'corrupt-record'],
    ['max-result-size', 'collect-action', 'large-result'],
    ['commit-job-failure', 'wide-shuffle', 'uniform'],
  ] as const) {
    const clusterRun = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType,
      dataProfile,
      deployMode: 'cluster',
      fault: faultId,
    });
    assertValidSimulatedRun(clusterRun, `terminal-${faultId}-cluster`);
    assertAmRegistrationInvariant(clusterRun, `terminal-${faultId}-cluster`);
    const unregFail = clusterRun.events.find(e => e.label === 'unregisterAM(FAILED)');
    assert.ok(unregFail, `${faultId}/cluster must end with unregisterAM(FAILED)`);
    assert.ok((unregFail.command ?? '').includes('exitCode=1'), `${faultId}/cluster must propagate exitCode=1`);
    assert.equal((unregFail.command ?? '').includes('exitCode=15'), false, `${faultId}/cluster must NOT use exitCode=15`);
    assert.equal(clusterRun.events.some(e => e.label === 'unregisterAM(SUCCEEDED)'), false);

    const clientRun = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType,
      dataProfile,
      deployMode: 'client',
      fault: faultId,
    });
    assertValidSimulatedRun(clientRun, `terminal-${faultId}-client`);
    assertAmRegistrationInvariant(clientRun, `terminal-${faultId}-client`);
    const pyExit = clientRun.events.find(e => e.label === 'orders.py exits 1' && e.isError === true);
    const shut0 = clientRun.events.find(e => e.label === 'Shutdown(exit=0) -> AM');
    const unregOk = clientRun.events.find(e => e.label === 'unregisterAM(SUCCEEDED)');
    assert.ok(pyExit && shut0 && unregOk, `${faultId}/client must show orders.py exits 1 + Shutdown(0) + unregisterAM(SUCCEEDED)`);
    assert.ok((shut0.command ?? '').includes('Shutdown(exitCode=0)'));
    assert.ok(collectRunText(clientRun).includes('stop(0)'));
  }

  // port-bind-exhausted exits directly with System.exit(1) and ZERO unregisterAM in both modes
  for (const deployMode of ['cluster', 'client'] as const) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      deployMode,
      fault: 'port-bind-exhausted',
    });
    assertValidSimulatedRun(run, `port-bind-${deployMode}`);
    assertAmRegistrationInvariant(run, `port-bind-${deployMode}`);
    const text = collectRunText(run);
    assert.ok(text.includes('System.exit(1)'), `port-bind-${deployMode} must show System.exit(1)`);
    assert.equal(run.events.some(e => e.label.toLowerCase().includes('unregister')), false);
  }

  // dynalloc-no-shuffle-service:
  // - client: AM registers first, validateSettings throws SparkException, SparkContext.scala:789 catch calls stop()
  //   sending Shutdown(0) -> AM unregisters SUCCEEDED on YARN while orders.py exits 1!
  // - cluster: exits 13 (EXIT_SC_NOT_INITED) with zero unregisterAM
  const daClient = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'client',
    dynamicAllocation: true,
    externalShuffleService: false,
    fault: 'dynalloc-no-shuffle-service',
  });
  assertValidSimulatedRun(daClient, 'dynalloc-no-ess-client');
  assertAmRegistrationInvariant(daClient, 'dynalloc-no-ess-client');
  const daClientRegIdx = daClient.events.findIndex(e => e.label === 'registerAM(port=-1)');
  const daClientFailIdx = daClient.events.findIndex(e => e.label === 'validateSettings() FAIL');
  const daClientShut0Idx = daClient.events.findIndex(e => e.label === 'Shutdown(exit=0) -> AM');
  const daClientUnregIdx = daClient.events.findIndex(e => e.label === 'unregisterAM(SUCCEEDED)');
  const daClientPyExitIdx = daClient.events.findIndex(e => e.label === 'orders.py exits 1' && e.isError === true);
  assert.ok(
    daClientRegIdx >= 0 &&
      daClientFailIdx > daClientRegIdx &&
      daClientShut0Idx > daClientFailIdx &&
      daClientUnregIdx > daClientShut0Idx &&
      daClientPyExitIdx > daClientUnregIdx,
    `Expected register(${daClientRegIdx}) < validateFail(${daClientFailIdx}) < Shutdown(0)(${daClientShut0Idx}) < unregisterAM(SUCCEEDED)(${daClientUnregIdx}) < orders.py exits 1(${daClientPyExitIdx})`,
  );
  const daClientText = collectRunText(daClient);
  assert.ok(
    daClientText.includes('spark.shuffle.service.enabled') &&
      daClientText.includes('spark.dynamicAllocation.shuffleTracking.enabled') &&
      daClientText.includes('spark.storage.decommission.shuffleBlocks.enabled') &&
      daClientText.includes('ShuffleDataIO') &&
      daClientText.includes('Shutdown(exitCode=0)') &&
      daClientText.includes('stop(0)'),
  );

  const daCluster = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'cluster',
    dynamicAllocation: true,
    externalShuffleService: false,
    fault: 'dynalloc-no-shuffle-service',
  });
  assertValidSimulatedRun(daCluster, 'dynalloc-no-ess-cluster');
  assertAmRegistrationInvariant(daCluster, 'dynalloc-no-ess-cluster');
  assert.equal(daCluster.events.some(e => e.label.toLowerCase().includes('unregister')), false);
  assert.ok(collectRunText(daCluster).includes('EXIT_SC_NOT_INITED=13'));

  // am-invalid-java-opts contacts RM :8032 getNewApplication BEFORE failing with ZERO submitApplication
  const amOpts = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    deployMode: 'client',
    fault: 'am-invalid-java-opts',
  });
  assertValidSimulatedRun(amOpts, 'am-invalid-java-opts');
  assert.equal(
    amOpts.events.some(e => e.fromInstance === 'yarn-client' && e.toInstance === 'resource-manager' && e.port === 'rm01:8032' && e.label.includes('getNewApplication')),
    true,
    'am-invalid-java-opts must contact RM :8032 getNewApplication before failing',
  );
  assert.equal(amOpts.events.some(e => e.label.includes('submitApplication')), false);
  assert.equal(collectRunText(amOpts).includes('without contacting YARN RM'), false);
});

test('Astra regression: exec-intentional-remove, ess-unavailable, codegen-huge-method, multiple-spark-contexts & torrent-piece-corrupt', () => {
  // 1. exec-intentional-remove (DA off & DA on): NO bogus replacement container, ABORTED (-100)
  for (const dynamicAllocation of [false, true]) {
    const run = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      jobType: 'wide-shuffle',
      numExecutors: 2,
      dynamicAllocation,
      fault: 'exec-intentional-remove',
    });
    assertValidSimulatedRun(run, `exec-remove-da-${dynamicAllocation}`);
    assert.equal(
      run.events.some(e => e.label.includes('Exec-repl') || e.toInstance === 'exec-repl'),
      false,
      `exec-intentional-remove (DA=${dynamicAllocation}) must NOT allocate a bogus replacement container`,
    );
    const text = collectRunText(run);
    assert.ok(text.includes('ABORTED') && text.includes('-100') && text.includes('exitCausedByApp=false'));
    if (dynamicAllocation) {
      assert.ok(text.includes('adjustTargetNumExecutors=false'));
    }
  }

  // 2. ess-unavailable keeps worker executor JVMs alive
  const essDown = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'wide-shuffle',
    numExecutors: 4,
    externalShuffleService: true,
    fault: 'ess-unavailable',
  });
  assertValidSimulatedRun(essDown, 'ess-unavailable-alive');
  const stoppedExecs = essDown.events.filter(e => e.label.startsWith('StopExecutor')).map(e => e.toInstance);
  assert.deepEqual(
    stoppedExecs,
    ['exec-1', 'exec-2', 'exec-3', 'exec-4'],
    'ess-unavailable must keep all worker executor JVMs alive through shutdown',
  );
  assert.ok(collectRunText(essDown).includes('spark.files.fetchFailure.unRegisterOutputOnHost'));

  // 3. codegen-huge-method rejects cached-rdd and uses valid bytecode size < 65535 (9420 > 8000 INFO)
  const badCodegen = validateSimulationConfig({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'cached-rdd',
    fault: 'codegen-huge-method',
  });
  assert.equal(badCodegen.valid, false, 'codegen-huge-method must reject cached-rdd');
  const fixedCodegen = resolveValidSimulationConfig({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'cached-rdd',
    fault: 'codegen-huge-method',
  });
  assert.equal(fixedCodegen.jobType, 'wide-shuffle');
  const cgRun = buildSimulatedRun(fixedCodegen);
  assertValidSimulatedRun(cgRun, 'codegen-huge-method-valid');
  const cgText = collectRunText(cgRun);
  assert.ok(cgText.includes('9420') && cgText.includes('8000') && cgText.includes('INFO'));
  assert.equal(cgText.includes('71420') || cgText.includes('71,420'), false);

  // 4. multiple-spark-contexts: registered AM & active SC #1 before rejecting SC #2, never mentions allowMultipleContexts
  for (const deployMode of ['cluster', 'client'] as const) {
    const msc = buildSimulatedRun({
      ...DEFAULT_SIMULATION_CONFIG,
      deployMode,
      fault: 'multiple-spark-contexts',
    });
    assertValidSimulatedRun(msc, `multi-sc-${deployMode}`);
    assertAmRegistrationInvariant(msc, `multi-sc-${deployMode}`);
    const regIdx = msc.events.findIndex(e => e.fromComponent === 'application-master' && e.toComponent === 'resource-manager' && e.label.includes('registerAM'));
    const rejectIdx = msc.events.findIndex(e => e.stepId === 'sim-step-multi-sc');
    const expectedUnregLabel = deployMode === 'cluster' ? 'unregisterAM(FAILED)' : 'unregisterAM(SUCCEEDED)';
    const unregIdx = msc.events.findIndex(e => e.label === expectedUnregLabel);
    assert.ok(regIdx >= 0 && rejectIdx > regIdx && unregIdx > rejectIdx, `${deployMode}: expected registerAM < reject < ${expectedUnregLabel}`);
    const mscText = collectRunText(msc);
    assert.ok(mscText.includes('_ensure_initialized') && mscText.includes('assertNoOtherContextIsRunning'));
    assert.equal(mscText.includes('allowMultipleContexts'), false);
  }

  // 5. torrent-piece-corrupt fails task attempt with IOException and retries rather than continuing in-place
  const tpc = buildSimulatedRun({
    ...DEFAULT_SIMULATION_CONFIG,
    jobType: 'broadcast-join',
    dataProfile: 'small-dim',
    numExecutors: 2,
    fault: 'torrent-piece-corrupt',
  });
  assertValidSimulatedRun(tpc, 'torrent-piece-corrupt-retry');
  const ioeIdx = tpc.events.findIndex(e => (e.command ?? '').includes('IOException') && e.stepId === 'sim-step-torrent-corrupt');
  const taskFailIdx = tpc.events.findIndex(e => e.label === 'Task attempt 0 FAIL (IOE)');
  const retryLaunchIdx = tpc.events.findIndex(e => e.label === 'LaunchTask (attempt 1)');
  const cleanFetchIdx = tpc.events.findIndex(e => e.label === 'Retry piece1 OK (Driver)');
  assert.ok(ioeIdx >= 0 && taskFailIdx > ioeIdx && retryLaunchIdx > taskFailIdx && cleanFetchIdx > retryLaunchIdx);
});

test('Astra regression: remaining YARN/Broadcast/DAG/RPC/Observability/PySpark precision fixes', () => {
  // Item 5: yarn-app-kill uses Hadoop 3.4.1 AMLauncher.stopContainers -> am-nm (NEVER AM_SHUTDOWN) + worker NM :8031
  for (const deployMode of ['cluster', 'client'] as const) {
    const appKill = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, deployMode, fault: 'yarn-app-kill' });
    assertValidSimulatedRun(appKill, `yarn-app-kill-${deployMode}`);
    const killText = collectRunText(appKill);
    assert.equal(killText.includes('AM_SHUTDOWN'), false, `yarn-app-kill (${deployMode}) must NEVER mention deprecated AM_SHUTDOWN`);
    assert.equal(appKill.events.some(e => e.label.includes('KillApplication')), false);
    assert.equal(
      appKill.events.some(
        e =>
          e.fromInstance === 'resource-manager' &&
          e.toInstance === 'am-nm' &&
          e.protocol.includes('ContainerManagementProtocol') &&
          (e.command ?? '').includes('stopContainers'),
      ),
      true,
      `yarn-app-kill (${deployMode}) must stop AM via RM AMLauncher -> am-nm stopContainers`,
    );
    assert.equal(
      appKill.events.some(
        e => e.fromInstance === 'am-nm' && e.toInstance === 'application-master' && e.kind === 'local',
      ),
      true,
      `yarn-app-kill (${deployMode}) must show am-nm stopping AM container locally`,
    );
    assert.equal(
      appKill.events.some(e => e.fromInstance === 'resource-manager' && e.toComponent === 'node-manager' && e.port === 'rm01:8031'),
      true,
    );
    assert.ok(killText.includes('-106') && killText.includes('-107') && killText.includes('AMLauncher'));
  }
  const yarnKillMeta = FAULT_MAP['yarn-app-kill'];
  assert.equal(JSON.stringify(yarnKillMeta).includes('AM_SHUTDOWN'), false);

  // sigterm-escalated-sigkill: never claims exit 143
  const sigterm = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'sigterm-escalated-sigkill' });
  assert.equal(collectRunText(sigterm).includes('SIGTERM(143)'), false);
  assert.equal(collectRunText(sigterm).includes('exit 143'), false);

  // node-crash-power-loss: distinguishes no OS exit code from RM synthesized ABORTED (-100)
  const ncText = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'node-crash-power-loss' }));
  assert.ok(ncText.includes('ABORTED') && ncText.includes('-100') && ncText.includes('NMLivelinessMonitor'));

  // max-executor-failures: uses exit 1 (NOT -104 PMEM) and DA uses maxExecutors=6 -> 12
  const mefDa = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, numExecutors: 2, dynamicAllocation: true, fault: 'max-executor-failures' });
  const mefText = collectRunText(mefDa);
  assert.ok(mefText.includes('exitStatus=1') && mefText.includes('12/12'));
  assert.equal(mefText.includes('exitStatus=-104'), false);

  // Item 6: all-nodes-excluded uses strict > check (3 > 2 per host, 6 total < global budget >= 8, queue default-node-label-expression=workers -> 2/2 RM cluster nodes)
  const ane = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'all-nodes-excluded' });
  const aneText = collectRunText(ane);
  assert.ok(aneText.includes('spark.yarn.executor.launch.excludeOnFailure.enabled=true'));
  assert.ok(aneText.includes('default-node-label-expression=workers'));
  assert.ok(aneText.includes('spark.yarn.am.nodeLabelExpression=am-nodes'));
  assert.ok(aneText.includes('3 > 2') && aneText.includes('6 / 8') && aneText.includes('2/2 RM cluster nodes in label=workers'));
  assert.ok(aneText.includes('amClient.getClusterNodeCount()=2'));
  assert.equal(aneText.includes('failures=2/2'), false, 'all-nodes-excluded must use strict > (3 > 2), NOT 2/2');
  assert.equal(aneText.includes('2 / 2 (excluded)'), false, 'all-nodes-excluded must NOT exclude on 2/2');
  assert.equal(ane.events.some(e => e.fromInstance === 'nm-a' && e.toInstance === 'resource-manager' && e.label === 'worker-a fail (3>2, ex=1)'), true);
  assert.equal(ane.events.some(e => e.fromInstance === 'nm-b' && e.toInstance === 'resource-manager' && e.label === 'worker-b fail (3>2, ex=1)'), true);

  // Item 9: broadcast-over-8gb cites 357,913,941 ((1 << 29) / 1.5) and 512,000,000 (NEVER 341,333,333)
  const b8g = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', dataProfile: 'small-dim', fault: 'broadcast-over-8gb' }));
  assert.ok(
    b8g.includes('512,000,000') &&
      b8g.includes('357,913,941') &&
      b8g.includes('1 << 29') &&
      b8g.includes('_LEGACY_ERROR_TEMP_2248') &&
      b8g.includes('_LEGACY_ERROR_TEMP_2249'),
  );
  assert.equal(b8g.includes('341,333,333') || b8g.includes('341.3'), false, 'broadcast-over-8gb must NOT cite stale 341,333,333');
  const b8gMeta = JSON.stringify(FAULT_MAP['broadcast-over-8gb']);
  assert.ok(b8gMeta.includes('357,913,941') && b8gMeta.includes('512,000,000'));
  assert.equal(b8gMeta.includes('341,333,333') || b8gMeta.includes('341.3'), false);

  const bto = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', dataProfile: 'small-dim', fault: 'broadcast-timeout' }));
  assert.ok(bto.includes('cancelJobsWithTag') && bto.includes('relationFuture.cancel(true)'));

  const boom = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'broadcast-join', dataProfile: 'small-dim', fault: 'broadcast-driver-oom' }));
  assert.ok(boom.includes('_LEGACY_ERROR_TEMP_2250') && boom.includes('Not enough memory to build and broadcast the table to all worker nodes'));

  // Item 10: indeterminate-stage-rollback cites spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false
  const indet = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'indeterminate-stage-rollback' }));
  assert.ok(
    indet.includes('checksumMismatchFullRetryEnabled') &&
      indet.includes('spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false') &&
      indet.includes('does NOT unset checksumMismatchFullRetryEnabled'),
  );
  const indetMeta = JSON.stringify(FAULT_MAP['indeterminate-stage-rollback']);
  assert.ok(indetMeta.includes('spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch'));

  // rpc-ask-timeout: timeout expires ON caller executor (worker-a), not inside driver
  const rpcTo = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'rpc-ask-timeout' });
  const rpcExpireEv = rpcTo.events.find(e => e.label === 'RpcTimeoutException 120s');
  assert.ok(rpcExpireEv && rpcExpireEv.fromInstance === 'exec-1/bm' && rpcExpireEv.toInstance === 'exec-1/bm');

  // listener-bus-overflow: separate appStatus vs eventLog queues
  const lbo = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'listener-bus-overflow' }));
  assert.ok(lbo.includes('queue.appStatus.numDroppedEvents=1420') && lbo.includes('queue.eventLog.numDroppedEvents=0'));

  // python-daemon-stdout-corrupt: fails before any worker process spawned (no python-worker instance)
  const pyDaemon = buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'python-udf', fault: 'python-daemon-stdout-corrupt' });
  assert.equal(Object.values(pyDaemon.instances).some(i => i.componentId === 'python-worker'), false);

  // python-version-mismatch: split_index then python_version, error via report_init_response
  const pyVer = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'python-udf', fault: 'python-version-mismatch' }));
  assert.ok(pyVer.includes('split_index') && pyVer.includes('report_init_response'));

  // python-idle-timeout: uses ProcessHandle.destroy()
  const pyIdle = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, jobType: 'python-udf', fault: 'python-idle-timeout' }));
  assert.ok(pyIdle.includes('ProcessHandle.destroy()'));
  assert.equal(pyIdle.includes('destroyForcibly() -> retry'), false);

  // all-executors-excluded: branches on dynamicAllocation
  const exclOff = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, dynamicAllocation: false, fault: 'all-executors-excluded' }));
  assert.ok(exclOff.includes('Immediate abort'));
  const exclOn = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, dynamicAllocation: true, fault: 'all-executors-excluded' }));
  assert.ok(exclOn.includes('unschedulableTaskSetTimeout') && exclOn.includes('120s'));

  // jvm-heap-oom & closure-not-serializable cleanups
  const heapText = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'jvm-heap-oom' }));
  assert.equal(heapText.includes('if hook hangs'), false);
  const closText = collectRunText(buildSimulatedRun({ ...DEFAULT_SIMULATION_CONFIG, fault: 'closure-not-serializable' }));
  assert.equal(closText.includes('spark.closure.serializer') || closText.includes('Kryo'), false);
});

