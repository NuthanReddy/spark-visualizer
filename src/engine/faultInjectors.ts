import type { ExecutionRun } from '../data/runModel.ts';
import type { SimRunBuilder, SimRunMeta, SimRuntimeState } from './simulator.ts';
import type { SimulationConfig } from './simulatorTypes.ts';

export interface ShuffleReduceContext {
  aqeOffNote: string;
  isLocalShuffle: boolean;
  shuffleServerPort: string;
  producerExec: string;
  producerExecNum: number;
  producerBm: string;
  producerZone: 'worker-node-a' | 'worker-node-b';
  producerHost: 'worker-a' | 'worker-b';
  reduceConsumer: {
    consumerExec: string;
    consumerBm: string;
    hostZone: 'worker-node-a' | 'worker-node-b';
    hostName: 'worker-a' | 'worker-b';
  };
}

export interface PythonUdfContext {
  isArrow: boolean;
  udfConsumer: {
    consumerExec: string;
    consumerBm: string;
    hostZone: 'worker-node-a' | 'worker-node-b';
    hostName: 'worker-a' | 'worker-b';
  };
  udfOp: string;
  pyFactory: string;
  pyRunner: string;
  pyWorker?: string;
}

export type TerminalFailureOrigin =
  | 'user-app-failure'
  | 'am-max-exec-failures'
  | 'am-sc-not-inited'
  | 'driver-port-bind';

export interface TerminalAppOutcomeOptions {
  stepId?: string;
  origin: TerminalFailureOrigin;
  diagnostic: string;
  stopLiveExecutors?: boolean;
  isLastAttempt?: boolean;
}

function finalizeShortCircuit(
  b: SimRunBuilder,
  meta: SimRunMeta,
  layerTag: string,
): ExecutionRun {
  return b.finalize(
    meta.runId,
    meta.runName,
    meta.runSubtitle,
    meta.runSummary,
    layerTag,
    meta.telemetry,
  );
}

/**
 * Unified multi-language terminal application finalizer across all 59 faults:
 * - Never emit unregisterApplicationMaster when !b.isAmRegistered(state.activeAm)
 * - Cluster unregistered AM finish -> EXIT_SC_NOT_INITED (13) or System.exit(1) on WebUI bind
 * - Cluster registered user app failure:
 *   - PySpark: orders.py exits 1 -> PythonRunner throws SparkUserAppException(1) -> AM FAILED/1
 *   - Scala/Java: uncaught main() -> InvocationTargetException -> AM FAILED/15 (EXIT_EXCEPTION_USER_CLASS)
 * - Client registered user app failure:
 *   - PySpark: orders.py exits 1; default SparkContext.stop() = stop(0) sends Shutdown(0) -> AM SUCCEEDED/0
 *   - Scala/Java: JavaMainApplication invokes main, SparkSubmit unwraps & rethrows (exits 1; NO automatic
 *     sc.stop(1), AM NEVER selects exit 15); on ordinary JVM shutdown, SparkContext's shutdown hook
 *     (SparkContext.scala:757-769) calls stop() -> Shutdown(0) -> AM SUCCEEDED/0 (explicit sc.stop(1)
 *     sends Shutdown(1) -> FAILED; abrupt client death without Shutdown -> SUCCEEDED/0 or FAILED/17
 *     with clientModeTreatDisconnectAsFailed=true)
 * - Explicit AM failure ceilings -> EXIT_MAX_EXECUTOR_FAILURES (11)
 * - Managed cluster AM only calls unregisterApplicationMaster(FAILED) on final attempt
 *   (isLastAttempt=true / maxAppAttempts=1; retryable earlier attempt omits unregister so RM retries)
 */
export function emitTerminalAppOutcome(
  b: SimRunBuilder,
  state: SimRuntimeState,
  opts: TerminalAppOutcomeOptions,
): void {
  const isCluster = state.cfg.deployMode === 'cluster';
  const isLastAttempt = opts.isLastAttempt ?? true;
  const registered = state.amRegistered && b.isAmRegistered(state.activeAm);

  if (!registered && isCluster) {
    const exitCode = opts.origin === 'driver-port-bind' ? 1 : 13;
    const stepId =
      opts.stepId ??
      b.addStep({
        id: 'sim-step-terminal-outcome',
        title:
          exitCode === 1
            ? 'Driver JVM exits directly via System.exit(1) before AM registers (no unregisterAM)'
            : 'Unregistered cluster AM exits locally with EXIT_SC_NOT_INITED (13, no unregisterAM)',
        explanation:
          exitCode === 1
            ? `WebUI.bind() calls System.exit(1) directly inside the AM JVM before AM registers with RM (registered=false). ApplicationMaster shutdown hook skips unregisterApplicationMaster, and driver-1 NM reports exitStatus=1 on :8031.`
            : `Because AM never registered with RM (registered=false), ApplicationMaster.finish overrides exitCode to EXIT_SC_NOT_INITED (13) and skips unregisterApplicationMaster. driver-1 NM reports exitStatus=13 on :8031.`,
        category: 'yarn-lifecycle',
        keyState: `FAILED (exit=${exitCode}, registered=false, no unregisterAM)`,
        command:
          exitCode === 1
            ? 'WebUI.bind() -> System.exit(1) // registered=false, skip unregister'
            : 'ApplicationMaster.finish(FAILED, EXIT_SC_NOT_INITED=13) // registered=false, skip unregister',
        detailsPassed: [
          { name: 'amRegistered', value: 'false (unregisterApplicationMaster skipped)' },
          { name: 'exitCode', value: String(exitCode) },
        ],
        deepDiveComponentId: 'application-master',
      });
    const amNm = state.amNm ?? b.amNmInstance();
    b.addEvent({
      stepId,
      from: amNm,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: `AM container exit (${exitCode})`,
      info: `driver-1 NM reports AM container COMPLETE (exitStatus=${exitCode}) to RM :8031; no unregisterAM RPC was sent because registered=false.`,
      size: 'ContainerStatus',
      command: `NM ContainerStatus(container_e0${state.activeAttempt}_000001, COMPLETE, exitStatus=${exitCode})`,
      fields: [
        { name: 'exitStatus', value: String(exitCode) },
        { name: 'unregisterSent', value: 'false (registered=false)' },
      ],
      senderState: 'AM container exited on driver-1',
      receiverState: 'RM marks application attempt FAILED',
      isError: true,
    });
    return;
  }

  const isUserAppFailure = opts.origin === 'user-app-failure';
  const isClientUserFailure = b.isClient && isUserAppFailure;
  const exitCode =
    opts.origin === 'am-max-exec-failures'
      ? 11
      : isUserAppFailure
        ? b.isPySpark
          ? 1
          : 15
        : 13;
  const exitLabel =
    opts.origin === 'am-max-exec-failures'
      ? 'EXIT_MAX_EXECUTOR_FAILURES (11)'
      : b.isPySpark
        ? 'SparkUserAppException(exitCode=1)'
        : 'EXIT_EXCEPTION_USER_CLASS (15)';
  const shouldStopExecs =
    (opts.stopLiveExecutors ?? isUserAppFailure) && state.liveExecs.length > 0;
  const jvmShortName = b.isScala ? 'OrdersJob' : 'OrdersJavaJob';

  const stepId =
    opts.stepId ??
    b.addStep({
      id: 'sim-step-terminal-outcome',
      title: isCluster
        ? b.isPySpark
          ? `orders.py exits ${exitCode} -> SparkContext.stop() & final-attempt AM unregisters FAILED`
          : `Uncaught ${b.appEntryLabel}() -> AM exit 15 (EXIT_EXCEPTION_USER_CLASS) & unregister FAILED`
        : isClientUserFailure
          ? b.isPySpark
            ? 'orders.py exits 1; default SparkContext.stop() sends Shutdown(0) -> YARN AM SUCCEEDED'
            : `${jvmShortName} exits 1 (JavaMainApplication); SC shutdown hook sends Shutdown(0) -> AM SUCCEEDED`
          : `Client AM unregisters FAILED (exit=${exitCode})`,
      explanation: isCluster
        ? b.isPySpark
          ? `User Python script orders.py fails and exits ${exitCode}; PythonRunner throws SparkUserAppException(exitCode=${exitCode}) (NOT EXIT_EXCEPTION_USER_CLASS=15). SparkContext.stop() halts live executors, ApplicationMaster.finish records FAILED (exitCode=${exitCode}), and on the final attempt (isLastAttempt=true, spark.yarn.maxAppAttempts=1; a retryable attempt 1 < maxAppAttempts would omit unregister(FAILED) so RM can retry) AM cleans staging and calls unregisterApplicationMaster(FAILED).`
          : `Uncaught exception escapes ${b.appEntryLabel}() inside driver-1 AM JVM (${opts.diagnostic}). ApplicationMaster.startUserApplication (ApplicationMaster.scala:775-785, 911) catches InvocationTargetException (NOT SparkUserAppException), halts live executors, records FinalApplicationStatus.FAILED with EXIT_EXCEPTION_USER_CLASS = 15, and on the final attempt (isLastAttempt=true) calls unregisterApplicationMaster(FAILED).`
        : isClientUserFailure
          ? b.isPySpark
            ? `Client driver orders.py on edge-01 fails and exits 1 (${opts.diagnostic}). During teardown, standard PySpark SparkContext.stop() invokes zero-arg JVM SparkContext.stop() = stop(0) (SparkContext.scala:2607); YarnClientSchedulerBackend.stop() (YarnClientSchedulerBackend.scala:174-190) first sends Shutdown(exitCode=0) over edge-01:41000 to the AM and THEN calls super.stop() to send StopExecutor to live executors. Per ApplicationMaster.scala:834-837,878-886, the client AM therefore unregisters FinalApplicationStatus.SUCCEEDED on YARN RM :8030 even though orders.py exited 1! YARN reports FAILED only if caller explicitly invokes JVM SparkContext.stop(1) (sending Shutdown(1)) or crashes without stop() when spark.yarn.am.clientModeTreatDisconnectAsFailed=true.`
            : `Uncaught exception escapes ${b.appEntryLabel}() on edge-01 (${opts.diagnostic}). JavaMainApplication invokes main() and SparkSubmit unwraps & rethrows so the submit JVM fails with exit code 1 (NO automatic sc.stop(1), and client AM NEVER selects EXIT_EXCEPTION_USER_CLASS=15). On ordinary JVM shutdown, SparkContext's shutdown hook (SparkContext.scala:757-769) invokes zero-arg stop() = stop(0) (SparkContext.scala:2607); YarnClientSchedulerBackend.stop() (YarnClientSchedulerBackend.scala:174-190) first sends Shutdown(exitCode=0) over edge-01:41000 to the AM and THEN calls super.stop() to send StopExecutor to live executors, after which the client AM disconnects and unregisters FinalApplicationStatus.SUCCEEDED (0) while the launcher fails! Calling explicit sc.stop(1) sends Shutdown(1) -> YARN FAILED, or abrupt client death without Shutdown hits clientModeTreatDisconnectAsFailed=true -> FAILED (17).`
          : `Client AM triggers ${exitLabel} and calls unregisterApplicationMaster(FAILED) on RM :8030.`,
      category: 'yarn-lifecycle',
      keyState: isClientUserFailure
        ? b.isPySpark
          ? 'orders.py FAILED (exit=1) | YARN AM SUCCEEDED via default SparkContext.stop() -> Shutdown(0) (need sc.stop(1) for YARN FAILED)'
          : `${jvmShortName} / JavaMainApplication FAILED (exit=1) | SC shutdown hook Shutdown(0) -> YARN AM SUCCEEDED (never exit 15)`
        : `FAILED (exit=${exitCode} ${exitLabel}) | FinalApplicationStatus.FAILED`,
      command: isCluster
        ? b.isPySpark
          ? `SparkUserAppException(exitCode=${exitCode}) -> finish(FAILED, ${exitCode}) -> unregister(FAILED)`
          : `InvocationTargetException -> finish(FAILED, EXIT_EXCEPTION_USER_CLASS=15) -> unregister(FAILED)`
        : isClientUserFailure
          ? b.isPySpark
            ? 'orders.py exits 1 | SparkContext.stop() -> Shutdown(0) -> super.stop(StopExecutors) -> amClient.unregister(SUCCEEDED)'
            : `${b.appEntryLabel}() throws -> JavaMainApplication exits 1 | ShutdownHook stop() -> Shutdown(0) -> super.stop(StopExecutors) -> amClient.unregister(SUCCEEDED)`
          : `finish(FinalApplicationStatus.FAILED, ${exitCode}) -> amClient.unregister(FAILED)`,
      detailsPassed: [
        { name: 'language', value: b.langDisplay },
        { name: 'deployMode', value: state.cfg.deployMode },
        { name: 'amRegistered', value: 'true' },
        ...(isClientUserFailure
          ? b.isPySpark
            ? [
                { name: 'clientDriverExit', value: '1 (orders.py FAILED on edge-01)' },
                {
                  name: 'yarnAmStatus',
                  value: 'FinalApplicationStatus.SUCCEEDED (default SparkContext.stop() calls stop(0) -> Shutdown(0) before super.stop())',
                },
                {
                  name: 'explicitFailedOpt',
                  value: 'Call JVM SparkContext.stop(1) -> Shutdown(1) -> FinalApplicationStatus.FAILED',
                },
                {
                  name: 'sourceRef',
                  value: 'SparkContext.scala:2607; YarnClientSchedulerBackend.scala:174-190; ApplicationMaster.scala:834-886',
                },
              ]
            : [
                { name: 'clientDriverExit', value: `1 (${jvmShortName} / JavaMainApplication FAILED on edge-01; NO automatic sc.stop(1))` },
                {
                  name: 'yarnAmStatus',
                  value: 'FinalApplicationStatus.SUCCEEDED (SC shutdown hook calls stop() -> Shutdown(0) before super.stop(); AM NEVER selects exit 15)',
                },
                {
                  name: 'explicitFailedOpt',
                  value: 'Call explicit sc.stop(1) -> Shutdown(1) -> FAILED, or abrupt disconnect with clientModeTreatDisconnectAsFailed=true -> FAILED (17)',
                },
                {
                  name: 'sourceRef',
                  value: 'SparkContext.scala:757-769,2607; YarnClientSchedulerBackend.scala:174-190; JavaMainApplication; ApplicationMaster.scala:834-886',
                },
              ]
          : [
              { name: 'exitCode', value: `${exitCode} (${exitLabel})` },
              {
                name: 'attemptPolicy',
                value: isCluster
                  ? 'isLastAttempt=true (maxAppAttempts=1; retryable attempt < maxAppAttempts omits unregister so RM retries)'
                  : 'Client AM unregisters FAILED on AM-originated ceiling',
              },
            ]),
      ],
      deepDiveComponentId: 'application-master',
    });

  const emitStopLiveExecutors = (clientAfterShutdown = false) => {
    if (!shouldStopExecs) return;
    for (const liveExecId of state.liveExecs) {
      b.addEvent({
        stepId,
        from: state.schedBackend,
        to: liveExecId,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: `StopExecutor(${liveExecId})`,
        info: clientAfterShutdown
          ? `After YarnClientSchedulerBackend.stop() sends Shutdown(0) to AM (YarnClientSchedulerBackend.scala:174-190), super.stop() sends StopExecutor to live executor ${liveExecId}.`
          : `SparkContext.stop() sends StopExecutor to live executor ${liveExecId} during failure cleanup.`,
        size: '340 B',
        command: `driverEndpoint.askSync[Boolean](StopExecutors) // target=${liveExecId}`,
        fields: [{ name: 'executorId', value: liveExecId }],
        senderState: clientAfterShutdown
          ? 'YarnClientSchedulerBackend super.stop() halting live executors'
          : 'Cleaning up live executors after failure',
        receiverState: `${liveExecId} shutting down`,
      });
    }
  };

  if (!isClientUserFailure) {
    emitStopLiveExecutors(false);
  }

  if (isUserAppFailure) {
    if (b.isClient) {
      const sc = b.std('spark-context');
      const yarnBackend = b.std('yarn-scheduler-backend');
      const link = b.driverToAmLink();
      if (b.isPySpark) {
        const pyDriver = b.std('python-driver');
        b.addEvent({
          stepId,
          from: sc,
          to: pyDriver,
          channel: 'error',
          protocol: 'Py4J Loopback Exception',
          port: '127.0.0.1:44108',
          label: 'orders.py exits 1',
          info: `Unhandled Spark failure propagates over Py4J loopback to orders.py on edge-01: ${opts.diagnostic}. The client Python driver process fails with exit code 1 (tracked separately from YARN AM status).`,
          size: 'exit code 1',
          command: `orders.py exits 1 // ${opts.diagnostic}`,
          fields: [
            { name: 'pythonProcessExit', value: '1 (FAILED on edge-01)' },
            { name: 'yarnAmDivergence', value: 'YARN AM status depends on exitCode passed to SparkContext.stop(code)' },
          ],
          senderState: 'Exception propagated to client Python driver',
          receiverState: 'orders.py exiting non-zero (exit 1) after teardown',
          isError: true,
        });
        b.addEvent({
          stepId,
          from: yarnBackend,
          to: state.activeAm,
          kind: link.kind,
          channel: 'rpc',
          protocol: link.protocol,
          port: link.port,
          label: 'Shutdown(exit=0) -> AM',
          info: `Default SparkContext.stop() delegates to stop(0) (SparkContext.scala:2607; PySpark stop() calls _jsc.stop() with no exitCode arg); YarnClientSchedulerBackend.stop() (YarnClientSchedulerBackend.scala:174-190) calls yarnSchedulerEndpoint.signalDriverStop(0) -> Shutdown(exitCode=0) over ${link.port} BEFORE calling super.stop() (StopExecutor). Explicit JVM sc.stop(1) would send Shutdown(1) instead.`,
          size: '640 B',
          command: 'yarnSchedulerEndpoint.signalDriverStop(0) -> Shutdown(exitCode=0) // before super.stop()',
          fields: [
            { name: 'exitCode', value: '0 (default SparkContext.stop() = stop(0))' },
            { name: 'order', value: 'YarnClientSchedulerBackend.scala:174-190 signals AM Shutdown(0) BEFORE super.stop() StopExecutor' },
            { name: 'channel', value: link.port },
            { name: 'explicitOverride', value: 'SparkContext.stop(1) -> Shutdown(1) -> YARN FAILED' },
          ],
          senderState: 'Client SparkContext.stop() sent Shutdown(0) before super.stop()',
          receiverState: 'AMEndpoint marking shutdown=true, exitCode=0',
        });
        emitStopLiveExecutors(true);
        b.addEvent({
          stepId,
          from: state.activeAm,
          to: state.rm,
          channel: 'yarn',
          protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
          port: 'rm01:8030',
          label: 'unregisterAM(SUCCEEDED)',
          info: `Client AM (${state.activeAm}) received Shutdown(0) and on driver disconnect (ApplicationMaster.scala:878-886) calls finish(FinalApplicationStatus.SUCCEEDED, EXIT_SUCCESS=0) on RM :8030. YARN UI therefore reports SUCCEEDED even though orders.py exited 1 (famous client-mode SparkContext.stop() default-0 gotcha; pass explicit JVM sc.stop(1) for YARN FAILED).`,
          size: '1.1 KB',
          command: 'amClient.unregister(FinalApplicationStatus.SUCCEEDED, "") // exitCode=0 from default sc.stop(); orders.py exited 1',
          fields: [
            { name: 'yarnFinalStatus', value: 'FinalApplicationStatus.SUCCEEDED (EXIT_SUCCESS=0)' },
            { name: 'clientDriverExit', value: '1 (orders.py FAILED)' },
            { name: 'whySucceeded', value: 'Default SparkContext.stop() calls stop(0); explicit JVM sc.stop(1) is required for YARN FAILED' },
          ],
          senderState: 'Client AM exiting SUCCEEDED (exitCode=0 from Shutdown(0))',
          receiverState: 'RM marks app FINISHED / SUCCEEDED despite orders.py exit 1',
        });
      } else {
        const jvmApp = b.std('jvm-user-app');
        b.addEvent({
          stepId,
          from: sc,
          to: jvmApp,
          kind: 'local',
          channel: 'error',
          protocol: 'In-JVM JavaMainApplication',
          port: 'edge-01 in-JVM',
          label: `${jvmShortName} exits 1`,
          info: `Uncaught exception escapes ${b.appEntryLabel}() on edge-01: ${opts.diagnostic}. JavaMainApplication invokes main and SparkSubmit unwraps & rethrows so the submit JVM fails with exit code 1 (NO automatic sc.stop(1); client AM NEVER selects exit 15).`,
          size: 'exit code 1',
          command: `JavaMainApplication.start() -> ${b.appEntryLabel}() throws -> SparkSubmit exits 1`,
          fields: [
            { name: 'submitJvmExit', value: '1 (JavaMainApplication failed on edge-01)' },
            { name: 'noAutoStop1', value: 'NO automatic sc.stop(1) and AM NEVER selects exit 15 in client mode' },
          ],
          senderState: `Exception escaped ${b.appEntryLabel}() on edge-01`,
          receiverState: 'SparkSubmit unwrapping & exiting 1; JVM shutdown hooks running',
          isError: true,
        });
        b.addEvent({
          stepId,
          from: yarnBackend,
          to: state.activeAm,
          kind: link.kind,
          channel: 'rpc',
          protocol: link.protocol,
          port: link.port,
          label: 'Shutdown(exit=0) -> AM',
          info: `On ordinary JVM shutdown, SparkContext's shutdown hook (SparkContext.scala:757-769) calls zero-arg stop() = stop(0) (SparkContext.scala:2607); YarnClientSchedulerBackend.stop() (YarnClientSchedulerBackend.scala:174-190) calls yarnSchedulerEndpoint.signalDriverStop(0) -> Shutdown(exitCode=0) over ${link.port} BEFORE calling super.stop() (StopExecutor). (Explicit sc.stop(1) sends Shutdown(1); abrupt disconnect uses clientModeTreatDisconnectAsFailed -> exit 17 if true.)`,
          size: '640 B',
          command: 'ShutdownHook (SparkContext.scala:757-769) -> stop() -> yarnSchedulerEndpoint.signalDriverStop(0) // Shutdown(exitCode=0) before super.stop()',
          fields: [
            { name: 'exitCode', value: '0 (SparkContext shutdown hook calls zero-arg stop())' },
            { name: 'order', value: 'YarnClientSchedulerBackend.scala:174-190 signals AM Shutdown(0) BEFORE super.stop() StopExecutor' },
            { name: 'channel', value: link.port },
            { name: 'alternatives', value: 'Explicit sc.stop(1) -> FAILED; clientModeTreatDisconnectAsFailed=true on crash -> exit 17' },
          ],
          senderState: `SC shutdown hook sent Shutdown(0) before super.stop()`,
          receiverState: 'AMEndpoint marking shutdown=true, exitCode=0',
        });
        emitStopLiveExecutors(true);
        b.addEvent({
          stepId,
          from: state.activeAm,
          to: state.rm,
          channel: 'yarn',
          protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
          port: 'rm01:8030',
          label: 'unregisterAM(SUCCEEDED)',
          info: `Client AM (${state.activeAm}) received Shutdown(0) from SparkContext's shutdown hook and on driver disconnect calls finish(FinalApplicationStatus.SUCCEEDED, EXIT_SUCCESS=0) on RM :8030 even though ${b.appEntryLabel} / JavaMainApplication exited 1 (client AM NEVER selects exit 15).`,
          size: '1.1 KB',
          command: `amClient.unregister(FinalApplicationStatus.SUCCEEDED, "") // Shutdown(0) from SC shutdown hook; ${jvmShortName} exited 1`,
          fields: [
            { name: 'yarnFinalStatus', value: 'FinalApplicationStatus.SUCCEEDED (EXIT_SUCCESS=0)' },
            { name: 'clientLauncherExit', value: `1 (${jvmShortName} / JavaMainApplication FAILED)` },
            { name: 'whySucceeded', value: 'SparkContext shutdown hook sent Shutdown(0) without explicit sc.stop(1)' },
          ],
          senderState: 'Client AM exiting SUCCEEDED (exitCode=0 from Shutdown(0))',
          receiverState: `RM marks app FINISHED / SUCCEEDED despite ${jvmShortName} exit 1`,
        });
      }
      return;
    } else if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      b.addEvent({
        stepId,
        from: pyDriver,
        to: state.activeAm,
        kind: 'local',
        channel: 'error',
        protocol: 'ProcessBuilder Child Exit',
        port: 'driver-1 OS exit',
        label: `UserApp exit=${exitCode} -> AM`,
        info: `orders.py exits with status ${exitCode}; deploy.PythonRunner throws SparkUserAppException(exitCode=${exitCode}) (NOT exit 15), and ApplicationMaster.startUserApplication calls finish(FAILED, ${exitCode}).`,
        size: `exit code ${exitCode}`,
        command: `finish(FinalApplicationStatus.FAILED, exitCode=${exitCode}, "${opts.diagnostic}")`,
        fields: [
          { name: 'exception', value: `SparkUserAppException(exitCode=${exitCode})` },
          { name: 'notExit15', value: 'Python subprocess status propagates directly (not EXIT_EXCEPTION_USER_CLASS=15)' },
        ],
        senderState: 'Python driver subprocess exited 1',
        receiverState: `AM finish(FAILED, ${exitCode})`,
        isError: true,
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      b.addEvent({
        stepId,
        from: jvmApp,
        to: state.activeAm,
        kind: 'local',
        channel: 'error',
        protocol: 'In-JVM InvocationTargetException',
        port: 'driver-1 in-JVM',
        label: 'Uncaught main() -> exit 15',
        info: `Uncaught exception escapes ${b.appEntryLabel}() inside driver-1 AM JVM; ApplicationMaster.startUserApplication (ApplicationMaster.scala:775-785, 911) catches InvocationTargetException (NOT SparkUserAppException) and calls finish(FinalApplicationStatus.FAILED, EXIT_EXCEPTION_USER_CLASS=15): ${opts.diagnostic}.`,
        size: 'exit code 15',
        command: `finish(FinalApplicationStatus.FAILED, EXIT_EXCEPTION_USER_CLASS=15, "${opts.diagnostic}")`,
        fields: [
          { name: 'exception', value: 'InvocationTargetException -> EXIT_EXCEPTION_USER_CLASS (15)' },
          { name: 'sourceRef', value: 'ApplicationMaster.scala:775-785, 911 (vs PySpark SparkUserAppException=1)' },
        ],
        senderState: `${b.appEntryLabel}() threw uncaught exception`,
        receiverState: 'AM finish(FAILED, EXIT_EXCEPTION_USER_CLASS=15)',
        isError: true,
      });
    }
  }

  if (isLastAttempt || !isCluster) {
    b.addEvent({
      stepId,
      from: state.activeAm,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: 'unregisterAM(FAILED)',
      info: `Registered AM (${state.activeAm}) on final attempt (isLastAttempt=true${isCluster ? ', spark.yarn.maxAppAttempts=1; retryable attempt 1 < maxAppAttempts would skip unregister(FAILED)' : ''}) unregisters FinalApplicationStatus.FAILED (exitCode=${exitCode}): ${opts.diagnostic}`,
      size: '1.1 KB',
      command: `amClient.unregister(FinalApplicationStatus.FAILED, "${opts.diagnostic}") // exitCode=${exitCode}`,
      fields: [
        { name: 'finalStatus', value: 'FAILED' },
        { name: 'exitCode', value: `${exitCode} (${exitLabel})` },
        {
          name: 'attemptDisposition',
          value: isCluster
            ? 'isLastAttempt=true (maxAppAttempts=1; non-final attempt omits unregister so RM retries)'
            : 'Client-mode AM finalization',
        },
      ],
      senderState: `AM exiting FAILED (exit=${exitCode})`,
      receiverState: 'RM marks application FINISHED / FAILED',
      isError: true,
    });
  }
}

export function replaceDeadExecutor(
  b: SimRunBuilder,
  state: SimRuntimeState,
  stepId: string,
  deadExecId: string,
  targetZone?: 'worker-node-a' | 'worker-node-b',
  replaceInPlace = false,
  emitBmAndHeartbeat = false,
): { replExec: string; replBm: string; replNum: number; replCid: string } {
  const deadZone = b.instanceWorkerHost(deadExecId).hostZone;
  const deadIdx = state.liveExecs.indexOf(deadExecId);
  if (deadIdx !== -1) {
    state.liveExecs.splice(deadIdx, 1);
    state.liveBms.splice(deadIdx, 1);
    state.liveExecNums.splice(deadIdx, 1);
  }
  const { execNum: replNum, containerId: replCid } = b.allocateExec(state.activeAttempt);
  state.nextExecNum = b.peekNextExecNum(state.activeAttempt);
  const replZone = targetZone ?? deadZone;
  const replHost = replZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
  const replNm = b.nmInstance(replZone);
  const replExec = b.execReplInstance(replNum, replZone, state.activeAttempt, replCid);
  const replBm = b.execReplBmInstance(replNum, replZone, state.activeAttempt, replCid);
  const replPort = b.execDataPort(replNum);

  b.addEvent({
    stepId,
    from: state.alloc,
    to: replNm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ContainerManagementProtocol)',
    port: `${replHost}:dynamic`,
    label: 'startContainer(Exec-repl)',
    info: `YarnAllocator launches replacement ${replCid} (Exec-${replNum}) on ${replHost} after ${deadExecId} loss.`,
    size: '9.4 KB',
    command: `nmClient.startContainer(${replCid}, ctx[exec=${replNum}])`,
    fields: [
      { name: 'replacementExecutor', value: `${replExec} (id=${replNum})` },
      { name: 'containerId', value: replCid },
      { name: 'host', value: replHost },
    ],
    senderState: 'Dispatched replacement executor container',
    receiverState: `Booting replacement executor JVM on ${replHost}`,
  });
  b.addEvent({
    stepId,
    from: replExec,
    to: state.schedBackend,
    channel: 'rpc',
    protocol: 'Spark Netty RPC',
    port: b.driverRpc,
    label: 'RegisterExecutor(repl)',
    info: `Replacement executor (${replExec}, id=${replNum}, ${replCid}) on ${replHost} registers ${state.cfg.executorCores} core(s) and BM ${replHost}:${replPort} to finish retried tasks.`,
    size: '1.8 KB',
    command: `ref.ask[Boolean](RegisterExecutor("${replNum}", self, "${replHost}", ${state.cfg.executorCores}))`,
    fields: [
      { name: 'executorId', value: `"${replNum}"` },
      { name: 'containerId', value: replCid },
      { name: 'cores', value: String(state.cfg.executorCores) },
      { name: 'blockMgrPort', value: `${replHost}:${replPort}` },
    ],
    senderState: `${replExec} ready for rescheduled tasks`,
    receiverState: `Registered replacement ${replExec} (${state.liveExecs.length + 1} live)`,
  });

  if (emitBmAndHeartbeat) {
    b.addEvent({
      stepId,
      from: replBm,
      to: state.bmm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: `RegisterBM(Exec-${replNum})`,
      info: `Replacement ${replBm} (${replCid}) creates local dirs cleanly on ${replHost}, binds ${replHost}:${replPort}, and registers with BlockManagerMaster.`,
      size: '1.2 KB',
      command: `master.registerBlockManager(BlockManagerId("${replNum}", "${replHost}", ${replPort}), ...)`,
      fields: [
        { name: 'executorId', value: `"${replNum}"` },
        { name: 'dataPort', value: String(replPort) },
      ],
      senderState: `Listening on ${replHost}:${replPort}`,
      receiverState: `BlockManager directory recorded replacement Exec-${replNum}`,
    });
    b.addEvent({
      stepId,
      from: replExec,
      to: state.hbRx,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (Heartbeat)',
      port: b.driverRpc,
      label: `Heartbeat(exec=${replNum}, 10s)`,
      info: `Replacement Exec-${replNum} starts 10s heartbeats to HeartbeatReceiver.`,
      size: '780 B',
      command: `heartbeatReceiverRef.askSync[HeartbeatResponse](Heartbeat("${replNum}", accumUpdates, bmId))`,
      fields: [{ name: 'interval', value: '10s' }],
      senderState: 'Heartbeat timer active on replacement executor',
      receiverState: `Recorded executorLastSeen("${replNum}")`,
    });
  }

  if (replaceInPlace && deadIdx !== -1) {
    state.liveExecs.splice(deadIdx, 0, replExec);
    state.liveBms.splice(deadIdx, 0, replBm);
    state.liveExecNums.splice(deadIdx, 0, replNum);
  } else {
    state.liveExecs.push(replExec);
    state.liveBms.push(replBm);
    state.liveExecNums.push(replNum);
  }
  return { replExec, replBm, replNum, replCid };
}

function retireHostExecutors(
  b: SimRunBuilder,
  state: SimRuntimeState,
  hostZone: 'worker-node-a' | 'worker-node-b',
): string[] {
  const retired: string[] = [];
  const survivingExecs: string[] = [];
  const survivingBms: string[] = [];
  const survivingNums: number[] = [];
  for (let i = 0; i < state.liveExecs.length; i++) {
    if (b.instanceWorkerHost(state.liveExecs[i]).hostZone === hostZone) {
      retired.push(state.liveExecs[i]);
    } else {
      survivingExecs.push(state.liveExecs[i]);
      survivingBms.push(state.liveBms[i]);
      survivingNums.push(state.liveExecNums[i]);
    }
  }
  state.liveExecs.splice(0, state.liveExecs.length, ...survivingExecs);
  state.liveBms.splice(0, state.liveBms.length, ...survivingBms);
  state.liveExecNums.splice(0, state.liveExecNums.length, ...survivingNums);
  return retired;
}

// =============================================================================
// PHASE 1: Pre-Submit / Client Validation Faults (before RM submitApplication)
// =============================================================================
export function tryInjectPreSubmitFault(
  b: SimRunBuilder,
  cfg: SimulationConfig,
  meta: SimRunMeta,
): ExecutionRun | null {
  const submit = b.std('spark-submit');

  if (cfg.deployMode === 'client' && cfg.fault === 'class-not-found-jar') {
    const jvmApp = b.std('jvm-user-app');
    const missingClass = b.mainClass;
    const s1 = b.addStep({
      id: 'sim-step-cnf-client-parse',
      title: `Client spark-submit prepares classpath & invokes Utils.classForName(${missingClass})`,
      explanation:
        `In --deploy-mode client, SparkSubmit.runMain (SparkSubmit.scala:992-1025) builds MutableURLClassLoader (or ChildFirstURLClassLoader when spark.driver.userClassPathFirst=true; note userClassPathFirst changes lookup order for version conflicts only and CANNOT supply a missing class) over ${b.appArtifact} and calls Utils.classForName("${missingClass}") locally on edge-01 before any SparkContext or YARN AM exists.`,
      category: 'yarn-lifecycle',
      keyState: `Client SparkSubmit looking up ${missingClass} locally on edge-01 (0 YARN contact)`,
      command: `spark-submit --master yarn --deploy-mode client --class ${missingClass} ${b.appArtifact}`,
      detailsPassed: [
        { name: 'deployMode', value: 'client (lookup occurs locally on edge-01 before YARN submission)' },
        { name: 'missingClass', value: missingClass },
        { name: 'userClassPathFirstNote', value: 'spark.driver.userClassPathFirst changes lookup order only; cannot supply a missing class' },
        { name: 'sourceRef', value: 'SparkSubmit.scala:992-1025; SparkExitCode.scala:44' },
      ],
      deepDiveComponentId: 'spark-submit',
    });
    b.addEvent({
      stepId: s1,
      from: submit,
      to: submit,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM SparkSubmit Classpath',
      port: 'edge-01 in-JVM',
      label: 'Parse CLI & classpath',
      info: `SparkSubmit.prepareSubmitEnvironment resolves ${b.appArtifact} & --jars onto edge-01 URLClassLoader (spark.driver.userClassPathFirst changes lookup order only, cannot supply a missing class).`,
      size: 'in-memory',
      command: `SparkSubmit.prepareSubmitEnvironment(--class ${missingClass} ${b.appArtifact})`,
      fields: [
        { name: 'artifact', value: b.appArtifact },
        { name: 'yarnContacted', value: 'false' },
      ],
      senderState: 'Prepared client classpath on edge-01',
      receiverState: 'Invoking Utils.classForName(childMainClass)',
    });
    b.addEvent({
      stepId: s1,
      from: submit,
      to: jvmApp,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM ClassLoader Lookup',
      port: 'edge-01 in-JVM',
      label: 'classForName(mainClass)',
      info: `SparkSubmit.runMain (SparkSubmit.scala:992-1025) calls Utils.classForName("${missingClass}") on edge-01 before any SparkContext or YARN AM exists.`,
      size: 'in-memory',
      command: `Utils.classForName("${missingClass}") // SparkSubmit.scala:992-1025`,
      fields: [{ name: 'targetClass', value: missingClass }],
      senderState: 'Resolving user main class',
      receiverState: 'Class absent from local submit classpath',
    });

    const s2 = b.addStep({
      id: 'sim-step-cnf-client-fail',
      title: 'Utils.classForName fails -> SparkUserAppException(CLASS_NOT_FOUND = 101, 0 AM/RM)',
      explanation:
        `Utils.classForName("${missingClass}") throws ClassNotFoundException / NoClassDefFoundError. SparkSubmit.runMain (SparkSubmit.scala:992-1025) wraps it into SparkUserAppException(SparkExitCode.CLASS_NOT_FOUND = 101) and exits the submit JVM with code 101 on edge-01 BEFORE any SparkContext or YARN ApplicationMaster is created.`,
      category: 'yarn-lifecycle',
      keyState: 'FAILED pre-submit on edge-01: SparkUserAppException(SparkExitCode.CLASS_NOT_FOUND = 101) | 0 AM/RM events',
      command: 'throw new SparkUserAppException(SparkExitCode.CLASS_NOT_FOUND) // exitCode = 101',
      detailsPassed: [
        { name: 'exitCode', value: '101 (SparkExitCode.CLASS_NOT_FOUND)' },
        { name: 'exception', value: 'SparkUserAppException(101) wrapping ClassNotFoundException / NoClassDefFoundError' },
        { name: 'yarnRmContacted', value: 'false (0 RM/AM RPCs, 0 SparkContext)' },
      ],
      deepDiveComponentId: 'spark-submit',
    });
    b.addEvent({
      stepId: s2,
      from: jvmApp,
      to: submit,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ClassNotFoundException',
      port: 'edge-01 in-JVM',
      label: 'ClassNotFound -> exit 101',
      info: `ClassNotFoundException / NoClassDefFoundError for ${missingClass} caught by SparkSubmit.runMain and wrapped as SparkUserAppException(SparkExitCode.CLASS_NOT_FOUND = 101).`,
      size: 'Exit 101',
      command: `catch ClassNotFoundException -> throw new SparkUserAppException(101)`,
      fields: [
        { name: 'exitCode', value: '101 (SparkExitCode.CLASS_NOT_FOUND)' },
        { name: 'sparkContextCreated', value: 'false' },
      ],
      senderState: 'Class lookup failed on edge-01',
      receiverState: 'Wrapping in SparkUserAppException(101)',
      isError: true,
    });
    b.addEvent({
      stepId: s2,
      from: submit,
      to: submit,
      kind: 'local',
      channel: 'error',
      protocol: 'Submit JVM Exit (101)',
      port: 'edge-01 in-JVM',
      label: 'Launcher exits 101',
      info: 'spark-submit exits 101 (SparkExitCode.CLASS_NOT_FOUND) on edge-01 with ZERO SparkContext and ZERO YARN RM/AM contact (cluster mode instead fails in AM loadClass with unregistered exit 13).',
      size: 'Exit 101',
      command: 'exitFn(101) // SparkExitCode.CLASS_NOT_FOUND',
      fields: [
        { name: 'exitCode', value: '101' },
        { name: 'yarnContainers', value: '0' },
      ],
      senderState: 'Aborted on edge-01 with exit 101',
      receiverState: 'Shell receives exit status 101',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'CLASS NOT FOUND (101)');
  }

  if (cfg.fault === 'oversized-request') {
    const yClient = b.std('yarn-client');
    const rm = b.std('resource-manager');
    const s1 = b.addStep({
      id: 'sim-s1-oversize',
      title: 'CLI invokes yarn.Client & queries RM maximum container capability',
      explanation:
        'spark-submit launches yarn.Client, which calls yarnClient.createApplication() on RM :8032 and receives yarn.scheduler.maximum-allocation-mb = 8192 MiB.',
      category: 'yarn-lifecycle',
      keyState: 'RM max container memory = 8192 MiB; app ID reserved but not submitted',
      command: b.submitCommand(' --executor-memory 8g'),
      detailsPassed: [
        { name: 'executorMemory', value: '8192 MiB (--executor-memory 8g)' },
        { name: 'executorOverhead', value: '819 MiB (10% overhead)' },
        { name: 'rmMaxAllocationMb', value: '8192 MiB (yarn.scheduler.maximum-allocation-mb)' },
      ],
      deepDiveComponentId: 'yarn-client',
    });
    b.addEvent({
      stepId: s1,
      from: submit,
      to: yClient,
      channel: 'internal',
      protocol: 'In-JVM SparkSubmit',
      port: 'edge-01 in-JVM',
      label: 'Invoke yarn.Client',
      info: 'spark-submit parses --executor-memory 8g and invokes yarn.Client.',
      size: 'in-memory',
      command: 'new Client(args, sparkConf).run()',
      fields: [
        { name: 'spark.executor.memory', value: '8g' },
        { name: 'spark.executor.instances', value: String(cfg.numExecutors) },
      ],
      senderState: 'Parsed CLI flags',
      receiverState: 'Starting YarnClient RPC to RM :8032',
    });
    b.addEvent({
      stepId: s1,
      from: yClient,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationClientProtocol)',
      port: 'rm01:8032',
      label: 'createApplication()',
      info: 'Client reserves an application ID and queries RM maximumResourceCapability.',
      size: '1.1 KB',
      command: 'val newApp = yarnClient.createApplication()',
      fields: [
        { name: 'applicationId', value: 'application_1740_0099' },
        { name: 'requestedMb', value: '9011 (8192 + 819 overhead)' },
      ],
      senderState: 'Awaiting GetNewApplicationResponse',
      receiverState: 'Computing cluster max capability (8192 MiB)',
    });
    b.addEvent({
      stepId: s1,
      from: rm,
      to: yClient,
      channel: 'yarn',
      protocol: 'Hadoop RPC (GetNewApplicationResponse)',
      port: 'rm01:8032',
      label: 'maxMem = 8192 MiB',
      info: 'RM returns GetNewApplicationResponse advertising maximum-allocation-mb = 8192 MiB.',
      size: '960 B',
      command: 'GetNewApplicationResponse(appId=application_1740_0099, maxMem=8192)',
      fields: [
        { name: 'maxMemoryMb', value: '8192' },
        { name: 'submitted', value: 'false' },
      ],
      senderState: 'Returned cluster limits',
      receiverState: 'Running Client.verifyClusterResources()',
    });

    const s2 = b.addStep({
      id: 'sim-s2-oversize-fail',
      title: 'Client.verifyClusterResources throws BEFORE submitApplication()',
      explanation:
        'Client.verifyClusterResources checks container memory (executor heap 8192 MiB + off-heap + overhead 819 MiB + PySpark memory = 9011 MiB > yarn.scheduler.maximum-allocation-mb 8192 MiB; vcores are not checked here) and throws IllegalArgumentException locally on edge-01. No AM or executor container is ever requested.',
      category: 'yarn-lifecycle',
      keyState: 'FAILED pre-submit on edge-01 | 0 YARN containers allocated',
      command: 'Client.verifyClusterResources(newAppResponse) // throws IllegalArgumentException',
      detailsPassed: [
        { name: 'totalRequestedMb', value: '9011 MiB (heap + off-heap + overhead + PySpark memory)' },
        { name: 'clusterMaxMb', value: '8192 MiB (yarn.scheduler.maximum-allocation-mb; memory check only, not vcores)' },
        { name: 'submitCalled', value: 'false (failed fast on edge-01)' },
      ],
      deepDiveComponentId: 'yarn-client',
    });
    b.addEvent({
      stepId: s2,
      from: yClient,
      to: yClient,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Validation Throw',
      port: 'edge-01 in-JVM',
      label: 'Exceeds maxMem (9011>8192)',
      info: 'IllegalArgumentException: Required executor memory (heap + off-heap + overhead + PySpark memory = 9011 MB) is above cluster max threshold (8192 MB).',
      size: 'Exception',
      command: 'throw new IllegalArgumentException("...above the max threshold (8192 MB)")',
      fields: [
        { name: 'exception', value: 'IllegalArgumentException' },
        { name: 'containersLaunched', value: '0' },
      ],
      senderState: 'Aborted before submitApplication()',
      receiverState: 'spark-submit exits non-zero on edge-01',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'PRE-SUBMIT FAIL');
  }

  if (cfg.fault === 'am-invalid-java-opts') {
    const sc = b.std('spark-context');
    const yarnBackend = b.std('yarn-scheduler-backend');
    const yClient = b.std('yarn-client');
    const rm = b.std('resource-manager');
    const s1 = b.addStep({
      id: 'sim-s1-am-opts',
      title: 'Client driver gets appId from RM (:8032), then builds AM launch context',
      explanation:
        'Per Client.scala:207-234, in yarn-client mode SparkContext starts on edge-01, YarnClientSchedulerBackend invokes yarn.Client.submitApplication(), yarn.Client ALREADY contacts ResourceManager (:8032) via createApplication() / getNewApplication() to reserve an applicationId and runs verifyClusterResources(), and then calls createContainerLaunchContext().',
      category: 'yarn-lifecycle',
      keyState: 'appId reserved on RM :8032; entering Client.createContainerLaunchContext()',
      command: b.submitCommand(' --conf spark.yarn.am.extraJavaOptions="-Xmx2g -Dspark.foo=bar"'),
      detailsPassed: [
        { name: 'deployMode', value: 'client' },
        { name: 'rmContacted', value: 'Yes: getNewApplication on rm01:8032 -> appId reserved (no submitApplication yet)' },
        { name: 'badOption', value: 'spark.yarn.am.extraJavaOptions="-Xmx2g -Dspark.foo=bar"' },
      ],
      deepDiveComponentId: 'yarn-client',
    });
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      b.addEvent({
        stepId: s1,
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
        fields: [{ name: 'host', value: 'edge-01' }],
        senderState: 'Hosting client driver JVM',
        receiverState: 'Initializing SparkContext',
      });
      b.addEvent({
        stepId: s1,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback TCP',
        port: '127.0.0.1:44108',
        label: 'Init client SparkContext',
        info: 'orders.py creates SparkContext on edge-01 with spark.yarn.am.extraJavaOptions set.',
        size: '2.0 KB',
        command: 'SparkContext.getOrCreate(conf)',
        fields: [{ name: 'amExtraJavaOpts', value: '-Xmx2g -Dspark.foo=bar' }],
        senderState: 'Awaiting YARN scheduler backend',
        receiverState: 'Starting YarnClientSchedulerBackend',
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      b.addEvent({
        stepId: s1,
        from: submit,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM JavaMainApplication',
        port: 'edge-01 in-JVM',
        label: b.isScala ? 'JavaMainApp -> OrdersJob' : 'JavaMainApp -> JavaJob',
        info: `spark-submit invokes JavaMainApplication.start() directly in-JVM on edge-01 -> ${b.appMainClass}.main().`,
        size: 'in-memory',
        command: `JavaMainApplication.start() -> ${b.appMainClass}.main(args)`,
        fields: [{ name: 'host', value: 'edge-01' }],
        senderState: 'Hosting client driver JVM',
        receiverState: 'Initializing SparkContext',
      });
      b.addEvent({
        stepId: s1,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM SparkContext Init',
        port: 'edge-01 in-JVM',
        label: 'Init client SparkContext',
        info: `${b.appEntryLabel}() creates SparkContext on edge-01 with spark.yarn.am.extraJavaOptions set.`,
        size: 'in-memory',
        command: 'SparkContext.getOrCreate(conf)',
        fields: [{ name: 'amExtraJavaOpts', value: '-Xmx2g -Dspark.foo=bar' }],
        senderState: 'Awaiting YARN scheduler backend',
        receiverState: 'Starting YarnClientSchedulerBackend',
      });
    }
    b.addEvent({
      stepId: s1,
      from: yarnBackend,
      to: yClient,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM yarn.Client',
      port: 'edge-01 in-JVM',
      label: 'Invoke Client.submitApp',
      info: 'YarnClientSchedulerBackend invokes Client.submitApplication() for ExecutorLauncher.',
      size: 'in-memory',
      command: 'new Client(args, sparkConf).submitApplication() // Client.scala:207-234',
      fields: [{ name: 'sourceRef', value: 'Client.scala:207-234,1094-1105' }],
      senderState: 'Waiting for AM submission',
      receiverState: 'Calling yarnClient.createApplication() on RM :8032',
    });
    b.addEvent({
      stepId: s1,
      from: yClient,
      to: rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationClientProtocol)',
      port: 'rm01:8032',
      label: 'getNewApplication()',
      info: 'yarn.Client contacts RM :8032 via createApplication() / getNewApplication() to obtain application_1740_0099 and max resource capabilities before building the AM ContainerLaunchContext.',
      size: '1.1 KB',
      command: 'val newApp = yarnClient.createApplication() // Client.scala:207',
      fields: [
        { name: 'applicationId', value: 'application_1740_0099' },
        { name: 'submitApplicationCalled', value: 'false (not called yet)' },
      ],
      senderState: 'Received appId; passing verifyClusterResources',
      receiverState: 'Assigned application_1740_0099 (not yet submitted)',
    });

    const s2 = b.addStep({
      id: 'sim-s2-am-opts-fail',
      title: 'createContainerLaunchContext rejects -Xmx / -Dspark BEFORE submitApplication()',
      explanation:
        'After getNewApplication and verifyClusterResources succeed, Client.createContainerLaunchContext (Client.scala:228, 1094-1105) validates spark.yarn.am.extraJavaOptions (AM_JAVA_OPTIONS) in client mode and throws SparkException when it contains -Dspark.* or -Xmx (use spark.yarn.am.memory instead). yarnClient.submitApplication() is NEVER called, and the catch block cleans up any staging directory.',
      category: 'yarn-lifecycle',
      keyState: 'FAILED after getNewApplication, BEFORE submitApplication(): SparkException (Client.scala:1094-1105)',
      command: 'throw new SparkException("spark.yarn.am.extraJavaOptions is not allowed to set Spark options / -Xmx")',
      detailsPassed: [
        { name: 'exception', value: 'SparkException (AM_JAVA_OPTIONS validation in createContainerLaunchContext)' },
        { name: 'remediation', value: 'Set spark.yarn.am.memory instead of -Xmx; pass Spark props directly' },
        { name: 'rmSubmitCalled', value: 'false (0 AM/executor containers launched)' },
      ],
      deepDiveComponentId: 'yarn-client',
    });
    b.addEvent({
      stepId: s2,
      from: yClient,
      to: yClient,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM AM_JAVA_OPTIONS Check',
      port: 'edge-01 in-JVM',
      label: 'Reject -Xmx / -Dspark',
      info: 'Client.createContainerLaunchContext rejects spark.yarn.am.extraJavaOptions containing -Dspark.* or -Xmx with SparkException before yarnClient.submitApplication().',
      size: 'SparkException',
      command: 'Client.createContainerLaunchContext() // Client.scala:1094-1105 throws SparkException',
      fields: [
        { name: 'disallowed', value: '-Xmx2g, -Dspark.foo=bar' },
        { name: 'submitApplicationCalled', value: 'false (0 containers launched)' },
      ],
      senderState: 'Cleaned staging dir; aborted before submitApplication()',
      receiverState: 'Throwing SparkException to YarnClientSchedulerBackend',
      isError: true,
    });
    b.addEvent({
      stepId: s2,
      from: yClient,
      to: yarnBackend,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Exception Propagate',
      port: 'edge-01 in-JVM',
      label: 'Abort SC init (FAILED)',
      info: 'YarnClientSchedulerBackend aborts SparkContext startup on edge-01 after getNewApplication failed in createContainerLaunchContext (zero submitApplication RPC sent).',
      size: 'SparkException',
      command: 'sc.stop() // failed before yarnClient.submitApplication()',
      fields: [{ name: 'finalStatus', value: 'FAILED (pre-submitApplication)' }],
      senderState: 'Client failed before submitting AM',
      receiverState: `${b.appEntryLabel} exits non-zero with SparkException`,
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'INVALID AM OPTS');
  }

  // Client-mode SparkUI bind failure exits directly on edge-01 via System.exit(1) before YARN submission.
  if (cfg.deployMode === 'client' && cfg.fault === 'port-bind-exhausted') {
    return emitPortBindExhaustedClient(b, meta);
  }

  return null;
}

function emitPortBindExhaustedClient(b: SimRunBuilder, meta: SimRunMeta): ExecutionRun {
  const submit = b.std('spark-submit');
  const sc = b.std('spark-context');
  const sparkEnv = b.std('spark-env');
  const sparkUi = b.std('spark-ui');

  const sBoot = b.addStep({
    id: 'sim-step-port-bind-boot',
    title: 'Client driver starts on edge-01 & initializes SparkContext / SparkEnv',
    explanation:
      `In client mode, spark-submit launches ${b.appEntryLabel} locally on edge-01 and begins creating SparkContext and SparkEnv before contacting YARN.`,
    category: 'driver-python-bridge',
    keyState: 'Initializing SparkContext & SparkEnv on edge-01',
    command: b.submitCommand(),
    detailsPassed: [
      { name: 'host', value: 'edge-01 (client driver)' },
      { name: 'spark.ui.port', value: '4040' },
    ],
    deepDiveComponentId: 'spark-context',
  });
  if (b.isPySpark) {
    const pyDriver = b.std('python-driver');
    b.addEvent({
      stepId: sBoot,
      from: submit,
      to: pyDriver,
      kind: 'local',
      channel: 'python',
      protocol: 'ProcessBuilder (edge-01)',
      port: 'edge-01 OS',
      label: 'Run orders.py locally',
      info: 'spark-submit starts local Python driver on edge-01.',
      size: 'OS fork/exec',
      command: 'PythonRunner.main(Array("orders.py", ""))',
      fields: [{ name: 'host', value: 'edge-01' }],
      senderState: 'Hosting client driver JVM',
      receiverState: 'Creating SparkContext',
    });
    b.addEvent({
      stepId: sBoot,
      from: pyDriver,
      to: sc,
      channel: 'python',
      protocol: 'Py4J Loopback TCP',
      port: '127.0.0.1:44108',
      label: 'SparkContext.getOrCreate',
      info: 'orders.py initializes SparkContext over Py4J on edge-01.',
      size: '2.0 KB',
      command: 'SparkContext.getOrCreate(conf)',
      fields: [{ name: 'spark.ui.port', value: '4040' }],
      senderState: 'Awaiting SparkContext',
      receiverState: 'Creating SparkEnv and binding SparkUI',
    });
  } else {
    const jvmApp = b.std('jvm-user-app');
    b.addEvent({
      stepId: sBoot,
      from: submit,
      to: jvmApp,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM JavaMainApplication',
      port: 'edge-01 in-JVM',
      label: b.isScala ? 'JavaMainApp -> OrdersJob' : 'JavaMainApp -> JavaJob',
      info: `spark-submit invokes JavaMainApplication.start() directly in-JVM on edge-01 -> ${b.appMainClass}.main().`,
      size: 'in-memory',
      command: `JavaMainApplication.start() -> ${b.appMainClass}.main(args)`,
      fields: [{ name: 'host', value: 'edge-01' }],
      senderState: 'Hosting client driver JVM',
      receiverState: 'Creating SparkContext',
    });
    b.addEvent({
      stepId: sBoot,
      from: jvmApp,
      to: sc,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM SparkContext Init',
      port: 'edge-01 in-JVM',
      label: 'SparkContext.getOrCreate',
      info: `${b.appEntryLabel}() initializes SparkContext directly in-JVM on edge-01.`,
      size: 'in-memory',
      command: 'SparkContext.getOrCreate(conf)',
      fields: [{ name: 'spark.ui.port', value: '4040' }],
      senderState: 'Awaiting SparkContext',
      receiverState: 'Creating SparkEnv and binding SparkUI',
    });
  }
  b.addEvent({
    stepId: sBoot,
    from: sc,
    to: sparkEnv,
    kind: 'local',
    channel: 'internal',
    protocol: 'In-JVM SparkEnv Init',
    port: 'edge-01 in-JVM',
    label: 'createDriverEnv()',
    info: 'SparkEnv attempts service port binding via Utils.startServiceOnPort (Utils.scala:2283-2358).',
    size: 'in-memory',
    command: 'SparkEnv.createDriverEnv(conf, isLocal=false, listenerBus)',
    fields: [{ name: 'spark.port.maxRetries', value: '16' }],
    senderState: 'Initializing SparkUI server',
    receiverState: 'Trying ports 4040..4056',
  });

  const sPort = b.addStep({
    id: 'sim-step-port-bind',
    title: 'Client driver SparkUI exhausts spark.port.maxRetries (16) -> BindException',
    explanation:
      'On edge-01, Utils.startServiceOnPort("SparkUI", 4040, maxRetries=16) finds all 17 ports (4040..4056) occupied by sibling jobs and throws java.net.BindException BEFORE YarnClient submits any application to RM.',
    category: 'driver-python-bridge',
    keyState: 'FAILED on edge-01: BindException after 16 retries (ports 4040..4056 busy)',
    command: 'Utils.startServiceOnPort("SparkUI", 4040, maxRetries=16) // throws BindException',
    detailsPassed: [
      { name: 'spark.port.maxRetries', value: '16 (ports 4040..4056 all in use)' },
      { name: 'exception', value: 'java.net.BindException: Service SparkUI failed after 16 retries' },
      { name: 'containersLaunched', value: '0 (aborted before YARN submission)' },
    ],
    deepDiveComponentId: 'spark-ui',
  });
  b.addEvent({
    stepId: sPort,
    from: sc,
    to: sparkUi,
    kind: 'local',
    channel: 'error',
    protocol: 'Jetty Bind -> BindException',
    port: 'edge-01 in-JVM',
    label: 'BindException -> exit(1)',
    info: 'All 17 candidate ports (4040..4056) are occupied; Utils.startServiceOnPort throws BindException and WebUI.bind() catches it and calls System.exit(1) directly (NOT exit 15).',
    size: 'BindException',
    command: 'WebUI.bind() -> catch BindException -> System.exit(1)',
    fields: [
      { name: 'portsTried', value: '4040..4056 (17 attempts)' },
      { name: 'driverExit', value: 'System.exit(1) directly in WebUI.bind() (WebUI.scala:170)' },
      { name: 'remediation', value: 'Raise spark.port.maxRetries or set spark.ui.port=0' },
    ],
    senderState: 'SparkUI bind failed after 16 retries',
    receiverState: 'WebUI.bind() calling System.exit(1) directly',
    isError: true,
  });
  if (b.isPySpark) {
    const pyDriver = b.std('python-driver');
    b.addEvent({
      stepId: sPort,
      from: sc,
      to: pyDriver,
      channel: 'error',
      protocol: 'Py4J Loopback EOF (System.exit(1))',
      port: '127.0.0.1:43122',
      label: 'Driver JVM System.exit(1)',
      info: 'Driver JVM exits directly with System.exit(1) on edge-01 before any YARN application is submitted.',
      size: 'Exit 1',
      command: 'System.exit(1) // driver JVM exits 1 (not 15)',
      fields: [{ name: 'finalStatus', value: 'FAILED (System.exit(1); 0 YARN containers)' }],
      senderState: 'Driver JVM exited with status 1',
      receiverState: 'orders.py Py4J connection closed (exit 1)',
      isError: true,
    });
  } else {
    const jvmApp = b.std('jvm-user-app');
    b.addEvent({
      stepId: sPort,
      from: sc,
      to: jvmApp,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM System.exit(1)',
      port: 'edge-01 in-JVM',
      label: 'Driver JVM System.exit(1)',
      info: `Driver JVM exits directly with System.exit(1) inside ${b.appEntryLabel} on edge-01 before any YARN application is submitted (NOT exit 15).`,
      size: 'Exit 1',
      command: 'System.exit(1) // driver JVM exits 1 (not 15)',
      fields: [{ name: 'finalStatus', value: 'FAILED (System.exit(1); 0 YARN containers)' }],
      senderState: 'Driver JVM exited with status 1',
      receiverState: `${b.appEntryLabel} terminated with exit 1`,
      isError: true,
    });
  }
  return finalizeShortCircuit(b, meta, 'PORT BIND FAIL');
}

// =============================================================================
// PHASE 3: Client-Mode Post-AM-Submit Startup Faults (dynalloc-no-shuffle-service)
// =============================================================================
export function tryInjectClientDynAllocNoEssFault(
  b: SimRunBuilder,
  cfg: SimulationConfig,
  am: string,
  meta: SimRunMeta,
): ExecutionRun | null {
  if (cfg.deployMode !== 'client' || cfg.fault !== 'dynalloc-no-shuffle-service') {
    return null;
  }

  const sc = b.std('spark-context');
  const yarnBackend = b.std('yarn-scheduler-backend');
  const rm = b.std('resource-manager');
  const eam = b.std('executor-allocation-manager');
  const link = b.driverToAmLink();
  const jvmShortName = b.isScala ? 'OrdersJob' : 'OrdersJavaJob';
  const driverExitLabel = b.isPySpark ? 'orders.py exits 1' : `${jvmShortName} exits 1`;

  const sAmRun = b.addStep({
    id: 'sim-step-dynalloc-am-running',
    title: 'Client AM registers with RM (RUNNING) before SparkContext validates dynamic allocation',
    explanation:
      'In client mode (SparkContext.scala:678, 722-736), _taskScheduler.start() / YarnClientSchedulerBackend submits ExecutorLauncher AM and waits for the AM to register with RM (driver-1:-1) and transition to RUNNING BEFORE SparkContext constructs ExecutorAllocationManager.',
    category: 'yarn-lifecycle',
    keyState: 'Client AM registered (RUNNING) on RM :8030; SparkContext next creates ExecutorAllocationManager',
    command: 'YarnClientSchedulerBackend.start() -> amClient.register("driver-1", -1) -> waitForApplication() == RUNNING',
    detailsPassed: [
      { name: 'amRegistered', value: 'true (RM state = RUNNING)' },
      { name: 'sparkContextOrder', value: '_taskScheduler.start() completes BEFORE ExecutorAllocationManager init' },
    ],
    deepDiveComponentId: 'yarn-scheduler-backend',
  });
  b.addEvent({
    stepId: sAmRun,
    from: am,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: 'registerAM(port=-1)',
    info: 'Client-mode ExecutorLauncher AM registers driver-1:-1 with RM :8030; RM marks application RUNNING.',
    size: '1.6 KB',
    command: `amClient.register("driver-1", -1, conf, "${b.driverUiUrl}")`,
    fields: [
      { name: 'amRpcAddress', value: 'driver-1:-1' },
      { name: 'yarnState', value: 'RUNNING' },
    ],
    senderState: 'AM registered with RM',
    receiverState: 'RM marks app RUNNING',
  });
  b.addEvent({
    stepId: sAmRun,
    from: am,
    to: yarnBackend,
    channel: 'rpc',
    protocol: 'Spark Netty RPC',
    port: b.driverRpc,
    label: 'RegisterClusterManager',
    info: 'AMEndpoint connects outbound to edge-01:41000 and registers with YarnClientSchedulerBackend; waitForApplication() returns.',
    size: '1.4 KB',
    command: 'driver.send(RegisterClusterManager(self))',
    fields: [{ name: 'driver', value: 'edge-01:41000' }],
    senderState: 'Linked to client driver',
    receiverState: '_taskScheduler.start() completed; proceeding to ExecutorAllocationManager',
  });

  const sGuard = b.addStep({
    id: 'sim-step-dynalloc-guard',
    title: `validateSettings fails -> SparkContext.stop() sends Shutdown(0) (YARN SUCCEEDED) while ${b.appEntryLabel} exits 1`,
    explanation:
      `After AM reaches RUNNING, SparkContext constructs ExecutorAllocationManager. validateSettings() (ExecutorAllocationManager.scala:234-255) throws IllegalArgumentException / SparkException because ALL 4 shuffle-preservation prerequisites are off: (1) spark.shuffle.service.enabled=false, (2) spark.dynamicAllocation.shuffleTracking.enabled=false, (3) shuffle-block decommission fallback off (spark.decommission.enabled / spark.storage.decommission.shuffleBlocks.enabled), and (4) no reliable ShuffleDataIO storage (supportsReliableStorage()=false). In SparkContext.scala:785-795, the constructor catch block calls zero-arg stop() (def stop(): Unit = stop(0) at SparkContext.scala:2607, NOT stop(1)!), which sends Shutdown(exitCode=0) to the connected client AM so YARN UI records FinalApplicationStatus.SUCCEEDED even though finally { throw e } causes ${b.isPySpark ? 'orders.py to fail with Python exit code 1' : `JavaMainApplication / ${b.appEntryLabel} to fail with IllegalArgumentException`}.`,
    category: 'yarn-lifecycle',
    keyState: `${b.appEntryLabel} FAILED (exit=1) | YARN AM SUCCEEDED (SparkContext init catch calls stop() -> Shutdown(0))`,
    command: `validateSettings() -> SparkException -> SparkContext.stop() -> Shutdown(0) -> unregisterAM(SUCCEEDED) | ${driverExitLabel}`,
    detailsPassed: [
      { name: 'prereq1_ESS', value: 'spark.shuffle.service.enabled = false' },
      { name: 'prereq2_tracking', value: 'spark.dynamicAllocation.shuffleTracking.enabled = false' },
      { name: 'prereq3_decommission', value: 'spark.decommission.enabled / spark.storage.decommission.shuffleBlocks.enabled = false' },
      { name: 'prereq4_shuffleDataIO', value: 'ShuffleDataIO.driver().supportsReliableStorage() = false' },
      { name: 'yarnVsClientOutcome', value: `SparkContext.scala:789 calls stop() = stop(0) -> Shutdown(0) -> YARN SUCCEEDED; ${driverExitLabel}` },
    ],
    deepDiveComponentId: 'executor-allocation-manager',
  });
  b.addEvent({
    stepId: sGuard,
    from: sc,
    to: eam,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM validateSettings',
    port: 'edge-01 in-JVM',
    label: 'validateSettings() FAIL',
    info: 'ExecutorAllocationManager.validateSettings() throws IllegalArgumentException / SparkException: all 4 prerequisites off (spark.shuffle.service.enabled=false, spark.dynamicAllocation.shuffleTracking.enabled=false, decommission shuffle-block fallback off, and no reliable ShuffleDataIO).',
    size: 'SparkException',
    command: 'throw new SparkException("Dynamic allocation of executors requires one of the following conditions: 1) ESS 2) shuffleTracking 3) shuffleBlocks decommission 4) reliable ShuffleDataIO")',
    fields: [
      { name: 'sourceRef', value: 'ExecutorAllocationManager.scala:234-255' },
      { name: 'missingPrerequisites', value: 'ESS, shuffleTracking, decommission shuffleBlocks, reliable ShuffleDataIO' },
    ],
    senderState: 'SparkContext.scala:789 catch calling stop() (default exitCode=0)',
    receiverState: 'Rejected dynamic allocation without any shuffle-preservation option',
    isError: true,
  });
  b.addEvent({
    stepId: sGuard,
    from: yarnBackend,
    to: am,
    kind: link.kind,
    channel: 'rpc',
    protocol: link.protocol,
    port: link.port,
    label: 'Shutdown(exit=0) -> AM',
    info: 'SparkContext.scala:789 init catch calls zero-arg stop() (def stop(): Unit = stop(0) at SparkContext.scala:2607, NOT stop(1)!), so YarnClientSchedulerBackend sends Shutdown(exitCode=0) over edge-01:41000 to the registered client AM.',
    size: '640 B',
    command: 'amEndpoint.send(Shutdown(exitCode=0)) // SparkContext.scala:789 catch calls stop() = stop(0)',
    fields: [
      { name: 'channel', value: link.port },
      { name: 'exitCodeSent', value: '0 (SparkContext.stop() default, NOT stop(1))' },
    ],
    senderState: 'SparkContext init catch sent Shutdown(0) before rethrowing exception',
    receiverState: 'AMEndpoint marking shutdown=true, exitCode=0',
  });
  b.addEvent({
    stepId: sGuard,
    from: am,
    to: rm,
    channel: 'yarn',
    protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
    port: 'rm01:8030',
    label: 'unregisterAM(SUCCEEDED)',
    info: `Because Shutdown(0) set exitCode=0, client AM on disconnect (ApplicationMaster.scala:878-886) calls finish(FinalApplicationStatus.SUCCEEDED, EXIT_SUCCESS=0) on RM :8030! YARN UI displays SUCCEEDED even though ${b.appEntryLabel} fails with exit code 1.`,
    size: '1.1 KB',
    command: 'amClient.unregister(FinalApplicationStatus.SUCCEEDED, "") // exitCode=0 from SparkContext init catch stop()',
    fields: [
      { name: 'amRegistered', value: 'true (registered prior to validateSettings)' },
      { name: 'yarnFinalStatus', value: 'FinalApplicationStatus.SUCCEEDED (EXIT_SUCCESS=0)' },
      { name: 'clientDriverExit', value: `1 (${b.appEntryLabel} fails despite YARN SUCCEEDED)` },
    ],
    senderState: 'Client AM exiting SUCCEEDED (exitCode=0)',
    receiverState: 'RM marks application FINISHED / SUCCEEDED despite client init error',
  });
  if (b.isPySpark) {
    const pyDriver = b.std('python-driver');
    b.addEvent({
      stepId: sGuard,
      from: sc,
      to: pyDriver,
      channel: 'error',
      protocol: 'Py4J Loopback Exception',
      port: '127.0.0.1:43122',
      label: 'orders.py exits 1',
      info: 'SparkContext.scala:793 finally { throw e } rethrows SparkException to orders.py on edge-01, so the client Python driver exits 1 even though YARN RM recorded FinalApplicationStatus.SUCCEEDED.',
      size: 'SparkException',
      command: 'orders.py exits 1 (SparkException from ExecutorAllocationManager.validateSettings)',
      fields: [
        { name: 'pythonExitCode', value: '1 (FAILED)' },
        { name: 'yarnStatus', value: 'SUCCEEDED (SparkContext.stop() default-0 gotcha)' },
      ],
      senderState: 'SparkContext stopped (Shutdown(0)) and rethrew SparkException',
      receiverState: 'orders.py exited non-zero (exit 1)',
      isError: true,
    });
  } else {
    const jvmApp = b.std('jvm-user-app');
    b.addEvent({
      stepId: sGuard,
      from: sc,
      to: jvmApp,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM SparkContext Init Catch',
      port: 'edge-01 in-JVM',
      label: `${jvmShortName} exits 1`,
      info: `SparkContext.scala:793 finally { throw e } rethrows IllegalArgumentException / SparkException to ${b.appEntryLabel}() on edge-01 -> JavaMainApplication exits nonzero even though YARN RM recorded FinalApplicationStatus.SUCCEEDED via SparkContext.scala:789 stop() -> Shutdown(0).`,
      size: 'IllegalArgumentException',
      command: `${b.appEntryLabel}() -> JavaMainApplication exits 1 (IllegalArgumentException / SparkException)`,
      fields: [
        { name: 'launcherExitCode', value: '1 (JavaMainApplication FAILED)' },
        { name: 'yarnStatus', value: 'SUCCEEDED (SparkContext.scala:789 catch called stop() -> Shutdown(0))' },
      ],
      senderState: 'SparkContext stopped (Shutdown(0)) and rethrew init failure',
      receiverState: `${jvmShortName} / JavaMainApplication exited non-zero`,
      isError: true,
    });
  }
  return finalizeShortCircuit(b, meta, 'DYNALLOC GUARD');
}

// =============================================================================
// PHASE 2: Cluster-Mode Pre-Driver Startup Faults (after AM boots, before SC ready)
// =============================================================================
export function tryInjectPreDriverStartupFault(
  b: SimRunBuilder,
  cfg: SimulationConfig,
  am: string,
  amNm: string,
  meta: SimRunMeta,
): ExecutionRun | null {
  const rm = b.std('resource-manager');

  if (cfg.fault === 'class-not-found-jar') {
    const jvmApp = b.std('jvm-user-app');
    const missingClass = b.mainClass;
    const sCnf = b.addStep({
      id: 'sim-step-cnf-cluster',
      title: `Cluster AM loadClass(${missingClass}) fails pre-init -> EXIT_SC_NOT_INITED (13, 0 unreg)`,
      explanation:
        `Inside container_e01_000001 on driver-1, ApplicationMaster.startUserApplication (ApplicationMaster.scala:741-758, 741-793) calls userClassLoader.loadClass(args.userClass = "${missingClass}").getMethod("main", ...) BEFORE starting the user-thread handler and BEFORE AM registers on :8030. Note that spark.driver.userClassPathFirst=true switches AM to ChildFirstURLClassLoader (lookup order only; cannot supply a missing class). ClassNotFoundException hits the outer AM catch while registered=false (ApplicationMaster.scala:377-392), so finish() overrides exitCode to EXIT_SC_NOT_INITED = 13 with ZERO unregisterApplicationMaster RPCs. Contrast: a missing runtime dependency first loaded inside invoked main() AFTER AM registration exits EXIT_EXCEPTION_USER_CLASS = 15.`,
      category: 'yarn-lifecycle',
      keyState: 'FAILED pre-registration: EXIT_SC_NOT_INITED = 13 (0 unregisterApplicationMaster; post-reg dep in main() -> exit 15)',
      command: `userClassLoader.loadClass("${missingClass}").getMethod("main", String[].class) -> finish(FAILED, EXIT_SC_NOT_INITED=13)`,
      detailsPassed: [
        { name: 'userClass', value: missingClass },
        { name: 'failureSite', value: 'ApplicationMaster.scala:741-758 loadClass(args.userClass).getMethod() BEFORE user thread starts' },
        { name: 'amRegistered', value: 'false (ApplicationMaster.scala:377-392 -> EXIT_SC_NOT_INITED=13, 0 unregisterApplicationMaster)' },
        { name: 'postRegContrast', value: 'Missing dependency first loaded inside invoked main() AFTER AM registration exits EXIT_EXCEPTION_USER_CLASS=15' },
        { name: 'userClassPathFirstNote', value: 'spark.driver.userClassPathFirst=true changes lookup order only; cannot supply a missing class' },
      ],
      deepDiveComponentId: 'application-master',
    });
    b.addEvent({
      stepId: sCnf,
      from: am,
      to: jvmApp,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ClassLoader Lookup',
      port: 'driver-1 in-JVM',
      label: 'loadClass() -> CNFE',
      info: `ApplicationMaster.startUserApplication calls userClassLoader.loadClass("${missingClass}").getMethod("main", ...) BEFORE user-thread handler starts; throws ClassNotFoundException (spark.driver.userClassPathFirst cannot supply missing class).`,
      size: 'ClassNotFound',
      command: `userClassLoader.loadClass("${missingClass}").getMethod("main", String[].class) // throws ClassNotFoundException`,
      fields: [
        { name: 'exception', value: `java.lang.ClassNotFoundException: ${missingClass}` },
        { name: 'registeredWithRM', value: 'false (before registerApplicationMaster)' },
      ],
      senderState: 'startUserApplication() threw before starting Driver thread',
      receiverState: 'Entry class missing from localized __app__.jar / --jars',
      isError: true,
    });
    b.addEvent({
      stepId: sCnf,
      from: am,
      to: am,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM AM Outer Catch',
      port: 'driver-1 in-JVM',
      label: 'finish(FAILED, exit=13)',
      info: 'ClassNotFoundException hits outer AM catch while registered=false (ApplicationMaster.scala:377-392): finish() records EXIT_SC_NOT_INITED = 13 with ZERO unregisterApplicationMaster RPC. (Contrast: a missing dependency first loaded inside invoked main() AFTER AM registration exits EXIT_EXCEPTION_USER_CLASS = 15.)',
      size: 'exit code 13',
      command: 'finish(FinalApplicationStatus.FAILED, EXIT_SC_NOT_INITED=13) // registered=false, 0 unregisterAM',
      fields: [
        { name: 'exitCode', value: '13 (EXIT_SC_NOT_INITED; 0 unregisterApplicationMaster RPCs)' },
        { name: 'postRegistrationContrast', value: 'Missing dependency inside main() after AM registration -> EXIT_EXCEPTION_USER_CLASS = 15' },
      ],
      senderState: 'AM exiting locally with code 13',
      receiverState: 'AM JVM terminating inside container_e01_000001',
      isError: true,
    });
    b.addEvent({
      stepId: sCnf,
      from: amNm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'AM container exit (13)',
      info: 'driver-1 NodeManager reports AM container exitStatus=13 (EXIT_SC_NOT_INITED) to RM :8031; 0 executors ever allocated and 0 unregisterApplicationMaster RPCs sent.',
      size: 'ContainerStatus',
      command: 'NM ContainerStatus(container_e01_000001, COMPLETE, exitStatus=13)',
      fields: [
        { name: 'containerId', value: 'container_e01_000001' },
        { name: 'exitStatus', value: '13 (EXIT_SC_NOT_INITED, unregistered)' },
      ],
      senderState: 'AM container exited on driver-1',
      receiverState: 'RM marks Attempt 1 FAILED (0 executors allocated)',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'CLASS NOT FOUND (13)');
  }

  if (cfg.fault === 'am-sc-timeout') {
    const sTimeout = b.addStep({
      id: 'sim-step-am-timeout',
      title: `${b.appEntryLabel} stalls before getOrCreate(); AM times out at 100s`,
      explanation: b.isPySpark
        ? 'ApplicationMaster starts deploy.PythonRunner and python3 orders.py, then blocks on sparkContextPromise.future up to spark.yarn.am.waitTime (100s). Because AM never registered with RM (registered==false, ApplicationMaster.scala:367 skips finishApplicationMaster), AM exits locally with EXIT_SC_NOT_INITED (13) and driver-1 NM reports container exit status 13 to RM with 0 executors ever requested.'
        : `ApplicationMaster reflects ${b.appMainClass}.main() directly in-JVM on the Driver thread, then blocks on sparkContextPromise.future up to spark.yarn.am.waitTime (100s). Because AM never registered with RM (registered==false, ApplicationMaster.scala:367 skips finishApplicationMaster), AM exits locally with EXIT_SC_NOT_INITED (13) and driver-1 NM reports container exit status 13 to RM with 0 executors ever requested.`,
      category: 'driver-python-bridge',
      keyState: 'FAILED (EXIT_SC_NOT_INITED=13, registered=false) | 0 executors ever requested',
      command: 'ThreadUtils.awaitResult(sparkContextPromise.future, 100s) // TimeoutException',
      detailsPassed: [
        { name: 'spark.yarn.am.waitTime', value: '100s' },
        { name: 'registeredWithRM', value: 'false (ApplicationMaster.scala:367 skips finishApplicationMaster)' },
        { name: 'exitCode', value: '13 (EXIT_SC_NOT_INITED)' },
        { name: 'executorsRequested', value: '0' },
      ],
      deepDiveComponentId: 'application-master',
    });
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      b.addEvent({
        stepId: sTimeout,
        from: am,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder (PythonRunner)',
        port: 'driver-1 OS',
        label: 'Spawn orders.py (stalls)',
        info: 'AM launches orders.py, which hangs waiting on an external service before SparkSession.builder.getOrCreate().',
        size: 'OS fork/exec',
        command: 'PythonRunner.main(Array("orders.py", ""))',
        fields: [
          { name: 'PYSPARK_GATEWAY_PORT', value: '43122' },
          { name: 'scriptState', value: 'Blocked before SparkContext init' },
        ],
        senderState: 'AM awaiting sparkContextPromise (100s)',
        receiverState: 'Python driver stalled before Py4J call',
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      b.addEvent({
        stepId: sTimeout,
        from: am,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Reflection main()',
        port: 'driver-1 in-JVM',
        label: b.isScala ? 'OrdersJob.main stalls' : 'OrdersJavaJob stalls',
        info: `AM invokes ${b.appMainClass}.main() directly in-JVM, which hangs waiting on an external service before SparkSession.builder.getOrCreate().`,
        size: 'in-memory',
        command: `${b.appMainClass}.main(args) // blocked before SparkSession.builder.getOrCreate()`,
        fields: [{ name: 'driverThreadState', value: 'Blocked before SparkContext init' }],
        senderState: 'AM awaiting sparkContextPromise (100s)',
        receiverState: `${b.appEntryLabel}() stalled before SparkContext init`,
      });
    }
    b.addEvent({
      stepId: sTimeout,
      from: am,
      to: am,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ApplicationMaster.finish',
      port: 'driver-1 in-JVM',
      label: 'finish(FAILED, exit=13)',
      info: '100s waitTime expires; AM dumps driver thread stacks and calls local finish(FAILED, EXIT_SC_NOT_INITED=13). Because registered==false, ApplicationMaster.scala:367 skips RM finishApplicationMaster.',
      size: 'exit code 13',
      command: 'finish(FinalApplicationStatus.FAILED, EXIT_SC_NOT_INITED=13, "Timed out waiting for SparkContext.")',
      fields: [
        { name: 'registered', value: 'false (no RM finish RPC sent)' },
        { name: 'exitCode', value: '13 (EXIT_SC_NOT_INITED)' },
      ],
      senderState: 'AM exiting locally with code 13',
      receiverState: 'AM JVM terminating inside container_e01_000001',
      isError: true,
    });
    b.addEvent({
      stepId: sTimeout,
      from: amNm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'AM container exit (13)',
      info: 'driver-1 NodeManager reports AM container exitStatus=13 to RM :8031; RM marks Attempt 1 FAILED with 0 executors ever requested.',
      size: 'ContainerStatus',
      command: 'NM ContainerStatus(container_e01_000001, COMPLETE, exitStatus=13)',
      fields: [
        { name: 'containerId', value: 'container_e01_000001' },
        { name: 'exitStatus', value: '13 (EXIT_SC_NOT_INITED)' },
      ],
      senderState: 'AM container exited on driver-1',
      receiverState: 'RM marks Attempt 1 FAILED (0 executor containers allocated)',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'SC INIT TIMEOUT');
  }

  if (cfg.fault === 'port-bind-exhausted') {
    const sc = b.std('spark-context');
    const sparkEnv = b.std('spark-env');
    const sparkUi = b.std('spark-ui');
    const sPort = b.addStep({
      id: 'sim-step-port-bind',
      title: 'Driver SparkUI exhausts spark.port.maxRetries (16) -> WebUI.bind calls System.exit(1)',
      explanation:
        'Inside the AM container on driver-1, SparkContext starts SparkEnv and calls Utils.startServiceOnPort("SparkUI", 4040, maxRetries=16). All 17 candidate ports (4040..4056) are occupied; WebUI.bind() (WebUI.scala:170) catches BindException and exits the driver/AM JVM directly via System.exit(1) (NOT exit 15!) before AM registers (registered=false -> zero unregisterAM, 0 executors allocated).',
      category: 'driver-python-bridge',
      keyState: 'FAILED: BindException -> WebUI.bind() System.exit(1) (registered=false, no unregisterAM)',
      command: 'Utils.startServiceOnPort("SparkUI", 4040, maxRetries=16) -> WebUI.bind() -> System.exit(1)',
      detailsPassed: [
        { name: 'spark.port.maxRetries', value: '16 (ports 4040..4056 occupied)' },
        { name: 'exception', value: 'java.net.BindException: Service SparkUI failed after 16 retries' },
        { name: 'jvmExitCode', value: 'System.exit(1) directly in WebUI.bind() (NOT exit 15; amRegistered=false)' },
      ],
      deepDiveComponentId: 'spark-ui',
    });
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      b.addEvent({
        stepId: sPort,
        from: am,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder (PythonRunner)',
        port: 'driver-1 OS',
        label: 'Spawn orders.py',
        info: 'AM launches orders.py inside container_e01_000001.',
        size: 'OS fork/exec',
        command: 'PythonRunner.main(Array("orders.py", ""))',
        fields: [{ name: 'PYSPARK_GATEWAY_PORT', value: '43122' }],
        senderState: 'AM awaiting SparkContext init',
        receiverState: 'orders.py calling SparkContext.getOrCreate()',
      });
      b.addEvent({
        stepId: sPort,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback TCP',
        port: '127.0.0.1:43122',
        label: 'SparkContext.getOrCreate',
        info: 'orders.py initializes SparkContext inside the AM JVM.',
        size: '2.1 KB',
        command: 'SparkContext.getOrCreate(conf)',
        fields: [{ name: 'spark.ui.port', value: '4040' }],
        senderState: 'Awaiting SparkContext',
        receiverState: 'Starting SparkEnv and Jetty SparkUI',
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      b.addEvent({
        stepId: sPort,
        from: am,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Reflection main()',
        port: 'driver-1 in-JVM',
        label: b.isScala ? 'Reflect OrdersJob.main' : 'Reflect OrdersJavaJob',
        info: `AM reflects ${b.appMainClass}.main() directly inside container_e01_000001.`,
        size: 'in-memory',
        command: `${b.appMainClass}.main(args)`,
        fields: [{ name: 'pythonProcesses', value: '0' }],
        senderState: 'AM awaiting SparkContext init',
        receiverState: `${b.appEntryLabel}() calling SparkContext.getOrCreate()`,
      });
      b.addEvent({
        stepId: sPort,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM SparkContext Init',
        port: 'driver-1 in-JVM',
        label: 'SparkContext.getOrCreate',
        info: `${b.appEntryLabel}() initializes SparkContext directly inside the AM JVM.`,
        size: 'in-memory',
        command: 'SparkContext.getOrCreate(conf)',
        fields: [{ name: 'spark.ui.port', value: '4040' }],
        senderState: 'Awaiting SparkContext',
        receiverState: 'Starting SparkEnv and Jetty SparkUI',
      });
    }
    b.addEvent({
      stepId: sPort,
      from: sc,
      to: sparkEnv,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM SparkEnv Init',
      port: 'driver-1 in-JVM',
      label: 'createDriverEnv()',
      info: 'SparkEnv binds driver services using Utils.startServiceOnPort (Utils.scala:2283-2358).',
      size: 'in-memory',
      command: 'SparkEnv.createDriverEnv(conf, false, listenerBus)',
      fields: [{ name: 'spark.port.maxRetries', value: '16' }],
      senderState: 'Binding SparkUI port',
      receiverState: 'Trying ports 4040..4056',
    });
    b.addEvent({
      stepId: sPort,
      from: sc,
      to: sparkUi,
      kind: 'local',
      channel: 'error',
      protocol: 'Jetty Bind -> System.exit(1)',
      port: 'driver-1 in-JVM',
      label: 'BindException -> exit(1)',
      info: 'All 17 ports (4040..4056) are in use on driver-1; Utils.startServiceOnPort throws BindException and WebUI.bind() (WebUI.scala:170) calls System.exit(1) directly (NOT exit 15).',
      size: 'BindException',
      command: 'WebUI.bind() -> catch BindException -> System.exit(1)',
      fields: [
        { name: 'portsTried', value: '4040..4056' },
        { name: 'exitMechanism', value: 'System.exit(1) directly (not EXIT_EXCEPTION_USER_CLASS=15)' },
        { name: 'remediation', value: 'Raise spark.port.maxRetries or set spark.ui.port=0' },
      ],
      senderState: 'SparkUI bind failed; JVM calling System.exit(1)',
      receiverState: 'Jetty bind aborted',
      isError: true,
    });
    b.addEvent({
      stepId: sPort,
      from: sc,
      to: am,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ShutdownHook',
      port: 'driver-1 in-JVM',
      label: 'AM hook (exit 1, unreg)',
      info: 'System.exit(1) fires AM shutdown hook while registered=false (ApplicationMaster.scala:367 skips RM finish RPC); container exits with status 1.',
      size: 'exit code 1',
      command: 'System.exit(1) // AM registered=false, no RM finish RPC',
      fields: [
        { name: 'amRegistered', value: 'false (no RM finish RPC sent)' },
        { name: 'exitCode', value: '1 (System.exit(1))' },
      ],
      senderState: 'Driver JVM exiting directly with code 1',
      receiverState: 'AM shutdown hook skips RM finish RPC',
      isError: true,
    });
    b.addEvent({
      stepId: sPort,
      from: amNm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'AM container exit (1)',
      info: 'driver-1 NM reports AM container exitStatus=1 to RM :8031 with 0 executors ever allocated and no unregisterAM sent.',
      size: 'ContainerStatus',
      command: 'NM ContainerStatus(container_e01_000001, COMPLETE, exitStatus=1)',
      fields: [{ name: 'exitStatus', value: '1 (System.exit(1), FAILED)' }],
      senderState: 'AM container exited on driver-1',
      receiverState: 'RM marks attempt FAILED',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'PORT BIND FAIL');
  }

  if (cfg.fault === 'dynalloc-no-shuffle-service') {
    const sc = b.std('spark-context');
    const sparkEnv = b.std('spark-env');
    const eam = b.std('executor-allocation-manager');
    const sGuard = b.addStep({
      id: 'sim-step-dynalloc-guard',
      title: 'ExecutorAllocationManager.validateSettings rejects startup -> cluster AM EXIT 13 (no unregister)',
      explanation:
        'Inside the cluster-mode AM container, SparkContext fails in ExecutorAllocationManager.validateSettings() (ExecutorAllocationManager.scala:234-255) because ALL 4 shuffle-preservation prerequisites are off: (1) spark.shuffle.service.enabled=false, (2) spark.dynamicAllocation.shuffleTracking.enabled=false, (3) shuffle-block decommission fallback off (spark.decommission.enabled / spark.storage.decommission.shuffleBlocks.enabled), and (4) no reliable ShuffleDataIO storage (supportsReliableStorage()=false). Because this occurs BEFORE postStartHook / AM registration (registered=false), ApplicationMaster.finish overrides exit code to EXIT_SC_NOT_INITED (13) with NO unregisterAM.',
      category: 'yarn-lifecycle',
      keyState: 'FAILED startup: SparkException -> cluster AM EXIT_SC_NOT_INITED (13, registered=false, no unregisterAM)',
      command: 'ExecutorAllocationManager.validateSettings() -> finish(FAILED, EXIT_SC_NOT_INITED=13)',
      detailsPassed: [
        { name: 'prereq1_ESS', value: 'spark.shuffle.service.enabled = false' },
        { name: 'prereq2_tracking', value: 'spark.dynamicAllocation.shuffleTracking.enabled = false' },
        { name: 'prereq3_decommission', value: 'spark.decommission.enabled / spark.storage.decommission.shuffleBlocks.enabled = false' },
        { name: 'prereq4_shuffleDataIO', value: 'ShuffleDataIO.driver().supportsReliableStorage() = false' },
        { name: 'amExitCode', value: '13 (EXIT_SC_NOT_INITED; registered=false -> no unregisterAM)' },
      ],
      deepDiveComponentId: 'executor-allocation-manager',
    });
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      b.addEvent({
        stepId: sGuard,
        from: am,
        to: pyDriver,
        kind: 'local',
        channel: 'python',
        protocol: 'ProcessBuilder (PythonRunner)',
        port: 'driver-1 OS',
        label: 'Spawn orders.py',
        info: 'AM starts orders.py inside container_e01_000001.',
        size: 'OS fork/exec',
        command: 'PythonRunner.main(Array("orders.py", ""))',
        fields: [{ name: 'dynamicAllocation', value: 'true' }],
        senderState: 'AM awaiting SparkContext',
        receiverState: 'Creating SparkContext',
      });
      b.addEvent({
        stepId: sGuard,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback TCP',
        port: '127.0.0.1:43122',
        label: 'Init SC (dynAlloc=true)',
        info: 'orders.py initializes SparkContext with dynamicAllocation=true and all 4 shuffle-preservation options off.',
        size: '2.1 KB',
        command: 'SparkContext.getOrCreate(conf[dynamicAllocation=true, ess=false])',
        fields: [{ name: 'essEnabled', value: 'false' }],
        senderState: 'Awaiting SparkContext',
        receiverState: 'Creating SparkEnv & ExecutorAllocationManager',
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      b.addEvent({
        stepId: sGuard,
        from: am,
        to: jvmApp,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Reflection main()',
        port: 'driver-1 in-JVM',
        label: b.isScala ? 'Reflect OrdersJob.main' : 'Reflect OrdersJavaJob',
        info: `AM invokes ${b.appMainClass}.main() directly inside container_e01_000001.`,
        size: 'in-memory',
        command: `${b.appMainClass}.main(args)`,
        fields: [{ name: 'dynamicAllocation', value: 'true' }],
        senderState: 'AM awaiting SparkContext',
        receiverState: 'Creating SparkContext',
      });
      b.addEvent({
        stepId: sGuard,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM SparkContext Init',
        port: 'driver-1 in-JVM',
        label: 'Init SC (dynAlloc=true)',
        info: `${b.appEntryLabel}() initializes SparkContext with dynamicAllocation=true and all 4 shuffle-preservation options off.`,
        size: 'in-memory',
        command: 'SparkContext.getOrCreate(conf[dynamicAllocation=true, ess=false])',
        fields: [{ name: 'essEnabled', value: 'false' }],
        senderState: 'Awaiting SparkContext',
        receiverState: 'Creating SparkEnv & ExecutorAllocationManager',
      });
    }
    b.addEvent({
      stepId: sGuard,
      from: sc,
      to: sparkEnv,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM SparkEnv Init',
      port: 'driver-1 in-JVM',
      label: 'createDriverEnv()',
      info: 'Driver SparkEnv initializes prior to ExecutorAllocationManager validation (still BEFORE postStartHook / registerAM).',
      size: 'in-memory',
      command: 'SparkEnv.createDriverEnv(conf, false, listenerBus)',
      fields: [{ name: 'rpc', value: 'driver-1:41000' }],
      senderState: 'Instantiating ExecutorAllocationManager',
      receiverState: 'Driver env initialized',
    });
    b.addEvent({
      stepId: sGuard,
      from: sc,
      to: eam,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM validateSettings',
      port: 'driver-1 in-JVM',
      label: 'validateSettings() FAIL',
      info: 'SparkException / IllegalArgumentException: Dynamic allocation requires ESS, shuffleTracking, shuffle-block decommission fallback (spark.decommission.enabled + spark.storage.decommission.shuffleBlocks.enabled), OR reliable ShuffleDataIO storage.',
      size: 'SparkException',
      command: 'throw new SparkException("Dynamic allocation of executors requires one of the following conditions...")',
      fields: [
        { name: 'sourceRef', value: 'ExecutorAllocationManager.scala:234-255' },
        { name: 'executorsAllocated', value: '0' },
      ],
      senderState: 'SparkContext startup aborted before postStartHook',
      receiverState: 'Rejected dynamic allocation with all 4 prerequisites off',
      isError: true,
    });
    b.addEvent({
      stepId: sGuard,
      from: sc,
      to: am,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ApplicationMaster.finish',
      port: 'driver-1 in-JVM',
      label: 'finish(FAILED, exit=13)',
      info: `${b.appEntryLabel} fails before SparkContext initializes -> ApplicationMaster.finish sees !registered && isClusterMode (ApplicationMaster.scala:386) and exits locally with EXIT_SC_NOT_INITED (13) without calling RM finish RPC.`,
      size: 'exit code 13',
      command: 'finish(FinalApplicationStatus.FAILED, EXIT_SC_NOT_INITED=13) // registered=false, no RM finish RPC',
      fields: [
        { name: 'amRegistered', value: 'false (no RM finish RPC sent)' },
        { name: 'exitCode', value: '13 (EXIT_SC_NOT_INITED)' },
      ],
      senderState: 'Driver aborted before registerAM',
      receiverState: 'AM terminating locally with code 13',
      isError: true,
    });
    b.addEvent({
      stepId: sGuard,
      from: amNm,
      to: rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'AM container exit (13)',
      info: 'driver-1 NM reports AM container exitStatus=13 (EXIT_SC_NOT_INITED) to RM :8031; 0 executors ever allocated and no unregisterAM sent.',
      size: 'ContainerStatus',
      command: 'NM ContainerStatus(container_e01_000001, COMPLETE, exitStatus=13)',
      fields: [{ name: 'exitStatus', value: '13 (EXIT_SC_NOT_INITED, FAILED)' }],
      senderState: 'AM container exited on driver-1',
      receiverState: 'RM marks attempt FAILED',
      isError: true,
    });
    return finalizeShortCircuit(b, meta, 'DYNALLOC GUARD');
  }

  return null;
}

// =============================================================================
// PHASE 4: Executor Startup Faults (right after sAlloc, before workload runs)
// =============================================================================
export function injectExecutorStartupFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
): void {
  if (state.cfg.fault !== 'disk-dir-create-fail') return;

  const deadExec = state.liveExecs[0];
  const deadBm = state.liveBms[0];
  const deadExecNum = b.instanceExecutorNumber(deadExec);
  const { hostZone, hostName } = b.instanceWorkerHost(deadExec);
  const nm = b.nmInstance(hostZone);
  const cid = b.instanceContainerId(deadExec);

  const sDir = b.addStep({
    id: 'sim-step-disk-dir-fail',
    title: `Exec-${deadExecNum} DiskBlockManager fails local dir init -> EXIT 53 (before BM reg) -> replaced`,
    explanation:
      `During CoarseGrainedExecutorBackend / SparkEnv.createExecutorEnv startup right after ${hostName} NM launches ${cid} -- BEFORE Exec-${deadExecNum} registers its Executor, registers its BlockManager, or sends any Heartbeat -- DiskBlockManager.createLocalDirs() fails on all yarn.nodemanager.local-dirs (read-only mount or full disk) and calls System.exit(53) (ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR). Because exit 53 is an unexpected, unreleased exit, YarnAllocator's default arm calls handleResourceAllocationFailure and classifies it as exitCausedByApp=true, then launches a replacement container with a fresh monotonic container ID.`,
    category: 'yarn-lifecycle',
    keyState: `Exec-${deadExecNum} exited 53 BEFORE RegisterBM/Heartbeat (exitCausedByApp=true) -> replacement registered`,
    command: 'SparkEnv.createExecutorEnv -> DiskBlockManager.createLocalDirs() -> System.exit(53)',
    detailsPassed: [
      { name: 'timing', value: 'During Executor/DiskBlockManager startup BEFORE RegisterBlockManager or Heartbeat' },
      { name: 'exitCode', value: '53 (ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)' },
      { name: 'sparkAttribution', value: 'exitCausedByApp=true (YarnAllocator default branch -> handleResourceAllocationFailure)' },
      { name: 'config', value: 'spark.local.dir / yarn.nodemanager.local-dirs' },
      { name: 'sourceRef', value: 'DiskBlockManager.scala:63-66; YarnAllocator.scala:900-922' },
    ],
    deepDiveComponentId: 'block-manager',
  });
  b.addEvent({
    stepId: sDir,
    from: deadExec,
    to: deadBm,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM DiskBlockManager Init',
    port: `${hostName} local-dir`,
    label: 'Local dir fail -> exit 53',
    info: `During SparkEnv.createExecutorEnv (before RegisterExecutor, RegisterBlockManager, or Heartbeat), DiskBlockManager.createLocalDirs() fails ("Failed to create any local dir.") and calls System.exit(53).`,
    size: 'exit code 53',
    command: 'System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR) // 53',
    fields: [
      { name: 'exitCode', value: '53 (DISK_STORE_FAILED_TO_CREATE_DIR)' },
      { name: 'bmRegistered', value: 'false (crashed before RegisterBlockManager)' },
      { name: 'explanation', value: 'Failed to create local directory (bad spark.local.dir?)' },
    ],
    senderState: `${deadExec} exiting immediately with code 53`,
    receiverState: 'DiskBlockManager aborted before registering or serving blocks',
    isError: true,
  });
  b.addEvent({
    stepId: sDir,
    from: nm,
    to: state.rm,
    channel: 'error',
    protocol: 'Hadoop RPC (ResourceTracker)',
    port: 'rm01:8031',
    label: 'Container exit (53)',
    info: `${hostName} NM reports ${cid} exitStatus=53 (DISK_STORE_FAILED_TO_CREATE_DIR) to RM :8031.`,
    size: 'ContainerStatus',
    command: `ContainerStatus(${cid}, COMPLETE, exitStatus=53)`,
    fields: [
      { name: 'containerId', value: cid },
      { name: 'exitStatus', value: '53' },
    ],
    senderState: 'Container exited; NM unhealthy dir excluded',
    receiverState: 'RM notifies AM of completed container',
    isError: true,
  });
  b.addEvent({
    stepId: sDir,
    from: state.alloc,
    to: state.schedBackend,
    channel: 'rpc',
    protocol: 'Spark Netty RPC (RemoveExecutor)',
    port: b.driverRpc,
    label: 'RemoveExec(exit=53,app)',
    info: `YarnAllocator handles unexpected, unreleased exit 53 via its default bad-node branch (handleResourceAllocationFailure, exitCausedByApp=true) and requests a replacement container.`,
    size: '720 B',
    command: `driverRef.send(RemoveExecutor("${deadExecNum}", ExecutorExited(53, exitCausedByApp=true, "Failed to create local dir")))`,
    fields: [
      { name: 'exitCode', value: '53' },
      { name: 'exitCausedByApp', value: 'true (YarnAllocator default branch -> handleResourceAllocationFailure)' },
      { name: 'recovery', value: 'Requesting replacement container with fresh monotonic containerId' },
    ],
    senderState: 'Allocating replacement executor',
    receiverState: 'Removed failed startup executor from pool',
  });

  // Replace index 0 in-place with RegisterBM + Heartbeat so state.liveExecs[0] stays alive for workload steps.
  replaceDeadExecutor(b, state, sDir, deadExec, hostZone, true, true);
}

// =============================================================================
// PHASE 5: Pre-Workload Faults (at job/TaskSet submission or planning)
// Returns true if abortedByFault stops the run; false to continue workload.
// =============================================================================
export function tryInjectPreWorkloadFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
): boolean {
  const fault = state.cfg.fault;

  if (fault === 'multiple-spark-contexts') {
    const sc = b.std('spark-context');
    state.abortedByFault = true;
    if (b.isPySpark) {
      const pyDriver = b.std('python-driver');
      const sMulti = b.addStep({
        id: 'sim-step-multi-sc',
        title: 'Second SparkContext rejected by PySpark _ensure_initialized (SPARK-2243)',
        explanation:
          'After the first SparkContext is active and the AM has registered with YARN RM, orders.py attempts to construct a second SparkContext. In PySpark, SparkContext._ensure_initialized (python/pyspark/core/context.py:451-475) checks SparkContext._active_spark_context and immediately raises ValueError ("Cannot run multiple SparkContexts at once...") on the Python side BEFORE JVM construction. On the JVM side, SparkContext.assertNoOtherContextIsRunning (SparkContext.scala:3380-3386) unconditionally enforces the same single-context invariant (SparkException, SPARK-2243).',
        category: 'driver-python-bridge',
        keyState: 'FAILED: ValueError in SparkContext._ensure_initialized (and unconditional JVM assertNoOtherContextIsRunning)',
        command: 'SparkContext._ensure_initialized() -> ValueError("Cannot run multiple SparkContexts at once")',
        detailsPassed: [
          { name: 'activeContext', value: 'First SparkContext live & AM registered with RM' },
          { name: 'pysparkGuard', value: 'SparkContext._ensure_initialized (python/pyspark/core/context.py:451-475) -> ValueError before JVM call' },
          { name: 'jvmGuard', value: 'Unconditional SparkContext.assertNoOtherContextIsRunning (SparkContext.scala:3380-3386) -> SparkException (SPARK-2243)' },
          { name: 'remediation', value: 'Reuse SparkSession.builder.getOrCreate() or call sc.stop() first' },
        ],
        deepDiveComponentId: 'spark-context',
      });
      b.addEvent({
        stepId: sMulti,
        from: pyDriver,
        to: pyDriver,
        kind: 'local',
        channel: 'error',
        protocol: 'Python Guard (_ensure_initialized)',
        port: `${b.driverHost} OS`,
        label: 'PySpark ValueError (2nd SC)',
        info: 'orders.py calls SparkContext(conf) while first SparkContext is active; PySpark SparkContext._ensure_initialized raises ValueError ("Cannot run multiple SparkContexts at once...") on the Python side BEFORE invoking the JVM constructor.',
        size: 'ValueError',
        command: 'SparkContext._ensure_initialized() // raises ValueError before JVM SparkContext()',
        fields: [
          { name: 'activeContext', value: 'SparkContext._active_spark_context != None' },
          { name: 'jvmGuardNote', value: 'JVM SparkContext.assertNoOtherContextIsRunning also unconditionally rejects a 2nd JVM context (SPARK-2243)' },
        ],
        senderState: 'Rejected 2nd Python SparkContext before Py4J JVM construction',
        receiverState: 'orders.py unwinding with ValueError (exit code 1)',
        isError: true,
      });
      b.addEvent({
        stepId: sMulti,
        from: pyDriver,
        to: sc,
        channel: 'python',
        protocol: 'Py4J Loopback -> sc.stop()',
        port: b.isClient ? '127.0.0.1:44108' : '127.0.0.1:43122',
        label: 'Stop live SparkContext #1',
        info: 'orders.py exception teardown stops the live first SparkContext (noting JVM guard SparkContext.assertNoOtherContextIsRunning is unconditional with no override flag).',
        size: '1.2 KB',
        command: 'SparkContext.stop() // unwinding after ValueError (SPARK-2243)',
        fields: [{ name: 'firstContextState', value: 'Shutting down cleanly' }],
        senderState: 'Tearing down active SparkContext #1',
        receiverState: 'Stopping scheduler backend & live executors',
      });
      emitTerminalAppOutcome(b, state, {
        origin: 'user-app-failure',
        diagnostic: 'ValueError: Cannot run multiple SparkContexts at once (SPARK-2243)',
        stopLiveExecutors: true,
      });
    } else {
      const jvmApp = b.std('jvm-user-app');
      const sMulti = b.addStep({
        id: 'sim-step-multi-sc',
        title: 'Second SparkContext rejected unconditionally by JVM assertNoOtherContextIsRunning (SPARK-2243)',
        explanation:
          `After the first SparkContext is active and AM is registered with YARN RM, ${b.appEntryLabel}() attempts to construct a second SparkContext in the same JVM. SparkContext.assertNoOtherContextIsRunning (SparkContext.scala:3380-3386) unconditionally throws SparkException ("Only one SparkContext should be running in this JVM", SPARK-2243; there is no override config flag). Reuse SparkSession.builder.getOrCreate()${b.isJava ? ' or JavaSparkContext.fromSparkContext(spark.sparkContext())' : ''}.`,
        category: 'driver-python-bridge',
        keyState: 'FAILED: SparkException in unconditional JVM SparkContext.assertNoOtherContextIsRunning (SPARK-2243)',
        command: 'new SparkContext(conf) -> SparkContext.assertNoOtherContextIsRunning() -> SparkException',
        detailsPassed: [
          { name: 'activeContext', value: 'First SparkContext live & AM registered with RM' },
          { name: 'jvmGuard', value: 'Unconditional SparkContext.assertNoOtherContextIsRunning (SparkContext.scala:3380-3386, SPARK-2243)' },
          {
            name: 'remediation',
            value: b.isJava
              ? 'Use JavaSparkContext.fromSparkContext(spark.sparkContext()) or call sc.stop() first'
              : 'Reuse SparkSession.builder.getOrCreate() or call sc.stop() first',
          },
        ],
        deepDiveComponentId: 'spark-context',
      });
      b.addEvent({
        stepId: sMulti,
        from: jvmApp,
        to: sc,
        kind: 'local',
        channel: 'error',
        protocol: 'In-JVM SparkContext Guard',
        port: `${b.driverHost} in-JVM`,
        label: '2nd SC -> SparkException',
        info: `${b.appEntryLabel}() constructs a second SparkContext in the same JVM; unconditional SparkContext.assertNoOtherContextIsRunning (SparkContext.scala:3380-3386) immediately throws SparkException ("Only one SparkContext should be running in this JVM", SPARK-2243).`,
        size: 'SparkException',
        command: 'SparkContext.assertNoOtherContextIsRunning() // SparkException: Only one SparkContext should be running in this JVM',
        fields: [
          { name: 'exception', value: 'SparkException: Only one SparkContext should be running in this JVM' },
          { name: 'sourceRef', value: 'SparkContext.scala:3380-3386 (unconditional guard; SPARK-2243)' },
        ],
        senderState: 'Attempted 2nd SparkContext construction in-JVM',
        receiverState: 'Unconditionally rejected 2nd context',
        isError: true,
      });
      b.addEvent({
        stepId: sMulti,
        from: sc,
        to: jvmApp,
        kind: 'local',
        channel: 'error',
        protocol: 'In-JVM Exception Unwind',
        port: `${b.driverHost} in-JVM`,
        label: 'Unwind & fail main()',
        info: `SparkException unwinds out of ${b.appEntryLabel}() while the first SparkContext is torn down cleanly.`,
        size: 'SparkException',
        command: `throw new SparkException("Only one SparkContext should be running in this JVM")`,
        fields: [{ name: 'firstContextState', value: 'Stopping live executors during teardown' }],
        senderState: 'Rethrew SparkException to user main()',
        receiverState: `${b.appEntryLabel}() aborting with uncaught SparkException`,
        isError: true,
      });
      emitTerminalAppOutcome(b, state, {
        origin: 'user-app-failure',
        diagnostic: 'SparkException: Only one SparkContext should be running in this JVM (SPARK-2243)',
        stopLiveExecutors: true,
      });
    }
    return true;
  }

  if (fault === 'java-bean-encoder-error') {
    const jvmApp = b.std('jvm-user-app');
    const driverEncoder = b.driverEncoderInstance();
    state.abortedByFault = true;
    const sBean = b.addStep({
      id: 'sim-step-bean-encoder-error',
      title: 'Driver Encoders.bean(OrderNode.class) -> CIRCULAR_CLASS_REFERENCE (0 stages launched)',
      explanation:
        `On the driver (${b.driverHost}) BEFORE spark.createDataset(...) is ever entered or any stage launches, ${b.appEntryLabel}() evaluates static Encoders.bean(OrderNode.class) (Encoders.scala:211) on a self-referential JavaBean (OrderNode getParent()). Encoders.bean delegates directly to JavaTypeInference.encoderFor (JavaTypeInference.scala:153-186), which detects OrderNode already in seenTypeSet and throws SparkUnsupportedOperationException (errorClass="CIRCULAR_CLASS_REFERENCE", ExecutionErrors.scala:181-192). Sibling driver encoder errors include ENCODER_NOT_FOUND and BEAN_ENCODER_INTERFACE_NOT_SUPPORTED.`,
      category: 'sql-planning-aqe',
      keyState: 'FAILED pre-stage on Driver: SparkUnsupportedOperationException (CIRCULAR_CLASS_REFERENCE | 0 stages launched)',
      command: 'Encoders.bean(OrderNode.class) -> JavaTypeInference.encoderFor() -> CIRCULAR_CLASS_REFERENCE',
      detailsPassed: [
        { name: 'errorClass', value: 'CIRCULAR_CLASS_REFERENCE (SparkUnsupportedOperationException)' },
        { name: 'callOrder', value: 'Static Encoders.bean(OrderNode.class) (Encoders.scala:211) fails BEFORE spark.createDataset(...) is entered' },
        { name: 'siblings', value: 'ENCODER_NOT_FOUND & BEAN_ENCODER_INTERFACE_NOT_SUPPORTED' },
        { name: 'stagesLaunched', value: '0 (failed on driver during encoder derivation)' },
        { name: 'sourceRef', value: 'Encoders.scala:211; JavaTypeInference.scala:153-186; ExecutionErrors.scala:181-192' },
      ],
      deepDiveComponentId: 'dataset-encoder-udf',
    });
    b.addEvent({
      stepId: sBean,
      from: jvmApp,
      to: driverEncoder,
      kind: 'local',
      channel: 'internal',
      protocol: 'Encoders.scala:211 -> JavaTypeInference',
      port: `${b.driverHost} in-JVM`,
      label: 'Encoders.bean(OrderNode)',
      info: `${b.appEntryLabel}() evaluates static Encoders.bean(OrderNode.class) (Encoders.scala:211 -> JavaTypeInference.encoderFor, JavaTypeInference.scala:153-186) directly BEFORE spark.createDataset(...) is ever entered; introspecting readable bean properties with seenTypeSet={OrderNode} hits self-referential getParent(): OrderNode.`,
      size: 'in-memory',
      command: 'Encoders.bean(OrderNode.class) -> JavaTypeInference.encoderFor(OrderNode.class) // before spark.createDataset()',
      fields: [
        { name: 'beanClass', value: 'com.acme.OrderNode (cyclic OrderNode getParent())' },
        { name: 'directHandoff', value: 'Encoders.scala:211 -> JavaTypeInference.encoderFor (SparkSession.createDataset never entered)' },
        { name: 'cycleDetected', value: 'Introspector.getBeanInfo(OrderNode.class) -> getParent(): OrderNode' },
      ],
      senderState: 'Evaluating static Encoders.bean(OrderNode.class) before createDataset()',
      receiverState: 'Detected circular class reference in seenTypeSet={OrderNode}',
    });
    b.addEvent({
      stepId: sBean,
      from: driverEncoder,
      to: jvmApp,
      kind: 'local',
      channel: 'error',
      protocol: 'ExecutionErrors.scala:181',
      port: `${b.driverHost} in-JVM`,
      label: 'CIRCULAR_CLASS_REFERENCE',
      info: 'Throws SparkUnsupportedOperationException (errorClass="CIRCULAR_CLASS_REFERENCE", ExecutionErrors.scala:181-192) before spark.createDataset(...) is entered and before launching any stage (sibling errors: ENCODER_NOT_FOUND, BEAN_ENCODER_INTERFACE_NOT_SUPPORTED).',
      size: 'UnsupportedOp',
      command: 'throw ExecutionErrors.cannotHaveCircularReferencesInBeanClassError(OrderNode.class)',
      fields: [
        { name: 'errorClass', value: 'CIRCULAR_CLASS_REFERENCE' },
        { name: 'siblingErrors', value: 'ENCODER_NOT_FOUND, BEAN_ENCODER_INTERFACE_NOT_SUPPORTED' },
        { name: 'stagesLaunched', value: '0' },
      ],
      senderState: 'Rejected self-referential JavaBean on driver',
      receiverState: `${b.appEntryLabel}() unwinding before spark.createDataset() or any stage launch`,
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'SparkUnsupportedOperationException [CIRCULAR_CLASS_REFERENCE]: Cannot have circular references in bean class OrderNode',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'kryo-buffer-overflow') {
    const sc = b.std('spark-context');
    const driverSer = b.std('serializer-manager');
    state.abortedByFault = true;
    const pyHelperNote = b.isPySpark
      ? ' In PySpark mode, this invokes an explicit JVM helper path because Python RDD.aggregateByKey (python/pyspark/core/rdd.py:3909-3919) clones zeroValue via copy.deepcopy instead of KryoSerializerInstance.serialize().'
      : '';
    const sKryoBuf = b.addStep({
      id: 'sim-step-kryo-buffer-overflow',
      title: 'Driver JVM PairRDDFunctions.aggregateByKey zeroValue (72 MiB) > 64m -> KRYO_BUFFER_OVERFLOW',
      explanation:
        `With spark.serializer=org.apache.spark.serializer.KryoSerializer and default spark.kryoserializer.buffer.max=64m, Driver JVM PairRDDFunctions.aggregateByKey (PairRDDFunctions.scala:155-165) calls bounded KryoSerializerInstance.serialize(zeroValue) on an oversized 72 MiB zeroValue (distinct from streamed shuffle partitions and SQL ShuffleExchangeExec, which uses UnsafeRowSerializer). KryoSerializer.scala:448-461 catches KryoException("Buffer overflow") and wraps it into SparkException (errorClass="KRYO_BUFFER_OVERFLOW") before launching the stage.${pyHelperNote}`,
      category: 'sql-planning-aqe',
      keyState: 'FAILED on Driver: SparkException (KRYO_BUFFER_OVERFLOW: 72 MiB > spark.kryoserializer.buffer.max=64m)',
      command: 'PairRDDFunctions.aggregateByKey -> KryoSerializerInstance.serialize(zeroValue) -> KRYO_BUFFER_OVERFLOW',
      detailsPassed: [
        { name: 'callSite', value: 'JVM PairRDDFunctions.aggregateByKey (PairRDDFunctions.scala:155-165) bounded KryoSerializerInstance.serialize()' },
        { name: 'spark.kryoserializer.buffer.max', value: '64m default (must be < 2048m)' },
        { name: 'errorClass', value: 'KRYO_BUFFER_OVERFLOW (KryoSerializer.scala:448-461 wrapping KryoException: Buffer overflow)' },
        ...(b.isPySpark
          ? [{ name: 'pysparkDistinction', value: 'Invokes JVM helper; Python RDD.aggregateByKey (python/pyspark/core/rdd.py:3909-3919) uses copy.deepcopy instead of KryoSerializerInstance.serialize()' }]
          : []),
        { name: 'remediation', value: 'Raise spark.kryoserializer.buffer.max (< 2048m) or broadcast large lookup state instead of zeroValue' },
      ],
      deepDiveComponentId: 'serializer-manager',
    });
    b.addEvent({
      stepId: sKryoBuf,
      from: sc,
      to: driverSer,
      kind: 'local',
      channel: 'internal',
      protocol: 'PairRDDFunctions.aggregateByKey',
      port: `${b.driverHost} in-JVM`,
      label: 'serialize(zeroVal 72 MiB)',
      info: `Driver JVM PairRDDFunctions.aggregateByKey (PairRDDFunctions.scala:155-165) invokes bounded KryoSerializerInstance.serialize(zeroValue) on a 72 MiB object exceeding default spark.kryoserializer.buffer.max (64m).${pyHelperNote}`,
      size: '72.0 MiB',
      command: 'val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue) // 72 MiB > 64m',
      fields: [
        { name: 'objectSizeVsMax', value: '72 MiB > 64m (spark.kryoserializer.buffer.max)' },
        { name: 'serializerPath', value: 'Bounded KryoSerializerInstance.serialize() on driver (SQL ShuffleExchangeExec uses UnsafeRowSerializer)' },
        ...(b.isPySpark
          ? [{ name: 'pythonRddNote', value: 'JVM helper path (Python RDD.aggregateByKey uses copy.deepcopy, python/pyspark/core/rdd.py:3909-3919)' }]
          : []),
      ],
      senderState: 'Cloning oversized aggregateByKey zeroValue via Kryo',
      receiverState: 'KryoOutput buffer reached 64 MiB ceiling',
    });
    b.addEvent({
      stepId: sKryoBuf,
      from: driverSer,
      to: sc,
      kind: 'local',
      channel: 'error',
      protocol: 'KryoSerializer.scala:448',
      port: `${b.driverHost} in-JVM`,
      label: 'KRYO_BUFFER_OVERFLOW',
      info: 'KryoSerializer.scala:448-461 catches KryoException("Buffer overflow") and wraps it into SparkException (errorClass="KRYO_BUFFER_OVERFLOW") citing spark.kryoserializer.buffer.max.',
      size: 'SparkException',
      command: 'catch e: KryoException("Buffer overflow") -> throw SparkException(errorClass="KRYO_BUFFER_OVERFLOW")',
      fields: [
        { name: 'errorClass', value: 'KRYO_BUFFER_OVERFLOW' },
        { name: 'cause', value: 'com.esotericsoftware.kryo.KryoException: Buffer overflow' },
        { name: 'sourceRef', value: 'KryoSerializer.scala:448-461; PairRDDFunctions.scala:155-165; python/pyspark/core/rdd.py:3909-3919' },
      ],
      senderState: 'Wrapped KryoException("Buffer overflow") in SparkException',
      receiverState: `Driver aborted aggregateByKey in ${b.appEntryLabel}`,
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'SparkException [KRYO_BUFFER_OVERFLOW]: Buffer overflow exceeding spark.kryoserializer.buffer.max (64m)',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'codegen-huge-method') {
    const sqlPlanner = b.std('sql-planner');
    const dag = b.std('dag-scheduler');
    const sCg = b.addStep({
      id: 'sim-step-codegen-huge',
      title: 'WholeStageCodegen compiled method (9,420 B) > lowered hugeMethodLimit (8,000 B) -> INFO fallback',
      explanation:
        'A valid JVM method cannot exceed 64 KiB (65,535 bytes). With spark.sql.codegen.hugeMethodLimit lowered to 8,000 B (HotSpot JIT HugeMethodLimit threshold), CodeGenerator.compile produces a valid 9,420 B method; WholeStageCodegenExec.doExecute (WholeStageCodegenExec.scala:1032-1044) logs INFO ("the bytecode size (9420) is above the limit 8000") and falls back to interpreted child.execute(). (Separately, exceeding the JVM 64 KiB limit hits the Janino compile-failure WARN fallback branch at lines 1022-1029.)',
      category: 'sql-planning-aqe',
      keyState: 'DEGRADED (non-fatal INFO): 9,420 B > hugeMethodLimit 8,000 B -> interpreted child.execute()',
      command: 'WholeStageCodegenExec.doExecute() // maxMethodCodeSize (9420) > hugeMethodLimit (8000) -> INFO -> child.execute()',
      detailsPassed: [
        { name: 'spark.sql.codegen.hugeMethodLimit', value: '8000 bytes (lowered for HotSpot JIT; default 65535 B = JVM 64 KiB max)' },
        { name: 'compiledMethodSize', value: '9420 bytes (valid JVM bytecode < 65535 B, above lowered limit 8000 B)' },
        { name: 'logLevelAndFallback', value: 'INFO log -> fallback to interpreted child.execute() (separate WARN branch if Janino fails > 64 KiB)' },
        { name: 'sourceRef', value: 'WholeStageCodegenExec.scala:1022-1044' },
      ],
      deepDiveComponentId: 'sql-planner',
    });
    b.addEvent({
      stepId: sCg,
      from: sqlPlanner,
      to: sqlPlanner,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM CodeGenerator.compile',
      port: `${b.driverHost} in-JVM`,
      label: 'Codegen 9420B>8000B INFO',
      info: 'CodeGenerator.compile produces valid 9,420 B bytecode (< 65,535 B JVM limit); WholeStageCodegenExec.doExecute logs INFO ("the bytecode size (9420) is above the limit 8000") and disables whole-stage codegen (separate WARN branch handles Janino compile failures > 64 KiB).',
      size: '9,420 B bytecode',
      command: 'logInfo("...the bytecode size (9420) is above the limit 8000, and the whole-stage codegen was disabled")',
      fields: [
        { name: 'maxMethodCodeSize', value: '9420 B (< 65535 B JVM spec limit)' },
        { name: 'hugeMethodLimit', value: '8000 B (lowered JIT limit)' },
        { name: 'logLevel', value: 'INFO (sibling Janino >64 KiB compile-failure branch logs WARN)' },
      ],
      senderState: 'Disabling whole-stage codegen for operator subtree',
      receiverState: 'Falling back to interpreted Volcano iterator',
    });
    b.addEvent({
      stepId: sCg,
      from: sqlPlanner,
      to: dag,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Interpreted Fallback',
      port: `${b.driverHost} in-JVM`,
      label: 'Fallback: child.execute()',
      info: 'WholeStageCodegenExec falls back to interpreted child.execute(); job runs slower (degraded) without aborting.',
      size: 'in-memory',
      command: 'WholeStageCodegenExec.doExecute() -> child.execute() // Volcano fallback',
      fields: [
        { name: 'severity', value: 'degraded (job succeeds)' },
        { name: 'sourceRef', value: 'WholeStageCodegenExec.scala:1032-1044' },
      ],
      senderState: 'Submitted interpreted physical plan',
      receiverState: 'Scheduling stages normally',
    });
    return false;
  }

  if (fault === 'closure-not-serializable') {
    const sc = b.std('spark-context');
    const dag = b.std('dag-scheduler');
    state.abortedByFault = true;
    const sSer = b.addStep({
      id: 'sim-step-not-serializable',
      title: 'SparkClosureCleaner fails before scheduling -> Task not serializable',
      explanation:
        'Before any task is dispatched to executors, SparkClosureCleaner.clean inspects the job closure on the driver using JavaSerializer unconditionally (SparkEnv.scala:734 constructs new JavaSerializer(conf) regardless of spark.serializer) and hits java.io.NotSerializableException. Spark throws SparkException: Task not serializable with 0 tasks scheduled.',
      category: 'sql-planning-aqe',
      keyState: 'FAILED pre-schedule: SparkException (Task not serializable | 0 tasks launched)',
      command: 'SparkClosureCleaner.clean(func) // JavaSerializer -> throws SparkException("Task not serializable")',
      detailsPassed: [
        { name: 'exception', value: 'SparkException: Task not serializable (cause: java.io.NotSerializableException)' },
        { name: 'closureSerializer', value: 'JavaSerializer unconditionally (SparkEnv.scala:734, regardless of spark.serializer)' },
        { name: 'tasksScheduled', value: '0 (failed on driver before TaskSet submission)' },
        { name: 'sourceRef', value: 'SparkClosureCleaner.scala:42-48; SparkEnv.scala:734' },
      ],
      deepDiveComponentId: 'dag-scheduler',
    });
    b.addEvent({
      stepId: sSer,
      from: sc,
      to: dag,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM SparkContext.clean',
      port: `${b.driverHost} in-JVM`,
      label: 'SparkClosureCleaner.clean',
      info: 'Driver cleans and tests Java serialization of the user closure unconditionally via JavaSerializer before submitting any stage.',
      size: 'in-memory',
      command: 'sc.clean(func, checkSerializable = true) // SparkEnv.closureSerializer = new JavaSerializer(conf)',
      fields: [{ name: 'serializer', value: 'JavaSerializer (unconditional; independent of spark.serializer)' }],
      senderState: 'Checking closure serializability',
      receiverState: 'Serializing captured closure fields with JavaSerializer',
    });
    b.addEvent({
      stepId: sSer,
      from: dag,
      to: sc,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM NotSerializableException',
      port: `${b.driverHost} in-JVM`,
      label: 'Task not serializable',
      info: 'JavaSerializer fails on unserializable driver field (NotSerializableException); SparkClosureCleaner throws SparkException("Task not serializable").',
      size: 'SparkException',
      command: 'throw new SparkException("Task not serializable", NotSerializableException)',
      fields: [
        { name: 'tasksDispatched', value: '0' },
        { name: 'cause', value: 'java.io.NotSerializableException' },
      ],
      senderState: 'Aborted before creating TaskSet',
      receiverState: 'Job failed immediately on driver',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'SparkException: Task not serializable (java.io.NotSerializableException)',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'launch-task-too-large') {
    const dag = b.std('dag-scheduler');
    const tsm = b.std('task-set-manager');
    state.abortedByFault = true;
    const sLarge = b.addStep({
      id: 'sim-step-launch-too-large',
      title: 'Serialized LaunchTask (142 MiB) >= spark.rpc.message.maxSize (128 MiB) -> abort',
      explanation:
        'A task closure captures a giant un-broadcast driver collection, producing a 142 MiB serialized TaskDescription. CoarseGrainedSchedulerBackend.DriverEndpoint.launchTasks checks serializedTask.limit() >= maxRpcMessageSize (128 MiB), refuses to send LaunchTask, and aborts the TaskSet.',
      category: 'sql-planning-aqe',
      keyState: 'ABORTED before LaunchTask: 142 MiB >= spark.rpc.message.maxSize (128 MiB)',
      command: 'DriverEndpoint.launchTasks() // serializedTask.limit() >= 134217728 -> taskSetManager.abort()',
      detailsPassed: [
        { name: 'serializedTaskBytes', value: '148897792 bytes (142 MiB)' },
        { name: 'spark.rpc.message.maxSize', value: '134217728 bytes (128 MiB default)' },
        { name: 'remediation', value: 'Use sc.broadcast() for large lookup tables or increase spark.rpc.message.maxSize' },
      ],
      deepDiveComponentId: 'driver-scheduler-backend',
    });
    b.addEvent({
      stepId: sLarge,
      from: dag,
      to: tsm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM TaskSet Submit',
      port: `${b.driverHost} in-JVM`,
      label: 'Submit TaskSet 0.0',
      info: 'DAGScheduler submits TaskSet 0.0 containing tasks with an oversized captured closure.',
      size: '142 MiB closure',
      command: 'taskScheduler.submitTasks(TaskSet 0.0)',
      fields: [{ name: 'serializedTaskSize', value: '142 MiB' }],
      senderState: 'Stage 0 submitted',
      receiverState: 'Preparing serialized TaskDescription',
    });
    b.addEvent({
      stepId: sLarge,
      from: state.schedBackend,
      to: tsm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM RPC Frame Guard',
      port: `${b.driverHost} in-JVM`,
      label: 'Task > 128 MiB -> abort',
      info: 'CoarseGrainedSchedulerBackend.launchTasks detects serializedTask.limit() (142 MiB) >= spark.rpc.message.maxSize (128 MiB) and aborts TaskSet without sending LaunchTask.',
      size: 'SparkException',
      command: 'taskSetManager.abort("Serialized task 0 was 148897792 bytes, exceeds spark.rpc.message.maxSize (128 MiB)")',
      fields: [
        { name: 'maxRpcMessageSize', value: '128 MiB' },
        { name: 'launchTaskSent', value: 'false' },
      ],
      senderState: 'Refused to send oversized LaunchTask RPC',
      receiverState: 'TaskSet aborted; ActiveJob failed',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Serialized task exceeded spark.rpc.message.maxSize (128 MiB)',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'all-executors-excluded') {
    const dag = b.std('dag-scheduler');
    const ts = b.std('task-scheduler');
    const tsm = b.std('task-set-manager');
    const producerExec = state.liveExecs[0];
    const isDyn = state.cfg.dynamicAllocation;
    state.abortedByFault = true;
    const sExcl = b.addStep({
      id: 'sim-step-all-execs-excluded',
      title: isDyn
        ? 'All executors excluded -> DA requests capacity -> 120s timeout expires -> abort'
        : 'All executors excluded (DA off) -> TaskSchedulerImpl aborts TaskSet immediately',
      explanation: isDyn
        ? 'With spark.excludeOnFailure.enabled=true and Dynamic Allocation ON, HealthTracker excludes every live executor. TaskSchedulerImpl (TaskSchedulerImpl.scala:700-725, 835-855) notifies ExecutorAllocationManager via dagScheduler.unschedulableTaskSetAdded to request new capacity and starts the timer for spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout (120s). When no unexcluded executor arrives before 120s expires, TaskSetManager.abortSinceCompletelyExcludedOnFailure() aborts the TaskSet.'
        : 'With spark.excludeOnFailure.enabled=true and Dynamic Allocation OFF (and no idle excluded executor to kill), HealthTracker excludes every live executor. TaskSchedulerImpl (TaskSchedulerImpl.scala:715-720) immediately invokes TaskSetManager.abortSinceCompletelyExcludedOnFailure() without waiting for unschedulableTaskSetTimeout.',
      category: 'sql-planning-aqe',
      keyState: isDyn
        ? `ABORTED after 120s unschedulableTaskSetTimeout: all ${state.liveExecs.length} live executor(s) excluded`
        : `ABORTED immediately (DA off): all ${state.liveExecs.length} live executor(s) excluded`,
      command: isDyn
        ? 'TaskSchedulerImpl -> unschedulableTaskSetAdded -> wait 120s -> abortSinceCompletelyExcludedOnFailure()'
        : 'TaskSchedulerImpl -> abortSinceCompletelyExcludedOnFailure() // immediate abort (DA off)',
      detailsPassed: [
        { name: 'spark.excludeOnFailure.enabled', value: 'true' },
        { name: 'dynamicAllocation', value: String(isDyn) },
        {
          name: 'abortTiming',
          value: isDyn
            ? 'Waits spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout (120s) while requesting DA capacity'
            : 'Immediate abort (DA off and no idle excluded executor)',
        },
        { name: 'excludedExecutors', value: state.liveExecs.join(', ') },
        { name: 'sourceRef', value: 'TaskSchedulerImpl.scala:700-725,835-855; TaskSetManager.scala:790-794' },
      ],
      deepDiveComponentId: 'task-set-manager',
    });
    b.addEvent({
      stepId: sExcl,
      from: producerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Task FAIL -> exclude exec',
      info: `Repeated task failures across ${state.liveExecs.join(', ')} update HealthTracker / TaskSetExcludeList until 0 schedulable executors remain.`,
      size: '1.8 KB',
      command: 'healthTracker.updateExcludedForFailedTask(host, execId, taskIndex)',
      fields: [
        { name: 'excludedCount', value: `${state.liveExecs.length} / ${state.liveExecs.length}` },
        { name: 'config', value: 'spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor' },
      ],
      senderState: 'Failed attempt reported',
      receiverState: 'All live executors now marked excluded for TaskSet 0.0',
      isError: true,
    });
    if (isDyn) {
      const eam = b.std('executor-allocation-manager');
      b.addEvent({
        stepId: sExcl,
        from: ts,
        to: eam,
        kind: 'local',
        channel: 'internal',
        protocol: 'In-JVM Unschedulable TaskSet',
        port: `${b.driverHost} in-JVM`,
        label: 'Request DA capacity (120s)',
        info: 'Dynamic Allocation is ON: TaskSchedulerImpl calls dagScheduler.unschedulableTaskSetAdded to ask ExecutorAllocationManager for fresh executors and starts the 120s spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout timer.',
        size: 'in-memory',
        command: 'dagScheduler.unschedulableTaskSetAdded(stageId, attemptId); startAbortTimer(120s)',
        fields: [{ name: 'unschedulableTaskSetTimeout', value: '120s (default)' }],
        senderState: 'Waiting up to 120s for unexcluded executor',
        receiverState: 'Attempting to provision replacement capacity',
      });
    }
    b.addEvent({
      stepId: sExcl,
      from: ts,
      to: tsm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM CompletelyExcluded',
      port: `${b.driverHost} in-JVM`,
      label: isDyn ? '120s expired -> abort' : 'Immediate abort (DA off)',
      info: isDyn
        ? '120s spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout expires with zero schedulable executors; TaskSetManager.abortSinceCompletelyExcludedOnFailure() aborts the TaskSet.'
        : 'DA is off and no idle excluded executor exists; TaskSchedulerImpl immediately invokes TaskSetManager.abortSinceCompletelyExcludedOnFailure().',
      size: 'SparkException',
      command: 'taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)',
      fields: [{ name: 'schedulableExecutors', value: '0' }],
      senderState: 'No eligible resource offers',
      receiverState: 'TaskSet marked zombie and aborted',
      isError: true,
    });
    b.addEvent({
      stepId: sExcl,
      from: tsm,
      to: dag,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM TaskSetFailed',
      port: `${b.driverHost} in-JVM`,
      label: 'TaskSetFailed -> JobFailed',
      info: 'DAGScheduler fails the active job after TaskSetManager aborts due to full executor exclusion.',
      size: 'in-memory',
      command: 'dagScheduler.taskSetFailed(taskSet, "all executors excluded")',
      fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException)' }],
      senderState: 'TaskSet aborted',
      receiverState: 'ActiveJob failed',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Aborting TaskSet: all executors excluded on failure',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'all-nodes-excluded') {
    const nmA = b.nmInstance('worker-node-a');
    const nmB = b.nmInstance('worker-node-b');
    const totalMaxFailures = Math.max(
      2 * (state.cfg.dynamicAllocation ? 6 : state.cfg.numExecutors),
      8,
    );
    state.abortedByFault = true;
    const sNodes = b.addStep({
      id: 'sim-step-all-nodes-excluded',
      title: 'YarnAllocatorNodeHealthTracker excludes 2/2 workers-label nodes (3 > 2 per host) -> AM EXIT 11',
      explanation:
        `With spark.yarn.executor.launch.excludeOnFailure.enabled=true (defaults to false!), YARN queue default-node-label-expression=workers covering [worker-a, worker-b], and spark.yarn.am.nodeLabelExpression=am-nodes placing the AM on driver-1, Hadoop DefaultAMSProcessor.java:351-365 reads the queue's default node label and reports amClient.getClusterNodeCount()=2. YarnAllocatorNodeHealthTracker.scala:91 checks strictly failuresOnHost > maxFailuresPerHost (> NOT >=): with spark.excludeOnFailure.application.maxFailedExecutorsPerNode=2, worker-a suffers 3 failures (3 > 2) and worker-b suffers 3 failures (3 > 2) = 6 total failures (< spark.executor.maxNumFailures=${totalMaxFailures}). AM reporter hits allocator.isAllNodeExcluded == true (numExcludedNodes=2 >= numClusterNodes=2 for queue default label=workers) and calls finish(FAILED, EXIT_MAX_EXECUTOR_FAILURES = 11).`,
      category: 'yarn-lifecycle',
      keyState: `FAILED (EXIT_MAX_EXECUTOR_FAILURES=11): 2/2 queue default-label=workers nodes excluded (3>2 on worker-a & worker-b; 6 < budget ${totalMaxFailures})`,
      command: 'ApplicationMaster.reporterThread // allocator.isAllNodeExcluded (2 >= 2 in queue default-label=workers) -> finish(FAILED, 11)',
      detailsPassed: [
        { name: 'spark.yarn.executor.launch.excludeOnFailure.enabled', value: 'true (default is false!)' },
        {
          name: 'queue default-node-label-expression',
          value: 'workers ([worker-a, worker-b]); AM placed on driver-1 via spark.yarn.am.nodeLabelExpression=am-nodes -> DefaultAMSProcessor reports 2 nodes',
        },
        {
          name: 'maxFailedExecutorsPerNode',
          value: '2 per host; strict > check (YarnAllocatorNodeHealthTracker.scala:91): 3 > 2 excludes worker-a, 3 > 2 excludes worker-b (6 total)',
        },
        { name: 'clusterNodesExcluded', value: '2 / 2 RM workers-label cluster nodes (numExcludedNodes=2 >= amClient.getClusterNodeCount()=2)' },
        { name: 'spark.executor.maxNumFailures', value: `${totalMaxFailures} (> 6 so isAllNodeExcluded branch is reached before total budget)` },
        { name: 'exitCode', value: '11 (ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES)' },
        { name: 'sourceRef', value: 'ApplicationMaster.scala:597-600; YarnAllocatorNodeHealthTracker.scala:91-112; DefaultAMSProcessor.java:351-365' },
      ],
      deepDiveComponentId: 'yarn-allocator',
    });
    b.addEvent({
      stepId: sNodes,
      from: nmA,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'worker-a fail (3>2, ex=1)',
      info: '3rd counted container launch failure (exitStatus=1 -> handleResourceAllocationFailure) on worker-a satisfies strict failuresOnHost (3) > maxFailedExecutorsPerNode (2) (YarnAllocatorNodeHealthTracker.scala:91); worker-a is excluded (1/2 workers-label cluster nodes).',
      size: 'ContainerStatus',
      command: `ContainerStatus(${b.execContainerId(1)}, COMPLETE, exitStatus=1) // worker-a failures=3 (> maxFailedExecutorsPerNode=2)`,
      fields: [
        { name: 'hostFailures', value: 'worker-a: 3 > 2 (excluded on 3rd failure)' },
        { name: 'exitStatus', value: '1 (counted allocation failure)' },
        { name: 'excludedClusterNodes', value: '1 / 2 workers-label nodes (label=workers)' },
      ],
      senderState: '3rd container launch failed on worker-a',
      receiverState: 'Reporting worker-a failures to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sNodes,
      from: nmB,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'worker-b fail (3>2, ex=1)',
      info: `3rd counted container failure (exitStatus=1) on worker-b satisfies strict failuresOnHost (3) > maxFailedExecutorsPerNode (2) (6 total failures < spark.executor.maxNumFailures=${totalMaxFailures}); both worker-a and worker-b (2/2 RM cluster nodes in label=workers, amClient.getClusterNodeCount()=2) are now excluded.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${b.execContainerId(2)}, COMPLETE, exitStatus=1) // worker-b failures=3 (> maxFailedExecutorsPerNode=2)`,
      fields: [
        { name: 'hostFailures', value: 'worker-b: 3 > 2 (excluded on 3rd failure)' },
        { name: 'totalVsMaxBudget', value: `6 / ${totalMaxFailures} (budget > 6 so isAllNodeExcluded trips first)` },
        { name: 'excludedClusterNodes', value: '2 / 2 RM cluster nodes in label=workers (amClient.getClusterNodeCount()=2)' },
      ],
      senderState: '3rd container launch failed on worker-b',
      receiverState: 'Reporting worker-b failures to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sNodes,
      from: state.alloc,
      to: state.activeAm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM isAllNodeExcluded',
      port: 'driver-1 in-JVM',
      label: 'AllNodesExcluded -> EXIT11',
      info: 'YarnAllocatorNodeHealthTracker sees numExcludedNodes (2) >= numClusterNodes (2) in YARN node-label partition workers=[worker-a, worker-b] (amClient.getClusterNodeCount()=2) with spark.yarn.executor.launch.excludeOnFailure.enabled=true; AM reporter detects isAllNodeExcluded == true and triggers EXIT_MAX_EXECUTOR_FAILURES (11).',
      size: 'exit code 11',
      command: 'finish(FinalApplicationStatus.FAILED, EXIT_MAX_EXECUTOR_FAILURES=11, "Due to executor failures all available nodes are excluded")',
      fields: [
        { name: 'isAllNodeExcluded', value: 'true (2/2 workers-label RM cluster nodes excluded)' },
        { name: 'exitCode', value: '11' },
      ],
      senderState: 'Zero unexcluded hosts left to allocate (2/2 excluded)',
      receiverState: 'AM unregistering FAILED (exit 11)',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      stepId: sNodes,
      origin: 'am-max-exec-failures',
      diagnostic: 'Due to executor failures all available nodes are excluded',
      stopLiveExecutors: false,
    });
    return true;
  }

  if (fault === 'max-executor-failures') {
    const nmA = b.nmInstance('worker-node-a');
    const isDyn = state.cfg.dynamicAllocation;
    const effectiveMaxExecs = isDyn ? 6 : state.cfg.numExecutors;
    const maxFailures = Math.max(2 * effectiveMaxExecs, 3);
    state.abortedByFault = true;
    const sMaxExec = b.addStep({
      id: 'sim-step-max-exec-fail',
      title: `Counted executor crashes (exit 1) hit spark.executor.maxNumFailures (${maxFailures}) -> AM EXIT 11`,
      explanation:
        `Repeated unexpected counted executor exits (exitStatus=1 entering YarnAllocator's default arm and invoking handleResourceAllocationFailure -> ExecutorFailureTracker -- explicitly NOT -104 PMEM / -103 VMEM, which do not increment ExecutorFailureTracker) reach spark.executor.maxNumFailures = max(2 * ${isDyn ? 'maxExecutors(6)' : `numExecutors(${state.cfg.numExecutors})`}, 3) = ${maxFailures}. The AM reporter thread aborts the application with EXIT_MAX_EXECUTOR_FAILURES (11) and FinalApplicationStatus.FAILED.`,
      category: 'yarn-lifecycle',
      keyState: `FAILED (EXIT_MAX_EXECUTOR_FAILURES=11): ${maxFailures}/${maxFailures} counted failures (exit 1) reached`,
      command: `ApplicationMaster.reporterThread // numExecutorsFailed (${maxFailures}) >= maxNumExecutorFailures (${maxFailures}) -> finish(FAILED, 11)`,
      detailsPassed: [
        {
          name: 'spark.executor.maxNumFailures',
          value: isDyn
            ? `${maxFailures} (default max(2 * spark.dynamicAllocation.maxExecutors=6, 3); NOT initialExecutors=${state.cfg.numExecutors})`
            : `${maxFailures} (default max(2 * numExecutors=${state.cfg.numExecutors}, 3))`,
        },
        {
          name: 'countedExitKind',
          value: 'exitStatus=1 (default bad-node arm -> handleResourceAllocationFailure -> ExecutorFailureTracker; NOT -104 PMEM)',
        },
        { name: 'exitCode', value: '11 (EXIT_MAX_EXECUTOR_FAILURES / SparkExitCode.EXCEED_MAX_EXECUTOR_FAILURES)' },
        { name: 'sourceRef', value: 'ApplicationMaster.scala:100,593-596; YarnAllocator.scala:900-922; ExecutorFailureTracker.scala:82-101' },
      ],
      deepDiveComponentId: 'application-master',
    });
    b.addEvent({
      stepId: sMaxExec,
      from: nmA,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: `Exec fail (${maxFailures}/${maxFailures})`,
      info: `Counted container failure #${maxFailures} (exitStatus=1 entering YarnAllocator default arm -> handleResourceAllocationFailure -> ExecutorFailureTracker; NOT -104 PMEM) reaches spark.executor.maxNumFailures (${maxFailures}).`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${b.execContainerId(1)}, COMPLETE, exitStatus=1) // failure #${maxFailures}`,
      fields: [
        { name: 'exitStatus', value: '1 (generic crash/bad-node arm -> ExecutorFailureTracker)' },
        { name: 'failureCount', value: `${maxFailures} / ${maxFailures}` },
        { name: 'exitCausedByApp', value: 'true' },
      ],
      senderState: 'Container exited with code 1 on worker-a',
      receiverState: 'RM reports completed container to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sMaxExec,
      from: state.alloc,
      to: state.activeAm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ExecutorFailureTracker',
      port: 'driver-1 in-JVM',
      label: 'maxExecFailures -> EXIT 11',
      info: `ExecutorFailureTracker reaches ceiling (${maxFailures}); AM reporter calls finish(FAILED, EXIT_MAX_EXECUTOR_FAILURES=11).`,
      size: 'exit code 11',
      command: `finish(FinalApplicationStatus.FAILED, 11, "Max number of executor failures (${maxFailures}) reached")`,
      fields: [
        { name: 'exitCode', value: '11 (EXIT_MAX_EXECUTOR_FAILURES)' },
        { name: 'maxNumExecutorFailures', value: String(maxFailures) },
      ],
      senderState: 'Stopped requesting replacement containers',
      receiverState: 'AM unregistering FAILED (exit 11)',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      stepId: sMaxExec,
      origin: 'am-max-exec-failures',
      diagnostic: `Max number of executor failures (${maxFailures}) reached`,
      stopLiveExecutors: false,
    });
    return true;
  }

  if (fault === 'yarn-app-kill') {
    const producerExec = state.liveExecs[0];
    const amNm = state.amNm ?? b.amNmInstance();
    const amContainerId = `container_e0${state.activeAttempt}_000001`;
    state.abortedByFault = true;
    const sKill = b.addStep({
      id: 'sim-step-yarn-app-kill',
      title: 'yarn application -kill -> RM AMLauncher stops AM via am-nm & worker NMs on :8031',
      explanation:
        'An operator executes yarn application -kill <appId> on RM :8032. ResourceManager transitions the application to KILLING / FinalApplicationStatus.KILLED. Following Hadoop 3.4.1 (AMLauncher.java:142-151), RM AMLauncher sends ContainerManagementProtocol.stopContainers(amContainerId) directly to the AM host NodeManager (am-nm), which terminates the AM container (SIGTERM -> SIGKILL 137). RM also instructs worker NodeManagers via NM-RM ResourceTracker heartbeats (:8031, containersToCleanup / applicationsToCleanup) to stop executor containers (KILLED_BY_RESOURCEMANAGER=-106 for RM-directed stop; KILLED_AFTER_APP_COMPLETION=-107 for post-app-finish NM cleanup; OS SIGTERM -> SIGKILL 137; Spark exitCausedByApp=false).',
      category: 'yarn-lifecycle',
      keyState: 'KILLED (FinalApplicationStatus.KILLED | AMLauncher.stopContainers -> am-nm | worker NM stop :8031 -106/-107)',
      command: 'yarn application -kill application_1740_0051',
      detailsPassed: [
        { name: 'finalApplicationStatus', value: 'FinalApplicationStatus.KILLED' },
        {
          name: 'amTermination',
          value: `RM AMLauncher -> am-nm ContainerManagementProtocol.stopContainers(${amContainerId}) (Hadoop 3.4.1 AMLauncher.java:142-151)`,
        },
        { name: 'nmNotification', value: 'NM-RM nodeHeartbeat (:8031) containersToCleanup -> -106 (RM stop) / -107 (post-finish cleanup)' },
        { name: 'osAndSparkAttribution', value: 'OS SIGTERM -> SIGKILL (137); Spark exitCausedByApp=false (NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS)' },
        { name: 'sourceRef', value: 'Hadoop 3.4.1 AMLauncher.java:142-151; YarnAllocator.scala:1058-1064 (-106, -107)' },
      ],
      deepDiveComponentId: 'resource-manager',
    });
    b.addEvent({
      stepId: sKill,
      from: state.schedBackend,
      to: producerExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: 'LaunchTask (in-flight)',
      info: `Tasks are actively running across ${state.liveExecs.length} executor(s) when external kill arrives.`,
      size: '2.2 KB',
      command: 'executorEndpoint.send(LaunchTask(serializedTask))',
      fields: [{ name: 'activeExecutors', value: String(state.liveExecs.length) }],
      senderState: 'Stage in progress',
      receiverState: 'Executing tasks before kill arrives',
    });
    b.addEvent({
      stepId: sKill,
      from: state.submit,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ApplicationClientProtocol)',
      port: 'rm01:8032',
      label: 'yarn application -kill',
      info: 'Operator sends ForceKillApplicationRequest(application_1740_0051) to RM :8032; RM transitions application to KILLING / FinalApplicationStatus.KILLED.',
      size: '540 B',
      command: 'yarn application -kill application_1740_0051',
      fields: [
        { name: 'applicationId', value: 'application_1740_0051' },
        { name: 'finalApplicationStatus', value: 'FinalApplicationStatus.KILLED' },
      ],
      senderState: 'Kill request accepted by RM',
      receiverState: 'Transitioning application to KILLING / KILLED',
      isError: true,
    });
    b.addEvent({
      stepId: sKill,
      from: state.rm,
      to: amNm,
      channel: 'error',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: 'driver-1:dynamic',
      label: 'AMLauncher.stopContainers',
      info: `RM AMLauncher (Hadoop 3.4.1 AMLauncher.java:142-151) sends ContainerManagementProtocol.stopContainers(${amContainerId}) to driver-1 NodeManager (am-nm) to terminate the AM container.`,
      size: '820 B',
      command: `AMLauncher.cleanup() -> cmProxy.stopContainers(StopContainersRequest(${amContainerId}))`,
      fields: [
        { name: 'amContainerId', value: amContainerId },
        { name: 'sourceRef', value: 'Hadoop 3.4.1 AMLauncher.java:142-151' },
        { name: 'finalStatus', value: 'FinalApplicationStatus.KILLED' },
      ],
      senderState: 'AMLauncher dispatched stopContainers to am-nm',
      receiverState: `am-nm stopping AM container ${amContainerId}`,
      isError: true,
    });
    b.addEvent({
      stepId: sKill,
      from: amNm,
      to: state.activeAm,
      kind: 'local',
      channel: 'error',
      protocol: 'ContainerExecutor SIGTERM/KILL',
      port: 'driver-1 OS',
      label: 'Stop AM container (137)',
      info: `driver-1 NodeManager (am-nm) terminates ${amContainerId} via ContainerExecutor (SIGTERM -> SIGKILL 137 after sleep-delay-before-sigkill.ms).`,
      size: 'SIGTERM -> SIGKILL',
      command: `ContainerExecutor.signalContainer(${amContainerId}, SIGTERM -> SIGKILL 137)`,
      fields: [
        { name: 'amContainerId', value: amContainerId },
        { name: 'signalEscalation', value: 'SIGTERM -> SIGKILL (137)' },
      ],
      senderState: 'Reaped AM container on driver-1',
      receiverState: 'AM container terminated (KILLED)',
      isError: true,
    });
    const hostZones = Array.from(
      new Set(state.liveExecs.map(id => b.instanceWorkerHost(id).hostZone)),
    );
    for (const hz of hostZones) {
      const nm = b.nmInstance(hz);
      const host = hz === 'worker-node-a' ? 'worker-a' : 'worker-b';
      b.addEvent({
        stepId: sKill,
        from: state.rm,
        to: nm,
        channel: 'error',
        protocol: 'Hadoop RPC (ResourceTracker :8031)',
        port: 'rm01:8031',
        label: 'NM heartbeat cleanup -106',
        info: `RM responds to ${host} NM nodeHeartbeat (:8031) with containersToCleanup / applicationsToCleanup: RM-directed stop reports KILLED_BY_RESOURCEMANAGER (-106), post-app-finish NM cleanup reports KILLED_AFTER_APP_COMPLETION (-107), OS escalates SIGTERM -> SIGKILL (137), and Spark marks exitCausedByApp=false.`,
        size: '1.2 KB',
        command: 'NodeHeartbeatResponse(containersToCleanup, applicationsToCleanup) // -106 / -107',
        fields: [
          { name: 'rmStopStatus', value: '-106 (KILLED_BY_RESOURCEMANAGER)' },
          { name: 'postFinishCleanupStatus', value: '-107 (KILLED_AFTER_APP_COMPLETION)' },
          { name: 'osCleanupSignal', value: 'SIGTERM -> SIGKILL (137); Spark exitCausedByApp=false' },
        ],
        senderState: `Dispatched container cleanup to ${host} NM via :8031 heartbeat`,
        receiverState: `Killed executor containers on ${host}`,
        isError: true,
      });
    }
    return true;
  }

  if (fault === 'rpc-ask-timeout') {
    const rpcEnv = b.std('rpc-env');
    const tsm = b.std('task-set-manager');
    const producerExec = state.liveExecs[0];
    const producerBm = state.liveBms[0];
    const producerHost = b.instanceWorkerHost(producerExec).hostName;
    state.abortedByFault = true;
    const sRpc = b.addStep({
      id: 'sim-step-rpc-timeout',
      title: 'Executor askSync future expires after 120s -> RpcTimeoutException -> job fails',
      explanation:
        `The caller (${producerBm} on ${producerHost}) owns the synchronous RpcEndpointRef.askSync timeout future. When driver NettyRpcEnv (${b.driverRpc}) stalls in a long GC pause or saturated event loop past spark.rpc.askTimeout / spark.network.timeout (120s), the future expires ON the executor with org.apache.spark.rpc.RpcTimeoutException (cause: TimeoutException), retries exhaust, and the active task/job fails.`,
      category: 'heartbeats-metrics',
      keyState: `FAILED: RpcTimeoutException expired on caller ${producerBm} (${producerHost}) after 120s`,
      command: 'rpcEndpointRef.askSync(UpdateBlockInfo, timeout=120s) // future expires on executor',
      detailsPassed: [
        { name: 'timeoutOwner', value: `Caller executor ${producerBm} on ${producerHost} (NOT driver)` },
        { name: 'spark.rpc.askTimeout', value: '120s (defaults to spark.network.timeout)' },
        { name: 'exception', value: 'org.apache.spark.rpc.RpcTimeoutException (cause: java.util.concurrent.TimeoutException)' },
        { name: 'sourceRef', value: 'RpcTimeout.scala:31; NettyRpcEnv.scala' },
      ],
      deepDiveComponentId: 'rpc-env',
    });
    b.addEvent({
      stepId: sRpc,
      from: producerBm,
      to: rpcEnv,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (askSync)',
      port: b.driverRpc,
      label: 'askSync(UpdateBlockInfo)',
      info: `${producerBm} on ${producerHost} sends synchronous RPC askSync to driver NettyRpcEnv (${b.driverRpc}).`,
      size: '840 B',
      command: 'blockManagerMaster.driverEndpoint.askSync[Boolean](UpdateBlockInfo(...))',
      fields: [
        { name: 'timeout', value: '120s (spark.rpc.askTimeout)' },
        { name: 'driverState', value: 'Driver JVM stalled in long GC / saturated event loop' },
      ],
      senderState: `Caller ${producerBm} waiting on 120s askSync future`,
      receiverState: 'Reply delayed beyond 120s deadline',
    });
    b.addEvent({
      stepId: sRpc,
      from: producerBm,
      to: producerBm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Caller RpcTimeout',
      port: `${producerHost} in-JVM`,
      label: 'RpcTimeoutException 120s',
      info: `120s deadline expires ON the caller executor (${producerBm} on ${producerHost}); NettyRpcEnv completes the caller's askSync future with org.apache.spark.rpc.RpcTimeoutException (cause: TimeoutException).`,
      size: 'RpcTimeoutException',
      command: 'throw new RpcTimeoutException("Futures timed out after [120 seconds]", TimeoutException)',
      fields: [
        { name: 'expiredOn', value: `${producerBm} (${producerHost} caller JVM)` },
        { name: 'exception', value: 'org.apache.spark.rpc.RpcTimeoutException' },
        { name: 'config', value: 'spark.rpc.askTimeout / spark.network.timeout (120s)' },
      ],
      senderState: 'Caller RPC askSync future timed out after 120s',
      receiverState: 'RPC retries exhausted on executor; failing task',
      isError: true,
    });
    b.addEvent({
      stepId: sRpc,
      from: producerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Retries exhaust -> abort',
      info: `RPC retries exhaust and repeated task attempts fail with RpcTimeoutException; TaskSetManager aborts TaskSet and fails the active job in ${b.appEntryLabel}.`,
      size: '1.9 KB',
      command: 'StatusUpdate(FAILED, ExceptionFailure("RpcTimeoutException: 120s")) -> TaskSet aborted',
      fields: [{ name: 'jobOutcome', value: 'JobFailed(RpcTimeoutException)' }],
      senderState: 'Task attempts failed after RPC timeout exhaustion',
      receiverState: `TaskSet aborted; ${b.appEntryLabel} failing`,
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'org.apache.spark.rpc.RpcTimeoutException (120s) exhausted retries',
      stopLiveExecutors: true,
    });
    return true;
  }

  return false;
}

// =============================================================================
// PHASE 6: Shuffle-Plane & DAG-Shuffle Reduce Faults
// Returns true when it handles the reduce fetch step completely.
// =============================================================================
export function tryInjectShuffleReduceFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
  ctx: ShuffleReduceContext,
): boolean {
  const fault = state.cfg.fault;
  const dag = b.std('dag-scheduler');
  const motm = b.std('map-output-tracker-master');
  const shuffleReader = b.workerStd('shuffle-reader', ctx.reduceConsumer.hostZone);
  const motw = b.workerStd('map-output-tracker-worker', ctx.reduceConsumer.hostZone);
  const essNode = state.cfg.externalShuffleService ? b.essInstance(ctx.producerZone) : null;
  const shuffleServer = essNode ?? ctx.producerBm;

  if (fault === 'shuffle-fetch-failed') {
    const retiredExecs: string[] = [];
    if (state.cfg.externalShuffleService) {
      retiredExecs.push(...retireHostExecutors(b, state, ctx.producerZone));
    } else {
      const deadIdx = state.liveExecs.indexOf(ctx.producerExec);
      if (deadIdx !== -1) {
        retiredExecs.push(state.liveExecs[deadIdx]);
        state.liveExecs.splice(deadIdx, 1);
        state.liveBms.splice(deadIdx, 1);
        state.liveExecNums.splice(deadIdx, 1);
      }
    }

    let pendingRepl: { num: number; exec: string; bm: string; cid: string; host: string; zone: 'worker-node-a' | 'worker-node-b' } | null = null;
    if (state.liveExecs.length === 0) {
      const fallbackZone: 'worker-node-a' | 'worker-node-b' =
        ctx.producerZone === 'worker-node-a' ? 'worker-node-b' : 'worker-node-a';
      const fallbackHost = fallbackZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
      const { execNum: replNum, containerId: replCid } = b.allocateExec(state.activeAttempt);
      state.nextExecNum = b.peekNextExecNum(state.activeAttempt);
      const replExec = b.execReplInstance(replNum, fallbackZone, state.activeAttempt, replCid);
      const replBm = b.execReplBmInstance(replNum, fallbackZone, state.activeAttempt, replCid);
      state.liveExecs.push(replExec);
      state.liveBms.push(replBm);
      state.liveExecNums.push(replNum);
      pendingRepl = { num: replNum, exec: replExec, bm: replBm, cid: replCid, host: fallbackHost, zone: fallbackZone };
    }
    const recomputeExec = state.liveExecs[0];
    const recomputeHost = b.instanceWorkerHost(recomputeExec).hostName;

    const sFf = b.addStep({
      id: 'sim-step-fetchfailed',
      title: state.cfg.externalShuffleService
        ? `ESS (:7337) survives producer exit; ${ctx.producerHost} loss retires ${retiredExecs.length} exec(s) & reruns mapId=${state.recomputedMapId}`
        : `Producer ${ctx.producerExec} lost -> FetchFailed -> invalidate mapIndex=${state.targetPart} -> mapId=${state.recomputedMapId} rerun`,
      explanation: (state.cfg.externalShuffleService
        ? `With External Shuffle Service ON (:7337), ${ctx.producerExec} exiting alone STILL lets reducers fetch intact shuffle files from ${ctx.producerHost}:7337. A subsequent ${ctx.producerHost} node/disk failure causes FetchFailed, retires ALL executors on ${ctx.producerZone} (${retiredExecs.join(', ')}), invalidates ${ctx.producerHost} map outputs, recomputes missing logical mapIndex=${state.targetPart} on surviving ${recomputeExec} (${recomputeHost}) under new physical mapId=${state.recomputedMapId}, and retries ResultStage 1.1.`
        : `With ESS OFF, losing ${ctx.producerExec} breaks ${ctx.shuffleServerPort} fetches -> ShuffleBlockFetcherIterator reports FetchFailed -> DAGScheduler invalidates MapOutputTrackerMaster (bumping epoch), resubmits ONLY lost logical mapIndex=${state.targetPart} under new physical attempt mapId=${state.recomputedMapId} on surviving ${recomputeExec} (${recomputeHost}), and retries ResultStage 1.1 (counted against stage-attempt limits, NOT spark.task.maxFailures).`) + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: `FetchFailed recovered on ${recomputeExec} (${recomputeHost}) via mapId=${state.recomputedMapId} & ResultStage 1.1`,
      command: state.cfg.externalShuffleService
        ? `DAGScheduler.handleTaskCompletion(FetchFailed) -> removeExecutor(${ctx.producerHost}) -> unregisterAllMapOutput("${ctx.producerHost}")`
        : 'DAGScheduler.handleTaskCompletion(FetchFailed) -> unregisterMapOutput -> scheduleResubmit()',
      detailsPassed: [
        { name: 'externalShuffleService', value: state.cfg.externalShuffleService ? 'ON (:7337)' : 'OFF' },
        { name: 'retiredExecutors', value: retiredExecs.join(', ') },
        { name: 'survivingExecutors', value: state.liveExecs.join(', ') },
        { name: 'oldPhysicalBlock', value: `shuffle_0_${state.initialMapId}_${state.targetPart} (lost; mapIndex=${state.targetPart})` },
        { name: 'newPhysicalBlock', value: `shuffle_0_${state.recomputedMapId}_${state.targetPart} (recomputed on ${recomputeExec})` },
        { name: 'retryBudget', value: 'Stage attempt limit (4), NOT spark.task.maxFailures' },
      ],
      deepDiveComponentId: 'dag-scheduler',
    });
    if (essNode) {
      b.addEvent({
        stepId: sFf,
        from: shuffleReader,
        to: essNode,
        channel: 'data',
        protocol: 'BlockTransfer -> ESS :7337',
        port: `${ctx.producerHost}:7337`,
        label: 'ESS serves after Exec exit',
        info: `Even after ${ctx.producerExec} exits, ${ctx.producerHost} NM External Shuffle Service (:7337) serves intact local shuffle files cleanly.`,
        size: '4.6 MiB OK',
        command: `ExternalBlockStoreClient.fetchBlocks("${ctx.producerHost}", 7337, "${ctx.producerExecNum}", ["shuffle_0_${state.initialMapId}_${state.targetPart}"])`,
        fields: [{ name: 'essBenefit', value: 'Decouples shuffle serving from executor JVM lifetime' }],
        senderState: 'Fetch succeeded via ESS',
        receiverState: 'Served file segment from local disk',
      });
    }
    b.addEvent({
      stepId: sFf,
      from: shuffleReader,
      to: shuffleServer,
      channel: 'error',
      protocol: 'BlockTransfer (IOException)',
      port: ctx.shuffleServerPort,
      label: state.cfg.externalShuffleService ? 'Node loss -> FetchFailed' : 'FetchFailed (Exec-1 lost)',
      info: state.cfg.externalShuffleService
        ? `${ctx.producerHost} node/disk becomes unreachable -> retries exhausted -> FetchFailedException.`
        : `${ctx.producerExec} died -> ${ctx.shuffleServerPort} unreachable -> retries exhausted -> FetchFailedException.`,
      size: 'IOException',
      command: `throw new FetchFailedException(bmAddress, shuffleId=0, mapId=${state.initialMapId}, mapIndex=${state.targetPart}, reduceId=${state.targetPart})`,
      fields: [
        { name: 'failedAddress', value: ctx.shuffleServerPort },
        { name: 'mapIndex_vs_mapId', value: `logical mapIndex=${state.targetPart}, physical mapId=${state.initialMapId}` },
      ],
      senderState: 'Task reports FetchFailed to driver',
      receiverState: 'Producer unreachable',
      isError: true,
    });
    if (state.cfg.externalShuffleService && retiredExecs.length > 0) {
      b.addEvent({
        stepId: sFf,
        from: state.alloc,
        to: state.schedBackend,
        channel: 'error',
        protocol: 'Spark Netty RPC (RemoveExecutor)',
        port: b.driverRpc,
        label: `Retire ${ctx.producerHost} (${retiredExecs.length} exec${retiredExecs.length > 1 ? 's' : ''})`,
        info: `Lost host ${ctx.producerHost} retires ALL executors hosted on ${ctx.producerZone} (${retiredExecs.join(', ')}) from active executorDataMap.`,
        size: `${retiredExecs.length} removed`,
        command: `driverRef.send(RemoveExecutors([${retiredExecs.join(', ')}], SlaveLost("Host ${ctx.producerHost} lost")))`,
        fields: [
          { name: 'lostHost', value: `${ctx.producerHost} (${ctx.producerZone})` },
          { name: 'retiredExecutors', value: retiredExecs.join(', ') },
          { name: 'survivingExecutors', value: state.liveExecs.join(', ') },
        ],
        senderState: `Retired ${retiredExecs.length} ${ctx.producerHost} executor(s)`,
        receiverState: `Removed ${retiredExecs.join(', ')} from live pool`,
        isError: true,
      });
    }
    b.addEvent({
      stepId: sFf,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Invalidate + Epoch',
      port: `${b.driverHost} in-JVM`,
      label: `unregisterMapOutput(${state.targetPart})`,
      info: state.cfg.externalShuffleService
        ? `DAGScheduler invalidates all map outputs on lost host ${ctx.producerHost} (including mapIndex=${state.targetPart}), bumps epoch, and schedules Stage 0.1 -> Stage 1.1.`
        : `DAGScheduler invalidates lost logical mapIndex=${state.targetPart}, bumps epoch, and schedules Stage 0.1 -> Stage 1.1.`,
      size: 'in-memory',
      command: state.cfg.externalShuffleService
        ? `mapOutputTracker.unregisterAllMapOutput("${ctx.producerHost}"); scheduleResubmit()`
        : `mapOutputTracker.unregisterMapOutput(0, ${state.targetPart}, bmAddress); scheduleResubmit()`,
      fields: [
        { name: 'newEpoch', value: '2' },
        { name: 'resubmit', value: `ShuffleMapStage 0.1 (missing mapIndex=${state.targetPart}) -> ResultStage 1.1` },
      ],
      senderState: `Recomputing missing partition on surviving ${recomputeExec} (${recomputeHost})`,
      receiverState: 'Stale location evicted',
    });
    if (pendingRepl) {
      const nmFallback = b.nmInstance(pendingRepl.zone);
      const replCid = pendingRepl.cid;
      b.addEvent({
        stepId: sFf,
        from: state.alloc,
        to: nmFallback,
        channel: 'yarn',
        protocol: 'Hadoop RPC (ContainerManagementProtocol)',
        port: `${pendingRepl.host}:dynamic`,
        label: 'startContainer(Exec-repl)',
        info: `All previous executors were on lost ${ctx.producerHost}; YarnAllocator launches replacement ${replCid} on ${pendingRepl.host}.`,
        size: '9.4 KB',
        command: `nmClient.startContainer(${replCid}, ctx[exec=${pendingRepl.num}])`,
        fields: [{ name: 'replacementExecutor', value: pendingRepl.exec }],
        senderState: `Dispatched ${pendingRepl.host} replacement container`,
        receiverState: `Booting replacement executor JVM on ${pendingRepl.host}`,
      });
      b.addEvent({
        stepId: sFf,
        from: pendingRepl.exec,
        to: state.schedBackend,
        channel: 'rpc',
        protocol: 'Spark Netty RPC',
        port: b.driverRpc,
        label: 'RegisterExecutor(repl)',
        info: `Replacement executor ${pendingRepl.exec} on ${pendingRepl.host} registers ${state.cfg.executorCores} core(s) to recompute missing shuffle partitions.`,
        size: '1.8 KB',
        command: `ref.ask[Boolean](RegisterExecutor("${pendingRepl.num}", self, "${pendingRepl.host}", ${state.cfg.executorCores}))`,
        fields: [
          { name: 'executorId', value: `"${pendingRepl.num}"` },
          { name: 'host', value: pendingRepl.host },
        ],
        senderState: `${pendingRepl.exec} ready on ${pendingRepl.host}`,
        receiverState: `Registered ${pendingRepl.host} replacement (${state.liveExecs.length} live)`,
      });
    }
    b.addEvent({
      stepId: sFf,
      from: recomputeExec,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (MapStatus)',
      port: b.driverRpc,
      label: `Recompute mapId=${state.recomputedMapId} OK`,
      info: `Surviving ${recomputeExec} (${recomputeHost}) recomputes logical mapIndex=${state.targetPart} under new physical attempt mapId=${state.recomputedMapId} (shuffle_0_${state.recomputedMapId}_${state.targetPart}); Stage 1.1 succeeds.`,
      size: '640 B',
      command: `registerMapOutput(shuffleId=0, mapIndex=${state.targetPart}, MapStatus(${recomputeHost}, mapId=${state.recomputedMapId}))`,
      fields: [
        { name: 'recomputeExecutor', value: `${recomputeExec} (${recomputeHost})` },
        { name: 'newBlock', value: `shuffle_0_${state.recomputedMapId}_${state.targetPart}` },
      ],
      senderState: 'Recomputed map output served cleanly',
      receiverState: 'ResultStage 1.1 completes',
    });
    return true;
  }

  if (fault === 'stage-max-attempts') {
    state.abortedByFault = true;
    const sMaxStage = b.addStep({
      id: 'sim-step-stage-max-attempts',
      title: 'Repeated FetchFailed exhausts spark.stage.maxConsecutiveAttempts (4) -> stage aborted',
      explanation:
        'Consecutive reduce stage attempts (1.0, 1.1, 1.2, 1.3) hit FetchFailedException due to persistent shuffle fetch failures. Once fetchFailedAttemptIds.size reaches spark.stage.maxConsecutiveAttempts (4), DAGScheduler.abortStage() aborts Stage 1 and fails the job.' + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: 'ABORTED: Stage 1 failed maximum consecutive attempts (4/4)',
      command: 'DAGScheduler.handleTaskCompletion(FetchFailed) // fetchFailedAttemptIds.size >= 4 -> abortStage()',
      detailsPassed: [
        { name: 'spark.stage.maxConsecutiveAttempts', value: '4 (default)' },
        { name: 'failedAttempts', value: '1.0, 1.1, 1.2, 1.3 (4 consecutive FetchFailed)' },
        { name: 'exception', value: 'SparkException: Stage 1 has failed the maximum allowable number of times: 4' },
      ],
      deepDiveComponentId: 'dag-scheduler',
    });
    b.addEvent({
      stepId: sMaxStage,
      from: shuffleReader,
      to: shuffleServer,
      channel: 'error',
      protocol: 'BlockTransfer (FetchFailed x4)',
      port: ctx.shuffleServerPort,
      label: 'FetchFailed (attempt 4/4)',
      info: `Reduce stage attempt 1.3 hits persistent FetchFailedException fetching from ${ctx.shuffleServerPort}.`,
      size: 'FetchFailed',
      command: `throw new FetchFailedException(bmAddress, shuffleId=0, mapId=${state.initialMapId}, reduceId=${state.targetPart}) // attempt 4`,
      fields: [
        { name: 'stageAttempt', value: '1.3 (4th consecutive fetch failure)' },
        { name: 'limit', value: 'spark.stage.maxConsecutiveAttempts = 4' },
      ],
      senderState: 'Reporting 4th FetchFailed to DAGScheduler',
      receiverState: 'Shuffle server persistently failing fetches',
      isError: true,
    });
    b.addEvent({
      stepId: sMaxStage,
      from: ctx.reduceConsumer.consumerExec,
      to: dag,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'StatusUpdate(FetchFailed)',
      info: 'DAGScheduler adds attempt 3 to fetchFailedAttemptIds (size == 4 >= maxConsecutiveStageAttempts).',
      size: '1.1 KB',
      command: 'dagScheduler.handleTaskCompletion(FetchFailed) // size == 4 -> abortStage(ResultStage 1)',
      fields: [{ name: 'fetchFailedAttemptIds', value: '[0, 1, 2, 3]' }],
      senderState: 'Stage 1.3 failed',
      receiverState: 'DAGScheduler invoking abortStage(ResultStage 1)',
      isError: true,
    });
    b.addEvent({
      stepId: sMaxStage,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM abortStage',
      port: `${b.driverHost} in-JVM`,
      label: 'abortStage(1): 4/4 fails',
      info: 'DAGScheduler aborts Stage 1 with SparkException: Stage 1 has failed the maximum allowable number of times: 4.',
      size: 'SparkException',
      command: 'abortStage(ResultStage 1, "Stage 1 has failed the maximum allowable number of times: 4")',
      fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException)' }],
      senderState: 'ActiveJob failed',
      receiverState: 'Shuffle 0 abandoned',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Stage 1 has failed the maximum allowable number of times: 4',
      stopLiveExecutors: true,
    });
    return true;
  }

  if (fault === 'indeterminate-stage-rollback') {
    const recomputeExec = state.liveExecs[state.liveExecs.length - 1];
    const recomputeHost = b.instanceWorkerHost(recomputeExec).hostName;
    const sIndet = b.addStep({
      id: 'sim-step-indet-rollback',
      title: 'Statically INDETERMINATE map stage (!checksumMismatchFullRetryEnabled) -> full rollback',
      explanation:
        'Per DAGScheduler.scala:4289-4303, 4875-4907 and ShuffleExchangeExec.scala:607-615, lost shuffle output belongs to a statically INDETERMINATE map stage (mapStage.isStaticallyIndeterminate=true) with spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false (defaults to true; setting spark.shuffle.checksum.enabled=false alone does NOT unset checksumMismatchFullRetryEnabled!). Because !checksumMismatchFullRetryEnabled, DAGScheduler immediately invokes rollbackSucceedingStages(mapStage, rollbackCurrentStage=true), unregisters ALL shuffle outputs via MapOutputTrackerMaster.unregisterAllMapAndMergeOutput(0), and recomputes all partitions of ShuffleMapStage 0.1 + ResultStage 1.1. (When enableFullRetryOnMismatch=true, Spark recomputes the lost map partition first and triggers full rollback only if order-independent checksum verification detects a mismatch on recompute.)' + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: 'Statically INDETERMINATE + enableFullRetryOnMismatch=false -> full Stage 0.1 + Stage 1.1 recompute OK',
      command: 'if (isStaticallyIndeterminate && !checksumMismatchFullRetryEnabled) rollbackSucceedingStages(mapStage, rollbackCurrentStage=true)',
      detailsPassed: [
        { name: 'deterministicLevel', value: 'DeterministicLevel.INDETERMINATE (mapStage.isStaticallyIndeterminate=true)' },
        {
          name: 'checksumGuardConfig',
          value: 'spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false (default true; disabling spark.shuffle.checksum.enabled alone does NOT unset checksumMismatchFullRetryEnabled)',
        },
        { name: 'rollbackScope', value: 'MapOutputTrackerMaster.unregisterAllMapAndMergeOutput(0) + full ShuffleMapStage 0.1 + ResultStage 1.1' },
        { name: 'sourceRef', value: 'DAGScheduler.scala:4289-4303,4875-4907; ShuffleExchangeExec.scala:607-615' },
      ],
      deepDiveComponentId: 'dag-scheduler',
    });
    b.addEvent({
      stepId: sIndet,
      from: shuffleReader,
      to: shuffleServer,
      channel: 'error',
      protocol: 'BlockTransfer (FetchFailed)',
      port: ctx.shuffleServerPort,
      label: 'FetchFailed (INDETERM)',
      info: `Reducer hits FetchFailed on partition ${state.targetPart} belonging to statically INDETERMINATE ShuffleMapStage 0 with spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false (!checksumMismatchFullRetryEnabled).`,
      size: 'FetchFailed',
      command: `throw new FetchFailedException(bmAddress, shuffleId=0, mapId=${state.initialMapId}, reduceId=${state.targetPart})`,
      fields: [
        { name: 'deterministicLevel', value: 'Statically INDETERMINATE' },
        {
          name: 'checksumMismatchFullRetryEnabled',
          value: 'false (spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false)',
        },
      ],
      senderState: 'Reported FetchFailed to DAGScheduler',
      receiverState: 'Map output unavailable',
      isError: true,
    });
    b.addEvent({
      stepId: sIndet,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Full Stage Rollback',
      port: `${b.driverHost} in-JVM`,
      label: 'rollbackCurrentStage=true',
      info: 'DAGScheduler sees mapStage.isStaticallyIndeterminate && !checksumMismatchFullRetryEnabled (from spark.sql.shuffle.orderIndependentChecksum.enableFullRetryOnMismatch=false; disabling spark.shuffle.checksum.enabled alone does NOT unset it), calls rollbackSucceedingStages(mapStage, rollbackCurrentStage=true), and clears ALL map outputs via MapOutputTrackerMaster.unregisterAllMapAndMergeOutput(0).',
      size: 'in-memory',
      command: 'mapOutputTracker.unregisterAllMapAndMergeOutput(0); resubmitStage(ShuffleMapStage 0.1)',
      fields: [
        { name: 'invalidatedPartitions', value: `All ${state.cfg.numPartitions} map partitions` },
        {
          name: 'checksumMismatchFullRetryEnabled',
          value: 'false (enableFullRetryOnMismatch=false; with default true, rollback triggers on recompute checksum mismatch)',
        },
        { name: 'rollbackCurrentStage', value: 'true' },
      ],
      senderState: 'Rolling back succeeding ResultStage 1 and all Stage 0 outputs',
      receiverState: 'Epoch bumped; all shuffle 0 MapStatuses cleared',
    });
    b.addEvent({
      stepId: sIndet,
      from: recomputeExec,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (MapStatus)',
      port: b.driverRpc,
      label: `Recompute all ${state.cfg.numPartitions} maps`,
      info: `All ${state.cfg.numPartitions} partitions of ShuffleMapStage 0.1 are recomputed consistently across live executors; ResultStage 1.1 fetches fresh blocks and succeeds.`,
      size: '1.8 KB',
      command: `registerMapOutputs(shuffleId=0, statuses[0..${state.cfg.numPartitions - 1}]) // host=${recomputeHost}`,
      fields: [
        { name: 'recomputedStage', value: 'ShuffleMapStage 0.1 (full)' },
        { name: 'reduceAttempt', value: 'ResultStage 1.1 SUCCEEDED' },
      ],
      senderState: 'Full indeterminate map stage recomputed cleanly',
      receiverState: 'Consistent shuffle 0 outputs published',
    });
    return true;
  }

  if (fault === 'metadata-fetch-failed') {
    const recomputeExec = state.liveExecs[0];
    const recomputeHost = b.instanceWorkerHost(recomputeExec).hostName;
    const sMeta = b.addStep({
      id: 'sim-step-metadata-ff',
      title: `MapOutputTracker missing status for mapIndex=${state.targetPart} -> MetadataFetchFailedException`,
      explanation:
        `Reducer MapOutputTrackerWorker fetches shuffle 0 statuses from MapOutputTrackerMaster; validateStatus() detects a null MapStatus entry for logical mapIndex=${state.targetPart} and throws MetadataFetchFailedException. DAGScheduler resubmits the missing map task and retries ResultStage 1.1.` + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: `MetadataFetchFailed recovered via ShuffleMapStage 0.1 (mapId=${state.recomputedMapId}) & ResultStage 1.1`,
      command: `MapOutputTracker.validateStatus() // throws MetadataFetchFailedException("Missing an output location for shuffle 0")`,
      detailsPassed: [
        { name: 'exception', value: 'org.apache.spark.shuffle.MetadataFetchFailedException' },
        { name: 'missingMapIndex', value: String(state.targetPart) },
        { name: 'sourceRef', value: 'MapOutputTracker.scala (MetadataFetchFailedException)' },
      ],
      deepDiveComponentId: 'map-output-tracker-master',
    });
    b.addEvent({
      stepId: sMeta,
      from: motw,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: 'GetMapOutputStatuses(0)',
      info: 'MapOutputTrackerWorker fetches serialized MapStatus array for shuffle 0 from driver.',
      size: '1.9 KB',
      command: 'askTracker[Array[Byte]](GetMapOutputStatuses(0))',
      fields: [{ name: 'shuffleId', value: '0' }],
      senderState: 'Deserializing MapStatus array',
      receiverState: `Returned array with null entry at mapIndex=${state.targetPart}`,
    });
    b.addEvent({
      stepId: sMeta,
      from: motw,
      to: shuffleReader,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM MetadataFetchFailed',
      port: `${ctx.reduceConsumer.hostName} in-JVM`,
      label: 'MetadataFetchFailed',
      info: `MapOutputTracker.validateStatus throws MetadataFetchFailedException: Missing an output location for shuffle 0 partition ${state.targetPart}.`,
      size: 'Exception',
      command: `throw new MetadataFetchFailedException(0, ${state.targetPart}, "Missing an output location for shuffle 0")`,
      fields: [{ name: 'missingMapIndex', value: String(state.targetPart) }],
      senderState: 'Validation detected null MapStatus',
      receiverState: 'Reporting FetchFailed to DAGScheduler',
      isError: true,
    });
    b.addEvent({
      stepId: sMeta,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Resubmit Missing Map',
      port: `${b.driverHost} in-JVM`,
      label: `Resubmit mapIndex=${state.targetPart}`,
      info: `DAGScheduler schedules ShuffleMapStage 0.1 for missing mapIndex=${state.targetPart} and queues ResultStage 1.1.`,
      size: 'in-memory',
      command: `dagScheduler.resubmitFailedStages() // mapIndex=${state.targetPart}`,
      fields: [{ name: 'newEpoch', value: '2' }],
      senderState: `Recomputing mapIndex=${state.targetPart} on ${recomputeExec}`,
      receiverState: 'Awaiting fresh MapStatus',
    });
    b.addEvent({
      stepId: sMeta,
      from: recomputeExec,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (MapStatus)',
      port: b.driverRpc,
      label: `Register mapId=${state.recomputedMapId}`,
      info: `${recomputeExec} (${recomputeHost}) recomputes mapIndex=${state.targetPart} as mapId=${state.recomputedMapId}; ResultStage 1.1 completes cleanly.`,
      size: '640 B',
      command: `registerMapOutput(0, ${state.targetPart}, MapStatus(${recomputeHost}, mapId=${state.recomputedMapId}))`,
      fields: [{ name: 'status', value: 'ResultStage 1.1 SUCCEEDED' }],
      senderState: 'Missing map output recomputed',
      receiverState: 'Metadata complete; reduce stage succeeded',
    });
    return true;
  }

  if (fault === 'shuffle-corrupt-checksum') {
    const recomputeExec = state.liveExecs[state.liveExecs.length - 1];
    const recomputeHost = b.instanceWorkerHost(recomputeExec).hostName;
    const sCksum = b.addStep({
      id: 'sim-step-shuffle-checksum',
      title: 'Corrupt shuffle block triggers ADLER32 checksum diagnosis (DISK_ISSUE) -> FetchFailed retry',
      explanation:
        'ShuffleBlockFetcherIterator detects a corrupt shuffle stream from the producer, retries, and with spark.shuffle.checksum.enabled=true queries the server .checksum file via ShuffleChecksumHelper.diagnoseCorruption. Checksum diagnosis classifies CAUSE: DISK_ISSUE and escalates to FetchFailedException so DAGScheduler recomputes the offending map task.' + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: `Checksum diagnosed DISK_ISSUE -> FetchFailed -> mapId=${state.recomputedMapId} recomputed OK`,
      command: 'ShuffleChecksumHelper.diagnoseCorruption(ADLER32) -> DISK_ISSUE -> FetchFailedException',
      detailsPassed: [
        { name: 'spark.shuffle.checksum.enabled', value: 'true (ADLER32 default)' },
        { name: 'diagnosis', value: 'CAUSE: DISK_ISSUE (corrupt on-disk shuffle data)' },
        { name: 'sourceRef', value: 'ShuffleBlockFetcherIterator.scala; ShuffleChecksumHelper.java' },
      ],
      deepDiveComponentId: 'shuffle-reader',
    });
    b.addEvent({
      stepId: sCksum,
      from: shuffleReader,
      to: shuffleServer,
      channel: 'error',
      protocol: 'BlockTransfer (Corrupt Stream)',
      port: ctx.shuffleServerPort,
      label: 'Corrupt block -> diagnose',
      info: `Repeated LZ4 corruption on shuffle_0_${state.initialMapId}_${state.targetPart}; ShuffleBlockFetcherIterator invokes DiagnoseCorruption(ADLER32) on ${ctx.shuffleServerPort}.`,
      size: 'Checksum check',
      command: `ShuffleChecksumHelper.diagnoseCorruption(ADLER32, shuffle_0_${state.initialMapId}_${state.targetPart}) -> DISK_ISSUE`,
      fields: [
        { name: 'algorithm', value: 'ADLER32 (.checksum sidecar)' },
        { name: 'diagnosedCause', value: 'DISK_ISSUE -> throw FetchFailedException' },
      ],
      senderState: 'Escalated corrupt fetch to FetchFailedException',
      receiverState: 'Server .checksum mismatch confirmed DISK_ISSUE',
      isError: true,
    });
    b.addEvent({
      stepId: sCksum,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Invalidate Corrupt Map',
      port: `${b.driverHost} in-JVM`,
      label: `Invalidate mapIndex=${state.targetPart}`,
      info: `DAGScheduler invalidates corrupt map output at mapIndex=${state.targetPart} and reschedules ShuffleMapStage 0.1 -> ResultStage 1.1.`,
      size: 'in-memory',
      command: `mapOutputTracker.unregisterMapOutput(0, ${state.targetPart}, bmAddress); scheduleResubmit()`,
      fields: [{ name: 'newEpoch', value: '2' }],
      senderState: `Recomputing mapIndex=${state.targetPart} on ${recomputeExec}`,
      receiverState: 'Corrupt location evicted',
    });
    b.addEvent({
      stepId: sCksum,
      from: recomputeExec,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (MapStatus)',
      port: b.driverRpc,
      label: `Recompute mapId=${state.recomputedMapId} OK`,
      info: `${recomputeExec} (${recomputeHost}) writes clean shuffle_0_${state.recomputedMapId}_${state.targetPart} + .checksum; ResultStage 1.1 passes checksum and succeeds.`,
      size: '640 B',
      command: `registerMapOutput(0, ${state.targetPart}, MapStatus(${recomputeHost}, mapId=${state.recomputedMapId}))`,
      fields: [{ name: 'checksumResult', value: 'ADLER32 VERIFIED OK' }],
      senderState: 'Clean shuffle block published',
      receiverState: 'ResultStage 1.1 succeeded',
    });
    return true;
  }

  if (fault === 'ess-unavailable') {
    const essTarget = b.essInstance(ctx.producerZone);
    // ESS failure does NOT kill or retire executor JVMs on ctx.producerZone!
    // Executor liveness is independent of shuffle-output availability.
    const healthyExec =
      state.liveExecs.find(id => b.instanceWorkerHost(id).hostZone !== ctx.producerZone) ??
      state.liveExecs[state.liveExecs.length - 1];
    const recomputeHost = b.instanceWorkerHost(healthyExec).hostName;

    const sEss = b.addStep({
      id: 'sim-step-ess-down',
      title: `YarnShuffleService (${ctx.producerHost}:7337) refused -> FetchFailed (exec JVMs stay ALIVE)`,
      explanation:
        `Reducer ExternalBlockStoreClient connects to NM YarnShuffleService on ${ctx.producerHost}:7337 and gets Connection refused / executor not registered. Crucially, ESS outage does NOT kill or retire executor JVMs on ${ctx.producerHost} (executor liveness != shuffle-output availability). FetchFailed marks affected shuffle outputs lost on MapOutputTrackerMaster (unregistering that executor's outputs by default; host-wide output unregistration requires spark.files.fetchFailure.unRegisterOutputOnHost=true, default false), and DAGScheduler resubmits map tasks onto ${healthyExec} (${recomputeHost}) whose ESS :7337 is healthy.` + ctx.aqeOffNote,
      category: 'shuffle-internals',
      keyState: `ESS ${ctx.producerHost}:7337 refused -> exec JVMs remain ALIVE -> recomputed mapId=${state.recomputedMapId} on ${healthyExec}`,
      command: `ExternalBlockStoreClient("${ctx.producerHost}", 7337) -> FetchFailedException -> unregisterMapOutput (exec JVMs stay alive)`,
      detailsPassed: [
        { name: 'essEndpoint', value: `${ctx.producerHost}:7337 (YarnShuffleService)` },
        { name: 'executorJvmLiveness', value: `ALL executor JVMs on ${ctx.producerHost} remain ALIVE (liveness != shuffle serving)` },
        { name: 'hostWideUnregisterConfig', value: 'spark.files.fetchFailure.unRegisterOutputOnHost=false (default; must be true to unregister whole host)' },
        { name: 'recomputeExecutor', value: `${healthyExec} (${recomputeHost}:7337)` },
      ],
      deepDiveComponentId: 'external-shuffle-service',
    });
    b.addEvent({
      stepId: sEss,
      from: shuffleReader,
      to: essTarget,
      channel: 'error',
      protocol: 'ExternalBlockStoreClient (:7337)',
      port: `${ctx.producerHost}:7337`,
      label: 'ESS :7337 refused -> FF',
      info: `YarnShuffleService on ${ctx.producerHost}:7337 returns Connection refused / executor not registered; reducer throws FetchFailedException (executor JVMs on ${ctx.producerHost} stay alive).`,
      size: 'FetchFailed',
      command: `ExternalBlockStoreClient -> IOException("${ctx.producerHost}:7337 unavailable") -> FetchFailedException`,
      fields: [
        { name: 'spark.shuffle.service.port', value: '7337' },
        { name: 'executorsKilled', value: '0 (executor JVMs remain alive)' },
        { name: 'sourceRef', value: 'YarnShuffleService.java:107-108; DAGScheduler.scala:4875-4907' },
      ],
      senderState: 'Reported FetchFailed to DAGScheduler',
      receiverState: `YarnShuffleService down on ${ctx.producerHost}:7337 (worker JVMs alive)`,
      isError: true,
    });
    b.addEvent({
      stepId: sEss,
      from: dag,
      to: motm,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Shuffle Output Invalidate',
      port: `${b.driverHost} in-JVM`,
      label: `unregisterMapOutput(${state.targetPart})`,
      info: `DAGScheduler marks affected shuffle output lost without killing ${ctx.producerHost} executor JVMs (host-wide output unregistration requires spark.files.fetchFailure.unRegisterOutputOnHost=true, default false) and resubmits missing map task.`,
      size: 'in-memory',
      command: `mapOutputTracker.unregisterMapOutput(0, ${state.targetPart}, bmAddress); scheduleResubmit() // unRegisterOutputOnHost=false default`,
      fields: [
        { name: 'unRegisterOutputOnHost', value: 'false (default; only failed executor output unregistered unless set true)' },
        { name: 'liveExecutors', value: state.liveExecs.join(', ') },
      ],
      senderState: `Resubmitting map task onto ${healthyExec} (${recomputeHost})`,
      receiverState: `Evicted unreachable ${ctx.producerHost}:7337 map output`,
    });
    b.addEvent({
      stepId: sEss,
      from: healthyExec,
      to: motm,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (MapStatus)',
      port: b.driverRpc,
      label: `Recompute mapId=${state.recomputedMapId} OK`,
      info: `${healthyExec} (${recomputeHost}) regenerates shuffle output served by healthy ${recomputeHost}:7337; ResultStage 1.1 succeeds with all executor JVMs still alive.`,
      size: '640 B',
      command: `registerMapOutput(0, ${state.targetPart}, MapStatus(${recomputeHost}:7337, mapId=${state.recomputedMapId}))`,
      fields: [{ name: 'healthyEss', value: `${recomputeHost}:7337` }],
      senderState: 'Regenerated map output served cleanly',
      receiverState: 'ResultStage 1.1 completed',
    });
    return true;
  }

  return false;
}

// =============================================================================
// PHASE 7: Broadcast & TorrentBroadcast Faults (inside broadcast-join branch)
// =============================================================================
export function tryInjectBroadcastFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
): 'handled-abort' | 'none' {
  const fault = state.cfg.fault;
  const bcastEx = b.std('broadcast-exchange');
  const dag = b.std('dag-scheduler');

  if (fault === 'broadcast-over-8gb') {
    state.abortedByFault = true;
    const sOver = b.addStep({
      id: 'sim-step-bcast-8gb',
      title: 'BroadcastExchangeExec exceeds 8 GiB HashedRelation / row guard -> SparkException',
      explanation:
        'BroadcastExchangeExec.relationFuture (BroadcastExchangeExec.scala:163-216) enforces two hard guards: first row count numRows >= maxBroadcastRows (512,000,000 for single LongType key LongHashedRelation; (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong = ((1 << 29) / 1.5).toLong = (536,870,912 / 1.5).toLong = 357,913,941 (~357.9M) for other hashed keys -> _LEGACY_ERROR_TEMP_2248), and then after building the HashedRelation, relation.estimatedSize >= 8 GiB (spark.sql.maxBroadcastTableSize / maxBroadcastTableSizeInBytes -> _LEGACY_ERROR_TEMP_2249, measured on the built HashedRelation rather than collected serialized bytes).',
      category: 'broadcast-torrent',
      keyState: 'ABORTED: Built HashedRelation 9.2 GiB >= 8 GiB guard (_LEGACY_ERROR_TEMP_2249)',
      command: 'BroadcastExchangeExec.relationFuture // built HashedRelation.estimatedSize (9.2 GiB) >= 8 GiB -> SparkException',
      detailsPassed: [
        { name: 'byteGuard', value: '8 GiB (_LEGACY_ERROR_TEMP_2249) measured on built HashedRelation.estimatedSize (not raw serialized rows)' },
        { name: 'rowGuard_LongKey', value: '512,000,000 rows for single LongType key (LongHashedRelation -> _LEGACY_ERROR_TEMP_2248)' },
        {
          name: 'rowGuard_OtherKeys',
          value: '(BytesToBytesMap.MAX_CAPACITY / 1.5).toLong = ((1 << 29) / 1.5).toLong = 357,913,941 (~357.9M rows, UnsafeHashedRelation -> _LEGACY_ERROR_TEMP_2248)',
        },
        { name: 'sourceRef', value: 'BroadcastExchangeExec.scala:163-216; BytesToBytesMap.java:132; QueryExecutionErrors.scala:2344-2362' },
      ],
      deepDiveComponentId: 'broadcast-exchange',
    });
    b.addEvent({
      stepId: sOver,
      from: dag,
      to: bcastEx,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM HashedRelation Build',
      port: `${b.driverHost} in-JVM`,
      label: 'Build HashedRel (9.2G)',
      info: 'BroadcastExchangeExec.relationFuture checks maxBroadcastRows (512,000,000 for LongHashedRelation; 357,913,941 = (1 << 29) / 1.5 for UnsafeHashedRelation), then calls mode.transform() to build HashedRelation whose estimatedSize is 9.2 GiB.',
      size: '9.2 GiB built',
      command: 'val relation = mode.transform(input, Some(numRows)) // relation.estimatedSize = 9.2 GiB',
      fields: [
        { name: 'measuredOn', value: 'Built HashedRelation.estimatedSize (not collected serialized bytes)' },
        { name: 'limit', value: '8 GiB (spark.sql.maxBroadcastTableSize)' },
      ],
      senderState: 'Build-side rows collected & transformed',
      receiverState: 'Checking 8 GiB built-HashedRelation byte guard',
    });
    b.addEvent({
      stepId: sOver,
      from: bcastEx,
      to: bcastEx,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Broadcast Size Guard',
      port: `${b.driverHost} in-JVM`,
      label: 'Abort: > 8 GiB (_2249)',
      info: 'SparkException [_LEGACY_ERROR_TEMP_2249]: Cannot broadcast the table that is larger than 8GB: 9.2 GB (measured on built HashedRelation; row guard uses _LEGACY_ERROR_TEMP_2248).',
      size: 'SparkException',
      command: 'throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(8 GiB, 9.2 GiB) // _LEGACY_ERROR_TEMP_2249',
      fields: [
        { name: 'errorClass', value: '_LEGACY_ERROR_TEMP_2249 (byte guard); _LEGACY_ERROR_TEMP_2248 (row guard)' },
        { name: 'piecesPublished', value: '0 (aborted before TorrentBroadcast)' },
        { name: 'sourceRef', value: 'BroadcastExchangeExec.scala:206-215' },
      ],
      senderState: 'Broadcast future failed',
      receiverState: 'Query aborted before BroadcastHashJoinExec',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Cannot broadcast the table that is larger than 8GB (_LEGACY_ERROR_TEMP_2249)',
      stopLiveExecutors: true,
    });
    return 'handled-abort';
  }

  if (fault === 'broadcast-timeout') {
    state.abortedByFault = true;
    const sTo = b.addStep({
      id: 'sim-step-bcast-timeout',
      title: 'BroadcastExchangeExec exceeds 300s -> cancelJobsWithTag + relationFuture.cancel(true)',
      explanation:
        'BroadcastExchangeExec.doExecuteBroadcast (BroadcastExchangeExec.scala:259-269) blocks in relationFuture.get(timeout, TimeUnit.SECONDS) (spark.sql.broadcastTimeout = 300s). On TimeoutException, the driver calls sparkContext.cancelJobsWithTag(jobTag, ...) to cancel the tagged broadcast Spark job AND calls relationFuture.cancel(true), then throws SparkException: Could not execute broadcast in 300 secs.',
      category: 'broadcast-torrent',
      keyState: 'ABORTED: 300s timeout -> cancelJobsWithTag + relationFuture.cancel(true)',
      command: 'relationFuture.get(300, SECONDS) // TimeoutException -> cancelJobsWithTag + relationFuture.cancel(true)',
      detailsPassed: [
        { name: 'spark.sql.broadcastTimeout', value: '300s (default)' },
        { name: 'cancellationCalls', value: 'sparkContext.cancelJobsWithTag(jobTag) + relationFuture.cancel(true)' },
        { name: 'exception', value: 'SparkException: Could not execute broadcast in 300 secs (TimeoutException)' },
        { name: 'sourceRef', value: 'BroadcastExchangeExec.scala:179,259-269; SQLConf.scala:2555' },
      ],
      deepDiveComponentId: 'broadcast-exchange',
    });
    b.addEvent({
      stepId: sTo,
      from: bcastEx,
      to: dag,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Timeout -> Cancel',
      port: `${b.driverHost} in-JVM`,
      label: '300s -> cancelJobsWithTag',
      info: 'spark.sql.broadcastTimeout (300s) expires; BroadcastExchangeExec calls sparkContext.cancelJobsWithTag(jobTag, "The corresponding broadcast query has failed.") AND relationFuture.cancel(true).',
      size: 'TimeoutException',
      command: 'sparkContext.cancelJobsWithTag(jobTag, ...); relationFuture.cancel(true)',
      fields: [
        { name: 'timeout', value: '300s' },
        { name: 'futureCancelled', value: 'relationFuture.cancel(true)' },
        { name: 'remediation', value: 'Increase spark.sql.broadcastTimeout or disable broadcast join' },
      ],
      senderState: 'Cancelled tagged broadcast Spark job & relationFuture',
      receiverState: 'Aborted pending build-side tasks',
      isError: true,
    });
    b.addEvent({
      stepId: sTo,
      from: bcastEx,
      to: bcastEx,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM SparkException',
      port: `${b.driverHost} in-JVM`,
      label: 'Broadcast timeout FAILED',
      info: 'Throws SparkException: Could not execute broadcast in 300 secs.',
      size: 'SparkException',
      command: 'throw QueryExecutionErrors.executeBroadcastTimeoutError(300, Some(ex))',
      fields: [{ name: 'jobOutcome', value: 'FAILED' }],
      senderState: 'Query failed on driver',
      receiverState: 'Zero probe tasks launched',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Could not execute broadcast in 300 secs (spark.sql.broadcastTimeout)',
      stopLiveExecutors: true,
    });
    return 'handled-abort';
  }

  if (fault === 'broadcast-driver-oom') {
    state.abortedByFault = true;
    const sOom = b.addStep({
      id: 'sim-step-bcast-driver-oom',
      title: 'Driver OOM building HashedRelation -> wrapped SparkException (_LEGACY_ERROR_TEMP_2250)',
      explanation:
        'A forced broadcast() hint collects too many rows into driver JVM heap while constructing HashedRelation inside BroadcastExchangeExec.relationFuture (BroadcastExchangeExec.scala:230-235). BroadcastExchangeExec catches OutOfMemoryError and throws SparkException (_LEGACY_ERROR_TEMP_2250: "Not enough memory to build and broadcast the table to all worker nodes") wrapped via SparkFatalException to fail the query.',
      category: 'broadcast-torrent',
      keyState: 'FAILED: SparkException [_LEGACY_ERROR_TEMP_2250] ("Not enough memory to build and broadcast the table to all worker nodes")',
      command: 'BroadcastExchangeExec.relationFuture // catch oe: OutOfMemoryError -> SparkFatalException(SparkException[_LEGACY_ERROR_TEMP_2250])',
      detailsPassed: [
        { name: 'errorClass', value: '_LEGACY_ERROR_TEMP_2250 (QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError)' },
        { name: 'message', value: 'Not enough memory to build and broadcast the table to all worker nodes' },
        { name: 'config', value: 'spark.driver.memory / spark.sql.autoBroadcastJoinThreshold' },
        { name: 'sourceRef', value: 'BroadcastExchangeExec.scala:230-235; QueryExecutionErrors.scala:2364-2377' },
      ],
      deepDiveComponentId: 'broadcast-exchange',
    });
    b.addEvent({
      stepId: sOom,
      from: dag,
      to: bcastEx,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM HashedRelation Build',
      port: `${b.driverHost} in-JVM`,
      label: 'Build HashedRelation',
      info: 'Driver allocates UnsafeHashedRelation hash table in driver JVM heap for forced broadcast().',
      size: 'Driver Heap',
      command: 'HashedRelation(rows, key, sizeEstimate)',
      fields: [{ name: 'hint', value: 'Forced broadcast() exceeded driver heap' }],
      senderState: 'Streaming collected build rows',
      receiverState: 'Allocating hash map arrays in driver heap',
    });
    b.addEvent({
      stepId: sOom,
      from: bcastEx,
      to: bcastEx,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM Wrapped SparkException',
      port: `${b.driverHost} in-JVM`,
      label: 'Driver OOM (_2250)',
      info: 'BroadcastExchangeExec catches OutOfMemoryError while building HashedRelation and throws wrapped SparkException (_LEGACY_ERROR_TEMP_2250: "Not enough memory to build and broadcast the table to all worker nodes") via SparkFatalException.',
      size: 'SparkException',
      command: 'throw new SparkFatalException(QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError(oe, tables))',
      fields: [
        { name: 'errorClass', value: '_LEGACY_ERROR_TEMP_2250' },
        { name: 'remediation', value: 'Remove forced broadcast() hint or increase spark.driver.memory' },
        { name: 'sourceRef', value: 'BroadcastExchangeExec.scala:230-235' },
      ],
      senderState: 'Driver heap exhausted during relation build',
      receiverState: 'Query aborted with wrapped SparkException',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Not enough memory to build and broadcast the table to all worker nodes (_LEGACY_ERROR_TEMP_2250)',
      stopLiveExecutors: true,
    });
    return 'handled-abort';
  }

  return 'none';
}

export function injectTorrentPieceCorruptStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
  driverBm: string,
  producerBm: string,
  peerBm: string,
  peerExec: string,
): void {
  if (state.cfg.fault !== 'torrent-piece-corrupt') return;

  const tsm = b.std('task-set-manager');
  const prodPort = b.instanceHostLabel(producerBm);
  const peerHost = b.instanceWorkerHost(peerExec).hostName;
  const sCorrupt = b.addStep({
    id: 'sim-step-torrent-corrupt',
    title: 'Corrupt broadcast_0_piece1 -> IOException fails task attempt 0 -> retry attempt 1 OK',
    explanation:
      `Per TorrentBroadcast.scala:205-215, 255-285, with spark.broadcast.checksum=true ${peerExec} fetches broadcast_0_piece1 (4.0 MiB) from peer ${producerBm} (${prodPort}) and detects a calcChecksum() mismatch. TorrentBroadcast.readBlocks throws SparkException.internalError("corrupt remote block broadcast_0_piece1"), which readBroadcastBlock wraps via Utils.tryOrIOException into IOException -- failing the current task attempt 0 (NOT a silent in-place driver fallback inside the same read!). TaskSetManager retries attempt 1, which reads a clean verified copy from Driver BM (${b.driverBmPort}) and succeeds.`,
    category: 'broadcast-torrent',
    keyState: 'Corrupt peer piece1 -> IOException fails task attempt 0 -> TaskSetManager retries attempt 1 (clean copy OK)',
    command: 'TorrentBroadcast.readBlocks() -> IOException (fails attempt 0) -> TaskSetManager retries attempt 1 -> Driver BM OK',
    detailsPassed: [
      { name: 'spark.broadcast.checksum', value: 'true (ADLER32 per 4 MiB piece)' },
      { name: 'corruptBlock', value: 'broadcast_0_piece1 (from peer executor)' },
      { name: 'failureBehavior', value: 'readBlocks throws -> tryOrIOException converts to IOException -> fails task attempt 0 (no silent in-place fallback)' },
      { name: 'recovery', value: 'TaskSetManager reschedules attempt 1, which fetches a clean copy from Driver BM and succeeds' },
      { name: 'sourceRef', value: 'TorrentBroadcast.scala:205-215,255-285' },
    ],
    deepDiveComponentId: 'broadcast-manager',
  });
  b.addEvent({
    stepId: sCorrupt,
    from: producerBm,
    to: peerBm,
    channel: 'error',
    protocol: 'BlockTransfer - Checksum Fail',
    port: `${prodPort} -> ${peerHost}`,
    label: 'Corrupt piece1 -> IOExc',
    info: `${peerBm} receives corrupt broadcast_0_piece1 (4.0 MiB) from peer ${producerBm}; calcChecksum() fails and TorrentBroadcast.readBlocks / readBroadcastBlock throws IOException (failing task attempt 0; no silent in-place fallback).`,
    size: 'IOException',
    command: 'TorrentBroadcast.readBlocks() // calcChecksum mismatch -> Utils.tryOrIOException throws IOException',
    fields: [
      { name: 'blockId', value: 'broadcast_0_piece1' },
      { name: 'exception', value: 'IOException("corrupt remote block broadcast_0_piece1")' },
    ],
    senderState: 'Peer served bit-flipped/truncated piece1',
    receiverState: 'readBroadcastBlock threw IOException, failing attempt 0',
    isError: true,
    peerTag: 'E1 -> E2',
  });
  b.addEvent({
    stepId: sCorrupt,
    from: peerExec,
    to: tsm,
    channel: 'error',
    protocol: 'Spark Netty RPC (StatusUpdate)',
    port: b.driverRpc,
    label: 'Task attempt 0 FAIL (IOE)',
    info: `${peerExec} reports task attempt 0 failed with IOException ("corrupt remote block broadcast_0_piece1") to TaskSetManager (numFailures = 1 / 4).`,
    size: '1.9 KB',
    command: 'StatusUpdate(FAILED, ExceptionFailure("IOException: corrupt remote block broadcast_0_piece1"))',
    fields: [
      { name: 'taskAttempt', value: '0 FAILED (IOException)' },
      { name: 'numFailures', value: '1 / 4 (rescheduling attempt 1)' },
    ],
    senderState: 'Attempt 0 aborted cleanly',
    receiverState: 'TaskSetManager scheduling retry attempt 1',
    isError: true,
  });
  b.addEvent({
    stepId: sCorrupt,
    from: state.schedBackend,
    to: peerExec,
    channel: 'rpc',
    protocol: 'Spark Netty RPC (LaunchTask)',
    port: b.driverRpc,
    label: 'LaunchTask (attempt 1)',
    info: `TaskSetManager dispatches retry task attempt 1 to ${peerExec} to re-read TorrentBroadcast pieces cleanly.`,
    size: '2.1 KB',
    command: 'executorEndpoint.send(LaunchTask(attempt=1))',
    fields: [{ name: 'taskAttempt', value: '1 (retry)' }],
    senderState: 'Dispatched retry attempt 1',
    receiverState: 'Attempt 1 calling TorrentBroadcast.readBroadcastBlock()',
  });
  b.addEvent({
    stepId: sCorrupt,
    from: driverBm,
    to: peerBm,
    channel: 'data',
    protocol: 'BlockTransfer - Retry Clean',
    port: `${b.driverBmPort} -> ${peerHost}`,
    label: 'Retry piece1 OK (Driver)',
    info: `Retry task attempt 1 on ${peerBm} fetches a clean copy of broadcast_0_piece1 (4.0 MiB) from Driver BM (${b.driverBmPort}); calcChecksum() passes and attempt 1 succeeds.`,
    size: '4.0 MiB OK',
    command: 'peerBM.getRemoteBytes(broadcast_0_piece1) // attempt 1 -> checksum OK',
    fields: [
      { name: 'retryAttempt', value: '1 SUCCEEDED' },
      { name: 'source', value: b.driverBmPort },
      { name: 'checksumStatus', value: 'VERIFIED OK' },
    ],
    senderState: 'Served authoritative piece1 from Driver BM',
    receiverState: `${peerExec} verified checksum & built HashedRelation on retry attempt 1`,
    peerTag: 'Driver -> E2',
  });
}

// =============================================================================
// PHASE 8: PySpark Worker IPC Faults (inside python-udf branch)
// Returns 'handled-abort' | 'handled-recovered' | 'none'
// =============================================================================
export function tryInjectPythonUdfFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
  ctx: PythonUdfContext,
): 'handled-abort' | 'handled-recovered' | 'none' {
  const fault = state.cfg.fault;
  const tsm = b.std('task-set-manager');
  const dag = b.std('dag-scheduler');

  if (fault === 'python-worker-crash') {
    const pyWorker = ctx.pyWorker!;
    const sCrash = b.addStep({
      id: 'sim-step-py-crash',
      title: `Python worker crashes mid-batch (EOFException) on ${ctx.udfConsumer.hostName} -> task retries on fresh worker`,
      explanation:
        `During ${ctx.isArrow ? 'ArrowEvalPythonExec' : 'BatchEvalPythonExec'} on ${ctx.udfConsumer.hostName}, the pyspark.worker subprocess crashes mid-batch (native C-extension segfault / worker OOM). BasePythonRunner ReaderIterator hits premature socket EOFException before END_OF_STREAM, throws SparkException("Python worker exited unexpectedly (crashed)"), destroys the dead worker, and TaskSetManager retries the task on a freshly forked worker.`,
      category: 'python-worker-ipc',
      keyState: 'Python worker crashed (EOFException) -> fresh worker forked -> retry succeeded',
      command: 'PythonRunner ReaderIterator -> EOFException -> SparkException("Python worker exited unexpectedly (crashed)")',
      detailsPassed: [
        { name: 'exception', value: 'SparkException: Python worker exited unexpectedly (crashed) (cause: EOFException)' },
        { name: 'recovery', value: 'PythonWorkerFactory destroys dead worker; TaskSetManager retries on fresh worker' },
        { name: 'sourceRef', value: 'PythonRunner.scala ("Python worker exited unexpectedly (crashed)")' },
      ],
      deepDiveComponentId: 'python-runner',
    });
    b.addEvent({
      stepId: sCrash,
      from: pyWorker,
      to: ctx.pyRunner,
      channel: 'error',
      protocol: 'Local Socket (Premature EOF)',
      port: '127.0.0.1:ephemeral',
      label: 'Worker crash -> EOF',
      info: 'pyspark.worker terminates abruptly mid-batch; PythonRunner socket read hits EOFException ("Python worker exited unexpectedly (crashed)").',
      size: 'EOFException',
      command: 'throw new SparkException("Python worker exited unexpectedly (crashed)", eof)',
      fields: [
        { name: 'cause', value: 'EOFException before END_OF_STREAM' },
        { name: 'host', value: ctx.udfConsumer.hostName },
      ],
      senderState: 'Python worker process crashed (segfault/OOM)',
      receiverState: 'Destroying broken worker & failing task attempt 0',
      isError: true,
    });
    b.addEvent({
      stepId: sCrash,
      from: ctx.udfConsumer.consumerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Task FAIL (retry 1/4)',
      info: 'Executor reports ExceptionFailure("Python worker exited unexpectedly (crashed)") to TaskSetManager, which reschedules attempt 1.',
      size: '1.9 KB',
      command: 'StatusUpdate(FAILED, ExceptionFailure("Python worker exited unexpectedly (crashed)"))',
      fields: [{ name: 'numFailures', value: '1 / 4 (retrying)' }],
      senderState: 'Attempt 0 failed cleanly; JVM executor stays alive',
      receiverState: 'Rescheduling attempt 1',
      isError: true,
    });
    b.addEvent({
      stepId: sCrash,
      from: ctx.udfOp,
      to: ctx.pyFactory,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Fresh Worker Fork',
      port: `${ctx.udfConsumer.hostName} in-JVM`,
      label: 'Fork fresh worker (retry)',
      info: `PythonWorkerFactory discards dead worker and forks a clean worker from pyspark.daemon on ${ctx.udfConsumer.hostName}.`,
      size: 'in-memory',
      command: 'PythonWorkerFactory.destroy(deadWorker); createThroughDaemon()',
      fields: [{ name: 'attempt', value: '1 (fresh Python PID)' }],
      senderState: 'Checked out fresh Python worker',
      receiverState: 'Forked clean worker process',
    });
    b.addEvent({
      stepId: sCrash,
      from: pyWorker,
      to: ctx.pyRunner,
      channel: 'python',
      protocol: ctx.isArrow ? 'Local Socket (Arrow IPC)' : 'Local Socket (CPickle)',
      port: '127.0.0.1:ephemeral',
      label: 'Retry UDF results OK',
      info: 'Fresh Python worker evaluates the batch cleanly and returns UDF results.',
      size: '96 KiB',
      command: 'outfile.flush() -> PythonWorkerFactory.releaseWorker()',
      fields: [{ name: 'status', value: 'Retry SUCCEEDED' }],
      senderState: 'Returned clean worker to idle pool',
      receiverState: 'UnsafeRow output ready for write',
    });
    return 'handled-recovered';
  }

  if (fault === 'python-daemon-stdout-corrupt') {
    state.abortedByFault = true;
    const sBad = b.addStep({
      id: 'sim-step-py-daemon-stdout',
      title: `pyspark.daemon startup stdout corrupted on ${ctx.udfConsumer.hostName} (0 workers spawned)`,
      explanation:
        `Stray print() output (e.g. from sitecustomize.py or conda activation banner) writes ASCII bytes to pyspark.daemon stdout during daemon startup BEFORE any Python worker process is spawned. PythonWorkerFactory.startDaemon (PythonWorkerFactory.scala:338-405) reads the corrupt binary listening port/socket handshake and throws SparkException("Bad data in pyspark.daemon's standard output. Invalid port number/socket path"), aborting the job with 0 worker processes created.`,
      category: 'python-worker-ipc',
      keyState: 'FAILED: Bad data in pyspark.daemon startup stdout (BEFORE any worker spawned)',
      command: 'PythonWorkerFactory.startDaemon() // throws SparkException("Bad data in pyspark.daemon\'s standard output")',
      detailsPassed: [
        { name: 'exception', value: 'SparkException: Bad data in pyspark.daemon\'s standard output' },
        { name: 'workersSpawned', value: '0 (fails during pyspark.daemon startup handshake before any worker process is spawned)' },
        { name: 'rootCause', value: 'Stray stdout text from sitecustomize.py / shell banner corrupts binary port handshake' },
        { name: 'sourceRef', value: 'PythonWorkerFactory.scala:338-405' },
      ],
      deepDiveComponentId: 'python-worker-factory',
    });
    b.addEvent({
      stepId: sBad,
      from: ctx.udfOp,
      to: ctx.pyFactory,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Daemon Boot',
      port: `${ctx.udfConsumer.hostName} in-JVM`,
      label: 'startDaemon() handshake',
      info: `PythonWorkerFactory spawns pyspark.daemon on ${ctx.udfConsumer.hostName} and reads the binary port/socket handshake from daemon stdout before forking any worker.`,
      size: '4 bytes expected',
      command: 'PythonWorkerFactory.startDaemon() -> dataInput.readInt()',
      fields: [{ name: 'spark.python.use.daemon', value: 'true' }],
      senderState: 'Requesting Python daemon startup',
      receiverState: 'Reading binary listening port from pyspark.daemon stdout pipe',
    });
    b.addEvent({
      stepId: sBad,
      from: ctx.pyFactory,
      to: ctx.pyFactory,
      kind: 'local',
      channel: 'error',
      protocol: 'pyspark.daemon Stdout Pipe',
      port: `${ctx.udfConsumer.hostName} OS`,
      label: 'Bad daemon stdout (0 wk)',
      info: 'Stray ASCII banner bytes ("Loading...") arrive on pyspark.daemon stdout BEFORE any worker process is spawned; PythonWorkerFactory.startDaemon throws SparkException: Bad data in pyspark.daemon\'s standard output.',
      size: 'SparkException',
      command: 'throw new SparkException("Bad data in pyspark.daemon\'s standard output. Invalid port number")',
      fields: [
        { name: 'corruptBytes', value: 'ASCII banner from sitecustomize.py' },
        { name: 'workersSpawned', value: '0 (failed during daemon startup handshake)' },
      ],
      senderState: 'pyspark.daemon stdout polluted before handshake',
      receiverState: 'Failed daemon startup before any worker process spawned',
      isError: true,
    });
    b.addEvent({
      stepId: sBad,
      from: ctx.udfConsumer.consumerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Abort: daemon stdout bad',
      info: 'Repeated attempts fail with bad daemon stdout; TaskSetManager aborts TaskSet and DAGScheduler fails the job.',
      size: '2.1 KB',
      command: 'taskSetManager.abort("Bad data in pyspark.daemon\'s standard output")',
      fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException)' }],
      senderState: 'Unable to start pyspark.daemon',
      receiverState: 'TaskSet aborted',
      isError: true,
    });
    b.addEvent({
      stepId: sBad,
      from: tsm,
      to: dag,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM TaskSetFailed',
      port: `${b.driverHost} in-JVM`,
      label: 'TaskSetFailed -> JobFailed',
      info: 'DAGScheduler fails ActiveJob due to corrupt pyspark.daemon stdout handshake.',
      size: 'in-memory',
      command: 'dagScheduler.taskSetFailed(taskSet, "Bad daemon stdout")',
      fields: [{ name: 'finalStatus', value: 'FAILED' }],
      senderState: 'TaskSet failed',
      receiverState: 'ActiveJob failed',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'Bad data in pyspark.daemon standard output',
      stopLiveExecutors: true,
    });
    return 'handled-abort';
  }

  if (fault === 'python-version-mismatch') {
    const pyWorker = ctx.pyWorker!;
    state.abortedByFault = true;
    const sVer = b.addStep({
      id: 'sim-step-py-version-mismatch',
      title: `Worker Python 3.10 != Driver Python 3.12 (init handshake) -> PYTHON_VERSION_MISMATCH`,
      explanation:
        `Inside the length-framed worker initialization message (WorkerInitInfo.from_stream, python/pyspark/worker_message.py:230-235 & worker.py:100-146) on ${ctx.udfConsumer.hostName}, the worker reads the partition index (split_index) first and THEN validates the driver python_version string ("3.12") against worker sys.version_info[:2] (3, 10) via check_python_version(), returning PYTHON_VERSION_MISMATCH through the init response (report_init_response) before deserializing any UDF or rows.`,
      category: 'python-worker-ipc',
      keyState: 'FAILED: PYTHON_VERSION_MISMATCH returned via init response (worker 3.10 vs driver 3.12)',
      command: 'WorkerInitInfo.from_stream (split_index -> python_version) -> report_init_response(PYTHON_VERSION_MISMATCH)',
      detailsPassed: [
        { name: 'wireOrder', value: 'Length-framed init message: split_index first, then python_version string (NOT first standalone frame)' },
        { name: 'errorReturnPath', value: 'Returned via worker init response (report_init_response) before UDF deserialization' },
        { name: 'PYSPARK_DRIVER_PYTHON', value: '3.12' },
        { name: 'PYSPARK_PYTHON', value: '3.10 (mismatch!)' },
        { name: 'sourceRef', value: 'python/pyspark/worker.py:100-146; worker_message.py:230-235; worker_util.py' },
      ],
      deepDiveComponentId: 'python-worker',
    });
    b.addEvent({
      stepId: sVer,
      from: ctx.pyRunner,
      to: pyWorker,
      channel: 'python',
      protocol: 'Local Socket (Framed Init)',
      port: '127.0.0.1:ephemeral',
      label: 'Init frame (split+ver3.12)',
      info: 'PythonRunner sends the length-framed initialization message containing split_index followed by driver python_version="3.12" to worker.py.',
      size: '112 B framed',
      command: 'writeInitMessage(split_index, python_version="3.12", ...) // WorkerInitInfo',
      fields: [
        { name: 'frameFields', value: 'split_index first, then python_version="3.12"' },
        { name: 'driverPython', value: '3.12' },
      ],
      senderState: 'Waiting for worker init response',
      receiverState: 'Reading split_index, then running check_python_version()',
    });
    b.addEvent({
      stepId: sVer,
      from: pyWorker,
      to: ctx.pyRunner,
      channel: 'error',
      protocol: 'Local Socket (Init Response)',
      port: '127.0.0.1:ephemeral',
      label: 'InitResp: VERSION_MISMATCH',
      info: 'check_python_version detects worker (3, 10) != driver 3.12 and returns PySparkRuntimeError [PYTHON_VERSION_MISMATCH] through the init response (report_init_response) before UDF deserialization.',
      size: 'PythonException',
      command: 'report_init_response(PySparkRuntimeError(errorClass="PYTHON_VERSION_MISMATCH"))',
      fields: [
        { name: 'responseChannel', value: 'Worker init response (report_init_response)' },
        { name: 'workerPython', value: '3.10' },
        { name: 'driverPython', value: '3.12' },
      ],
      senderState: 'Worker rejected init handshake via init response',
      receiverState: 'Failing task with PythonException',
      isError: true,
    });
    b.addEvent({
      stepId: sVer,
      from: ctx.udfConsumer.consumerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Abort: Python 3.10!=3.12',
      info: 'Deterministic PYTHON_VERSION_MISMATCH fails task attempts and aborts the stage.',
      size: '2.0 KB',
      command: 'taskSetManager.abort("PYTHON_VERSION_MISMATCH: worker 3.10 != driver 3.12")',
      fields: [{ name: 'jobOutcome', value: 'JobFailed(PythonException)' }],
      senderState: 'Tasks failing on every worker',
      receiverState: 'TaskSet aborted',
      isError: true,
    });
    b.addEvent({
      stepId: sVer,
      from: tsm,
      to: dag,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM TaskSetFailed',
      port: `${b.driverHost} in-JVM`,
      label: 'TaskSetFailed -> JobFailed',
      info: 'DAGScheduler fails ActiveJob due to PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON mismatch.',
      size: 'in-memory',
      command: 'dagScheduler.taskSetFailed(taskSet, "PYTHON_VERSION_MISMATCH")',
      fields: [{ name: 'finalStatus', value: 'FAILED' }],
      senderState: 'TaskSet failed',
      receiverState: 'ActiveJob failed',
      isError: true,
    });
    emitTerminalAppOutcome(b, state, {
      origin: 'user-app-failure',
      diagnostic: 'PYTHON_VERSION_MISMATCH (worker 3.10 vs driver 3.12)',
      stopLiveExecutors: true,
    });
    return 'handled-abort';
  }

  if (fault === 'python-idle-timeout') {
    const pyWorker = ctx.pyWorker!;
    const sIdle = b.addStep({
      id: 'sim-step-py-idle-timeout',
      title: `Stuck Python worker exceeds idleTimeoutSeconds -> ProcessHandle.destroy() & retry`,
      explanation:
        `A Python UDF on ${ctx.udfConsumer.hostName} stalls without producing output longer than spark.python.worker.idleTimeoutSeconds while spark.python.worker.killOnIdleTimeout=true. PythonRunner raises PythonWorkerException and PythonWorkerFactory / LocalPythonWorkerHandle invokes ProcessHandle.destroy() (graceful termination, NOT destroyForcibly() / guaranteed SIGKILL); TaskSetManager then retries the task on a freshly spawned worker.`,
      category: 'python-worker-ipc',
      keyState: 'Python worker idle timeout -> ProcessHandle.destroy() -> retry SUCCEEDED',
      command: 'PythonRunner // idle > spark.python.worker.idleTimeoutSeconds -> ProcessHandle.destroy() -> retry',
      detailsPassed: [
        { name: 'spark.python.worker.idleTimeoutSeconds', value: '60s (> 0)' },
        { name: 'spark.python.worker.killOnIdleTimeout', value: 'true' },
        { name: 'terminationApi', value: 'ProcessHandle.destroy() (graceful terminate, NOT destroyForcibly() / guaranteed SIGKILL)' },
        { name: 'sourceRef', value: 'PythonRunner.scala:885-905,1465-1475; PythonWorkerHandle.scala:62' },
      ],
      deepDiveComponentId: 'python-runner',
    });
    b.addEvent({
      stepId: sIdle,
      from: ctx.pyRunner,
      to: pyWorker,
      channel: 'error',
      protocol: 'Local Socket / ProcessHandle.destroy',
      port: '127.0.0.1:ephemeral',
      label: 'Idle -> destroy() py wk',
      info: 'Python worker produced no bytes for > spark.python.worker.idleTimeoutSeconds; PythonWorkerFactory invokes ProcessHandle.destroy() (graceful terminate, not destroyForcibly()) and raises PythonWorkerException.',
      size: 'PythonWorkerException',
      command: 'PythonWorkerFactory.destroy(worker) // LocalPythonWorkerHandle -> ProcessHandle.destroy()',
      fields: [
        { name: 'exception', value: 'PythonWorkerException (idle timeout)' },
        { name: 'terminationCall', value: 'ProcessHandle.destroy() (not destroyForcibly())' },
        { name: 'killOnIdleTimeout', value: 'true' },
      ],
      senderState: 'Invoked ProcessHandle.destroy() on stuck Python worker',
      receiverState: 'Worker process terminated',
      isError: true,
    });
    b.addEvent({
      stepId: sIdle,
      from: ctx.udfConsumer.consumerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Task FAIL (idle retry)',
      info: 'Task attempt 0 fails with PythonWorkerException; TaskSetManager schedules attempt 1.',
      size: '1.7 KB',
      command: 'StatusUpdate(FAILED, ExceptionFailure("PythonWorkerException: idle timeout"))',
      fields: [{ name: 'numFailures', value: '1 / 4 (retrying)' }],
      senderState: 'Executor JVM unaffected',
      receiverState: 'Retrying task on clean worker',
      isError: true,
    });
    b.addEvent({
      stepId: sIdle,
      from: pyWorker,
      to: ctx.pyRunner,
      channel: 'python',
      protocol: ctx.isArrow ? 'Local Socket (Arrow IPC)' : 'Local Socket (CPickle)',
      port: '127.0.0.1:ephemeral',
      label: 'Fresh worker retry OK',
      info: 'Newly forked Python worker evaluates UDF cleanly without stalling and returns results.',
      size: '96 KiB',
      command: 'outfile.flush() -> PythonWorkerFactory.releaseWorker()',
      fields: [{ name: 'status', value: 'Retry SUCCEEDED' }],
      senderState: 'Worker returned to idle pool',
      receiverState: 'UnsafeRow output ready for write',
    });
    return 'handled-recovered';
  }

  return 'none';
}

// =============================================================================
// MID-WORKLOAD HELPERS (Injected DURING task execution / BEFORE collect delivery)
// =============================================================================
export function injectTaskSpillOomStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
): void {
  if (state.cfg.fault !== 'task-spill-oom') return;
  const producerExec = state.liveExecs[0];
  const { hostZone: prodZone, hostName: prodHost } = b.instanceWorkerHost(producerExec);
  const tsm = b.std('task-set-manager');
  const taskAttempt = b.workerStd('task-attempt', prodZone);
  const retryExec = state.liveExecs[state.liveExecs.length - 1];
  const sSpill = b.addStep({
    id: 'sim-step-task-spill-oom',
    title: 'During task execution: TaskMemoryManager spill fails -> SparkOutOfMemoryError (JVM ALIVE)',
    explanation:
      `During active task execution on ${prodHost} (BEFORE task result serialization or stage completion), TaskMemoryManager triggers MemoryConsumer.spill() which hits an IOException and throws SparkOutOfMemoryError [SPILL_OUT_OF_MEMORY]. Unlike raw JVM OutOfMemoryError (exit 52), Executor.isFatalError returns false: the executor JVM stays ALIVE and TaskSetManager retries only the failed task.`,
    category: 'sql-planning-aqe',
    keyState: 'SparkOutOfMemoryError (SPILL_OUT_OF_MEMORY) mid-task -> executor ALIVE -> task retry OK',
    command: 'TaskMemoryManager.acquireExecutionMemory() -> SparkOutOfMemoryError("SPILL_OUT_OF_MEMORY")',
    detailsPassed: [
      { name: 'timing', value: 'During task execution (before task result serialization / delivery)' },
      { name: 'errorClass', value: 'SPILL_OUT_OF_MEMORY (SparkOutOfMemoryError)' },
      { name: 'executorJvmState', value: 'ALIVE (Executor.isFatalError == false; no exit 52 or 137)' },
      { name: 'sourceRef', value: 'TaskMemoryManager.java (acquireExecutionMemory)' },
    ],
    deepDiveComponentId: 'task-attempt',
  });
  b.addEvent({
    stepId: sSpill,
    from: taskAttempt,
    to: producerExec,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM TaskMemoryManager',
    port: `${prodHost} in-JVM`,
    label: 'SPILL_OUT_OF_MEMORY',
    info: 'Mid-task spill throws SparkOutOfMemoryError [SPILL_OUT_OF_MEMORY]; Executor.isFatalError=false keeps executor JVM alive.',
    size: 'SparkOutOfMemoryError',
    command: 'throw new SparkOutOfMemoryError("SPILL_OUT_OF_MEMORY") // non-fatal to executor JVM',
    fields: [
      { name: 'isFatalError', value: 'false (executor stays alive)' },
      { name: 'taskAttempt', value: '0 FAILED (before result serialization)' },
    ],
    senderState: 'Task thread failed cleanly during spill',
    receiverState: 'Executor stays alive; sending ExceptionFailure StatusUpdate',
    isError: true,
  });
  b.addEvent({
    stepId: sSpill,
    from: producerExec,
    to: tsm,
    channel: 'error',
    protocol: 'Spark Netty RPC (StatusUpdate)',
    port: b.driverRpc,
    label: 'Task FAIL (retry 1/4)',
    info: 'Executor reports ExceptionFailure(SPILL_OUT_OF_MEMORY) without dying; TaskSetManager reschedules task.',
    size: '1.8 KB',
    command: 'StatusUpdate(FAILED, ExceptionFailure("SparkOutOfMemoryError [SPILL_OUT_OF_MEMORY]"))',
    fields: [{ name: 'numFailures', value: '1 / 4 (retrying)' }],
    senderState: `${producerExec} remains live in pool`,
    receiverState: `Rescheduling attempt 1 onto ${retryExec}`,
    isError: true,
  });
  b.addEvent({
    stepId: sSpill,
    from: state.schedBackend,
    to: retryExec,
    channel: 'rpc',
    protocol: 'Spark Netty RPC (LaunchTask)',
    port: b.driverRpc,
    label: 'Retry task (attempt 1 OK)',
    info: `Retried task succeeds on ${retryExec} with healthy spill headroom before result serialization/commit.`,
    size: '2.1 KB',
    command: 'executorEndpoint.send(LaunchTask(attempt=1)) // SUCCEEDED',
    fields: [{ name: 'retryOutcome', value: 'SUCCEEDED' }],
    senderState: 'Task retry dispatched',
    receiverState: 'Attempt 1 completed cleanly',
  });
}

export function injectTaskResultLostStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
  opts: {
    inCollectStep: boolean;
    perPartMiB?: number;
    totalMiB?: number;
    stageLabel?: string;
    actionCompleteLabel?: string;
    actionCompleteInfo?: string;
    actionCompleteSenderState?: string;
  },
): void {
  if (state.cfg.fault !== 'task-result-lost') return;
  const producerExec = state.liveExecs[0];
  const producerBm = state.liveBms[0];
  const trg = b.std('task-result-getter');
  const tsm = b.std('task-set-manager');
  const retryIdx = state.liveExecs.length - 1;
  const retryExec = state.liveExecs[retryIdx];
  const retryBm = state.liveBms[retryIdx];
  const prodBmPort = b.instanceHostLabel(producerBm);
  const retryBmPort = b.instanceHostLabel(retryBm);
  const sizeMiB = opts.perPartMiB ?? 4;
  const stageContext = opts.stageLabel ?? (opts.inCollectStep ? 'collect() ResultStage' : 'active ResultStage');

  const sTrl = b.addStep({
    id: 'sim-step-task-result-lost',
    title: opts.inCollectStep
      ? 'IndirectTaskResult block unavailable before fetch -> TaskResultLost -> retry & deliver collect()'
      : `IndirectTaskResult lost during ${stageContext} -> TaskResultLost -> retry before completion`,
    explanation:
      `During ${stageContext}, ${producerExec} stored TaskResultBlockId (${sizeMiB} MiB > 1 MiB spark.task.maxDirectResultSize) with StorageLevel.MEMORY_AND_DISK_SER (Executor.scala:1030-1036, so mere memory pressure spills to disk rather than evicting it) and sent StatusUpdate(IndirectTaskResult). Between StatusUpdate and TaskResultGetter.getRemoteBytes (BEFORE stage/action completion), the executor/local disk became unavailable or the block was explicitly dropped/corrupted. Per TaskResultGetter.scala:89-96, TaskResultGetter invokes TaskSchedulerImpl.handleFailedTask(tsm, tid, TaskState.FINISHED, TaskResultLost) (TaskState.FINISHED, NOT TaskState.FAILED), and TaskSetManager retries attempt 1 on ${retryExec}, which delivers its clean result before ${stageContext} finishes.`,
    category: 'yarn-lifecycle',
    keyState: opts.inCollectStep
      ? 'TaskResultLost (TaskState.FINISHED, TaskResultLost) -> attempt 1 retry -> Result delivered'
      : `TaskResultLost during ${stageContext} (TaskState.FINISHED, TaskResultLost) -> attempt 1 retry -> result delivered`,
    command:
      'TaskResultGetter.enqueueSuccessfulTask() -> getRemoteBytes() failed -> handleFailedTask(tsm, tid, TaskState.FINISHED, TaskResultLost)',
    detailsPassed: [
      { name: 'storageLevel', value: 'StorageLevel.MEMORY_AND_DISK_SER (Executor.scala:1030-1036; NOT plain memory eviction)' },
      { name: 'lossCause', value: 'Executor/local disk became unavailable or block explicitly dropped/corrupted before fetch' },
      {
        name: 'schedulerCallback',
        value: 'TaskSchedulerImpl.handleFailedTask(taskSetManager, tid, TaskState.FINISHED, TaskResultLost) (TaskResultGetter.scala:89-96)',
      },
      { name: 'threshold', value: 'spark.task.maxDirectResultSize (1 MiB)' },
      { name: 'sourceRef', value: 'Executor.scala:1030-1036; TaskResultGetter.scala:89-96; DAGScheduler.scala' },
    ],
    deepDiveComponentId: 'task-result-getter',
  });
  if (!opts.inCollectStep) {
    b.addEvent({
      stepId: sTrl,
      from: producerExec,
      to: trg,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: `IndirectTaskResult(${sizeMiB}M)`,
      info: `During ${stageContext}, ${producerExec} stores taskresult_42 (${sizeMiB} MiB > 1 MiB maxDirectResultSize) in ${producerBm} with StorageLevel.MEMORY_AND_DISK_SER and sends IndirectTaskResult reference before action completion.`,
      size: '280 B ref',
      command: `new IndirectTaskResult(TaskResultBlockId(42), size=${sizeMiB * 1024 * 1024}) // MEMORY_AND_DISK_SER`,
      fields: [
        { name: 'blockId', value: 'taskresult_42' },
        { name: 'storageLevel', value: 'StorageLevel.MEMORY_AND_DISK_SER' },
      ],
      senderState: 'Task finished on executor; awaiting driver result fetch',
      receiverState: 'Enqueuing remote fetch in TaskResultGetter',
    });
  }
  b.addEvent({
    stepId: sTrl,
    from: trg,
    to: producerBm,
    channel: 'error',
    protocol: 'BlockTransfer (BlockNotFound)',
    port: prodBmPort,
    label: 'getRemoteBytes -> LOST',
    info: `Between StatusUpdate(IndirectTaskResult) and result delivery, Driver getRemoteBytes(taskresult_42) fails on ${prodBmPort} because the executor/local disk became unavailable or the block was explicitly dropped/corrupted (NOT mere memory eviction -- IndirectTaskResult uses StorageLevel.MEMORY_AND_DISK_SER).`,
    size: 'BlockNotFound',
    command: 'blockManager.getRemoteBytes(TaskResultBlockId(42)) -> None (TaskResultLost)',
    fields: [
      { name: 'taskFailedReason', value: 'TaskResultLost' },
      { name: 'storageLevel', value: 'StorageLevel.MEMORY_AND_DISK_SER (spills to disk on RAM pressure)' },
    ],
    senderState: 'Reporting TaskState.FINISHED, TaskResultLost BEFORE result delivery',
    receiverState: 'TaskResultBlock unavailable (executor/disk fault or explicit removal)',
    isError: true,
  });
  b.addEvent({
    stepId: sTrl,
    from: trg,
    to: tsm,
    kind: 'local',
    channel: 'internal',
    protocol: 'In-JVM TaskResultLost Retry',
    port: `${b.driverHost} in-JVM`,
    label: 'TaskResultLost -> retry',
    info: `Per TaskResultGetter.scala:89-96, TaskResultGetter invokes TaskSchedulerImpl.handleFailedTask(tsm, tid=42, TaskState.FINISHED, TaskResultLost) (TaskState.FINISHED, NOT TaskState.FAILED); TaskSetManager reschedules attempt 1 on ${retryExec}.`,
    size: 'in-memory',
    command: 'scheduler.handleFailedTask(taskSetManager, tid=42, TaskState.FINISHED, TaskResultLost)',
    fields: [
      { name: 'taskState', value: 'TaskState.FINISHED (TaskResultGetter.scala:89-96)' },
      { name: 'reason', value: 'TaskResultLost' },
      { name: 'retryExecutor', value: retryExec },
    ],
    senderState: 'Handed TaskState.FINISHED, TaskResultLost to TaskSchedulerImpl',
    receiverState: `Attempt 1 scheduled on ${retryExec}`,
  });
  b.addEvent({
    stepId: sTrl,
    from: state.schedBackend,
    to: retryExec,
    channel: 'rpc',
    protocol: 'Spark Netty RPC (LaunchTask)',
    port: b.driverRpc,
    label: 'LaunchTask (attempt 1)',
    info: `Driver dispatches retry attempt 1 to ${retryExec} during ${stageContext} to regenerate the lost IndirectTaskResult block.`,
    size: '2.1 KB',
    command: 'executorEndpoint.send(LaunchTask(attempt=1))',
    fields: [{ name: 'retryExecutor', value: retryExec }],
    senderState: 'Dispatched retry attempt 1',
    receiverState: 'Computing & storing clean TaskResultBlock (MEMORY_AND_DISK_SER)',
  });
  b.addEvent({
    stepId: sTrl,
    from: trg,
    to: retryBm,
    channel: 'data',
    protocol: 'BlockTransfer (TaskResultBlock)',
    port: retryBmPort,
    label: 'getRemoteBytes(taskres)',
    info: opts.inCollectStep
      ? `After retry attempt 1 succeeds, Driver TaskResultGetter fetches clean TaskResultBlock (${sizeMiB} MiB${opts.totalMiB ? `; ${opts.totalMiB} MiB total <= 1 GiB` : ''}) from ${retryBmPort} for handoff to TaskScheduler / DAGScheduler before RDD.collect() concatenation.`
      : `After retry attempt 1 succeeds during ${stageContext}, Driver TaskResultGetter fetches clean TaskResultBlock (${sizeMiB} MiB) from ${retryBmPort} so the stage/action can finish cleanly.`,
    size: `${sizeMiB} MiB`,
    command: 'sparkEnv.blockManager.getRemoteBytes(TaskResultBlockId(retryTid))',
    fields: [{ name: 'retryAttempt', value: '1 SUCCEEDED' }],
    senderState: opts.inCollectStep
      ? 'Fetched retry TaskResultBlock; handing partition result to TaskScheduler / DAGScheduler'
      : `Retry attempt 1 result fetched for ${stageContext}`,
    receiverState: 'TaskResultBlock removed after fetch',
  });
  if (opts.actionCompleteLabel) {
    const dag = b.std('dag-scheduler');
    b.addEvent({
      stepId: sTrl,
      from: trg,
      to: dag,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM Action Complete',
      port: `${b.driverHost} in-JVM`,
      label: opts.actionCompleteLabel,
      info:
        opts.actionCompleteInfo ??
        `Driver TaskResultGetter delivers retry attempt 1 result to DAGScheduler; ${stageContext} completes cleanly after TaskResultLost recovery.`,
      size: 'in-memory',
      command: `DAGScheduler.handleTaskCompletion(attempt=1, Success) // ${opts.actionCompleteLabel}`,
      fields: [
        { name: 'stage', value: stageContext },
        { name: 'outcome', value: 'SUCCEEDED after TaskResultLost retry' },
      ],
      senderState: opts.actionCompleteSenderState ?? `${opts.actionCompleteLabel} after TaskResultLost retry`,
      receiverState: 'ActiveJob finished cleanly',
    });
  }
}

// =============================================================================
// PHASE 9: Post-Workload Mid-Run Faults (Recoverable & Degraded)
// =============================================================================
export function injectPostWorkloadMidRunFault(
  b: SimRunBuilder,
  state: SimRuntimeState,
  opts?: { taskResultLostHandled?: boolean },
): void {
  const fault = state.cfg.fault;
  if (fault === 'task-spill-oom') return;
  if (fault === 'task-result-lost') {
    if (!opts?.taskResultLostHandled) {
      injectTaskResultLostStep(b, state, { inCollectStep: false });
    }
    return;
  }

  const producerExec = state.liveExecs[0];
  const producerBm = state.liveBms[0];
  const prodExecNum = b.instanceExecutorNumber(producerExec);
  const { hostZone: prodZone, hostName: prodHost } = b.instanceWorkerHost(producerExec);
  const prodCid = b.instanceContainerId(producerExec);
  const nmProd = b.nmInstance(prodZone);

  if (fault === 'oom-pmem' || fault === 'yarn-preemption') {
    const isPmem = fault === 'oom-pmem';
    const yarnCode = isPmem ? -104 : -102;
    const sLoss = b.addStep({
      id: `sim-step-${fault}`,
      title: isPmem
        ? 'YARN NM kills Exec-1 (KILLED_EXCEEDED_PMEM=-104) -- charged to task failures'
        : 'YARN preempts Exec-1 (PREEMPTED=-102) -- NOT charged to task failures',
      explanation: isPmem
        ? `Exec-1 container exceeds physical memory limit (${b.isPySpark ? 'JVM heap + Python worker + native RSS' : 'JVM heap + off-heap + native RSS'}). NodeManager terminates container via OS SIGKILL (137) and reports Hadoop ContainerExitStatus.KILLED_EXCEEDED_PMEM (-104); YarnAllocator marks exitCausedByApp=true, so TaskSetManager increments task failure count and retries on surviving/replacement executor.`
        : 'YARN CapacityScheduler preempts Exec-1 for queue sharing (ContainerExitStatus.PREEMPTED=-102). YarnAllocator marks exitCausedByApp=false, so TaskSetManager reschedules interrupted tasks WITHOUT charging spark.task.maxFailures.',
      category: 'yarn-lifecycle',
      keyState: isPmem
        ? 'KILLED_EXCEEDED_PMEM (-104) | exitCausedByApp=true -> replacement exec-repl registered'
        : 'PREEMPTED (-102) | exitCausedByApp=false (free retry) -> replacement exec-repl registered',
      command: isPmem
        ? 'YarnAllocator.processCompletedContainers(KILLED_EXCEEDED_PMEM=-104) // exitCausedByApp=true'
        : 'YarnAllocator.processCompletedContainers(PREEMPTED=-102) // exitCausedByApp=false',
      detailsPassed: isPmem
        ? [
            { name: 'yarnExitStatus', value: 'KILLED_EXCEEDED_PMEM (-104)' },
            { name: 'osSignalCause', value: 'SIGKILL (137) sent by NM memory monitor' },
            { name: 'exitCausedByApp', value: 'true' },
            { name: 'countsTowardsTaskFailures', value: 'true' },
          ]
        : [
            { name: 'yarnExitStatus', value: 'PREEMPTED (-102)' },
            { name: 'exitCausedByApp', value: 'false' },
            { name: 'countsTowardsTaskFailures', value: 'false' },
          ],
      deepDiveComponentId: 'yarn-allocator',
    });
    b.addEvent({
      stepId: sLoss,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: isPmem ? 'Kill Exec-1 (PMEM -104)' : 'Preempt Exec-1 (-102)',
      info: isPmem
        ? `NodeManager terminates ${prodCid} via OS SIGKILL for exceeding physical memory limits and reports ContainerExitStatus.KILLED_EXCEEDED_PMEM (-104).`
        : `YARN preempts ${prodCid} to rebalance queue capacity and reports ContainerExitStatus.PREEMPTED (-102).`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, exitStatus=${yarnCode})`,
      fields: isPmem
        ? [
            { name: 'containerId', value: prodCid },
            { name: 'containerExitStatus', value: '-104 (KILLED_EXCEEDED_PMEM)' },
            { name: 'osSignalCause', value: 'OS SIGKILL (137)' },
          ]
        : [
            { name: 'containerId', value: prodCid },
            { name: 'containerExitStatus', value: '-102 (PREEMPTED)' },
          ],
      senderState: `Container killed on ${prodHost}`,
      receiverState: 'RM reports completedContainer to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sLoss,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: isPmem ? 'RemoveExec(appFault=true)' : 'RemoveExec(preempted)',
      info: `YarnAllocator reports ExecutorExited(${yarnCode}, exitCausedByApp=${isPmem}) to driver; TaskSetManager reschedules affected tasks.`,
      size: '740 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(${yarnCode}, exitCausedByApp=${isPmem})))`,
      fields: [
        { name: 'exitCode', value: String(yarnCode) },
        { name: 'exitCausedByApp', value: String(isPmem) },
      ],
      senderState: 'Requesting replacement container from RM',
      receiverState: 'Tasks re-queued on surviving/replacement executor',
    });
    replaceDeadExecutor(b, state, sLoss, producerExec, prodZone);
    return;
  }

  if (fault === 'oom-vmem') {
    const sVmem = b.addStep({
      id: 'sim-step-oom-vmem',
      title: 'YARN NM kills Exec-1 (KILLED_EXCEEDED_VMEM=-103) despite healthy physical RSS',
      explanation:
        'NodeManager virtual memory monitor (yarn.nodemanager.vmem-check-enabled=true, yarn.nodemanager.vmem-pmem-ratio=2.1) observes Exec-1 virtual address space (mmap / glibc arenas) exceeding 2.1x container RAM even though physical RSS is well under budget. NM sends SIGKILL (137) and reports ContainerExitStatus.KILLED_EXCEEDED_VMEM (-103); YarnAllocator replaces the executor.',
      category: 'yarn-lifecycle',
      keyState: 'KILLED_EXCEEDED_VMEM (-103) | exitCausedByApp=true -> replacement exec-repl registered',
      command: 'YarnAllocator.processCompletedContainers(KILLED_EXCEEDED_VMEM=-103) // exitCausedByApp=true',
      detailsPassed: [
        { name: 'yarnExitStatus', value: 'KILLED_EXCEEDED_VMEM (-103)' },
        { name: 'config', value: 'yarn.nodemanager.vmem-check-enabled=true, vmem-pmem-ratio=2.1' },
        { name: 'distinction', value: 'High virtual address mappings (mmap/glibc arenas) != high physical RSS' },
      ],
      deepDiveComponentId: 'yarn-allocator',
    });
    b.addEvent({
      stepId: sVmem,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Kill Exec-1 (VMEM -103)',
      info: `NM virtual memory monitor kills ${prodCid} via SIGKILL (137) and reports ContainerExitStatus.KILLED_EXCEEDED_VMEM (-103): "10.8 GB of 8.4 GB virtual memory used."`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, exitStatus=-103) // KILLED_EXCEEDED_VMEM`,
      fields: [
        { name: 'containerExitStatus', value: '-103 (KILLED_EXCEEDED_VMEM)' },
        { name: 'physicalRss', value: '2.9 GB of 4.0 GB (healthy RSS!)' },
        { name: 'virtualMem', value: '10.8 GB of 8.4 GB (2.1x ratio tripped)' },
      ],
      senderState: `Container killed by NM vmem monitor on ${prodHost}`,
      receiverState: 'RM reports completedContainer (-103) to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sVmem,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(VMEM -103)',
      info: 'YarnAllocator reports ExecutorExited(-103, exitCausedByApp=true) and requests a replacement container.',
      size: '740 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(-103, exitCausedByApp=true)))`,
      fields: [
        { name: 'exitCode', value: '-103 (KILLED_EXCEEDED_VMEM)' },
        { name: 'exitCausedByApp', value: 'true' },
      ],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sVmem, producerExec, prodZone);
    return;
  }

  if (fault === 'kernel-node-oom') {
    const sKern = b.addStep({
      id: 'sim-step-kernel-oom',
      title: 'Linux kernel OOM killer sends SIGKILL (137) under node-wide memory pressure',
      explanation:
        `Host RAM on ${prodHost} is oversubscribed by sibling containers and OS page cache. The Linux kernel OOM killer kills Exec-1 with raw POSIX SIGKILL (exit 137 = 128 + 9) and logs "Out of memory: Killed process (java)" to dmesg. There is NO YARN PMEM/VMEM threshold message and NO JVM shutdown hook. ExecutorExitCode reports "Unknown executor exit code (137) (died from signal 9?)" and YarnAllocator replaces the executor.`,
      category: 'yarn-lifecycle',
      keyState: 'Linux kernel OOM killer -> raw SIGKILL (137) -> replacement exec-repl registered',
      command: 'dmesg: "Out of memory: Killed process <pid> (java)" -> exitStatus=137 -> ExecutorExitCode("died from signal 9?")',
      detailsPassed: [
        { name: 'exitCode', value: '137 (128 + SIGKILL 9; raw kernel kill, NOT YARN -104/-103)' },
        { name: 'evidence', value: 'dmesg/syslog: "Out of memory: Killed process <pid> (java) anon-rss:..."' },
        { name: 'sourceRef', value: 'ExecutorExitCode.scala:explainExitCode (exitCode > 128 -> signal 9?)' },
      ],
      deepDiveComponentId: 'yarn-container',
    });
    b.addEvent({
      stepId: sKern,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Kernel OOM -> exit 137',
      info: `Linux kernel OOM killer sends SIGKILL (9) to ${prodCid} on ${prodHost} under host-wide RAM exhaustion; NM reports raw exitStatus=137 with NO YARN PMEM/VMEM diagnostic.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, exitStatus=137) // Linux kernel OOM killer (dmesg)`,
      fields: [
        { name: 'exitStatus', value: '137 (128 + 9 SIGKILL)' },
        { name: 'dmesg', value: 'Out of memory: Killed process 29410 (java)' },
        { name: 'yarnPmemMessage', value: 'NONE (host-level kernel OOM, not YARN monitor)' },
      ],
      senderState: 'Executor log cut off mid-line; no JVM shutdown hook',
      receiverState: 'RM reports raw exit 137 to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sKern,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(137 sig 9?)',
      info: 'YarnAllocator treats raw exit 137 as exitCausedByApp=true; ExecutorExitCode explains "Unknown executor exit code (137) (died from signal 9?)".',
      size: '780 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(137, exitCausedByApp=true, "died from signal 9?")))`,
      fields: [
        { name: 'exitCode', value: '137' },
        { name: 'exitCausedByApp', value: 'true' },
      ],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sKern, producerExec, prodZone);
    return;
  }

  if (fault === 'exec-intentional-remove') {
    const eam = b.std('executor-allocation-manager');
    const yb = b.std('yarn-scheduler-backend');
    const sc = b.std('spark-context');
    const removeIdx = state.liveExecs.length - 1;
    const targetExec = state.liveExecs[removeIdx];
    const targetExecNum = b.instanceExecutorNumber(targetExec);
    const targetZone = b.instanceWorkerHost(targetExec).hostZone;
    const targetHost = targetZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const targetNm = b.nmInstance(targetZone);
    const targetCid = b.instanceContainerId(targetExec);
    const link = b.driverToAmLink();
    const isDyn = state.cfg.dynamicAllocation;

    const sRem = b.addStep({
      id: 'sim-step-exec-remove',
      title: isDyn
        ? `DA scales down idle ${targetExec} -> releaseAssignedContainer -> ABORTED (-100, not a fault)`
        : `Explicit killExecutors removes ${targetExec} -> releaseAssignedContainer -> ABORTED (-100)`,
      explanation: isDyn
        ? `ExecutorAllocationManager idles out ${targetExec} after spark.dynamicAllocation.executorIdleTimeout (60s), having ALREADY decremented its target, and invokes killExecutors([${targetExecNum}], adjustTargetNumExecutors=false). YarnAllocator.internalReleaseContainer records ${targetCid} in releasedContainers and calls amClient.releaseAssignedContainer(), so YARN scheduler returns ContainerExitStatus.ABORTED (-100) (KILLED_BY_APPMASTER=-105 occurs only on direct NM stopContainers). Because alreadyReleased=true, Spark marks exitCausedByApp=false independently of exit status, and surviving executor(s) finish the job with NO replacement container.`
        : `Explicit SparkContext.killExecutors([${targetExecNum}]) asks YarnSchedulerBackend.doKillExecutors(adjustTargetNumExecutors=true) to release ${targetExec}. YarnAllocator.internalReleaseContainer records ${targetCid} in releasedContainers and calls amClient.releaseAssignedContainer(), so YARN scheduler returns ContainerExitStatus.ABORTED (-100) (KILLED_BY_APPMASTER=-105 occurs only on direct NM stopContainers). Because alreadyReleased=true, Spark marks exitCausedByApp=false independently of exit status, and surviving executor(s) finish the job cleanly WITHOUT allocating a bogus replacement container.`,
      category: 'yarn-lifecycle',
      keyState: `${targetExec} intentionally released -> YARN ABORTED (-100), Spark alreadyReleased=true (exitCausedByApp=false); surviving exec(s) finish`,
      command: isDyn
        ? `ExecutorAllocationManager -> killExecutors(["${targetExecNum}"], adjustTargetNumExecutors=false) -> amClient.releaseAssignedContainer(${targetCid})`
        : `SparkContext.killExecutors(["${targetExecNum}"]) -> doKillExecutors(adjustTargetNumExecutors=true) -> amClient.releaseAssignedContainer(${targetCid})`,
      detailsPassed: [
        { name: 'yarnExitStatus', value: 'ContainerExitStatus.ABORTED (-100) via amClient.releaseAssignedContainer() (KILLED_BY_APPMASTER=-105 is NM direct stopContainers only)' },
        { name: 'sparkClassification', value: 'releasedContainers.remove() -> alreadyReleased=true -> exitCausedByApp=false independently of exit status' },
        {
          name: 'triggerAndTargetAdjust',
          value: isDyn
            ? 'DA idle timeout (60s): target already decremented -> killExecutors(..., adjustTargetNumExecutors=false)'
            : 'Explicit SparkContext.killExecutors (DA off): surviving executor(s) finish with 0 replacement containers',
        },
        { name: 'sourceRef', value: 'YarnAllocator.scala:855,926-930,1005-1008' },
      ],
      deepDiveComponentId: 'executor-allocation-manager',
    });
    b.addEvent({
      stepId: sRem,
      from: isDyn ? eam : sc,
      to: yb,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM KillExecutors',
      port: `${b.driverHost} in-JVM`,
      label: isDyn ? `DA scale-down (${targetExec})` : `killExecutors(${targetExec})`,
      info: isDyn
        ? `ExecutorAllocationManager idle timeout (60s) already decremented target and calls killExecutors(["${targetExecNum}"], adjustTargetNumExecutors=false).`
        : `Explicit SparkContext.killExecutors(["${targetExecNum}"]) invokes YarnSchedulerBackend.doKillExecutors(adjustTargetNumExecutors=true) while surviving executors finish the job.`,
      size: 'in-memory',
      command: isDyn
        ? `client.killExecutors(Seq("${targetExecNum}"), adjustTargetNumExecutors=false)`
        : `sc.killExecutors(Seq("${targetExecNum}")) -> doKillExecutors(adjustTargetNumExecutors=true)`,
      fields: [
        { name: 'targetExecutor', value: `${targetExec} (id=${targetExecNum})` },
        { name: 'adjustTargetNumExecutors', value: String(!isDyn) },
      ],
      senderState: isDyn ? 'Scaling down idle executor' : 'Explicitly removing executor',
      receiverState: 'Sending KillExecutors RPC to active AM',
    });
    b.addEvent({
      stepId: sRem,
      from: yb,
      to: state.activeAm,
      kind: link.kind,
      channel: 'rpc',
      protocol: link.protocol,
      port: link.port,
      label: `KillExecutors(${targetExec})`,
      info: `Driver asks active AM to release container ${targetCid} for ${targetExec}.`,
      size: '540 B',
      command: `amEndpoint.askSync[Boolean](KillExecutors(Seq("${targetExecNum}")))`,
      fields: [{ name: 'containerId', value: targetCid }],
      senderState: 'Requested intentional container release',
      receiverState: 'YarnAllocator.internalReleaseContainer recording releasedContainers',
    });
    b.addEvent({
      stepId: sRem,
      from: state.alloc,
      to: state.rm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ApplicationMasterProtocol)',
      port: 'rm01:8030',
      label: 'releaseContainer (-100)',
      info: `YarnAllocator.internalReleaseContainer records ${targetCid} in releasedContainers and calls amClient.releaseAssignedContainer(${targetCid}); YARN scheduler returns ContainerExitStatus.ABORTED (-100) (KILLED_BY_APPMASTER=-105 only happens on direct NM stopContainers). Spark marks alreadyReleased=true -> exitCausedByApp=false independently of exit status.`,
      size: '920 B',
      command: `amClient.releaseAssignedContainer(${targetCid}) // YARN ABORTED (-100); Spark alreadyReleased=true`,
      fields: [
        { name: 'yarnExitStatus', value: '-100 (ContainerExitStatus.ABORTED via releaseAssignedContainer)' },
        { name: 'noteOnMinus105', value: 'KILLED_BY_APPMASTER (-105) is NM direct stopContainers only' },
        { name: 'sparkAttribution', value: 'alreadyReleased=true -> exitCausedByApp=false (not charged to failure budget)' },
      ],
      senderState: 'Released container cleanly (0 replacement requested)',
      receiverState: `Instructing ${targetHost} NM to stop container via heartbeat`,
    });
    b.addEvent({
      stepId: sRem,
      from: state.rm,
      to: targetNm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ResourceTracker :8031)',
      port: 'rm01:8031',
      label: 'NM cleanup (ABORTED -100)',
      info: `RM notifies ${targetHost} NM via :8031 heartbeat to stop ${targetCid} (OS SIGTERM -> SIGKILL 137 if stalled); completed container status is ContainerExitStatus.ABORTED (-100) and surviving executor(s) finish the job with no replacement.`,
      size: '840 B',
      command: `NodeHeartbeatResponse(containersToCleanup=[${targetCid}]) // exitStatus=-100 (ABORTED)`,
      fields: [
        { name: 'exitStatus', value: '-100 (ContainerExitStatus.ABORTED)' },
        { name: 'replacementAllocated', value: 'false (surviving executors finish cleanly)' },
      ],
      senderState: 'Container released',
      receiverState: `${targetExec} stopped cleanly`,
    });

    // minExecutors >= 2 is enforced by faultCatalog.ts; remove target cleanly with NO bogus replacement!
    state.liveExecs.splice(removeIdx, 1);
    state.liveBms.splice(removeIdx, 1);
    state.liveExecNums.splice(removeIdx, 1);
    return;
  }

  if (fault === 'heartbeat-timeout') {
    const sHb = b.addStep({
      id: 'sim-step-hb-timeout',
      title: `Exec-${prodExecNum} misses 120s HeartbeatReceiver timeout -> driver kills & replaces it`,
      explanation:
        `Exec-${prodExecNum} pauses (e.g., long GC or network partition) and stops heartbeating even though ${prodHost} NodeManager is still healthy. HeartbeatReceiver.expireDeadHosts() calls sc.killAndReplaceExecutor("${prodExecNum}") and sends RemoveExecutor directly to CoarseGrainedSchedulerBackend (the timeout itself is NOT SIGKILL, though subsequent NM release may SIGKILL the hung JVM), then YarnAllocator launches replacement exec-repl.`,
      category: 'heartbeats-metrics',
      keyState: `Exec-${prodExecNum} expired by HeartbeatReceiver -> killAndReplaceExecutor("${prodExecNum}") -> exec-repl registered`,
      command: `HeartbeatReceiver.expireDeadHosts() -> sc.killAndReplaceExecutor("${prodExecNum}")`,
      detailsPassed: [
        { name: 'heartbeatInterval', value: '10s' },
        { name: 'timeout', value: '120s (spark.network.timeout)' },
        { name: 'sigkillDistinction', value: 'Timeout itself is NOT SIGKILL; subsequent NM cleanup may SIGKILL stale JVM' },
      ],
      deepDiveComponentId: 'heartbeat-receiver',
    });
    b.addEvent({
      stepId: sHb,
      from: state.hbRx,
      to: state.schedBackend,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ExpireDeadHosts',
      port: `${b.driverHost} in-JVM`,
      label: `Timeout -> RemoveExec(${prodExecNum})`,
      info: `now - executorLastSeen("${prodExecNum}") > 120000 ms; HeartbeatReceiver requests replacement and removes Exec-${prodExecNum}.`,
      size: 'in-memory',
      command: `sc.killAndReplaceExecutor("${prodExecNum}"); driverEndpoint.send(RemoveExecutor("${prodExecNum}", ...))`,
      fields: [{ name: 'reason', value: 'Executor heartbeat timed out after 120000 ms' }],
      senderState: `Removed Exec-${prodExecNum} from executorLastSeen`,
      receiverState: `Failed in-flight tasks on Exec-${prodExecNum} & asked AM for replacement`,
      isError: true,
    });
    replaceDeadExecutor(b, state, sHb, producerExec, prodZone);
    return;
  }

  if (fault === 'sigterm-escalated-sigkill') {
    const sEsc = b.addStep({
      id: 'sim-step-sigterm-sigkill',
      title: 'NM SIGTERM (sig 15) stalls past 250ms -> escalates to SIGKILL (sig 9) -> single exit 137',
      explanation:
        `During an unexpected container watchdog/stop on ${prodHost}, NodeManager sends SIGTERM (OS signal 15) and waits yarn.nodemanager.sleep-delay-before-sigkill.ms (250ms). Because the stalled JVM fails to exit in time, NM supervisor escalates to SIGKILL (OS signal 9), producing ONE single process/shell exit status of 137 (128 + 9 -- the process NEVER exited 143!). Spark attribution depends on release state: already-released containers are exitCausedByApp=false, whereas this unexpected, unreleased forced kill has alreadyReleased=false -> exitCausedByApp=true -> replaced.`,
      category: 'yarn-lifecycle',
      keyState: 'SIGTERM (sig 15) -> 250ms timeout -> SIGKILL (sig 9) -> single exit 137 (never 143) -> replaced',
      command: 'NM ContainerLaunch: SIGTERM (sig 15) -> sleep-delay-before-sigkill.ms (250ms) -> SIGKILL (sig 9) -> exit 137',
      detailsPassed: [
        { name: 'osSignals', value: 'SIGTERM (signal 15) -> 250ms grace expired -> SIGKILL (signal 9)' },
        { name: 'singleExitStatus', value: '137 (128 + 9; process never exited 143)' },
        { name: 'graceWindow', value: 'yarn.nodemanager.sleep-delay-before-sigkill.ms (default 250ms)' },
        { name: 'sparkAttribution', value: 'alreadyReleased=false (unexpected/unreleased kill) -> exitCausedByApp=true -> replaced (alreadyReleased=true would be false)' },
        { name: 'sourceRef', value: 'YarnAllocator.scala:855-930; Hadoop ContainerLaunch (sleep-delay-before-sigkill.ms)' },
      ],
      deepDiveComponentId: 'node-manager',
    });
    b.addEvent({
      stepId: sEsc,
      from: nmProd,
      to: producerExec,
      kind: 'local',
      channel: 'error',
      protocol: 'OS Signals (TERM -> KILL)',
      port: `${prodHost} OS`,
      label: 'SIGTERM -> 250ms -> SIGKILL',
      info: `NM sends SIGTERM (signal 15) to ${prodCid}; JVM stalls past yarn.nodemanager.sleep-delay-before-sigkill.ms (250ms), so NM supervisor escalates to SIGKILL (signal 9) yielding a single process exit code of 137 (never 143).`,
      size: 'Signal 15 -> 9',
      command: 'kill -TERM <pid> (sig 15) -> wait 250ms -> kill -KILL <pid> (sig 9) => single exit 137',
      fields: [
        { name: 'osSignals', value: 'SIGTERM (15) -> SIGKILL (9) after 250ms' },
        { name: 'singleShellExitCode', value: '137 (128 + 9; never exited 143)' },
      ],
      senderState: 'Escalated to SIGKILL after 250ms grace window expired',
      receiverState: 'Abrupt JVM termination with single OS exit code 137',
      isError: true,
    });
    b.addEvent({
      stepId: sEsc,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Container exit (137)',
      info: `${prodHost} NM reports ${prodCid} COMPLETE with single exitStatus=137 (128 + SIGKILL 9) to RM :8031.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=137)`,
      fields: [{ name: 'yarnContainerExitStatus', value: '137 (single OS exit from escalated SIGKILL)' }],
      senderState: 'Container reaped on worker',
      receiverState: 'RM reports completed container to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sEsc,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(137 unrel.)',
      info: `Because this kill was unexpected/unreleased (alreadyReleased=false), YarnAllocator classifies exit 137 as exitCausedByApp=true and requests a replacement container (an already-released kill would be exitCausedByApp=false).`,
      size: '740 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(137, exitCausedByApp=true)))`,
      fields: [
        { name: 'exitCode', value: '137' },
        { name: 'sparkAttribution', value: 'alreadyReleased=false -> exitCausedByApp=true -> replaced' },
      ],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sEsc, producerExec, prodZone);
    return;
  }

  if (fault === 'external-manual-kill') {
    const sMan = b.addStep({
      id: 'sim-step-manual-kill',
      title: 'External admin / watchdog runs kill -9 <pid> -> raw SIGKILL (137)',
      explanation:
        `An external host admin, watchdog, or security agent executes kill -9 directly against Exec-1 on ${prodHost}. The JVM dies instantly with raw exit 137 (128 + 9), bypassing shutdown hooks, with NO YARN PMEM/VMEM diagnostic and NO Linux kernel OOM entry in dmesg. ExecutorExitCode reports "died from signal 9?" and YarnAllocator replaces Exec-1.`,
      category: 'yarn-lifecycle',
      keyState: 'External kill -9 -> raw SIGKILL (137, no YARN/dmesg OOM) -> exec-repl registered',
      command: 'kill -9 <exec-1-pid> -> exitStatus=137 -> ExecutorExitCode("died from signal 9?")',
      detailsPassed: [
        { name: 'exitCode', value: '137 (128 + SIGKILL 9)' },
        { name: 'diagnostics', value: 'No YARN PMEM/VMEM message; no dmesg kernel OOM line' },
        { name: 'sourceRef', value: 'ExecutorExitCode.scala:62-66 ("died from signal 9?")' },
      ],
      deepDiveComponentId: 'yarn-container',
    });
    b.addEvent({
      stepId: sMan,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'External kill -9 (137)',
      info: `External watchdog sent kill -9 to ${prodCid} on ${prodHost}; NM observes vanished PID with raw exitStatus=137 and no YARN memory/preemption diagnostics.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=137) // external kill -9`,
      fields: [
        { name: 'exitStatus', value: '137 (SIGKILL 9)' },
        { name: 'yarnMemoryDiagnostics', value: 'NONE' },
        { name: 'dmesgOomEntry', value: 'NONE' },
      ],
      senderState: 'Container vanished with raw exit 137',
      receiverState: 'RM reports completed container to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sMan,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(137 sig 9?)',
      info: 'YarnAllocator notifies driver: ExecutorExitCode explains "Unknown executor exit code (137) (died from signal 9?)".',
      size: '760 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(137, exitCausedByApp=true, "died from signal 9?")))`,
      fields: [{ name: 'exitCode', value: '137' }],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sMan, producerExec, prodZone);
    return;
  }

  if (fault === 'jvm-heap-oom') {
    const taskAttempt = b.workerStd('task-attempt', prodZone);
    const sHeap = b.addStep({
      id: 'sim-step-jvm-heap-oom',
      title: 'JVM Heap OutOfMemoryError -> SparkUncaughtExceptionHandler exits 52 (SparkExitCode.OOM)',
      explanation:
        `A task inside Exec-${prodExecNum} exhausts JVM -Xmx heap (spark.executor.memory / spark.memory.fraction=0.6), throwing java.lang.OutOfMemoryError: Java heap space. SparkUncaughtExceptionHandler walks the cause chain (spark.executor.killOnFatalErrorDepth=5) and calls System.exit(52) (SparkExitCode.OOM = 52), falling back to Runtime.halt(52) if SparkUncaughtExceptionHandler itself encounters OOM -- NOT a YARN PMEM kill or SIGKILL.`,
      category: 'yarn-lifecycle',
      keyState: 'JVM Heap OOM -> SparkExitCode.OOM (52) -> replacement exec-repl registered',
      command: 'SparkUncaughtExceptionHandler.uncaughtException(OutOfMemoryError) -> System.exit(52)',
      detailsPassed: [
        { name: 'exitCode', value: '52 (SparkExitCode.OOM; NOT YARN -104 / NOT SIGKILL)' },
        { name: 'exception', value: 'java.lang.OutOfMemoryError: Java heap space' },
        { name: 'haltFallback', value: 'Falls back to Runtime.halt(52) if SparkUncaughtExceptionHandler itself encounters OOM' },
        { name: 'sourceRef', value: 'SparkUncaughtExceptionHandler.scala:71-108; SparkExitCode.scala:41' },
      ],
      deepDiveComponentId: 'executor',
    });
    b.addEvent({
      stepId: sHeap,
      from: taskAttempt,
      to: producerExec,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM SparkUncaughtExceptionHandler',
      port: `${prodHost} in-JVM`,
      label: 'Heap OOM -> exit 52',
      info: 'Task throws java.lang.OutOfMemoryError: Java heap space; SparkUncaughtExceptionHandler catches OOM and calls System.exit(52) (falling back to Runtime.halt(52) if SparkUncaughtExceptionHandler itself encounters OOM).',
      size: 'exit code 52',
      command: 'System.exit(SparkExitCode.OOM) // 52 (falls back to Runtime.halt(52) if handler itself hits OOM)',
      fields: [
        { name: 'sparkExitCode', value: 'SparkExitCode.OOM = 52' },
        { name: 'killOnFatalErrorDepth', value: '5 (spark.executor.killOnFatalErrorDepth)' },
      ],
      senderState: 'JVM heap exhausted (-Xmx)',
      receiverState: 'Executor JVM exiting with code 52',
      isError: true,
    });
    b.addEvent({
      stepId: sHeap,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Container exit (52 OOM)',
      info: `${prodHost} NM reports ${prodCid} exitStatus=52 (SparkExitCode.OOM) to RM :8031.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=52) // SparkExitCode.OOM`,
      fields: [
        { name: 'containerId', value: prodCid },
        { name: 'exitStatus', value: '52 (SparkExitCode.OOM)' },
      ],
      senderState: 'Container exited cleanly via System.exit(52)',
      receiverState: 'RM reports completed container (exit 52) to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sHeap,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(OOM exit=52)',
      info: 'YarnAllocator reports ExecutorExited(52, exitCausedByApp=true, "OutOfMemoryError") and requests replacement container.',
      size: '760 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(52, exitCausedByApp=true)))`,
      fields: [
        { name: 'exitCode', value: '52 (SparkExitCode.OOM)' },
        { name: 'exitCausedByApp', value: 'true' },
      ],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sHeap, producerExec, prodZone);
    return;
  }

  if (fault === 'node-crash-power-loss') {
    const deadZone = prodZone;
    const deadHost = prodHost;
    const healthyZone: 'worker-node-a' | 'worker-node-b' =
      deadZone === 'worker-node-a' ? 'worker-node-b' : 'worker-node-a';
    const healthyHost = healthyZone === 'worker-node-a' ? 'worker-a' : 'worker-b';
    const retiredExecs = retireHostExecutors(b, state, deadZone);

    const sNode = b.addStep({
      id: 'sim-step-node-crash',
      title: `Host ${deadHost} power loss -> NO OS exit code -> RM NMLivelinessMonitor synthesizes ABORTED (-100)`,
      explanation:
        `Worker host ${deadHost} suffers a hard kernel panic / power loss, leaving NO OS process exit code at all (not 137, not 52, not -104). Separately, ${deadHost} NM stops heartbeating RM on :8031 (yarn.nm.liveness-monitor.expiry-interval-ms = 600000ms); RM NMLivelinessMonitor expires the host as LOST and YARN scheduler synthesizes ContainerExitStatus.ABORTED (-100, which Spark classifies as exitCausedByApp=false). Meanwhile Driver HeartbeatReceiver expires ${retiredExecs.join(', ')} after 120s and YarnAllocator launches replacement on healthy ${healthyHost}.`,
      category: 'heartbeats-metrics',
      keyState: `Host ${deadHost} LOST (NO OS exit code; RM synthesizes ABORTED=-100, exitCausedByApp=false) -> exec-repl on ${healthyHost}`,
      command: `NMLivelinessMonitor(${deadHost} LOST -> ContainerExitStatus.ABORTED=-100) + HeartbeatReceiver.expireDeadHosts()`,
      detailsPassed: [
        { name: 'osProcessExitCode', value: 'NONE (no exit code -- host powered off / kernel panic, NOT 137 or 52)' },
        { name: 'yarnSynthesizedStatus', value: 'ContainerExitStatus.ABORTED (-100) synthesized by RM on NMLivelinessMonitor expiry' },
        { name: 'sparkAttribution', value: 'exitCausedByApp=false (ABORTED is in NOT_APP_AND_SYSTEM_FAULT_EXIT_STATUS)' },
        { name: 'nmLivenessExpiry', value: '600000ms (yarn.nm.liveness-monitor.expiry-interval-ms on :8031)' },
        { name: 'retiredExecutors', value: retiredExecs.join(', ') },
        { name: 'replacementHost', value: healthyHost },
      ],
      deepDiveComponentId: 'node-manager',
    });
    b.addEvent({
      stepId: sNode,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (NMLivelinessMonitor :8031)',
      port: 'rm01:8031',
      label: 'NM LOST -> ABORTED (-100)',
      info: `${deadHost} power loss produces NO OS process exit code (not 137). RM NMLivelinessMonitor expires the silent NM on :8031, marks node LOST, and YARN scheduler synthesizes ContainerExitStatus.ABORTED (-100; Spark exitCausedByApp=false).`,
      size: 'No heartbeat',
      command: `NMLivelinessMonitor -> ${deadHost} LOST -> synthesize ContainerExitStatus.ABORTED (-100) // no OS exit code`,
      fields: [
        { name: 'nodeState', value: 'LOST' },
        { name: 'osProcessExitCode', value: 'NONE (no exit code; host powered off)' },
        { name: 'yarnSynthesizedStatus', value: 'ContainerExitStatus.ABORTED (-100, exitCausedByApp=false)' },
      ],
      senderState: `Host ${deadHost} unreachable (power loss)`,
      receiverState: `RM marked ${deadHost} LOST & synthesized ABORTED (-100)`,
      isError: true,
    });
    b.addEvent({
      stepId: sNode,
      from: state.hbRx,
      to: state.schedBackend,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM ExpireDeadHosts',
      port: `${b.driverHost} in-JVM`,
      label: 'Host lost (no exit code)',
      info: `Driver HeartbeatReceiver expires ${retiredExecs.join(', ')} after spark.network.timeout (120s); NO OS process exit code exists (RM separately synthesizes ABORTED -100 on NM expiry).`,
      size: 'in-memory',
      command: `HeartbeatReceiver.expireDeadHosts() -> SlaveLost("${deadHost} lost; no exit code")`,
      fields: [
        { name: 'retiredExecutors', value: retiredExecs.join(', ') },
        { name: 'osExitCode', value: 'NONE (no exit code)' },
      ],
      senderState: `Expired dead executors on ${deadHost}`,
      receiverState: `Scheduling replacement on healthy ${healthyHost}`,
      isError: true,
    });

    // Launch replacement on healthy worker host using monotonic container allocator.
    const { execNum: replNum, containerId: replCid } = b.allocateExec(state.activeAttempt);
    state.nextExecNum = b.peekNextExecNum(state.activeAttempt);
    const healthyNm = b.nmInstance(healthyZone);
    const replExec = b.execReplInstance(replNum, healthyZone, state.activeAttempt, replCid);
    const replBm = b.execReplBmInstance(replNum, healthyZone, state.activeAttempt, replCid);
    b.addEvent({
      stepId: sNode,
      from: state.alloc,
      to: healthyNm,
      channel: 'yarn',
      protocol: 'Hadoop RPC (ContainerManagementProtocol)',
      port: `${healthyHost}:dynamic`,
      label: 'startContainer(Exec-repl)',
      info: `YarnAllocator launches replacement ${replCid} on healthy ${healthyHost} after ${deadHost} node loss.`,
      size: '9.4 KB',
      command: `nmClient.startContainer(${replCid}, ctx[exec=${replNum}])`,
      fields: [
        { name: 'containerId', value: replCid },
        { name: 'healthyHost', value: healthyHost },
      ],
      senderState: `Dispatched replacement to ${healthyHost}`,
      receiverState: `Booting replacement JVM on ${healthyHost}`,
    });
    b.addEvent({
      stepId: sNode,
      from: replExec,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC',
      port: b.driverRpc,
      label: 'RegisterExecutor(repl)',
      info: `Replacement ${replExec} registers on healthy ${healthyHost} to re-run affected tasks.`,
      size: '1.8 KB',
      command: `ref.ask[Boolean](RegisterExecutor("${replNum}", self, "${healthyHost}", ${state.cfg.executorCores}))`,
      fields: [
        { name: 'executorId', value: `"${replNum}"` },
        { name: 'host', value: healthyHost },
      ],
      senderState: `${replExec} ready on ${healthyHost}`,
      receiverState: `Registered ${replExec} (${state.liveExecs.length + 1} live)`,
    });
    state.liveExecs.push(replExec);
    state.liveBms.push(replBm);
    state.liveExecNums.push(replNum);
    return;
  }

  if (fault === 'executor-heartbeat-max-fail') {
    const sHbMax = b.addStep({
      id: 'sim-step-hb-max-fail',
      title: 'Exec-1 fails 60 consecutive Heartbeats -> self-exits with EXIT 56 (HEARTBEAT_FAILURE)',
      explanation:
        'Executor.reportHeartBeat() on Exec-1 fails spark.executor.heartbeat.maxFailures (60) consecutive Heartbeat RPCs to driver HeartbeatReceiver. Exec-1 self-terminates via System.exit(56) (ExecutorExitCode.HEARTBEAT_FAILURE = 56), NM reports exit 56, and YarnAllocator launches replacement exec-repl.',
      category: 'heartbeats-metrics',
      keyState: '60 failed heartbeats -> ExecutorExitCode.HEARTBEAT_FAILURE (56) -> exec-repl registered',
      command: 'Executor.reportHeartBeat() // heartbeatFailures >= 60 -> System.exit(56)',
      detailsPassed: [
        { name: 'spark.executor.heartbeat.maxFailures', value: '60 (default)' },
        { name: 'exitCode', value: '56 (ExecutorExitCode.HEARTBEAT_FAILURE)' },
        { name: 'sourceRef', value: 'Executor.scala:1587; ExecutorExitCode.scala:38' },
      ],
      deepDiveComponentId: 'executor',
    });
    b.addEvent({
      stepId: sHbMax,
      from: producerExec,
      to: state.hbRx,
      channel: 'error',
      protocol: 'Spark Netty RPC (Heartbeat x60)',
      port: b.driverRpc,
      label: 'Heartbeat FAIL (60/60)',
      info: '60th consecutive Heartbeat RPC from Exec-1 fails; Executor.reportHeartBeat() calls System.exit(56).',
      size: 'exit code 56',
      command: 'System.exit(ExecutorExitCode.HEARTBEAT_FAILURE) // 56 ("Unable to send heartbeats to driver")',
      fields: [
        { name: 'consecutiveFailures', value: '60 / 60' },
        { name: 'exitCode', value: '56 (HEARTBEAT_FAILURE)' },
      ],
      senderState: 'Exec-1 self-exiting with code 56',
      receiverState: 'No heartbeats received from Exec-1',
      isError: true,
    });
    b.addEvent({
      stepId: sHbMax,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Container exit (56)',
      info: `${prodHost} NM reports ${prodCid} exitStatus=56 (HEARTBEAT_FAILURE) to RM :8031.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=56)`,
      fields: [{ name: 'exitStatus', value: '56 (HEARTBEAT_FAILURE)' }],
      senderState: 'Container exited with code 56',
      receiverState: 'RM reports completed container to AM',
      isError: true,
    });
    b.addEvent({
      stepId: sHbMax,
      from: state.alloc,
      to: state.schedBackend,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (RemoveExecutor)',
      port: b.driverRpc,
      label: 'RemoveExec(HB exit=56)',
      info: 'YarnAllocator notifies driver of ExecutorExited(56) and launches replacement exec-repl.',
      size: '740 B',
      command: `driverRef.send(RemoveExecutor("${prodExecNum}", ExecutorExited(56, exitCausedByApp=true)))`,
      fields: [{ name: 'exitCode', value: '56' }],
      senderState: 'Allocating replacement executor',
      receiverState: 'Rescheduling interrupted tasks',
    });
    replaceDeadExecutor(b, state, sHbMax, producerExec, prodZone);
    return;
  }

  if (fault === 'task-reaper-kill') {
    const taskAttempt = b.workerStd('task-attempt', prodZone);
    const sReaper = b.addStep({
      id: 'sim-step-task-reaper',
      title: 'Killed task ignores interrupt past killTimeout -> TaskReaper exits JVM (EXIT 57)',
      explanation:
        'A killed/cancelled task on Exec-1 ignores Thread.interrupt() past spark.task.reaper.killTimeout (with spark.task.reaper.enabled=true). Executor TaskReaper throws KilledByTaskReaperException, SparkUncaughtExceptionHandler exits the executor JVM with ExecutorExitCode.KILLED_BY_TASK_REAPER (57), and YarnAllocator launches replacement exec-repl.',
      category: 'yarn-lifecycle',
      keyState: 'KilledByTaskReaperException -> EXIT 57 (KILLED_BY_TASK_REAPER) -> exec-repl registered',
      command: 'TaskReaper -> KilledByTaskReaperException -> System.exit(57)',
      detailsPassed: [
        { name: 'spark.task.reaper.enabled', value: 'true' },
        { name: 'spark.task.reaper.killTimeout', value: '30s (positive ms enables kill)' },
        { name: 'exitCode', value: '57 (ExecutorExitCode.KILLED_BY_TASK_REAPER)' },
        { name: 'sourceRef', value: 'Executor.scala:1331,1668; SparkUncaughtExceptionHandler.scala:78' },
      ],
      deepDiveComponentId: 'executor',
    });
    b.addEvent({
      stepId: sReaper,
      from: state.schedBackend,
      to: producerExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (KillTask)',
      port: b.driverRpc,
      label: 'KillTask(interrupt)',
      info: 'Driver sends KillTask to cancel a task on Exec-1; executor interrupts the task thread and starts TaskReaper.',
      size: '360 B',
      command: 'executorEndpoint.send(KillTask(taskId, interruptThread=true))',
      fields: [{ name: 'killTimeout', value: '30s (spark.task.reaper.killTimeout)' }],
      senderState: 'Task cancellation sent',
      receiverState: 'TaskReaper polling uninterruptible task thread',
    });
    b.addEvent({
      stepId: sReaper,
      from: taskAttempt,
      to: producerExec,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM TaskReaper -> Exit 57',
      port: `${prodHost} in-JVM`,
      label: 'TaskReaper -> exit 57',
      info: 'Task ignores interrupt past killTimeout; TaskReaper throws KilledByTaskReaperException and SparkUncaughtExceptionHandler calls System.exit(57).',
      size: 'exit code 57',
      command: 'throw new KilledByTaskReaperException() -> System.exit(57)',
      fields: [{ name: 'exitCode', value: '57 (KILLED_BY_TASK_REAPER)' }],
      senderState: 'Unkillable task thread timed out',
      receiverState: 'Executor JVM exiting with code 57',
      isError: true,
    });
    b.addEvent({
      stepId: sReaper,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Container exit (57)',
      info: `${prodHost} NM reports ${prodCid} exitStatus=57 (KILLED_BY_TASK_REAPER) to RM :8031.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=57)`,
      fields: [{ name: 'exitStatus', value: '57 (KILLED_BY_TASK_REAPER)' }],
      senderState: 'Container exited with code 57',
      receiverState: 'RM notifies AM',
      isError: true,
    });
    replaceDeadExecutor(b, state, sReaper, producerExec, prodZone);
    return;
  }

  if (fault === 'bm-reregister-rejected') {
    const sBm = b.addStep({
      id: 'sim-step-bm-reregister',
      title: 'BlockManager.reregister rejected (INVALID_EXECUTOR_ID) -> EXIT 58 -> replaced',
      explanation:
        'After driver evicts a stale executor ID, BlockManagerMaster returns false on BlockManagerHeartbeat and rejects BlockManager.reregister() with INVALID_EXECUTOR_ID. Executor logs "Exiting executor due to block manager re-registration failure" and calls System.exit(58) (ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED = 58); YarnAllocator replaces it.',
      category: 'yarn-lifecycle',
      keyState: 'INVALID_EXECUTOR_ID -> EXIT 58 (BLOCK_MANAGER_REREGISTRATION_FAILED) -> exec-repl registered',
      command: 'BlockManager.reregister() // INVALID_EXECUTOR_ID -> System.exit(58)',
      detailsPassed: [
        { name: 'rejectionReason', value: 'INVALID_EXECUTOR_ID from BlockManagerMasterEndpoint' },
        { name: 'exitCode', value: '58 (ExecutorExitCode.BLOCK_MANAGER_REREGISTRATION_FAILED)' },
        { name: 'sourceRef', value: 'BlockManager.scala:842-847; ExecutorExitCode.scala:45' },
      ],
      deepDiveComponentId: 'block-manager',
    });
    b.addEvent({
      stepId: sBm,
      from: producerBm,
      to: state.bmm,
      channel: 'error',
      protocol: 'Spark Netty RPC (RegisterBM)',
      port: b.driverRpc,
      label: 'reregister -> INVALID_ID',
      info: 'BlockManager.reregister() attempts to re-register with BlockManagerMasterEndpoint, which returns INVALID_EXECUTOR_ID.',
      size: '960 B',
      command: 'master.registerBlockManager(bmId) -> BlockManagerId(INVALID_EXECUTOR_ID)',
      fields: [
        { name: 'response', value: 'INVALID_EXECUTOR_ID' },
        { name: 'exitCode', value: '58 (BLOCK_MANAGER_REREGISTRATION_FAILED)' },
      ],
      senderState: 'BlockManager calling System.exit(58)',
      receiverState: 'Driver already evicted stale executor ID',
      isError: true,
    });
    b.addEvent({
      stepId: sBm,
      from: nmProd,
      to: state.rm,
      channel: 'error',
      protocol: 'Hadoop RPC (ResourceTracker)',
      port: 'rm01:8031',
      label: 'Container exit (58)',
      info: `${prodHost} NM reports ${prodCid} exitStatus=58 (BLOCK_MANAGER_REREGISTRATION_FAILED) to RM :8031.`,
      size: 'ContainerStatus',
      command: `ContainerStatus(${prodCid}, COMPLETE, exitStatus=58)`,
      fields: [{ name: 'exitStatus', value: '58' }],
      senderState: 'Container exited with code 58',
      receiverState: 'RM reports completed container to AM',
      isError: true,
    });
    replaceDeadExecutor(b, state, sBm, producerExec, prodZone);
    return;
  }

  if (fault === 'disk-store-full') {
    const tsm = b.std('task-set-manager');
    const taskAttempt = b.workerStd('task-attempt', prodZone);
    const retryExec = state.liveExecs[state.liveExecs.length - 1];
    const sDisk = b.addStep({
      id: 'sim-step-disk-store-full',
      title: `DiskStore.put hits ENOSPC on ${prodHost} -> partial block removed -> task retried`,
      explanation:
        `While spilling a block to YARN local-dirs on ${prodHost}, DiskStore.put encounters java.io.IOException: No space left on device (ENOSPC). DiskStore.put catches the error, calls remove(blockId) to delete the partial file so no corrupt block remains, and rethrows IOException; TaskSetManager retries the task cleanly.`,
      category: 'yarn-lifecycle',
      keyState: 'ENOSPC caught -> DiskStore.remove(blockId) cleanup -> task retry SUCCEEDED',
      command: 'DiskStore.put() -> IOException("No space left on device") -> remove(blockId) -> retry',
      detailsPassed: [
        { name: 'exception', value: 'java.io.IOException: No space left on device (ENOSPC)' },
        { name: 'cleanup', value: 'DiskStore.put deletes partial file via remove(blockId)' },
        { name: 'sourceRef', value: 'DiskStore.scala:64-100' },
      ],
      deepDiveComponentId: 'block-manager',
    });
    b.addEvent({
      stepId: sDisk,
      from: taskAttempt,
      to: producerBm,
      kind: 'local',
      channel: 'error',
      protocol: 'Local Disk Write (ENOSPC)',
      port: `${prodHost} local-dir`,
      label: 'DiskStore ENOSPC -> clean',
      info: `DiskStore.put hits IOException: No space left on device on ${prodHost} local-dir, removes partial block via remove(blockId), and rethrows.`,
      size: 'IOException',
      command: 'DiskStore.put(blockId) // IOException("No space left on device") -> remove(blockId)',
      fields: [
        { name: 'errno', value: 'ENOSPC (No space left on device)' },
        { name: 'partialFileCleaned', value: 'true (remove(blockId))' },
      ],
      senderState: 'Task spill aborted cleanly',
      receiverState: 'Partial block deleted; executor JVM stays alive',
      isError: true,
    });
    b.addEvent({
      stepId: sDisk,
      from: producerExec,
      to: tsm,
      channel: 'error',
      protocol: 'Spark Netty RPC (StatusUpdate)',
      port: b.driverRpc,
      label: 'Task FAIL (ENOSPC retry)',
      info: 'Executor reports ExceptionFailure(IOException: No space left on device); TaskSetManager retries on healthy disk.',
      size: '1.8 KB',
      command: 'StatusUpdate(FAILED, ExceptionFailure("IOException: No space left on device"))',
      fields: [{ name: 'numFailures', value: '1 / 4 (retrying)' }],
      senderState: 'Attempt 0 failed cleanly',
      receiverState: `Rescheduling attempt 1 on ${retryExec}`,
      isError: true,
    });
    b.addEvent({
      stepId: sDisk,
      from: state.schedBackend,
      to: retryExec,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (LaunchTask)',
      port: b.driverRpc,
      label: 'Retry task (disk OK)',
      info: `Attempt 1 runs on ${retryExec} with free local-dir space and succeeds.`,
      size: '2.1 KB',
      command: 'executorEndpoint.send(LaunchTask(attempt=1)) // SUCCEEDED',
      fields: [{ name: 'retryOutcome', value: 'SUCCEEDED' }],
      senderState: 'Dispatched retry',
      receiverState: 'Attempt 1 completed cleanly',
    });
    return;
  }

  if (fault === 'listener-bus-overflow') {
    const dag = b.std('dag-scheduler');
    const bus = b.std('listener-bus');
    const metrics = b.std('metrics-system');
    const ui = b.std('spark-ui');
    const eventLog = b.std('event-log');
    const sBus = b.addStep({
      id: 'sim-step-listener-overflow',
      title: 'LiveListenerBus appStatus queue overflows (10000) -> SparkUI drops while eventLog stays intact',
      explanation:
        'LiveListenerBus maintains SEPARATE AsyncEventQueue instances, consumer threads, and queue.<name>.numDroppedEvents counters for appStatus (Spark UI) and eventLog (HDFS EventLoggingListener). A burst of task completion events saturates the appStatus queue (capacity = 10,000), dropping 1,420 events (queue.appStatus.numDroppedEvents = 1420) so Spark UI misses live stage/task metrics, while the independent eventLog queue (queue.eventLog.numDroppedEvents = 0) continues writing intact events to HDFS and the job finishes SUCCEEDED.',
      category: 'heartbeats-metrics',
      keyState: 'DEGRADED: appStatus queue dropped 1420 events (SparkUI stale) | separate eventLog queue intact (0 drops)',
      command: 'AsyncEventQueue("appStatus").onDropEvent() // queue.appStatus.numDroppedEvents += 1420; eventLog queue independent',
      detailsPassed: [
        { name: 'separateQueues', value: 'appStatus and eventLog have independent AsyncEventQueue instances, consumers, and numDroppedEvents counters' },
        { name: 'appStatusQueue', value: 'capacity 10000 -> dropped 1420 events (queue.appStatus.numDroppedEvents=1420 -> SparkUI misses live metrics)' },
        { name: 'eventLogQueue', value: 'Independent queue -> 0 drops (continues writing intact events to HDFS unless eventLog overflows independently)' },
        { name: 'sourceRef', value: 'LiveListenerBus.scala:59-105,256-260; AsyncEventQueue.scala:54-85,179-205' },
      ],
      deepDiveComponentId: 'listener-bus',
    });
    b.addEvent({
      stepId: sBus,
      from: dag,
      to: bus,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM LiveListenerBus.post',
      port: `${b.driverHost} in-JVM`,
      label: 'Burst TaskEnd (>10000)',
      info: 'High burst of SparkListenerTaskEnd events fans out to separate appStatus and eventLog AsyncEventQueues; appStatus fills at capacity 10000.',
      size: '11420 events',
      command: 'listenerBus.post(SparkListenerTaskEnd(...)) // appStatus queue full at 10000',
      fields: [{ name: 'appStatusCapacity', value: '10000 (separate from eventLog queue)' }],
      senderState: 'Posting task completion events',
      receiverState: 'appStatus AsyncEventQueue.eventQueue.offer() returned false',
    });
    b.addEvent({
      stepId: sBus,
      from: bus,
      to: metrics,
      kind: 'local',
      channel: 'error',
      protocol: 'In-JVM AsyncEventQueue.onDropEvent',
      port: `${b.driverHost} in-JVM`,
      label: 'appStatus drops 1420 ev',
      info: 'AsyncEventQueue("appStatus").onDropEvent logs non-fatal ERROR ("Dropping 1420 events from event queue appStatus") and increments queue.appStatus.numDroppedEvents (while queue.eventLog.numDroppedEvents stays 0).',
      size: '1420 dropped',
      command: 'logError("Dropping 1420 events from event queue appStatus"); appStatus.numDroppedEvents.inc(1420)',
      fields: [
        { name: 'queue.appStatus.numDroppedEvents', value: '1420' },
        { name: 'queue.eventLog.numDroppedEvents', value: '0 (separate AsyncEventQueue)' },
        { name: 'jobAborted', value: 'false (degraded observability only)' },
      ],
      senderState: 'Dropped overflow events only on appStatus queue',
      receiverState: 'Recorded per-queue numDroppedEvents metrics',
      isError: true,
    });
    b.addEvent({
      stepId: sBus,
      from: bus,
      to: ui,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM AppStatusListener',
      port: `${b.driverHost} in-JVM`,
      label: 'SparkUI misses 1420 ev',
      info: 'Spark UI AppStatusStore (consuming the appStatus queue) misses 1,420 dropped live stage/task metrics.',
      size: 'partial',
      command: 'AppStatusListener // appStatus queue missed 1420 dropped SparkListenerTaskEnd events',
      fields: [{ name: 'uiImpact', value: 'Stale/missing live task metrics on Spark UI' }],
      senderState: 'Delivered surviving appStatus queue events',
      receiverState: 'Spark UI rendered with live metric gaps',
    });
    b.addEvent({
      stepId: sBus,
      from: bus,
      to: eventLog,
      kind: 'local',
      channel: 'internal',
      protocol: 'In-JVM EventLoggingListener',
      port: `${b.driverHost} in-JVM`,
      label: 'EventLog queue intact OK',
      info: 'EventLoggingListener runs on its own separate eventLog AsyncEventQueue (queue.eventLog.numDroppedEvents=0) and continues writing intact events to HDFS while the job finishes SUCCEEDED.',
      size: 'intact JSON',
      command: 'AsyncEventQueue("eventLog") -> EventLoggingListener.onEvent() // 0 drops on eventLog queue',
      fields: [
        { name: 'eventLogDropped', value: '0 (separate consumer & queue from appStatus)' },
        { name: 'severity', value: 'degraded (job succeeds)' },
      ],
      senderState: 'Independent eventLog queue drained cleanly',
      receiverState: 'HDFS event log written intact',
    });
    return;
  }

  if (fault === 'speculation-race') {
    const occ = b.std('output-commit-coordinator');
    const attempt0Exec = state.liveExecs[0];
    const attempt1Exec = state.liveExecs[1];
    const host0 = b.instanceWorkerHost(attempt0Exec).hostName;
    const host1 = b.instanceWorkerHost(attempt1Exec).hostName;
    const sSpec = b.addStep({
      id: 'sim-step-speculation',
      title: `Speculative Attempt 1 (${host1}) wins canCommit for p=${state.targetPart}; Attempt 0 (${host0}) denied`,
      explanation: `TaskSetManager marks straggler partition ${state.targetPart} (Attempt 0 on ${attempt0Exec} / ${host0}) speculatable and launches Attempt 1 on a distinct host (${attempt1Exec} / ${host1}). Attempt 1 asks OutputCommitCoordinator.canCommit(stage, 0, ${state.targetPart}, 1) first and wins the slot; slower Attempt 0 on ${attempt0Exec} is denied (TaskCommitDenied, NOT counted toward task failures) and aborts its attempt output.`,
      category: 'output-commit',
      keyState: `Attempt 1 (${host1}) authorized for p=${state.targetPart} | Attempt 0 (${host0}) received TaskCommitDenied (free)`,
      command: `OutputCommitCoordinator.canCommit(stage, 0, part=${state.targetPart}, attempt=1) -> true`,
      detailsPassed: [
        { name: 'partition', value: `${state.targetPart} (valid in 0..${state.cfg.numPartitions - 1})` },
        { name: 'winner', value: `Attempt 1 on ${attempt1Exec} (${host1}, first eligible asker)` },
        { name: 'loser', value: `Attempt 0 on ${attempt0Exec} (${host0}) -> TaskCommitDenied (countTowardsTaskFailures=false)` },
      ],
      deepDiveComponentId: 'output-commit-coordinator',
    });
    b.addEvent({
      stepId: sSpec,
      from: attempt1Exec,
      to: occ,
      channel: 'rpc',
      protocol: 'Spark Netty RPC (canCommit)',
      port: b.driverRpc,
      label: `canCommit(p=${state.targetPart},a=1) OK`,
      info: `Speculative Attempt 1 on ${attempt1Exec} (${host1}) reaches commit first and wins exclusive permission for partition ${state.targetPart}.`,
      size: '440 B',
      command: `AskPermissionToCommitOutput(stage, 0, partition=${state.targetPart}, attempt=1) -> true`,
      fields: [
        { name: 'host', value: `${host1} (${attempt1Exec})` },
        { name: 'authorizedOwner', value: `(stageAttempt=0, part=${state.targetPart}, taskAttempt=1)` },
      ],
      senderState: `Committing partition ${state.targetPart} output from ${host1}`,
      receiverState: `Slot (stage, ${state.targetPart}) locked to Attempt 1`,
    });
    b.addEvent({
      stepId: sSpec,
      from: attempt0Exec,
      to: occ,
      channel: 'error',
      protocol: 'Spark Netty RPC (canCommit)',
      port: b.driverRpc,
      label: `canCommit(p=${state.targetPart},a=0) NO`,
      info: `Original Attempt 0 on ${attempt0Exec} (${host0}) arrives second; coordinator returns false -> Attempt 0 aborts task with TaskCommitDenied.`,
      size: '440 B',
      command: `AskPermissionToCommitOutput(stage, 0, partition=${state.targetPart}, attempt=0) -> false`,
      fields: [
        { name: 'host', value: `${host0} (${attempt0Exec})` },
        { name: 'reason', value: 'TaskCommitDenied (not counted against maxFailures)' },
      ],
      senderState: `Attempt 0 aborts its temp output cleanly on ${host0}`,
      receiverState: 'Slot remains owned by Attempt 1',
      isError: true,
    });
  }
}

// =============================================================================
// PHASE 10: Mid-Execution JVM / Kryo / Security Terminal Faults
// =============================================================================
export function injectKryoUnregisteredClassStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
  stageLabel: string,
  failingExec: string,
): void {
  state.abortedByFault = true;
  const execTaskAttempt = b.execTaskAttemptInstance(failingExec);
  const execSer = b.execSerializerInstance(failingExec);
  const tsm = b.std('task-set-manager');
  const dag = b.std('dag-scheduler');
  const { hostName } = b.instanceWorkerHost(failingExec);

  const sKryo = b.addStep({
    id: 'sim-step-kryo-unregistered',
    title: 'Executor KryoSerializer rejects unregistered OrderEvent in Pair-RDD shuffle -> IllegalArgumentException (4/4)',
    explanation:
      `With spark.serializer=org.apache.spark.serializer.KryoSerializer and spark.kryo.registrationRequired=true, task closure serialization succeeded via SparkEnv.closureSerializer (always JavaSerializer -- NOT Kryo!). During ${stageLabel} on ${failingExec} (${hostName})${b.isPySpark ? ' (auxiliary JVM Pair-RDD helper executed alongside the PySpark job)' : ''}, Executor SerializerManager serializes an unregistered com.acme.OrderEvent DTO for ShuffledRDD[K, OrderEvent] (explicitly distinct from SQL ShuffleExchangeExec, ShuffleExchangeExec.scala:219-265, which ALWAYS uses UnsafeRowSerializer regardless of spark.serializer!). Kryo 4.0.3 throws java.lang.IllegalArgumentException: Class is not registered: com.acme.OrderEvent. Wrapped across the task serialization boundary, 4 deterministic failures exhaust spark.task.maxFailures (4) and TaskSetManager aborts the stage.`,
    category: 'sql-planning-aqe',
    keyState: 'ABORTED: Kryo 4.0.3 IllegalArgumentException (Class is not registered) on Pair-RDD ShuffledRDD[K, OrderEvent]',
    command: 'ShuffledRDD[K, OrderEvent] -> KryoSerializer (registrationRequired=true) -> IllegalArgumentException("Class is not registered")',
    detailsPassed: [
      { name: 'spark.serializer', value: 'org.apache.spark.serializer.KryoSerializer' },
      { name: 'spark.kryo.registrationRequired', value: 'true (default false)' },
      { name: 'rddPath', value: 'JVM Pair-RDD shuffle: rdd.reduceByKey / ShuffledRDD[K, OrderEvent]' },
      { name: 'sqlShuffleDistinction', value: 'SQL ShuffleExchangeExec (ShuffleExchangeExec.scala:219-265) explicitly uses UnsafeRowSerializer regardless of spark.serializer' },
      { name: 'closureDistinction', value: 'Task closures always use SparkEnv.closureSerializer (JavaSerializer)' },
      ...(b.isPySpark
        ? [{ name: 'pysparkPath', value: 'Auxiliary JVM-side Pair-RDD helper path executed alongside the PySpark job' }]
        : []),
      { name: 'exception', value: 'java.lang.IllegalArgumentException: Class is not registered (Kryo 4.0.3; wrapped at task boundary)' },
      { name: 'sourceRef', value: 'KryoSerializer.scala:90,143; Kryo.scala:24-27; SerializerManager.scala:85-107; ShuffleExchangeExec.scala:219-265' },
    ],
    deepDiveComponentId: 'serializer-manager',
  });
  b.addEvent({
    stepId: sKryo,
    from: execTaskAttempt,
    to: execSer,
    kind: 'local',
    channel: 'internal',
    protocol: 'KryoSerializer Pair-RDD',
    port: `${hostName} in-JVM`,
    label: 'Kryo Pair-RDD serialize',
    info: `During ${stageLabel} on ${hostName}${b.isPySpark ? ' (auxiliary JVM Pair-RDD helper path)' : ''}, SortShuffleWriter uses dep.serializer (SortShuffleWriter.scala:64-85) and ShufflePartitionPairsWriter calls writeKey/writeValue (ShufflePartitionPairsWriter.scala:55-64) through KryoSerializer on unregistered DTO com.acme.OrderEvent in ShuffledRDD[K, OrderEvent] (SQL ShuffleExchangeExec uses UnsafeRowSerializer; closures use JavaSerializer).`,
    size: 'in-memory',
    command: 'SortShuffleWriter.write(dep.serializer) -> ShufflePartitionPairsWriter.writeKey/writeValue -> KryoSerializationStream.writeObject(OrderEvent)',
    fields: [
      { name: 'callChain', value: 'SortShuffleWriter (dep.serializer, SortShuffleWriter.scala:64-85) -> ShufflePartitionPairsWriter.writeKey/writeValue (ShufflePartitionPairsWriter.scala:55-64)' },
      { name: 'dataClass', value: 'com.acme.OrderEvent (unregistered DTO in Pair-RDD shuffle)' },
      { name: 'sqlShuffleSerializer', value: 'UnsafeRowSerializer (SQL ShuffleExchangeExec never uses Kryo)' },
      { name: 'closureSerializer', value: 'JavaSerializer (unaffected by registrationRequired)' },
    ],
    senderState: 'Serializing Pair-RDD OrderEvent records on executor task thread',
    receiverState: 'Checking Kryo registrationRequired=true table',
  });
  b.addEvent({
    stepId: sKryo,
    from: execSer,
    to: execTaskAttempt,
    kind: 'local',
    channel: 'error',
    protocol: 'Kryo 4.0.3 Guard',
    port: `${hostName} in-JVM`,
    label: 'IllegalArgumentException',
    info: 'Kryo 4.0.3 throws java.lang.IllegalArgumentException: Class is not registered: com.acme.OrderEvent (wrapped on task boundary; fix via spark.kryo.classesToRegister or KryoRegistrator).',
    size: 'IllegalArgExc',
    command: 'throw new IllegalArgumentException("Class is not registered: com.acme.OrderEvent") // Kryo 4.0.3',
    fields: [
      { name: 'exception', value: 'java.lang.IllegalArgumentException: Class is not registered (Kryo 4.0.3)' },
      { name: 'remediation', value: 'Add class to spark.kryo.classesToRegister or KryoRegistrator' },
    ],
    senderState: 'Rejected unregistered class under registrationRequired=true',
    receiverState: 'Wrapped across task boundary; task attempt failed',
    isError: true,
  });
  b.addEvent({
    stepId: sKryo,
    from: failingExec,
    to: tsm,
    channel: 'error',
    protocol: 'Spark Netty RPC (StatusUpdate)',
    port: b.driverRpc,
    label: 'Task FAIL: Kryo (4/4)',
    info: `Deterministic Kryo IllegalArgumentException fails partition ${state.targetPart} across 4 attempts (spark.task.maxFailures=4).`,
    size: '2.2 KB',
    command: `StatusUpdate(FAILED, ExceptionFailure("IllegalArgumentException: Class is not registered")) // 4/4`,
    fields: [
      { name: 'partitionIndex', value: String(state.targetPart) },
      { name: 'numFailures', value: '4 / 4 (exhausted)' },
    ],
    senderState: '4th task attempt failed deterministically',
    receiverState: 'TaskSetManager aborting TaskSet',
    isError: true,
  });
  b.addEvent({
    stepId: sKryo,
    from: tsm,
    to: dag,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM TaskSetFailed',
    port: `${b.driverHost} in-JVM`,
    label: 'abortStage -> JobFailed',
    info: `TaskSetManager aborts stage and DAGScheduler fails ActiveJob back to ${b.appEntryLabel}.`,
    size: 'in-memory',
    command: 'dagScheduler.taskSetFailed(taskSet, "IllegalArgumentException: Class is not registered")',
    fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException)' }],
    senderState: 'TaskSet marked zombie',
    receiverState: 'ActiveJob failed',
    isError: true,
  });
  emitTerminalAppOutcome(b, state, {
    origin: 'user-app-failure',
    diagnostic: 'IllegalArgumentException: Class is not registered: com.acme.OrderEvent (Kryo 4.0.3)',
    stopLiveExecutors: true,
  });
}

export function injectScalaJavaUdfExceptionStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
  stageLabel: string,
  failingExec: string,
): void {
  state.abortedByFault = true;
  const execTaskAttempt = b.execTaskAttemptInstance(failingExec);
  const execEncoder = b.execEncoderInstance(failingExec);
  const tsm = b.std('task-set-manager');
  const dag = b.std('dag-scheduler');
  const { hostName } = b.instanceWorkerHost(failingExec);
  const udfKind = b.isScala
    ? 'ScalaUDF'
    : b.isJava
      ? 'Java UDF1<Double, Double>'
      : 'Java UDF1 (spark.udf.registerJavaFunction)';

  const sUdfFail = b.addStep({
    id: 'sim-step-jvm-udf-exception',
    title: `Executor ${udfKind} throws IllegalArgumentException -> FAILED_EXECUTE_UDF (4/4)`,
    explanation:
      `During ${stageLabel} on ${failingExec} (${hostName}), Executor JVM evaluates ${udfKind} directly on the JVM task thread (ZERO Python worker involved${b.isPySpark ? ' even in PySpark via spark.udf.registerJavaFunction' : ''}). On a deterministic bad input in partition ${state.targetPart}, user UDF code throws IllegalArgumentException. ScalaUDF.scala:1171 wraps it as SparkException (errorClass="FAILED_EXECUTE_UDF"). After 4 counted failures of the same task (spark.task.maxFailures=4), TaskSetManager aborts the stage and the unhandled exception escapes ${b.appEntryLabel}().`,
    category: 'sql-planning-aqe',
    keyState: 'ABORTED: SparkException (FAILED_EXECUTE_UDF) -- Stage aborted (4 counted failures of same task)',
    command: 'ScalaUDF.scala:1171 -> SparkException(FAILED_EXECUTE_UDF) -> 4/4 task failures -> abortStage()',
    detailsPassed: [
      { name: 'udfRuntime', value: `${udfKind} on Executor JVM (0 Python workers)` },
      { name: 'errorClass', value: 'FAILED_EXECUTE_UDF (SparkException wrapping IllegalArgumentException)' },
      { name: 'spark.task.maxFailures', value: '4 (4 counted failures of same task)' },
      { name: 'sourceRef', value: 'ScalaUDF.scala:1125-1175; ApplicationMaster.scala:775-785,911' },
    ],
    deepDiveComponentId: 'dataset-encoder-udf',
  });
  b.addEvent({
    stepId: sUdfFail,
    from: execTaskAttempt,
    to: execEncoder,
    kind: 'local',
    channel: 'internal',
    protocol: 'In-JVM ScalaUDF Eval',
    port: `${hostName} in-JVM`,
    label: b.isScala ? `Invoke ScalaUDF (p=${state.targetPart})` : `Invoke Java UDF1 (p=${state.targetPart})`,
    info: `Executor JVM task thread invokes ${udfKind} on partition ${state.targetPart} directly in-JVM (NO Python worker involved).`,
    size: 'in-memory',
    command: b.isScala
      ? 'ScalaUDF.eval(inputRow) // in-JVM task thread'
      : 'UDF1.call(java.lang.Double) // in-JVM via ScalaUDF (0 Python workers)',
    fields: [
      { name: 'udfKind', value: udfKind },
      { name: 'pythonWorkers', value: '0' },
    ],
    senderState: 'Evaluating JVM scalar UDF on task thread',
    receiverState: 'Deterministic bad row encountered in user UDF',
  });
  b.addEvent({
    stepId: sUdfFail,
    from: execEncoder,
    to: execTaskAttempt,
    kind: 'local',
    channel: 'error',
    protocol: 'ScalaUDF.scala:1171',
    port: `${hostName} in-JVM`,
    label: 'FAILED_EXECUTE_UDF',
    info: 'User UDF throws IllegalArgumentException; ScalaUDF.scala:1171 wraps it as SparkException (errorClass="FAILED_EXECUTE_UDF") on Executor JVM.',
    size: 'SparkException',
    command: 'throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(...) // FAILED_EXECUTE_UDF',
    fields: [
      { name: 'errorClass', value: 'FAILED_EXECUTE_UDF' },
      { name: 'cause', value: 'java.lang.IllegalArgumentException: negative amount' },
    ],
    senderState: 'Wrapped user UDF exception in FAILED_EXECUTE_UDF',
    receiverState: 'Failing task attempt on Executor JVM',
    isError: true,
  });
  b.addEvent({
    stepId: sUdfFail,
    from: failingExec,
    to: tsm,
    channel: 'error',
    protocol: 'Spark Netty RPC (StatusUpdate)',
    port: b.driverRpc,
    label: `Task p=${state.targetPart} FAIL (4/4)`,
    info: `Partition ${state.targetPart} fails 4 counted attempts with SparkException [FAILED_EXECUTE_UDF] (spark.task.maxFailures=4).`,
    size: '2.3 KB',
    command: `StatusUpdate(FAILED, ExceptionFailure("SparkException [FAILED_EXECUTE_UDF]")) // 4/4`,
    fields: [
      { name: 'partitionIndex', value: String(state.targetPart) },
      { name: 'numFailures', value: '4 / 4 (exhausted)' },
    ],
    senderState: '4th counted task attempt failed deterministically',
    receiverState: 'TaskSetManager calling abort()',
    isError: true,
  });
  b.addEvent({
    stepId: sUdfFail,
    from: tsm,
    to: dag,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM TaskSetFailed',
    port: `${b.driverHost} in-JVM`,
    label: 'abortStage -> JobFailed',
    info: `TaskSetManager aborts stage; unhandled SparkException [FAILED_EXECUTE_UDF] escapes ${b.appEntryLabel}().`,
    size: 'in-memory',
    command: 'dagScheduler.taskSetFailed(taskSet, "FAILED_EXECUTE_UDF (4/4 failures)")',
    fields: [{ name: 'jobOutcome', value: 'JobFailed(SparkException[FAILED_EXECUTE_UDF])' }],
    senderState: 'TaskSet marked zombie',
    receiverState: 'ActiveJob failed',
    isError: true,
  });
  emitTerminalAppOutcome(b, state, {
    origin: 'user-app-failure',
    diagnostic: `SparkException [FAILED_EXECUTE_UDF]: ${udfKind} threw IllegalArgumentException (4/4 failures)`,
    stopLiveExecutors: true,
  });
}

export function injectDelegationTokenExpiredStep(
  b: SimRunBuilder,
  state: SimRuntimeState,
  stageLabel: string,
  failingExec: string,
): void {
  state.abortedByFault = true;
  const dtm = b.std('delegation-token-manager');
  const nn = b.std('hdfs-namenode');
  const tsm = b.std('task-set-manager');
  const dag = b.std('dag-scheduler');

  const sTok = b.addStep({
    id: 'sim-step-delegation-token-expired',
    title: 'Expired HDFS delegation token fails fresh NameNode :8020 SASL IPC setup -> SecretManager$InvalidToken',
    explanation:
      `In a long-running YARN job, Hadoop 3.4.1 delegation token lifetime elapsed (24h renewal interval / 7-day max lifetime defaults, hdfs-default.xml:1458-1471) without Spark HadoopDelegationTokenManager (normally scheduling renewal at 0.75 * (earliestProviderRenewalTime - now), retrying via spark.security.credentials.retryWait, HadoopDelegationTokenManager.scala:274-313) acquiring fresh tokens. Per Hadoop Client.java:764-800, an existing authenticated IPC connection is reused without re-checking the token on every metadata call; however, during ${stageLabel}, ${failingExec} opens a fresh NameNode :8020 IPC connection / reconnect for a pending getBlockLocations/create metadata RPC (explicitly distinct from DataNode :9866 per-block access tokens!). During SASL token authentication BEFORE the pending metadata RPC executes, NameNode invokes AbstractDelegationTokenSecretManager.retrievePassword (AbstractDelegationTokenSecretManager.java:528-551) and rejects the expired token with SecretManager$InvalidToken, failing the task and aborting the job.`,
    category: 'yarn-lifecycle',
    keyState: 'FAILED: Fresh NameNode :8020 SASL IPC auth -> SecretManager$InvalidToken before metadata RPC (NOT DataNode block token)',
    command: 'Hadoop Client.setupIOstreams() SASL auth (nn01:8020) -> AbstractDelegationTokenSecretManager.retrievePassword() -> SecretManager$InvalidToken',
    detailsPassed: [
      { name: 'hadoopDefaults', value: 'Hadoop 3.4.1 (hdfs-default.xml:1458-1471): 24h renewal interval / 7d max lifetime' },
      { name: 'sparkRenewalFormula', value: '0.75 * (earliestProviderRenewalTime - now); retry spark.security.credentials.retryWait' },
      { name: 'saslIpcAuth', value: 'Hadoop Client.java:764-800 authenticates delegation token on fresh SASL IPC connection setup (existing connections reuse auth) BEFORE pending getBlockLocations/create RPC executes' },
      { name: 'nnVsDnBoundary', value: 'HDFS NameNode :8020 validates delegation tokens during SASL IPC auth; DataNode :9866 validates per-block access tokens' },
      { name: 'exception', value: 'RemoteException(org.apache.hadoop.security.token.SecretManager$InvalidToken) via AbstractDelegationTokenSecretManager.retrievePassword' },
      { name: 'sourceRef', value: 'HadoopDelegationTokenManager.scala:274-313; Hadoop Client.java:764-800; AbstractDelegationTokenSecretManager.java:528-551; hdfs-default.xml:1458-1471' },
    ],
    deepDiveComponentId: 'delegation-token-manager',
  });
  b.addEvent({
    stepId: sTok,
    from: dtm,
    to: dtm,
    kind: 'local',
    channel: 'error',
    protocol: 'HadoopDelegationTokenManager',
    port: `${b.driverHost} in-JVM`,
    label: 'No fresh token (0.75x)',
    info: 'HadoopDelegationTokenManager (0.75 * (earliestProviderRenewalTime - now); retry spark.security.credentials.retryWait) did not acquire fresh tokens before the 24h renewal / 7d max lifetime default elapsed.',
    size: 'Token expired',
    command: 'HadoopDelegationTokenManager.scheduleRenewal() // 0.75 * (earliestProviderRenewalTime - now) unrenewed',
    fields: [
      { name: 'renewalFormula', value: '0.75 * (earliestProviderRenewalTime - now)' },
      { name: 'retryConfig', value: 'spark.security.credentials.retryWait' },
    ],
    senderState: 'No fresh HDFS_DELEGATION_TOKEN pushed via UpdateDelegationTokens',
    receiverState: 'Executor UGI still holds expired launch delegation token',
    isError: true,
  });
  b.addEvent({
    stepId: sTok,
    from: failingExec,
    to: nn,
    channel: 'yarn',
    protocol: 'Hadoop SASL IPC Setup',
    port: 'nn01:8020',
    label: 'New NN IPC SASL auth',
    info: `${failingExec} opens a fresh HDFS NameNode :8020 IPC connection / reconnect (Client.java:764-800; existing authenticated connections are reused without re-checking the token) for a pending getBlockLocations/create metadata RPC, sending the expired HDFS_DELEGATION_TOKEN during SASL handshake BEFORE the metadata RPC executes (NOT a DataNode :9866 per-block access token!).`,
    size: '1.1 KB',
    command: 'Client.setupIOstreams() -> SASL token auth (nn01:8020) // before pending getBlockLocations/create RPC',
    fields: [
      { name: 'rpcTarget', value: 'HDFS NameNode nn01:8020 (fresh IPC connection / reconnect)' },
      { name: 'authPhase', value: 'SASL IPC connection setup BEFORE pending getBlockLocations/create RPC executes' },
      { name: 'datanodeDistinction', value: 'DataNode :9866 validates per-block access tokens, NOT delegation tokens' },
    ],
    senderState: 'Opening fresh NameNode :8020 SASL IPC connection for pending metadata RPC',
    receiverState: 'Authenticating HDFS_DELEGATION_TOKEN in SASL IPC setup',
  });
  b.addEvent({
    stepId: sTok,
    from: nn,
    to: failingExec,
    channel: 'error',
    protocol: 'Hadoop SASL / InvalidToken',
    port: 'nn01:8020',
    label: 'SecretManager$InvalidToken',
    info: 'During SASL IPC connection authentication BEFORE the pending getBlockLocations/create metadata RPC executes, NameNode :8020 calls AbstractDelegationTokenSecretManager.retrievePassword (AbstractDelegationTokenSecretManager.java:528-551) and rejects the expired token with RemoteException(org.apache.hadoop.security.token.SecretManager$InvalidToken: token is expired / cannot be found in cache).',
    size: 'InvalidToken',
    command: 'AbstractDelegationTokenSecretManager.retrievePassword() -> RemoteException(SecretManager$InvalidToken)',
    fields: [
      { name: 'exception', value: 'SecretManager$InvalidToken (NameNode :8020 SASL IPC auth)' },
      { name: 'sourceRef', value: 'AbstractDelegationTokenSecretManager.java:528-551; Client.java:764-800' },
      { name: 'notDataNodeBlockToken', value: 'Delegation token SASL auth failure on NameNode :8020, not DataNode block token' },
    ],
    senderState: 'NameNode SASL auth rejected expired delegation token before metadata RPC',
    receiverState: 'Fresh IPC connection failed before getBlockLocations/create could execute',
    isError: true,
  });
  b.addEvent({
    stepId: sTok,
    from: failingExec,
    to: tsm,
    channel: 'error',
    protocol: 'Spark Netty RPC (StatusUpdate)',
    port: b.driverRpc,
    label: 'Task FAIL: InvalidToken',
    info: `${failingExec} reports ExceptionFailure(SecretManager$InvalidToken) across attempts; TaskSetManager aborts stage.`,
    size: '2.0 KB',
    command: 'StatusUpdate(FAILED, ExceptionFailure("SecretManager$InvalidToken")) // retries exhausted',
    fields: [{ name: 'numFailures', value: '4 / 4 (all executors hold expired token)' }],
    senderState: 'Task attempts failed on NameNode RPC auth',
    receiverState: 'TaskSetManager aborting TaskSet',
    isError: true,
  });
  b.addEvent({
    stepId: sTok,
    from: tsm,
    to: dag,
    kind: 'local',
    channel: 'error',
    protocol: 'In-JVM TaskSetFailed',
    port: `${b.driverHost} in-JVM`,
    label: 'abortStage -> JobFailed',
    info: `TaskSetManager aborts stage and DAGScheduler fails ActiveJob back to ${b.appEntryLabel}.`,
    size: 'in-memory',
    command: 'dagScheduler.taskSetFailed(taskSet, "SecretManager$InvalidToken")',
    fields: [{ name: 'jobOutcome', value: 'JobFailed(InvalidToken)' }],
    senderState: 'TaskSet marked zombie',
    receiverState: 'ActiveJob failed',
    isError: true,
  });
  emitTerminalAppOutcome(b, state, {
    origin: 'user-app-failure',
    diagnostic: 'RemoteException(SecretManager$InvalidToken): HDFS delegation token expired on NameNode :8020 RPC',
    stopLiveExecutors: true,
  });
}
