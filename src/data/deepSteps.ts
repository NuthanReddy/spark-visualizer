import { DEEP_STEPS_CLUSTER, DEEP_STEPS_CLUSTER_JVM } from './deepStepsCluster.ts';
import {
  DEEP_STEPS_EXECUTION,
  DEEP_STEPS_EXECUTION_JVM,
} from './deepStepsExecution.ts';
import { hasJvmTraceOverride } from './jvmTraceOverrides.ts';
import type { DeepStepEnrichment } from './runModel.ts';
import type { SparkLanguage } from './types.ts';

export const DEEP_STEP_ENRICHMENTS: Record<string, DeepStepEnrichment> = {
  ...DEEP_STEPS_CLUSTER,
  ...DEEP_STEPS_EXECUTION,
};

export const DEEP_STEP_ENRICHMENTS_BY_LANG: Record<string, DeepStepEnrichment> = {
  ...DEEP_STEPS_CLUSTER_JVM,
  ...DEEP_STEPS_EXECUTION_JVM,
};

export function resolveDeepStepEnrichment(
  traceId: string,
  language?: SparkLanguage,
  wasLanguageOverridden?: boolean,
): DeepStepEnrichment | undefined {
  if (language === 'scala' || language === 'java') {
    const langSpecific = DEEP_STEP_ENRICHMENTS_BY_LANG[`${traceId}:${language}`];
    if (langSpecific) return langSpecific;
    const jvmSpecific = DEEP_STEP_ENRICHMENTS_BY_LANG[`${traceId}:jvm`];
    if (jvmSpecific) return jvmSpecific;
    const overridden = wasLanguageOverridden ?? hasJvmTraceOverride(traceId);
    if (overridden) return undefined;
  }
  return DEEP_STEP_ENRICHMENTS[traceId];
}
