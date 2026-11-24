import { DEEP_STEPS_CLUSTER } from './deepStepsCluster.ts';
import { DEEP_STEPS_EXECUTION } from './deepStepsExecution.ts';
import type { DeepStepEnrichment } from './runModel.ts';

export const DEEP_STEP_ENRICHMENTS: Record<string, DeepStepEnrichment> = {
  ...DEEP_STEPS_CLUSTER,
  ...DEEP_STEPS_EXECUTION,
};
