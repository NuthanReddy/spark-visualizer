import type {
  ExecutionRun,
  ProjectedFrame,
  ProjectedStep,
  ProjectedTimeline,
  RunEvent,
  RunStep,
  SkippedSpan,
  StepCategory,
} from '../data/runModel';
import { STEP_CATEGORIES } from '../data/runModel.ts';

function emptyCategoryCounts(): Record<StepCategory, number> {
  const counts = {} as Record<StepCategory, number>;
  for (const cat of STEP_CATEGORIES) counts[cat.id] = 0;
  return counts;
}

function hasVisibleAncestry(
  step: RunStep,
  stepById: ReadonlyMap<string, RunStep>,
  expandedStepIds: ReadonlySet<string>,
): boolean {
  const seen = new Set<string>([step.id]);
  let cur = step.parentId ? stepById.get(step.parentId) : undefined;
  while (cur && !seen.has(cur.id)) {
    seen.add(cur.id);
    if (!expandedStepIds.has(cur.id)) return false;
    cur = cur.parentId ? stepById.get(cur.parentId) : undefined;
  }
  return true;
}

/**
 * Returns all step IDs in canonical order that have at least one child step via parentId.
 */
export function getExpandableStepIds(run: ExecutionRun): string[] {
  const parentIds = new Set<string>();
  for (const step of run.steps) {
    if (step.parentId) parentIds.add(step.parentId);
  }
  return run.steps.filter(step => parentIds.has(step.id)).map(step => step.id);
}

/**
 * Returns direct child steps of `stepId` in canonical order.
 */
export function getDirectChildren(run: ExecutionRun, stepId: string): RunStep[] {
  return run.steps.filter(step => step.parentId === stepId);
}

/**
 * Returns the ordered ancestor chain (root first, direct parent last) for `stepId`.
 */
export function getStepAncestors(run: ExecutionRun, stepId: string): RunStep[] {
  const stepById = new Map<string, RunStep>();
  for (const step of run.steps) {
    stepById.set(step.id, step);
  }
  const target = stepById.get(stepId);
  if (!target) return [];

  const chain: RunStep[] = [];
  const seen = new Set<string>([target.id]);
  let cur = target.parentId ? stepById.get(target.parentId) : undefined;
  while (cur && !seen.has(cur.id)) {
    seen.add(cur.id);
    chain.unshift(cur);
    cur = cur.parentId ? stepById.get(cur.parentId) : undefined;
  }
  return chain;
}

/**
 * Returns all ancestor step IDs (root first) that must be expanded for `stepId` to be visible.
 */
export function collectStepIdsToExpandForStep(run: ExecutionRun, stepId: string): string[] {
  return getStepAncestors(run, stepId).map(ancestor => ancestor.id);
}

/**
 * Given a macro step (such as one in `emptyExpandedStepIds`), returns the exact sorted list
 * of disabled `StepCategory` IDs that caused its descendant events to be hidden.
 *
 * When `expandedStepIds` is provided, only inspects descendant events active at the current
 * expansion depth under `stepId` (skipping summary events of expanded sub-macros and
 * descendants whose intermediate ancestor below `stepId` is collapsed).
 */
export function computeHiddenCategoriesForStep(
  run: ExecutionRun,
  stepId: string,
  enabledCategories: ReadonlySet<StepCategory>,
  expandedStepIds?: ReadonlySet<string>,
): StepCategory[] {
  const stepById = new Map<string, RunStep>();
  const childrenByParent = new Map<string, RunStep[]>();
  for (const step of run.steps) {
    stepById.set(step.id, step);
    if (step.parentId) {
      const list = childrenByParent.get(step.parentId) ?? [];
      list.push(step);
      childrenByParent.set(step.parentId, list);
    }
  }
  if (!stepById.has(stepId)) return [];

  const activeDescendantIds = new Set<string>();
  const visited = new Set<string>([stepId]);
  const queue: RunStep[] = [...(childrenByParent.get(stepId) ?? [])];
  while (queue.length > 0) {
    const cur = queue.shift()!;
    if (visited.has(cur.id)) continue;
    visited.add(cur.id);

    const kids = childrenByParent.get(cur.id);
    const hasKids = Boolean(kids && kids.length > 0);

    if (expandedStepIds !== undefined) {
      if (hasKids && expandedStepIds.has(cur.id)) {
        for (const kid of kids!) queue.push(kid);
      } else {
        activeDescendantIds.add(cur.id);
      }
    } else {
      activeDescendantIds.add(cur.id);
      if (kids) {
        for (const kid of kids) queue.push(kid);
      }
    }
  }
  if (activeDescendantIds.size === 0) return [];

  const hidden = new Set<StepCategory>();
  for (const event of run.events) {
    if (!activeDescendantIds.has(event.stepId)) continue;
    const step = stepById.get(event.stepId);
    if (!step) continue;
    const effectiveCategory = event.categoryOverride ?? step.category;
    if (!enabledCategories.has(effectiveCategory)) {
      hidden.add(effectiveCategory);
    }
  }

  return [...hidden].sort();
}

/**
 * Counts how many canonical events exist in each category across the entire `run`,
 * including events inside collapsed sub-steps.
 */
export function computeRunWideCategoryCounts(run: ExecutionRun): Record<StepCategory, number> {
  const counts = emptyCategoryCounts();
  const stepById = new Map<string, RunStep>();
  for (const step of run.steps) {
    stepById.set(step.id, step);
  }
  for (const event of run.events) {
    const step = stepById.get(event.stepId);
    if (!step) continue;
    const effectiveCategory = event.categoryOverride ?? step.category;
    counts[effectiveCategory] = (counts[effectiveCategory] ?? 0) + 1;
  }
  return counts;
}

/**
 * Counts leaf events (excluding summary events of expandable steps) currently hidden
 * beneath at least one collapsed ancestor, grouped by effective category.
 *
 * Avoids summary/child double-counting across nested macro trees so collapsed
 * "+N deep" hints reflect exact reachable detail frames.
 */
export function computeDeepOnlyCategoryCounts(
  run: ExecutionRun,
  expandedStepIds: ReadonlySet<string>,
): Record<StepCategory, number> {
  const counts = emptyCategoryCounts();
  const stepById = new Map<string, RunStep>();
  const parentIds = new Set<string>();
  for (const step of run.steps) {
    stepById.set(step.id, step);
    if (step.parentId) parentIds.add(step.parentId);
  }

  for (const event of run.events) {
    const step = stepById.get(event.stepId);
    if (!step) continue;
    if (parentIds.has(step.id)) continue;
    if (hasVisibleAncestry(step, stepById, expandedStepIds)) continue;

    const effectiveCategory = event.categoryOverride ?? step.category;
    counts[effectiveCategory] = (counts[effectiveCategory] ?? 0) + 1;
  }

  return counts;
}

/**
 * Single deterministic timeline projector shared by TopologyCanvas,
 * ClusterMapCanvas, SequenceCanvas, MessageTable, and InspectorPanel.
 */
export function projectRun(
  run: ExecutionRun,
  expandedStepIds: ReadonlySet<string>,
  enabledCategories: ReadonlySet<StepCategory>,
): ProjectedTimeline {
  const stepById = new Map<string, RunStep>();
  const childrenByParent = new Map<string, RunStep[]>();
  const depthByStepId = new Map<string, number>();

  for (const step of run.steps) {
    stepById.set(step.id, step);
    if (step.parentId) {
      const list = childrenByParent.get(step.parentId) ?? [];
      list.push(step);
      childrenByParent.set(step.parentId, list);
    }
  }

  function getAncestors(step: RunStep): RunStep[] {
    const chain: RunStep[] = [];
    const seen = new Set<string>([step.id]);
    let cur = step.parentId ? stepById.get(step.parentId) : undefined;
    while (cur && !seen.has(cur.id)) {
      seen.add(cur.id);
      chain.unshift(cur);
      cur = cur.parentId ? stepById.get(cur.parentId) : undefined;
    }
    return chain;
  }

  for (const step of run.steps) {
    depthByStepId.set(step.id, getAncestors(step).length);
  }

  const categoryCounts = emptyCategoryCounts();
  const activeExpansionEvents: {
    canonicalIndex: number;
    event: RunEvent;
    step: RunStep;
    isSummary: boolean;
  }[] = [];

  for (let i = 0; i < run.events.length; i++) {
    const event = run.events[i];
    const step = stepById.get(event.stepId);
    if (!step) continue;

    const kids = childrenByParent.get(step.id);
    const hasKids = Boolean(kids && kids.length > 0);
    const isExpanded = expandedStepIds.has(step.id);

    // When a macro step with children is expanded, skip its own summary event(s)
    // so we never replay the summary + its expansion twice.
    if (hasKids && isExpanded) {
      continue;
    }

    if (!hasVisibleAncestry(step, stepById, expandedStepIds)) {
      continue;
    }

    const effectiveCategory = event.categoryOverride ?? step.category;
    categoryCounts[effectiveCategory] = (categoryCounts[effectiveCategory] ?? 0) + 1;

    activeExpansionEvents.push({
      canonicalIndex: i,
      event,
      step,
      isSummary: hasKids && !isExpanded,
    });
  }

  const visibleFrames: ProjectedFrame[] = [];
  const skippedSpans: SkippedSpan[] = [];
  const frameIndexByEventId = new Map<string, number>();

  let pendingSkippedCats = new Set<StepCategory>();
  let pendingSkippedSteps = new Set<string>();
  let pendingSkippedCount = 0;

  function flushSkipped(beforeFrameIndex: number) {
    if (pendingSkippedCount === 0) return;
    skippedSpans.push({
      id: `skip-${skippedSpans.length}-${beforeFrameIndex}`,
      beforeFrameIndex,
      categories: [...pendingSkippedCats],
      eventCount: pendingSkippedCount,
      stepIds: [...pendingSkippedSteps],
    });
    pendingSkippedCats = new Set();
    pendingSkippedSteps = new Set();
    pendingSkippedCount = 0;
  }

  const visibleStepMap = new Map<string, ProjectedStep>();
  const visibleSteps: ProjectedStep[] = [];

  for (const item of activeExpansionEvents) {
    const effectiveCategory = item.event.categoryOverride ?? item.step.category;
    if (!enabledCategories.has(effectiveCategory)) {
      pendingSkippedCats.add(effectiveCategory);
      pendingSkippedSteps.add(item.step.id);
      pendingSkippedCount += 1;
      continue;
    }

    const frameIndex = visibleFrames.length;
    flushSkipped(frameIndex);

    let projStep = visibleStepMap.get(item.step.id);
    if (!projStep) {
      const kids = childrenByParent.get(item.step.id) ?? [];
      projStep = {
        step: item.step,
        depth: depthByStepId.get(item.step.id) ?? 0,
        displayNumber: visibleSteps.length + 1,
        hasChildren: kids.length > 0,
        isExpanded: expandedStepIds.has(item.step.id),
        childCount: kids.length,
        firstFrameIndex: frameIndex,
        frameIndices: [],
      };
      visibleStepMap.set(item.step.id, projStep);
      visibleSteps.push(projStep);
    }

    const visibleStepIndex = projStep.displayNumber - 1;
    projStep.frameIndices.push(frameIndex);

    const frame: ProjectedFrame = {
      frameIndex,
      canonicalEventIndex: item.canonicalIndex,
      event: item.event,
      step: item.step,
      ancestors: getAncestors(item.step),
      visibleStepIndex,
      displayStepNumber: projStep.displayNumber,
      effectiveCategory,
      isSummaryFrame: item.isSummary,
    };

    visibleFrames.push(frame);
    frameIndexByEventId.set(item.event.id, frameIndex);
  }

  flushSkipped(-1);

  const stepsWithVisibleDescendants = new Set<string>();
  for (const frame of visibleFrames) {
    for (const ancestor of frame.ancestors) {
      stepsWithVisibleDescendants.add(ancestor.id);
    }
  }

  const emptyExpandedStepIds: string[] = [];
  for (const step of run.steps) {
    if (!expandedStepIds.has(step.id)) continue;
    const kids = childrenByParent.get(step.id);
    if (!kids || kids.length === 0) continue;
    if (!hasVisibleAncestry(step, stepById, expandedStepIds)) continue;
    if (!stepsWithVisibleDescendants.has(step.id)) {
      emptyExpandedStepIds.push(step.id);
    }
  }

  return {
    runId: run.id,
    visibleSteps,
    visibleFrames,
    skippedSpans,
    frameIndexByEventId,
    categoryCounts,
    totalCanonicalEvents: run.events.length,
    emptyExpandedStepIds,
  };
}

export function resolveEventAfterProjectionChange(
  previousEventId: string | null,
  run: ExecutionRun,
  projection: ProjectedTimeline,
): string | null {
  if (projection.visibleFrames.length === 0) return null;
  if (previousEventId && projection.frameIndexByEventId.has(previousEventId)) {
    return previousEventId;
  }

  if (!previousEventId) {
    return projection.visibleFrames[0].event.id;
  }

  const prevCanonicalIndex = run.events.findIndex(e => e.id === previousEventId);
  if (prevCanonicalIndex < 0) {
    return projection.visibleFrames[0].event.id;
  }

  const nextFrame = projection.visibleFrames.find(
    f => f.canonicalEventIndex >= prevCanonicalIndex,
  );
  if (nextFrame) return nextFrame.event.id;

  return projection.visibleFrames[projection.visibleFrames.length - 1].event.id;
}

export function stepJumpEventId(
  currentEventId: string | null,
  projection: ProjectedTimeline,
  direction: 1 | -1,
): string | null {
  if (projection.visibleSteps.length === 0) return null;
  if (!currentEventId) {
    return direction > 0
      ? projection.visibleSteps[0]
        ? projection.visibleFrames[projection.visibleSteps[0].firstFrameIndex]?.event.id ?? null
        : null
      : projection.visibleFrames[projection.visibleFrames.length - 1]?.event.id ?? null;
  }
  const curFrameIndex = projection.frameIndexByEventId.get(currentEventId);
  if (curFrameIndex === undefined) {
    return projection.visibleFrames[0]?.event.id ?? null;
  }
  const curStepIdx = projection.visibleFrames[curFrameIndex].visibleStepIndex;
  const targetStepIdx = Math.max(
    0,
    Math.min(projection.visibleSteps.length - 1, curStepIdx + direction),
  );
  const targetStep = projection.visibleSteps[targetStepIdx];
  return projection.visibleFrames[targetStep.firstFrameIndex]?.event.id ?? null;
}

export function resolveEventAfterToggleExpand(
  toggledStepId: string,
  wasExpanded: boolean,
  run: ExecutionRun,
  nextProjection: ProjectedTimeline,
  previousEventId: string | null,
): string | null {
  if (!wasExpanded) {
    const childFrame = nextProjection.visibleFrames.find(f =>
      f.ancestors.some(a => a.id === toggledStepId),
    );
    if (childFrame) return childFrame.event.id;
  } else {
    const parentFrame = nextProjection.visibleFrames.find(f => f.step.id === toggledStepId);
    if (parentFrame) return parentFrame.event.id;

    const ancestors = getStepAncestors(run, toggledStepId);
    for (let i = ancestors.length - 1; i >= 0; i--) {
      const ancestorFrame = nextProjection.visibleFrames.find(
        f => f.step.id === ancestors[i].id,
      );
      if (ancestorFrame) return ancestorFrame.event.id;
    }
  }
  return resolveEventAfterProjectionChange(previousEventId, run, nextProjection);
}
