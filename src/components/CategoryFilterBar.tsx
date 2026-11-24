import { Check, SlidersHorizontal, X } from 'lucide-react';
import {
  CATEGORY_PRESETS,
  STEP_CATEGORIES,
  detectCategoryPreset,
} from '../data/runModel';
import type { CategoryPresetId, StepCategory } from '../data/runModel';

export interface CategoryFilterBarProps {
  id?: string;
  enabledCategories: ReadonlySet<StepCategory>;
  categoryCounts: Record<StepCategory, number>;
  deepOnlyCounts: Record<StepCategory, number>;
  skippedEventCount: number;
  onToggleCategory: (category: StepCategory) => void;
  onApplyPreset: (presetId: Exclude<CategoryPresetId, 'custom'>) => void;
  onSelectAll: () => void;
  onClearAll: () => void;
  onClose: () => void;
}

export default function CategoryFilterBar({
  id,
  enabledCategories,
  categoryCounts,
  deepOnlyCounts,
  skippedEventCount,
  onToggleCategory,
  onApplyPreset,
  onSelectAll,
  onClearAll,
  onClose,
}: CategoryFilterBarProps) {
  const activePreset = detectCategoryPreset(enabledCategories);
  const allEnabled = enabledCategories.size === STEP_CATEGORIES.length;
  const noneEnabled = enabledCategories.size === 0;

  return (
    <div
      id={id}
      className="category-filter-bar"
      role="region"
      aria-label="Step category filters"
    >
      <div className="category-preset-row">
        <div className="category-preset-left">
          <span className="category-preset-label">
            <SlidersHorizontal size={11} aria-hidden="true" />
            <span>PRESETS</span>
          </span>

          <div
            className="category-preset-group"
            role="group"
            aria-label="Category filter presets"
          >
            {CATEGORY_PRESETS.map((preset) => {
              const isActive = activePreset === preset.id;
              return (
                <button
                  key={preset.id}
                  type="button"
                  className={`category-preset-btn ${isActive ? 'active' : ''}`}
                  aria-pressed={isActive}
                  title={preset.description}
                  onClick={() => onApplyPreset(preset.id)}
                >
                  {preset.label}
                </button>
              );
            })}
          </div>

          {activePreset === 'custom' && (
            <span
              className="category-custom-badge"
              title="Custom subset of step categories selected"
            >
              Custom
            </span>
          )}

          {skippedEventCount > 0 && (
            <span className="category-skipped-meta" aria-live="polite">
              {skippedEventCount}{' '}
              {skippedEventCount === 1 ? 'event' : 'events'} skipped
            </span>
          )}
        </div>

        <div className="category-preset-actions">
          <button
            type="button"
            className="category-action-btn"
            disabled={allEnabled}
            onClick={onSelectAll}
          >
            Select All
          </button>
          <button
            type="button"
            className="category-action-btn"
            disabled={noneEnabled}
            onClick={onClearAll}
          >
            Clear
          </button>
          <button
            type="button"
            className="icon-button category-close-btn"
            aria-label="Close category filters"
            title="Close category filters (C)"
            onClick={onClose}
          >
            <X size={13} />
          </button>
        </div>
      </div>

      <div
        className="category-chips-grid"
        role="group"
        aria-label="Toggle individual step categories"
      >
        {STEP_CATEGORIES.map((cat) => {
          const isEnabled = enabledCategories.has(cat.id);
          const currentCount = categoryCounts[cat.id] ?? 0;
          const deepOnlyCount = deepOnlyCounts[cat.id] ?? 0;
          const isDeepOnly = currentCount === 0 && deepOnlyCount > 0;
          const isAbsentAtDepth = currentCount === 0 && deepOnlyCount === 0;

          return (
            <button
              key={cat.id}
              type="button"
              className={[
                'category-chip',
                isEnabled ? 'is-enabled' : 'is-disabled',
                isDeepOnly ? 'is-deep-only' : '',
                isAbsentAtDepth ? 'is-absent' : '',
              ]
                .filter(Boolean)
                .join(' ')}
              aria-pressed={isEnabled}
              title={cat.description}
              onClick={() => onToggleCategory(cat.id)}
            >
              <span className="category-check-box" aria-hidden="true">
                {isEnabled ? <Check size={9} strokeWidth={3} /> : null}
              </span>
              <span className="category-chip-label">{cat.short}</span>
              {isDeepOnly ? (
                <span
                  className="category-chip-count is-deep"
                  title={`${deepOnlyCount} ${deepOnlyCount === 1 ? 'event' : 'events'} inside collapsed sub-steps`}
                >
                  +{deepOnlyCount} deep
                </span>
              ) : (
                <span className="category-chip-count">{currentCount}</span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}
