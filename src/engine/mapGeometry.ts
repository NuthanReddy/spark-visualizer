import { COMPONENTS } from '../data/components.ts';
import type { RunEvent, RuntimeInstance } from '../data/runModel.ts';
import type { ComponentId, ComponentSpec, HostZoneId } from '../data/types.ts';

function clamp(v: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, v));
}

export interface ClusterCardSize {
  cardW: number;
  cardH: number;
}

/**
 * Computes safe card dimensions from actual clusterPos spacings so rendered cards
 * never intersect on the Master Canvas.
 */
export function computeClusterCardSize(
  components: ReadonlyArray<Pick<ComponentSpec, 'clusterPos'>>,
): ClusterCardSize {
  let minSameRowDx = 190;
  let minSameColDy = 92;
  for (let i = 0; i < components.length; i++) {
    const a = components[i].clusterPos;
    for (let j = i + 1; j < components.length; j++) {
      const b = components[j].clusterPos;
      const dx = Math.abs(a[0] - b[0]);
      const dy = Math.abs(a[1] - b[1]);
      if (dy < 38 && dx > 4) {
        minSameRowDx = Math.min(minSameRowDx, dx);
      }
      if (dx < 140 && dy > 4) {
        minSameColDy = Math.min(minSameColDy, dy);
      }
    }
  }
  return {
    cardW: clamp(Math.floor(minSameRowDx - 14), 132, 166),
    cardH: clamp(Math.floor(minSameColDy - 10), 36, 44),
  };
}

export interface ResolvedMapRouteEndpoints {
  from: ComponentId;
  to: ComponentId;
  isPeerTransfer: boolean;
  isLocalOnly: boolean;
  isCrossCatalogLocal: boolean;
  runtimeHostZone: HostZoneId;
  runtimeHostLabel: string;
  runtimeProcessLabel: string;
}

/**
 * Resolves the Master Canvas endpoints for a RunEvent.
 *
 * When a local event connects two components whose static catalog cards live in
 * different hostZones (e.g. exec-spark-env -> memory-manager on worker-node-a, or
 * rpc-env -> driver-transport on driver-jvm), NEVER draw a misleading cross-host
 * arrow across catalog hostZones. Instead, anchor the hop as a same-card local
 * self-loop on the endpoint whose catalog hostZone matches the runtime hostZone
 * (preferring toComponent if it matches, else fromComponent).
 */
export function resolveMapRouteEndpoints(
  ev: RunEvent,
  instances: Record<string, RuntimeInstance>,
): ResolvedMapRouteEndpoints {
  const fromSpec = COMPONENTS[ev.fromComponent];
  const toSpec = COMPONENTS[ev.toComponent];
  const fromInst = instances[ev.fromInstance];
  const toInst = instances[ev.toInstance];

  const runtimeHostZone: HostZoneId =
    fromInst?.hostZone ??
    toInst?.hostZone ??
    toSpec?.hostZone ??
    fromSpec.hostZone;
  const runtimeHostLabel =
    fromInst?.hostLabel ?? toInst?.hostLabel ?? runtimeHostZone;
  const runtimeProcessLabel =
    fromInst?.processId ?? toInst?.processId ?? '';

  const isCrossCatalog =
    Boolean(fromSpec && toSpec) && fromSpec.hostZone !== toSpec.hostZone;
  const sameRuntimeHost =
    Boolean(fromInst && toInst) && fromInst.hostZone === toInst.hostZone;

  if (sameRuntimeHost && isCrossCatalog) {
    const anchor: ComponentId =
      toSpec.hostZone === runtimeHostZone
        ? ev.toComponent
        : fromSpec.hostZone === runtimeHostZone
          ? ev.fromComponent
          : ev.toComponent;
    return {
      from: anchor,
      to: anchor,
      isPeerTransfer: false,
      isLocalOnly: true,
      isCrossCatalogLocal: true,
      runtimeHostZone,
      runtimeHostLabel,
      runtimeProcessLabel,
    };
  }

  const isPeerTransfer =
    ev.fromComponent === ev.toComponent && ev.fromInstance !== ev.toInstance;
  const isLocalSelf =
    ev.kind === 'local' &&
    ev.fromComponent === ev.toComponent &&
    ev.fromInstance === ev.toInstance;

  return {
    from: ev.fromComponent,
    to: ev.toComponent,
    isPeerTransfer,
    isLocalOnly: isLocalSelf,
    isCrossCatalogLocal: false,
    runtimeHostZone,
    runtimeHostLabel,
    runtimeProcessLabel,
  };
}
