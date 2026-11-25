import { COMPONENTS } from '../data/components.ts';
import { ALL_TRACES } from '../data/traces.ts';
import { CHANNEL_META, HOST_ZONES } from '../data/types.ts';
import type { ChannelKind, ComponentId, HostZoneId } from '../data/types.ts';

export interface PeerInteractionItem {
  peerId: ComponentId;
  shortName: string;
  fullName: string;
  hostZoneLabel: string;
  hostZoneShort: string;
  primaryChannel: ChannelKind;
  primaryProtocol: string;
  labels: string[];
  messageCount: number;
}

export interface PeerInteractionsSummary {
  receivesFrom: PeerInteractionItem[];
  sendsTo: PeerInteractionItem[];
}

const HOST_ZONE_SHORT: Record<HostZoneId, string> = {
  'submit-host': 'Submit Host',
  'yarn-control': 'YARN Control',
  'storage-history': 'HDFS / History',
  'driver-jvm': 'Driver JVM',
  'worker-node-a': 'Worker A',
  'worker-node-b': 'Worker B',
};

interface PeerAgg {
  labels: Set<string>;
  channels: Map<ChannelKind, number>;
  protocols: Map<string, number>;
  count: number;
}

function buildPeerList(map: Map<ComponentId, PeerAgg>): PeerInteractionItem[] {
  const items: PeerInteractionItem[] = [];
  for (const [peerId, agg] of map.entries()) {
    const spec = COMPONENTS[peerId];
    if (!spec) continue;

    let primaryChannel: ChannelKind = 'internal';
    let bestNonErr = -1;
    let bestAny = -1;
    let fallbackChannel: ChannelKind = 'internal';
    for (const [ch, cnt] of agg.channels.entries()) {
      if (cnt > bestAny) {
        bestAny = cnt;
        fallbackChannel = ch;
      }
      if (ch !== 'error' && cnt > bestNonErr) {
        bestNonErr = cnt;
        primaryChannel = ch;
      }
    }
    if (bestNonErr < 0) {
      primaryChannel = fallbackChannel;
    }

    let primaryProtocol = CHANNEL_META[primaryChannel].short;
    let bestProto = -1;
    for (const [proto, cnt] of agg.protocols.entries()) {
      if (cnt > bestProto) {
        bestProto = cnt;
        primaryProtocol = proto;
      }
    }

    items.push({
      peerId,
      shortName: spec.shortName,
      fullName: spec.name,
      hostZoneLabel: HOST_ZONES[spec.hostZone].label,
      hostZoneShort: HOST_ZONE_SHORT[spec.hostZone],
      primaryChannel,
      primaryProtocol,
      labels: Array.from(agg.labels),
      messageCount: agg.count,
    });
  }

  items.sort(
    (a, b) =>
      b.messageCount - a.messageCount || a.shortName.localeCompare(b.shortName),
  );
  return items;
}

export function computePeerInteractions(
  componentId: ComponentId,
): PeerInteractionsSummary {
  const inbound = new Map<ComponentId, PeerAgg>();
  const outbound = new Map<ComponentId, PeerAgg>();

  const record = (
    targetMap: Map<ComponentId, PeerAgg>,
    peerId: ComponentId,
    label: string,
    channel: ChannelKind,
    protocol: string,
  ) => {
    let agg = targetMap.get(peerId);
    if (!agg) {
      agg = {
        labels: new Set<string>(),
        channels: new Map<ChannelKind, number>(),
        protocols: new Map<string, number>(),
        count: 0,
      };
      targetMap.set(peerId, agg);
    }
    agg.labels.add(label);
    agg.channels.set(channel, (agg.channels.get(channel) ?? 0) + 1);
    agg.protocols.set(protocol, (agg.protocols.get(protocol) ?? 0) + 1);
    agg.count += 1;
  };

  for (const trace of ALL_TRACES) {
    for (const msg of trace.messages) {
      if (msg.from === msg.to) continue;
      if (msg.to === componentId) {
        record(inbound, msg.from, msg.label, msg.channel, msg.protocol);
      } else if (msg.from === componentId) {
        record(outbound, msg.to, msg.label, msg.channel, msg.protocol);
      }
    }
  }

  return {
    receivesFrom: buildPeerList(inbound),
    sendsTo: buildPeerList(outbound),
  };
}
