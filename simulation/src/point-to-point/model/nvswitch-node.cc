#include "ns3/ipv4.h"
#include "ns3/packet.h"
#include "ns3/ipv4-header.h"
#include "ns3/pause-header.h"
#include "ns3/flow-id-tag.h"
#include "ns3/boolean.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "nvswitch-node.h"
#include "qbb-net-device.h"
#include "switch-node.h"
#include "ppp-header.h"
#include "ns3/int-header.h"
#include "ns3/simulator.h"
#include <algorithm>
#include <cmath>
#include <limits>

namespace ns3 {

TypeId NVSwitchNode::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::NVSwitchNode")
    .SetParent<Node> ()
    .AddConstructor<NVSwitchNode> ()
	.AddAttribute("AckHighPrio",
			"Set high priority for ACK/NACK or not",
			UintegerValue(0),
			MakeUintegerAccessor(&NVSwitchNode::m_ackHighPrio),
			MakeUintegerChecker<uint32_t>())
  ;
  return tid;
}

NVSwitchNode::NVSwitchNode(){
	m_ecmpSeed = m_id;
	m_node_type = 2;
	m_mmu = CreateObject<SwitchMmu>();
	for (uint32_t i = 0; i < pCnt; i++)
		for (uint32_t j = 0; j < pCnt; j++)
			for (uint32_t k = 0; k < qCnt; k++)
				m_bytes[i][j][k] = 0;
	for (uint32_t i = 0; i < pCnt; i++){
		m_txBytes[i] = 0;
		m_dynamicPortAssignments[i] = 0;
		last_txBytes[i] = 0;
		last_port_qlen[i] = 0;
	}
	for (uint32_t i = 0; i < pCnt; i++)
		m_lastPktSize[i] = m_lastPktTs[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_u[i] = 0;
}

int NVSwitchNode::GetOutDev(Ptr<const Packet> p, CustomHeader &ch){
	if (SwitchNode::DualTableRoutingEnabled() && ch.l3Prot == 0x11) {
		std::lock_guard<std::mutex> routeGuard(m_dynamicQpRoutesMutex);
		const QpRouteKey qpKey{
			ch.sip,
			ch.dip,
			ch.udp.sport,
			ch.udp.dport,
		};
		auto bound = m_dynamicQpRoutes.find(qpKey);
		if (bound != m_dynamicQpRoutes.end()) {
			const int outDev = bound->second;
			if (outDev >= 0 &&
				static_cast<uint32_t>(outDev) < GetNDevices() &&
				m_devices[outDev]->IsLinkUp()) {
				return outDev;
			}
			m_dynamicQpRoutes.erase(bound);
		}
	}

	// look up entries
	auto entry = m_rtTable.find(ch.dip);

	// no matching entry
	if (entry == m_rtTable.end())
		return -1;

	// entry found
	auto &nexthops = entry->second;
	if (nexthops.empty())
		return -1;

	union {
		uint8_t u8[4+4+2+2];
		uint32_t u32[3];
	} buf;
	buf.u32[0] = ch.sip;
	buf.u32[1] = ch.dip;
	buf.u32[2] = 0;
	if (ch.l3Prot == 0x6)
		buf.u32[2] = ch.tcp.sport | ((uint32_t)ch.tcp.dport << 16);
	else if (ch.l3Prot == 0x11)
		buf.u32[2] = ch.udp.sport | ((uint32_t)ch.udp.dport << 16);
	else if (ch.l3Prot == 0xFC || ch.l3Prot == 0xFD)
		buf.u32[2] = ch.ack.sport | ((uint32_t)ch.ack.dport << 16);

	if (SwitchNode::FlowletRoutingEnabled() &&
		ch.l3Prot == 0x11 && nexthops.size() > 1) {
		std::lock_guard<std::mutex> routeGuard(m_dynamicQpRoutesMutex);
		const QpRouteKey qpKey{
			ch.sip,
			ch.dip,
			ch.udp.sport,
			ch.udp.dport,
		};
		const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
		auto cached = m_flowletRoutes.find(qpKey);
		const bool hasCached = cached != m_flowletRoutes.end();
		const int previousDev = hasCached ? cached->second.outDev : -1;
		const bool previousEligible =
			hasCached && previousDev >= 0 &&
			static_cast<uint32_t>(previousDev) < GetNDevices() &&
			m_devices[previousDev]->IsLinkUp() &&
			std::find(nexthops.begin(), nexthops.end(), previousDev) !=
				nexthops.end();
		const uint64_t gapNs = SwitchNode::FlowletGapNs();
		const uint64_t maxBytes = SwitchNode::FlowletMaxBytes();
		const bool gapTriggered =
			hasCached && gapNs > 0 && nowNs >= cached->second.lastPacketNs &&
			nowNs - cached->second.lastPacketNs >= gapNs;
		const bool byteTriggered =
			hasCached && maxBytes > 0 &&
			ch.udp.seq >= cached->second.nextByteBoundary;
		const bool linkTriggered = hasCached && !previousEligible;
		const bool shouldReevaluate =
			!hasCached || gapTriggered || byteTriggered || linkTriggered;

		if (!shouldReevaluate) {
			cached->second.lastPacketNs = nowNs;
			return previousDev;
		}

		const uint64_t flowletId =
			hasCached ? cached->second.flowletId + 1 : 0;
		const uint32_t flowletSeed =
			m_ecmpSeed ^ static_cast<uint32_t>(flowletId * 0x9e3779b9ULL);
		const uint32_t start = EcmpHash(buf.u8, 12, flowletSeed) % nexthops.size();
		int bestDev = -1;
		uint64_t bestScoreNs = std::numeric_limits<uint64_t>::max();
		uint64_t bestQueueBytes = std::numeric_limits<uint64_t>::max();
		uint64_t bestTxBytes = std::numeric_limits<uint64_t>::max();
		uint64_t previousScoreNs = std::numeric_limits<uint64_t>::max();
		uint64_t previousQueueBytes = 0;
		uint64_t previousTxBytes = 0;
		uint32_t candidateCount = 0;
		for (uint32_t offset = 0; offset < nexthops.size(); ++offset) {
			const int candidate = nexthops[(start + offset) % nexthops.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= GetNDevices() ||
				!m_devices[candidate]->IsLinkUp()) {
				continue;
			}
			Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[candidate]);
			uint64_t scoreNs = 0;
			uint64_t queueBytes = 0;
			uint64_t propagationNs = 0;
			if (!SwitchNode::MeasureFlowletPort(
					device, p == nullptr ? 0 : p->GetSize(),
					&scoreNs, &queueBytes, &propagationNs)) {
				continue;
			}
			++candidateCount;
			const uint64_t txBytes =
				static_cast<uint32_t>(candidate) < pCnt ? m_txBytes[candidate] : 0;
			if (candidate == previousDev) {
				previousScoreNs = scoreNs;
				previousQueueBytes = queueBytes;
				previousTxBytes = txBytes;
			}
			if (scoreNs < bestScoreNs ||
				(scoreNs == bestScoreNs && txBytes < bestTxBytes)) {
				bestDev = candidate;
				bestScoreNs = scoreNs;
				bestQueueBytes = queueBytes;
				bestTxBytes = txBytes;
			}
		}

		if (bestDev >= 0) {
			int selectedDev = bestDev;
			uint64_t selectedScoreNs = bestScoreNs;
			uint64_t selectedQueueBytes = bestQueueBytes;
			uint64_t selectedTxBytes = bestTxBytes;
			if (previousEligible && bestDev != previousDev &&
				(previousScoreNs <= bestScoreNs ||
				 previousScoreNs - bestScoreNs <=
					SwitchNode::FlowletHysteresisNs())) {
				selectedDev = previousDev;
				selectedScoreNs = previousScoreNs;
				selectedQueueBytes = previousQueueBytes;
				selectedTxBytes = previousTxBytes;
			}
			const bool switched =
				hasCached && previousDev >= 0 && selectedDev != previousDev;
			if (!hasCached || switched) {
				if (switched && static_cast<uint32_t>(previousDev) < pCnt &&
					m_dynamicPortAssignments[previousDev] > 0) {
					--m_dynamicPortAssignments[previousDev];
				}
				if (static_cast<uint32_t>(selectedDev) < pCnt) {
					++m_dynamicPortAssignments[selectedDev];
				}
			}

			FlowletRouteState& state = m_flowletRoutes[qpKey];
			state.outDev = selectedDev;
			state.lastPacketNs = nowNs;
			state.flowletId = flowletId;
			if (maxBytes > 0) {
				const uint64_t chunk = ch.udp.seq / maxBytes;
				state.nextByteBoundary =
					chunk >= std::numeric_limits<uint64_t>::max() / maxBytes - 1
						? std::numeric_limits<uint64_t>::max()
						: (chunk + 1) * maxBytes;
			}
			SwitchNode::RecordFlowletDecisionStats(
				GetId(),
				static_cast<uint32_t>(selectedDev),
				candidateCount,
				selectedQueueBytes,
				selectedTxBytes,
				selectedScoreNs,
				previousScoreNs == std::numeric_limits<uint64_t>::max()
					? 0
					: previousScoreNs,
				flowletId,
				nowNs,
				switched,
				gapTriggered,
				byteTriggered,
				linkTriggered,
				ch);
			return selectedDev;
		}
	}

	if (SwitchNode::DynamicQpRoutingEnabled() &&
		!SwitchNode::FlowletRoutingEnabled() &&
		ch.l3Prot == 0x11 && nexthops.size() > 1) {
		std::lock_guard<std::mutex> routeGuard(m_dynamicQpRoutesMutex);
		const QpRouteKey qpKey{
			ch.sip,
			ch.dip,
			ch.udp.sport,
			ch.udp.dport,
		};
		auto cached = m_dynamicQpRoutes.find(qpKey);
		if (cached != m_dynamicQpRoutes.end()) {
			const int cachedDev = cached->second;
			const bool stillEligible =
				std::find(nexthops.begin(), nexthops.end(), cachedDev) !=
					nexthops.end() &&
				cachedDev >= 0 &&
				static_cast<uint32_t>(cachedDev) < GetNDevices() &&
				m_devices[cachedDev]->IsLinkUp();
			if (stillEligible) {
				return cachedDev;
			}
			m_dynamicQpRoutes.erase(cached);
		}

		const uint32_t start = EcmpHash(buf.u8, 12, m_ecmpSeed) % nexthops.size();
		int bestDev = -1;
		uint64_t bestQueueBytes = std::numeric_limits<uint64_t>::max();
		uint64_t bestAssignments = std::numeric_limits<uint64_t>::max();
		uint64_t bestTxBytes = std::numeric_limits<uint64_t>::max();
		uint32_t candidateCount = 0;
		for (uint32_t offset = 0; offset < nexthops.size(); ++offset) {
			const int candidate = nexthops[(start + offset) % nexthops.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= GetNDevices() ||
				!m_devices[candidate]->IsLinkUp()) {
				continue;
			}
			++candidateCount;
			Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[candidate]);
			const uint64_t queueBytes =
				device != nullptr && device->GetQueue() != nullptr
					? device->GetQueue()->GetNBytesTotal()
					: 0;
			const uint64_t assignments =
				static_cast<uint32_t>(candidate) < pCnt
					? m_dynamicPortAssignments[candidate]
					: 0;
			const uint64_t txBytes =
				static_cast<uint32_t>(candidate) < pCnt ? m_txBytes[candidate] : 0;
			const bool better =
				queueBytes < bestQueueBytes ||
				(queueBytes == bestQueueBytes && assignments < bestAssignments) ||
				(queueBytes == bestQueueBytes && assignments == bestAssignments &&
				 txBytes < bestTxBytes);
			if (better) {
				bestDev = candidate;
				bestQueueBytes = queueBytes;
				bestAssignments = assignments;
				bestTxBytes = txBytes;
			}
		}

		if (bestDev >= 0) {
			m_dynamicQpRoutes[qpKey] = bestDev;
			if (static_cast<uint32_t>(bestDev) < pCnt) {
				++m_dynamicPortAssignments[bestDev];
			}
			SwitchNode::RecordDynamicQpBindingStats(
				GetId(),
				static_cast<uint32_t>(bestDev),
				candidateCount,
				bestQueueBytes,
				bestTxBytes,
				bestAssignments,
				ch);
			return bestDev;
		}
	}

	uint32_t idx = EcmpHash(buf.u8, 12, m_ecmpSeed) % nexthops.size();
	return nexthops[idx];
}

void NVSwitchNode::SendToDev(Ptr<Packet>p, CustomHeader &ch){
	int idx = GetOutDev(p, ch);
	if (idx >= 0){
		NS_ASSERT_MSG(m_devices[idx]->IsLinkUp(), "The routing table look up should return link that is up");

		// determine the qIndex
		uint32_t qIndex;
		if (ch.l3Prot == 0xFF || ch.l3Prot == 0xFE || (m_ackHighPrio && (ch.l3Prot == 0xFD || ch.l3Prot == 0xFC))){  //QCN or PFC or NACK, go highest priority
			qIndex = 0;
		}else{
			qIndex = (ch.l3Prot == 0x06 ? 1 : ch.udp.pg); // if TCP, put to queue 1
		}


		// admission control
		FlowIdTag t;
		p->PeekPacketTag(t);
		uint32_t inDev = t.GetFlowId();
		if (qIndex != 0){ //not highest priority
			if (m_mmu->CheckIngressAdmission(inDev, qIndex, p->GetSize()) && m_mmu->CheckEgressAdmission(idx, qIndex, p->GetSize())){			// Admission control
				m_mmu->UpdateIngressAdmission(inDev, qIndex, p->GetSize());
				m_mmu->UpdateEgressAdmission(idx, qIndex, p->GetSize());
				}else{
					return; // Drop
				}
			}
			auto entry = m_rtTable.find(ch.dip);
			const uint32_t nextHopCount =
				entry == m_rtTable.end()
					? 0
					: static_cast<uint32_t>(entry->second.size());
			SwitchNode::RecordRouteChoiceStats(
				GetId(), GetNodeType(), inDev, idx, ch, p->GetSize(), nextHopCount);
			m_bytes[inDev][idx][qIndex] += p->GetSize();
			m_devices[idx]->SwitchSend(qIndex, p, ch);
	}else
	{
		return; // Drop
	}
}

uint32_t NVSwitchNode::EcmpHash(const uint8_t* key, size_t len, uint32_t seed) {
  uint32_t h = seed;
  if (len > 3) {
    const uint32_t* key_x4 = (const uint32_t*) key;
    size_t i = len >> 2;
    do {
      uint32_t k = *key_x4++;
      k *= 0xcc9e2d51;
      k = (k << 15) | (k >> 17);
      k *= 0x1b873593;
      h ^= k;
      h = (h << 13) | (h >> 19);
      h += (h << 2) + 0xe6546b64;
    } while (--i);
    key = (const uint8_t*) key_x4;
  }
  if (len & 3) {
    size_t i = len & 3;
    uint32_t k = 0;
    key = &key[i - 1];
    do {
      k <<= 8;
      k |= *key--;
    } while (--i);
    k *= 0xcc9e2d51;
    k = (k << 15) | (k >> 17);
    k *= 0x1b873593;
    h ^= k;
  }
  h ^= len;
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

void NVSwitchNode::SetEcmpSeed(uint32_t seed){
	m_ecmpSeed = seed;
}

void NVSwitchNode::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx){
	uint32_t dip = dstAddr.Get();
	m_rtTable[dip].push_back(intf_idx);
}

void NVSwitchNode::ClearTable(){
	m_rtTable.clear();
}

const std::vector<int>* NVSwitchNode::GetRouteNextHops(uint32_t dip) const {
	auto entry = m_rtTable.find(dip);
	return entry == m_rtTable.end() ? nullptr : &entry->second;
}

void NVSwitchNode::BindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint32_t outDev) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_dynamicQpRoutes[QpRouteKey{sip, dip, sport, dport}] =
		static_cast<int>(outDev);
}

void NVSwitchNode::UnbindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_dynamicQpRoutes.erase(QpRouteKey{sip, dip, sport, dport});
}

// This function can only be called in switch mode
bool NVSwitchNode::SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch){
	SendToDev(packet, ch);
	return true;
}

void NVSwitchNode::SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p){
	FlowIdTag t;
	p->PeekPacketTag(t);
	if (qIndex != 0){
		uint32_t inDev = t.GetFlowId();
		m_mmu->RemoveFromIngressAdmission(inDev, qIndex, p->GetSize());
		m_mmu->RemoveFromEgressAdmission(ifIndex, qIndex, p->GetSize());
		m_bytes[inDev][ifIndex][qIndex] -= p->GetSize();
	}
	m_txBytes[ifIndex] += p->GetSize();
	m_lastPktSize[ifIndex] = p->GetSize();
	m_lastPktTs[ifIndex] = Simulator::Now().GetTimeStep();
}
// for monitor
/**
 * outoput format:
 * time, sw_id, port_id, q_id, qlen, port_len
*/
void NVSwitchNode::PrintSwitchQlen(FILE* qlen_output){
	uint32_t n_dev = this->GetNDevices();
	for(uint32_t i = 1; i < n_dev; ++i){
		uint64_t port_len = 0;
		for(uint32_t j=0; j < qCnt; ++j){
			port_len += m_mmu->egress_bytes[i][j];
		}
		if(port_len == last_port_qlen[i]){
			continue;
		}
		for(uint32_t j=0; j < qCnt; ++j){
			fprintf(qlen_output, "%lu, %u, %u, %u, %u, %lu\n", Simulator::Now().GetTimeStep(), m_id, i, j, m_mmu->egress_bytes[i][j], port_len);
			fflush(qlen_output);
		}
		last_port_qlen[i] = port_len;
	}		
}

/**
 * outoput format:
 * time, sw_id, port_id, txBytes
*/
void NVSwitchNode::PrintSwitchBw(FILE* bw_output, uint32_t bw_mon_interval){
	uint32_t n_dev = this->GetNDevices();
	for(uint32_t i = 1; i < n_dev; ++i){
		if(last_txBytes[i] == m_txBytes[i]){
			continue;
		}
		double bw = (m_txBytes[i] - last_txBytes[i]) * 8 * 1e6 / bw_mon_interval; // bit/s
		bw = bw*1.0 / 1e9; // Gbps
		fprintf(bw_output, "%lu, %u, %u, %f\n", Simulator::Now().GetTimeStep(), m_id, i, bw);
		fflush(bw_output);
		last_txBytes[i] = m_txBytes[i];
	}		
}
} /* namespace ns3 */
