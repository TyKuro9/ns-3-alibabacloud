#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <string>
#include <ns3/node.h>
#include "qbb-net-device.h"
#include "switch-mmu.h"
#include "pint.h"

namespace ns3 {

class Packet;

class SwitchNode : public Node{
	struct QpRouteKey {
		uint32_t sip;
		uint32_t dip;
		uint16_t sport;
		uint16_t dport;

		bool operator==(const QpRouteKey& other) const {
			return sip == other.sip && dip == other.dip &&
				sport == other.sport && dport == other.dport;
		}
	};

	struct QpRouteKeyHash {
		std::size_t operator()(const QpRouteKey& key) const {
			std::size_t hash = key.sip;
			hash ^= static_cast<std::size_t>(key.dip) + 0x9e3779b9U +
				(hash << 6) + (hash >> 2);
			hash ^= static_cast<std::size_t>(key.sport) + 0x9e3779b9U +
				(hash << 6) + (hash >> 2);
			hash ^= static_cast<std::size_t>(key.dport) + 0x9e3779b9U +
				(hash << 6) + (hash >> 2);
			return hash;
		}
	};

	struct FlowletRouteState {
		int outDev = -1;
		uint64_t lastPacketNs = 0;
		uint64_t nextByteBoundary = 0;
		uint64_t flowletId = 0;
	};

	static const uint32_t pCnt = 1025;	// Number of ports used
	static const uint32_t qCnt = 8;	// Number of queues/priorities used
	uint32_t m_ecmpSeed;
	std::unordered_map<uint32_t, std::vector<int> > m_rtTable; // map from ip address (u32) to possible ECMP port (index of dev)
	std::unordered_map<QpRouteKey, int, QpRouteKeyHash> m_dynamicQpRoutes;
	std::unordered_map<QpRouteKey, FlowletRouteState, QpRouteKeyHash> m_flowletRoutes;
	std::mutex m_dynamicQpRoutesMutex;
	uint64_t m_dynamicPortAssignments[pCnt];
	std::set<uint32_t> active_ports;	// record active ports in switch

	// monitor of PFC
	uint32_t m_bytes[pCnt][pCnt][qCnt]; // m_bytes[inDev][outDev][qidx] is the bytes from inDev enqueued for outDev at qidx
	
	uint64_t m_txBytes[pCnt]; // counter of tx bytes

	uint32_t m_lastPktSize[pCnt];
	uint64_t m_lastPktTs[pCnt]; // ns
	double m_u[pCnt];

protected:
	bool m_ecnEnabled;
	uint32_t m_ccMode;
	uint64_t m_maxRtt;

	uint32_t m_ackHighPrio; // set high priority for ACK/NACK

private:
	int GetOutDev(Ptr<const Packet>, CustomHeader &ch);
	void SendToDev(Ptr<Packet>p, CustomHeader &ch);
	static uint32_t EcmpHash(const uint8_t* key, size_t len, uint32_t seed);
	void CheckAndSendPfc(uint32_t inDev, uint32_t qIndex);
	void CheckAndSendResume(uint32_t inDev, uint32_t qIndex);
public:
	Ptr<SwitchMmu> m_mmu;

	static TypeId GetTypeId (void);
	SwitchNode();
	void SetEcmpSeed(uint32_t seed);
	void AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx);
	void ClearTable();
	const std::vector<int>* GetRouteNextHops(uint32_t dip) const;
	void BindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint32_t outDev);
	void UnbindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport);
	bool SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
	void SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p);

	// for approximate calc in PINT
	int logres_shift(int b, int l);
	int log2apprx(int x, int b, int m, int l); // given x of at most b bits, use most significant m bits of x, calc the result in l bits
	
	// for monitor
	uint64_t last_txBytes[pCnt]; // last sampling of the counter of tx bytes
	uint64_t last_port_qlen[pCnt]; // last sampling of the port length
	
	/**
	 * outoput format:
	 * time, sw_id, port_id, q_id, qlen, port_len
	 */
	void PrintSwitchQlen(FILE* qlen_output);
	/**
	 * outoput format:
	 * time, sw_id, port_id, txBytes
	 */
	void PrintSwitchBw(FILE* bw_output, uint32_t bw_mon_interval);

	static bool DynamicQpRoutingEnabled();
	static bool PathAwareQpRoutingEnabled();
	static bool FlowletRoutingEnabled();
	static bool DualTableRoutingEnabled();
	static bool AdaptiveZcubeRoutingEnabled();
	static bool DynamicChunkRoutingEnabled();
	static uint64_t FlowletGapNs();
	static uint64_t FlowletMaxBytes();
	static uint64_t FlowletHysteresisNs();
	static bool MeasureFlowletPort(
		Ptr<QbbNetDevice> device,
		uint32_t packetBytes,
		uint64_t* scoreNs,
		uint64_t* queueBytes,
		uint64_t* propagationNs);
	static void RecordRouteChoiceStats(
		uint32_t switchId,
		uint32_t nodeType,
		uint32_t inDev,
		uint32_t outDev,
		const CustomHeader& ch,
		uint32_t packetBytes,
		uint32_t nextHopCount);
	static void RecordDynamicQpBindingStats(
		uint32_t switchId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t priorPortBindings,
		const CustomHeader& ch);
	static void RecordFlowletDecisionStats(
		uint32_t switchId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t selectedScoreNs,
		uint64_t previousScoreNs,
		uint64_t flowletId,
		uint64_t decisionTimeNs,
		bool switched,
		bool gapTriggered,
		bool byteTriggered,
		bool linkTriggered,
		const CustomHeader& ch);
	static void RecordSourceFlowletDecisionStats(
		uint32_t nodeId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t selectedScoreNs,
		uint64_t previousScoreNs,
		uint64_t pathQueueDelayNs,
		uint64_t pathPropagationNs,
		uint64_t pathReservedBytes,
		uint32_t pathHops,
		uint64_t flowletId,
		uint64_t decisionTimeNs,
		bool switched,
		bool gapTriggered,
		bool byteTriggered,
		bool linkTriggered,
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport);
	static void RecordSourceFlowletPacketStats(
		uint32_t nodeId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint32_t packetBytes);
	static void RecordSourceQpBindingStats(
		bool dynamic,
		bool pathAware,
		uint32_t nodeId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t activeBytes,
		uint64_t txBytes,
		uint64_t activeQps,
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint64_t qpBytes,
		uint64_t pathScoreNs,
		uint64_t pathQueueDelayNs,
		uint64_t pathPropagationNs,
		uint64_t pathReservedBytes,
		uint32_t pathHops);
	static void DumpRouteChoiceStats(const std::string& path);
	static void PrintFlowletRoutingSummary();
};

} /* namespace ns3 */

#endif /* SWITCH_NODE_H */
