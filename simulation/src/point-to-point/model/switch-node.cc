#include "ns3/ipv4.h"
#include "ns3/packet.h"
#include "ns3/ipv4-header.h"
#include "ns3/pause-header.h"
#include "ns3/flow-id-tag.h"
#include "ns3/boolean.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "switch-node.h"
#include "qbb-net-device.h"
#include "qbb-channel.h"
#include "ppp-header.h"
#include "ns3/int-header.h"
#include "ns3/simulator.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <thread>

namespace ns3 {

namespace {

struct RouteChoiceKey {
	uint32_t sip;
	uint32_t dip;
	uint16_t sport;
	uint16_t dport;
	uint32_t switchId;
	uint32_t inDev;
	uint32_t outDev;

	bool operator<(const RouteChoiceKey& other) const {
		if (sip != other.sip) return sip < other.sip;
		if (dip != other.dip) return dip < other.dip;
		if (sport != other.sport) return sport < other.sport;
		if (dport != other.dport) return dport < other.dport;
		if (switchId != other.switchId) return switchId < other.switchId;
		if (inDev != other.inDev) return inDev < other.inDev;
		return outDev < other.outDev;
	}
};

struct RouteChoiceStats {
	uint64_t packets = 0;
	uint64_t bytes = 0;
	uint32_t nextHopCount = 0;
	uint32_t nodeType = 0;
};

struct DynamicQpBindingKey {
	uint32_t sip;
	uint32_t dip;
	uint16_t sport;
	uint16_t dport;
	uint32_t switchId;

	bool operator<(const DynamicQpBindingKey& other) const {
		if (sip != other.sip) return sip < other.sip;
		if (dip != other.dip) return dip < other.dip;
		if (sport != other.sport) return sport < other.sport;
		if (dport != other.dport) return dport < other.dport;
		return switchId < other.switchId;
	}
};

struct DynamicQpBindingStats {
	uint32_t outDev = 0;
	uint32_t candidateCount = 0;
	uint64_t queueBytes = 0;
	uint64_t txBytes = 0;
	uint64_t priorPortBindings = 0;
	bool pathAware = false;
	uint64_t pathScoreNs = 0;
	uint64_t pathQueueDelayNs = 0;
	uint64_t pathPropagationNs = 0;
	uint64_t pathReservedBytes = 0;
	uint32_t pathHops = 0;
	bool flowletAware = false;
	uint64_t flowletDecisions = 0;
	uint64_t flowletSwitches = 0;
	uint64_t flowletGapTriggers = 0;
	uint64_t flowletByteTriggers = 0;
	uint64_t flowletLinkTriggers = 0;
	uint64_t flowletLastId = 0;
	uint64_t flowletLastDecisionNs = 0;
	uint64_t flowletSelectedScoreNs = 0;
	uint64_t flowletPreviousScoreNs = 0;
};

std::atomic<uint64_t>& FlowletDecisionCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& FlowletSwitchCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& FlowletGapTriggerCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& FlowletByteTriggerCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& FlowletLinkTriggerCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& SourceFlowletDecisionCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& SourceFlowletSwitchCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbOutOfOrderPackets() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbOutOfOrderBytes() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbReorderDrainedPackets() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbReorderDrainedBytes() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbReorderPeakBytes() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbDuplicatePackets() {
	static std::atomic<uint64_t> count{0};
	return count;
}

std::atomic<uint64_t>& PacketDlbReorderNacks() {
	static std::atomic<uint64_t> count{0};
	return count;
}

constexpr size_t kPathLengthBuckets = 8;

std::array<std::atomic<uint64_t>, kPathLengthBuckets>& PathBindingCounts() {
	static std::array<std::atomic<uint64_t>, kPathLengthBuckets> counts{};
	return counts;
}

std::array<std::atomic<uint64_t>, kPathLengthBuckets>& PathBindingBytes() {
	static std::array<std::atomic<uint64_t>, kPathLengthBuckets> bytes{};
	return bytes;
}

std::map<RouteChoiceKey, RouteChoiceStats>& RouteChoiceTable() {
	static std::map<RouteChoiceKey, RouteChoiceStats> table;
	return table;
}

std::map<DynamicQpBindingKey, DynamicQpBindingStats>& DynamicQpBindingTable() {
	static std::map<DynamicQpBindingKey, DynamicQpBindingStats> table;
	return table;
}

std::mutex& RouteChoiceMutex() {
	static std::mutex mutex;
	return mutex;
}

std::mutex& RouteDumpMutex() {
	static std::mutex mutex;
	return mutex;
}

const std::string& RoutingPolicyValue() {
	static const std::string normalized = []() {
		const char* value = std::getenv("AS_NS3_ROUTING_POLICY");
		if (value == nullptr || value[0] == '\0') {
			value = std::getenv("NS3_ROUTING_POLICY");
		}
		std::string result = value == nullptr ? std::string() : std::string(value);
		std::transform(
			result.begin(), result.end(), result.begin(),
			[](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		return result;
	}();
	return normalized;
}

bool UseDisjointChunkRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_disjoint_chunk" ||
		policy == "disjoint_chunk_spray" ||
		policy == "zcube_disjoint_chunk" ||
		policy == "spray_dynamic_pair";
}

bool UseDynamicChunkRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_dynamic_chunk" ||
		policy == "dynamic_chunk_spray" || policy == "chunk_spray" ||
		policy == "chunk_adaptive" || UseDisjointChunkRoutingImpl();
}

bool UsePacketDlbRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_packet_dlb" || policy == "packet_dlb" ||
		policy == "packet_spray" || policy == "dlb_spray";
}

bool UseMultiQpPacketDlbRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_multi_qp_dlb" ||
		policy == "multi_qp_packet_dlb" ||
		policy == "packet_dlb_multi_qp" ||
		policy == "realistic_packet_dlb";
}

bool UseAdaptiveZcubeRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_adaptive" || policy == "adaptive_spray" ||
		policy == "zcube_adaptive" || policy == "eta_spray" ||
		UseDynamicChunkRoutingImpl() || UsePacketDlbRoutingImpl();
}

bool UseDualTableRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_dual_table" || policy == "dual_table_spray" ||
		policy == "dual_table_flowlet" || policy == "zcube_dual_table" ||
		UseAdaptiveZcubeRoutingImpl();
}

bool UsePathAwareQpRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_path" || policy == "spray_path_aware" ||
		policy == "path_aware" || policy == "qp_path" ||
		UseDualTableRoutingImpl();
}

bool UseFlowletRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_flowlet" || policy == "flowlet_spray" ||
		policy == "dynamic_flowlet" || policy == "flowlet_dynamic" ||
		(UseDualTableRoutingImpl() && !UseAdaptiveZcubeRoutingImpl() &&
		 !UsePacketDlbRoutingImpl());
}

bool UseDynamicQpRoutingImpl() {
	const std::string& policy = RoutingPolicyValue();
	return policy == "spray_dynamic" || policy == "dynamic_spray" ||
		policy == "qp_dynamic" || policy == "dynamic_qp" ||
		UsePathAwareQpRoutingImpl() || UseFlowletRoutingImpl();
}

uint64_t ParseRoutingUintEnv(
		const char* primaryName,
		const char* fallbackName,
		uint64_t defaultValue) {
	const char* value = std::getenv(primaryName);
	if ((value == nullptr || value[0] == '\0') && fallbackName != nullptr) {
		value = std::getenv(fallbackName);
	}
	if (value == nullptr || value[0] == '\0') {
		return defaultValue;
	}
	errno = 0;
	char* end = nullptr;
	const unsigned long long parsed = std::strtoull(value, &end, 10);
	if (errno != 0 || end == value || end == nullptr || *end != '\0') {
		std::fprintf(stderr, "Invalid %s=%s: expected a non-negative integer\n",
			primaryName, value);
		std::exit(EXIT_FAILURE);
	}
	return static_cast<uint64_t>(parsed);
}

uint64_t FlowletGapNsImpl() {
	static const uint64_t value = ParseRoutingUintEnv(
		"AS_NS3_FLOWLET_GAP_NS", "NS3_FLOWLET_GAP_NS", 5000);
	return value;
}

uint64_t FlowletMaxBytesImpl() {
	static const uint64_t value = ParseRoutingUintEnv(
		"AS_NS3_FLOWLET_BYTES", "NS3_FLOWLET_BYTES", 0);
	return value;
}

uint64_t FlowletHysteresisNsImpl() {
	static const uint64_t value = ParseRoutingUintEnv(
		"AS_NS3_FLOWLET_HYSTERESIS_NS", "NS3_FLOWLET_HYSTERESIS_NS", 500);
	return value;
}

const std::string& RouteChoiceOutputPath();

bool RoutingStatsEnabledImpl() {
	static const bool enabled = []() {
		const char* value = std::getenv("AS_NS3_ROUTING_STATS");
		return value == nullptr || value[0] == '\0' ||
			std::string(value) != "0";
	}();
	return enabled;
}

void RecordDynamicQpBindingRawImpl(
		uint32_t switchId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t priorPortBindings,
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		bool pathAware = false,
		uint64_t pathScoreNs = 0,
		uint64_t pathQueueDelayNs = 0,
		uint64_t pathPropagationNs = 0,
		uint64_t pathReservedBytes = 0,
		uint32_t pathHops = 0) {
	if (RouteChoiceOutputPath().empty()) {
		return;
	}
	DynamicQpBindingKey key{
		sip,
		dip,
		sport,
		dport,
		switchId,
	};
	DynamicQpBindingStats stats;
	stats.outDev = outDev;
	stats.candidateCount = candidateCount;
	stats.queueBytes = queueBytes;
	stats.txBytes = txBytes;
	stats.priorPortBindings = priorPortBindings;
	stats.pathAware = pathAware;
	stats.pathScoreNs = pathScoreNs;
	stats.pathQueueDelayNs = pathQueueDelayNs;
	stats.pathPropagationNs = pathPropagationNs;
	stats.pathReservedBytes = pathReservedBytes;
	stats.pathHops = pathHops;
	std::lock_guard<std::mutex> guard(RouteChoiceMutex());
	DynamicQpBindingTable()[key] = stats;
}

void RecordDynamicQpBindingImpl(
		uint32_t switchId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t priorPortBindings,
		const CustomHeader& ch) {
	RecordDynamicQpBindingRawImpl(
		switchId,
		outDev,
		candidateCount,
		queueBytes,
		txBytes,
		priorPortBindings,
		ch.sip,
		ch.dip,
		ch.udp.sport,
			ch.udp.dport);
}

void RecordFlowletDecisionRawImpl(
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
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		bool sourceDecision = false,
		uint64_t pathQueueDelayNs = 0,
		uint64_t pathPropagationNs = 0,
			uint64_t pathReservedBytes = 0,
			uint32_t pathHops = 0) {
	if (!RoutingStatsEnabledImpl()) {
		return;
	}
	if (!RouteChoiceOutputPath().empty()) {
		const DynamicQpBindingKey key{
			sip,
			dip,
			sport,
			dport,
			switchId,
		};
		std::lock_guard<std::mutex> guard(RouteChoiceMutex());
		DynamicQpBindingStats& stats = DynamicQpBindingTable()[key];
		stats.outDev = outDev;
		stats.candidateCount = candidateCount;
		stats.queueBytes = queueBytes;
		stats.txBytes = txBytes;
		stats.flowletAware = true;
		stats.flowletDecisions++;
		stats.flowletSwitches += switched ? 1 : 0;
		stats.flowletGapTriggers += gapTriggered ? 1 : 0;
		stats.flowletByteTriggers += byteTriggered ? 1 : 0;
		stats.flowletLinkTriggers += linkTriggered ? 1 : 0;
		stats.flowletLastId = flowletId;
		stats.flowletLastDecisionNs = decisionTimeNs;
		stats.flowletSelectedScoreNs = selectedScoreNs;
		stats.flowletPreviousScoreNs = previousScoreNs;
		if (sourceDecision) {
			stats.pathAware = true;
			stats.pathScoreNs = selectedScoreNs;
			stats.pathQueueDelayNs = pathQueueDelayNs;
			stats.pathPropagationNs = pathPropagationNs;
			stats.pathReservedBytes = pathReservedBytes;
			stats.pathHops = pathHops;
		}
	}
	FlowletDecisionCount().fetch_add(1, std::memory_order_relaxed);
	if (switched) {
		FlowletSwitchCount().fetch_add(1, std::memory_order_relaxed);
	}
	if (gapTriggered) {
		FlowletGapTriggerCount().fetch_add(1, std::memory_order_relaxed);
	}
	if (byteTriggered) {
		FlowletByteTriggerCount().fetch_add(1, std::memory_order_relaxed);
	}
	if (linkTriggered) {
		FlowletLinkTriggerCount().fetch_add(1, std::memory_order_relaxed);
	}
	if (sourceDecision) {
		SourceFlowletDecisionCount().fetch_add(1, std::memory_order_relaxed);
		if (switched) {
			SourceFlowletSwitchCount().fetch_add(1, std::memory_order_relaxed);
		}
	}
}

void RecordFlowletDecisionImpl(
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
		const CustomHeader& ch) {
	RecordFlowletDecisionRawImpl(
		switchId,
		outDev,
		candidateCount,
		queueBytes,
		txBytes,
		selectedScoreNs,
		previousScoreNs,
		flowletId,
		decisionTimeNs,
		switched,
		gapTriggered,
		byteTriggered,
		linkTriggered,
		ch.sip,
		ch.dip,
		ch.udp.sport,
		ch.udp.dport);
}

const std::string& RouteChoiceOutputPath() {
	static const std::string path = []() {
		const char* value = std::getenv("AS_NS3_ROUTE_CHOICE_FILE");
		return value == nullptr ? std::string() : std::string(value);
	}();
	return path;
}

uint64_t RouteChoiceDumpIntervalMs() {
	static const uint64_t interval = []() {
		const char* value = std::getenv("AS_NS3_ROUTE_CHOICE_DUMP_INTERVAL_MS");
		if (value == nullptr || value[0] == '\0') {
			return uint64_t{2000};
		}
		uint64_t parsed = static_cast<uint64_t>(std::strtoull(value, nullptr, 10));
		return parsed == 0 ? uint64_t{2000} : parsed;
	}();
	return interval;
}

void DumpRouteChoiceStatsImpl(const std::string& path) {
	if (path.empty()) {
		return;
	}
	std::lock_guard<std::mutex> dumpGuard(RouteDumpMutex());
	const std::string tmpPath = path + ".tmp";
	FILE* output = fopen(tmpPath.c_str(), "w");
	if (output == nullptr) {
		perror(tmpPath.c_str());
		return;
	}
		fprintf(
			output,
			"sip_hex,dip_hex,sport,dport,sw_id,node_type,in_dev,out_dev,next_hop_count,"
			"packets,bytes,routing_mode,bind_candidate_count,bind_queue_bytes,"
			"bind_tx_bytes,bind_prior_port_qps,bind_path_score_ns,"
			"bind_path_queue_delay_ns,bind_path_propagation_ns,"
			"bind_path_reserved_bytes,bind_path_hops,flowlet_decisions,"
			"flowlet_switches,flowlet_gap_triggers,flowlet_byte_triggers,"
			"flowlet_link_triggers,flowlet_last_id,flowlet_last_decision_ns,"
			"flowlet_selected_score_ns,flowlet_previous_score_ns\n");
	{
		std::lock_guard<std::mutex> guard(RouteChoiceMutex());
		for (const auto& item : RouteChoiceTable()) {
			const RouteChoiceKey& key = item.first;
			const RouteChoiceStats& stats = item.second;
			const DynamicQpBindingKey bindingKey{
				key.sip,
				key.dip,
				key.sport,
				key.dport,
				key.switchId,
			};
			const auto binding = DynamicQpBindingTable().find(bindingKey);
			const bool isDynamic = binding != DynamicQpBindingTable().end();
			const DynamicQpBindingStats emptyBinding;
			const DynamicQpBindingStats& bindingStats =
				isDynamic ? binding->second : emptyBinding;
			const char* routingMode = UseMultiQpPacketDlbRoutingImpl()
				? "multi_qp_packet_dlb"
				: "ecmp_hash";
			if (isDynamic) {
				if (UseMultiQpPacketDlbRoutingImpl()) {
					routingMode = "multi_qp_packet_dlb";
				} else if (UsePacketDlbRoutingImpl()) {
					routingMode = "packet_dlb";
				} else if (UseDisjointChunkRoutingImpl()) {
					routingMode = "disjoint_chunk_qp";
				} else if (UseDynamicChunkRoutingImpl()) {
					routingMode = "dynamic_chunk_qp";
				} else if (UseAdaptiveZcubeRoutingImpl()) {
					routingMode = "adaptive_qp";
				} else if (UseDualTableRoutingImpl()) {
					routingMode = "dual_table_flowlet";
				} else if (bindingStats.flowletAware) {
					routingMode = "dynamic_flowlet";
				} else if (bindingStats.pathAware) {
					routingMode = "path_aware_qp";
				} else {
					routingMode = "dynamic_qp";
				}
			}
			fprintf(
				output,
				"%08x,%08x,%u,%u,%u,%u,%u,%u,%u,%lu,%lu,%s,%u,%lu,%lu,%lu,"
				"%lu,%lu,%lu,%lu,%u,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu\n",
				key.sip,
				key.dip,
				key.sport,
				key.dport,
				key.switchId,
				stats.nodeType,
				key.inDev,
				key.outDev,
				stats.nextHopCount,
				stats.packets,
				stats.bytes,
				routingMode,
				bindingStats.candidateCount,
				bindingStats.queueBytes,
				bindingStats.txBytes,
				bindingStats.priorPortBindings,
				bindingStats.pathScoreNs,
				bindingStats.pathQueueDelayNs,
				bindingStats.pathPropagationNs,
				bindingStats.pathReservedBytes,
				bindingStats.pathHops,
				bindingStats.flowletDecisions,
				bindingStats.flowletSwitches,
				bindingStats.flowletGapTriggers,
				bindingStats.flowletByteTriggers,
				bindingStats.flowletLinkTriggers,
				bindingStats.flowletLastId,
				bindingStats.flowletLastDecisionNs,
				bindingStats.flowletSelectedScoreNs,
				bindingStats.flowletPreviousScoreNs);
		}
	}
	fclose(output);
	if (std::rename(tmpPath.c_str(), path.c_str()) != 0) {
		perror(path.c_str());
	}
}

void StartRouteChoiceAutoDump() {
	static std::once_flag once;
	std::call_once(once, []() {
		const std::string path = RouteChoiceOutputPath();
		if (path.empty()) {
			return;
		}
		std::thread([path]() {
			const auto interval =
				std::chrono::milliseconds(RouteChoiceDumpIntervalMs());
			while (true) {
				std::this_thread::sleep_for(interval);
				DumpRouteChoiceStatsImpl(path);
			}
		}).detach();
	});
}

void RecordRouteChoiceImpl(
		uint32_t switchId,
		uint32_t nodeType,
		uint32_t inDev,
		uint32_t outDev,
		const CustomHeader& ch,
		uint32_t packetBytes,
		uint32_t nextHopCount) {
	if (ch.l3Prot != 0x11 || RouteChoiceOutputPath().empty()) {
		return;
	}
	StartRouteChoiceAutoDump();
	RouteChoiceKey key{
		ch.sip,
		ch.dip,
		ch.udp.sport,
		ch.udp.dport,
		switchId,
		inDev,
		outDev,
	};
	std::lock_guard<std::mutex> guard(RouteChoiceMutex());
	RouteChoiceStats& stats = RouteChoiceTable()[key];
	stats.packets++;
	stats.bytes += packetBytes;
	stats.nextHopCount = nextHopCount;
	stats.nodeType = nodeType;
}

}  // namespace

TypeId SwitchNode::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::SwitchNode")
    .SetParent<Node> ()
    .AddConstructor<SwitchNode> ()
	.AddAttribute("EcnEnabled",
			"Enable ECN marking.",
			BooleanValue(false),
			MakeBooleanAccessor(&SwitchNode::m_ecnEnabled),
			MakeBooleanChecker())
	.AddAttribute("CcMode",
			"CC mode.",
			UintegerValue(0),
			MakeUintegerAccessor(&SwitchNode::m_ccMode),
			MakeUintegerChecker<uint32_t>())
	.AddAttribute("AckHighPrio",
			"Set high priority for ACK/NACK or not",
			UintegerValue(0),
			MakeUintegerAccessor(&SwitchNode::m_ackHighPrio),
			MakeUintegerChecker<uint32_t>())
	.AddAttribute("MaxRtt",
			"Max Rtt of the network",
			UintegerValue(9000),
			MakeUintegerAccessor(&SwitchNode::m_maxRtt),
			MakeUintegerChecker<uint32_t>())
  ;
  return tid;
}

SwitchNode::SwitchNode(){
	m_ecmpSeed = m_id;
	m_node_type = 1;
	m_mmu = CreateObject<SwitchMmu>();
	for (uint32_t i = 0; i < pCnt; i++)
		for (uint32_t j = 0; j < pCnt; j++)
			for (uint32_t k = 0; k < qCnt; k++)
				m_bytes[i][j][k] = 0;
	for (uint32_t i = 0; i < pCnt; i++) {
		m_txBytes[i] = 0;
		m_dynamicPortAssignments[i] = 0;
	}
	for (uint32_t i = 0; i < pCnt; i++)
		m_lastPktSize[i] = m_lastPktTs[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_u[i] = 0;
}

int SwitchNode::GetOutDev(Ptr<const Packet> p, CustomHeader &ch){
	if ((PacketDlbRoutingEnabled() || MultiQpPacketDlbRoutingEnabled()) &&
		ch.l3Prot == 0x11) {
		std::lock_guard<std::mutex> routeGuard(m_dynamicQpRoutesMutex);
		const PacketRouteKey packetKey{
			ch.sip,
			ch.dip,
			ch.udp.sport,
			ch.udp.dport,
			ch.udp.seq,
		};
		auto bound = m_packetDlbRoutes.find(packetKey);
		if (bound != m_packetDlbRoutes.end()) {
			const int outDev = bound->second;
			m_packetDlbRoutes.erase(bound);
			if (outDev >= 0 &&
				static_cast<uint32_t>(outDev) < GetNDevices() &&
				m_devices[outDev]->IsLinkUp()) {
				return outDev;
			}
		}
	}

	if (DualTableRoutingEnabled() && !PacketDlbRoutingEnabled() &&
		ch.l3Prot == 0x11) {
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

	if (FlowletRoutingEnabled() && ch.l3Prot == 0x11 && nexthops.size() > 1) {
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
		const uint64_t gapNs = FlowletGapNs();
		const uint64_t maxBytes = FlowletMaxBytes();
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
			if (!MeasureFlowletPort(
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
				 previousScoreNs - bestScoreNs <= FlowletHysteresisNs())) {
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
			RecordFlowletDecisionStats(
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

	// Dynamic spray binds the first data packet of a QP to the least-loaded
	// eligible next hop. The cached decision keeps all later packets in order.
	if (DynamicQpRoutingEnabled() && !FlowletRoutingEnabled() &&
		!PacketDlbRoutingEnabled() &&
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
			RecordDynamicQpBindingStats(
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

	if ((PacketDlbRoutingEnabled() || MultiQpPacketDlbRoutingEnabled()) &&
		ch.l3Prot == 0x11 &&
		nexthops.size() > 1) {
		const uint32_t sequenceSeed =
			static_cast<uint32_t>(ch.udp.seq) ^
			static_cast<uint32_t>(ch.udp.seq >> 32) ^ m_ecmpSeed;
		const uint32_t start =
			EcmpHash(buf.u8, 12, sequenceSeed) % nexthops.size();
		int bestDev = -1;
		uint64_t bestScoreNs = std::numeric_limits<uint64_t>::max();
		uint64_t bestQueueBytes = std::numeric_limits<uint64_t>::max();
		uint64_t bestTxBytes = std::numeric_limits<uint64_t>::max();
		uint32_t candidateCount = 0;
		for (uint32_t offset = 0; offset < nexthops.size(); ++offset) {
			const int candidate = nexthops[(start + offset) % nexthops.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= GetNDevices() ||
				!m_devices[candidate]->IsLinkUp()) {
				continue;
			}
			uint64_t scoreNs = 0;
			uint64_t queueBytes = 0;
			uint64_t propagationNs = 0;
			Ptr<QbbNetDevice> device =
				DynamicCast<QbbNetDevice>(m_devices[candidate]);
			if (!MeasureFlowletPort(
					device, p == nullptr ? 0 : p->GetSize(),
					&scoreNs, &queueBytes, &propagationNs)) {
				continue;
			}
			++candidateCount;
			const uint64_t txBytes =
				static_cast<uint32_t>(candidate) < pCnt
					? m_txBytes[candidate]
					: 0;
			if (scoreNs < bestScoreNs ||
				(scoreNs == bestScoreNs && txBytes < bestTxBytes)) {
				bestDev = candidate;
				bestScoreNs = scoreNs;
				bestQueueBytes = queueBytes;
				bestTxBytes = txBytes;
			}
		}
		if (bestDev >= 0) {
			RecordFlowletDecisionStats(
				GetId(),
				static_cast<uint32_t>(bestDev),
				candidateCount,
				bestQueueBytes,
				bestTxBytes,
				bestScoreNs,
				0,
				ch.udp.seq,
				Simulator::Now().GetNanoSeconds(),
				false,
				false,
				false,
				false,
				ch);
			return bestDev;
		}
	}

	// ECMP and static QP spray keep the original five-tuple hash behavior.
	uint32_t idx = EcmpHash(buf.u8, 12, m_ecmpSeed) % nexthops.size();
	return nexthops[idx];
}

void SwitchNode::CheckAndSendPfc(uint32_t inDev, uint32_t qIndex){
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldPause(inDev, qIndex)){
		device->SendPfc(qIndex, 0);
		m_mmu->SetPause(inDev, qIndex);
	}
}
void SwitchNode::CheckAndSendResume(uint32_t inDev, uint32_t qIndex){
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldResume(inDev, qIndex)){
		device->SendPfc(qIndex, 1);
		m_mmu->SetResume(inDev, qIndex);
	}
}

void SwitchNode::SendToDev(Ptr<Packet>p, CustomHeader &ch){
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
		// std::cout << "qIndex is: " << qIndex << std::endl;

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
			CheckAndSendPfc(inDev, qIndex);
		}
		auto entry = m_rtTable.find(ch.dip);
		uint32_t nextHopCount =
			entry == m_rtTable.end() ? 0 : static_cast<uint32_t>(entry->second.size());
		RecordRouteChoiceStats(
			GetId(), GetNodeType(), inDev, idx, ch, p->GetSize(), nextHopCount);
		m_bytes[inDev][idx][qIndex] += p->GetSize();
		m_devices[idx]->SwitchSend(qIndex, p, ch);
	}else
	{
		return; // Drop
	}
}

uint32_t SwitchNode::EcmpHash(const uint8_t* key, size_t len, uint32_t seed) {
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

void SwitchNode::SetEcmpSeed(uint32_t seed){
	m_ecmpSeed = seed;
}

void SwitchNode::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx){
	uint32_t dip = dstAddr.Get();
	m_rtTable[dip].push_back(intf_idx);
}

void SwitchNode::ClearTable(){
	m_rtTable.clear();
}

const std::vector<int>* SwitchNode::GetRouteNextHops(uint32_t dip) const {
	auto entry = m_rtTable.find(dip);
	return entry == m_rtTable.end() ? nullptr : &entry->second;
}

void SwitchNode::BindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint32_t outDev) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_dynamicQpRoutes[QpRouteKey{sip, dip, sport, dport}] =
		static_cast<int>(outDev);
}

void SwitchNode::UnbindPathAwareQpRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_dynamicQpRoutes.erase(QpRouteKey{sip, dip, sport, dport});
}

void SwitchNode::BindPacketDlbRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint64_t seq,
		uint32_t outDev) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_packetDlbRoutes[
		PacketRouteKey{sip, dip, sport, dport, seq}] =
		static_cast<int>(outDev);
}

void SwitchNode::UnbindPacketDlbRoute(
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint64_t seq) {
	std::lock_guard<std::mutex> guard(m_dynamicQpRoutesMutex);
	m_packetDlbRoutes.erase(
		PacketRouteKey{sip, dip, sport, dport, seq});
}

// This function can only be called in switch mode
bool SwitchNode::SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch){
	SendToDev(packet, ch);
	return true;
}

void SwitchNode::SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p){
	FlowIdTag t;
	p->PeekPacketTag(t);
	if (qIndex != 0){
		uint32_t inDev = t.GetFlowId();
		m_mmu->RemoveFromIngressAdmission(inDev, qIndex, p->GetSize());
		m_mmu->RemoveFromEgressAdmission(ifIndex, qIndex, p->GetSize());
		m_bytes[inDev][ifIndex][qIndex] -= p->GetSize();
		if (m_ecnEnabled){
			bool egressCongested = m_mmu->ShouldSendCN(ifIndex, qIndex);
			if (egressCongested){
				PppHeader ppp;
				Ipv4Header h;
				p->RemoveHeader(ppp);
				p->RemoveHeader(h);
				h.SetEcn((Ipv4Header::EcnType)0x03);
				p->AddHeader(h);
				p->AddHeader(ppp);
			}
		}
		//CheckAndSendPfc(inDev, qIndex);
		CheckAndSendResume(inDev, qIndex);
	}
	if (1){
		uint8_t* buf = p->GetBuffer();
		if (buf[PppHeader::GetStaticSize() + 9] == 0x11){ // udp packet
			IntHeader *ih = (IntHeader*)&buf[PppHeader::GetStaticSize() + 20 + 8 + 6]; // ppp, ip, udp, SeqTs, INT
			Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
			if (m_ccMode == 3){ // HPCC
				ih->PushHop(Simulator::Now().GetTimeStep(), m_txBytes[ifIndex], dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
			}else if (m_ccMode == 10){ // HPCC-PINT
				uint64_t t = Simulator::Now().GetTimeStep();
				uint64_t dt = t - m_lastPktTs[ifIndex];
				if (dt > m_maxRtt)
					dt = m_maxRtt;
				uint64_t B = dev->GetDataRate().GetBitRate() / 8; //Bps
				uint64_t qlen = dev->GetQueue()->GetNBytesTotal();
				double newU;

				/**************************
				 * approximate calc
				 *************************/
				int b = 20, m = 16, l = 20; // see log2apprx's paremeters
				int sft = logres_shift(b,l);
				double fct = 1<<sft; // (multiplication factor corresponding to sft)
				double log_T = log2(m_maxRtt)*fct; // log2(T)*fct
				double log_B = log2(B)*fct; // log2(B)*fct
				double log_1e9 = log2(1e9)*fct; // log2(1e9)*fct
				double qterm = 0;
				double byteTerm = 0;
				double uTerm = 0;
				if ((qlen >> 8) > 0){
					int log_dt = log2apprx(dt, b, m, l); // ~log2(dt)*fct
					int log_qlen = log2apprx(qlen >> 8, b, m, l); // ~log2(qlen / 256)*fct
					qterm = pow(2, (
								log_dt + log_qlen + log_1e9 - log_B - 2*log_T
								)/fct
							) * 256;
					// 2^((log2(dt)*fct+log2(qlen/256)*fct+log2(1e9)*fct-log2(B)*fct-2*log2(T)*fct)/fct)*256 ~= dt*qlen*1e9/(B*T^2)
				}
				if (m_lastPktSize[ifIndex] > 0){
					int byte = m_lastPktSize[ifIndex];
					int log_byte = log2apprx(byte, b, m, l);
					byteTerm = pow(2, (
								log_byte + log_1e9 - log_B - log_T
								)/fct
							);
					// 2^((log2(byte)*fct+log2(1e9)*fct-log2(B)*fct-log2(T)*fct)/fct) ~= byte*1e9 / (B*T)
				}
				if (m_maxRtt > dt && m_u[ifIndex] > 0){
					int log_T_dt = log2apprx(m_maxRtt - dt, b, m, l); // ~log2(T-dt)*fct
					int log_u = log2apprx(int(round(m_u[ifIndex] * 8192)), b, m, l); // ~log2(u*512)*fct
					uTerm = pow(2, (
								log_T_dt + log_u - log_T
								)/fct
							) / 8192;
					// 2^((log2(T-dt)*fct+log2(u*512)*fct-log2(T)*fct)/fct)/512 = (T-dt)*u/T
				}
				newU = qterm+byteTerm+uTerm;

				#if 0
				/**************************
				 * accurate calc
				 *************************/
				double weight_ewma = double(dt) / m_maxRtt;
				double u;
				if (m_lastPktSize[ifIndex] == 0)
					u = 0;
				else{
					double txRate = m_lastPktSize[ifIndex] / double(dt); // B/ns
					u = (qlen / m_maxRtt + txRate) * 1e9 / B;
				}
				newU = m_u[ifIndex] * (1 - weight_ewma) + u * weight_ewma;
				printf(" %lf\n", newU);
				#endif

				/************************
				 * update PINT header
				 ***********************/
				uint16_t power = Pint::encode_u(newU);
				if (power > ih->GetPower())
					ih->SetPower(power);

				m_u[ifIndex] = newU;
			}
		}
	}
	m_txBytes[ifIndex] += p->GetSize();
	m_lastPktSize[ifIndex] = p->GetSize();
	m_lastPktTs[ifIndex] = Simulator::Now().GetTimeStep();
}

int SwitchNode::logres_shift(int b, int l){
	static int data[] = {0,0,1,2,2,3,3,3,3,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5};
	return l - data[b];
}

int SwitchNode::log2apprx(int x, int b, int m, int l){
	int x0 = x;
	int msb = int(log2(x)) + 1;
	if (msb > m){
		x = (x >> (msb - m) << (msb - m));
		#if 0
		x += + (1 << (msb - m - 1));
		#else
		int mask = (1 << (msb-m)) - 1;
		if ((x0 & mask) > (rand() & mask))
			x += 1<<(msb-m);
		#endif
	}
	return int(log2(x) * (1<<logres_shift(b, l)));
}

// for monitor
/**
 * outoput format:
 * time, sw_id, port_id, q_id, qlen, port_len
*/
void SwitchNode::PrintSwitchQlen(FILE* qlen_output){
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
 * time, sw_id, port_id, bandwidth
*/
void SwitchNode::PrintSwitchBw(FILE* bw_output, uint32_t bw_mon_interval){
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

bool SwitchNode::DynamicQpRoutingEnabled() {
	return UseDynamicQpRoutingImpl();
}

bool SwitchNode::PathAwareQpRoutingEnabled() {
	return UsePathAwareQpRoutingImpl();
}

bool SwitchNode::FlowletRoutingEnabled() {
	return UseFlowletRoutingImpl();
}

bool SwitchNode::DualTableRoutingEnabled() {
	return UseDualTableRoutingImpl();
}

bool SwitchNode::AdaptiveZcubeRoutingEnabled() {
	return UseAdaptiveZcubeRoutingImpl();
}

bool SwitchNode::DynamicChunkRoutingEnabled() {
	return UseDynamicChunkRoutingImpl();
}

bool SwitchNode::DisjointChunkRoutingEnabled() {
	return UseDisjointChunkRoutingImpl();
}

bool SwitchNode::PacketDlbRoutingEnabled() {
	return UsePacketDlbRoutingImpl();
}

bool SwitchNode::MultiQpPacketDlbRoutingEnabled() {
	return UseMultiQpPacketDlbRoutingImpl();
}

uint64_t SwitchNode::FlowletGapNs() {
	return FlowletGapNsImpl();
}

uint64_t SwitchNode::FlowletMaxBytes() {
	return FlowletMaxBytesImpl();
}

uint64_t SwitchNode::FlowletHysteresisNs() {
	return FlowletHysteresisNsImpl();
}

bool SwitchNode::MeasureFlowletPort(
		Ptr<QbbNetDevice> device,
		uint32_t packetBytes,
		uint64_t* scoreNs,
		uint64_t* queueBytes,
		uint64_t* propagationNs) {
	if (device == nullptr || !device->IsLinkUp() || scoreNs == nullptr ||
		queueBytes == nullptr || propagationNs == nullptr) {
		return false;
	}
	const uint64_t bitRate = device->GetDataRate().GetBitRate();
	if (bitRate == 0) {
		return false;
	}
	*queueBytes = device->GetQueue() == nullptr
		? 0
		: device->GetQueue()->GetNBytesTotal();
	Ptr<QbbChannel> channel = DynamicCast<QbbChannel>(device->GetChannel());
	*propagationNs = channel == nullptr
		? 0
		: static_cast<uint64_t>(channel->GetDelay().GetNanoSeconds());
	const long double queueAndSerializationNs =
		static_cast<long double>(*queueBytes + packetBytes) * 8.0L *
		1000000000.0L / static_cast<long double>(bitRate);
	const long double totalNs =
		static_cast<long double>(*propagationNs) + queueAndSerializationNs;
	const long double maximum =
		static_cast<long double>(std::numeric_limits<uint64_t>::max());
	*scoreNs = totalNs >= maximum
		? std::numeric_limits<uint64_t>::max()
		: static_cast<uint64_t>(std::llround(totalNs));
	return true;
}

void SwitchNode::RecordRouteChoiceStats(
		uint32_t switchId,
		uint32_t nodeType,
		uint32_t inDev,
		uint32_t outDev,
		const CustomHeader& ch,
		uint32_t packetBytes,
		uint32_t nextHopCount) {
	RecordRouteChoiceImpl(
		switchId,
		nodeType,
		inDev,
		outDev,
		ch,
		packetBytes,
		nextHopCount);
}

void SwitchNode::RecordDynamicQpBindingStats(
		uint32_t switchId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint64_t queueBytes,
		uint64_t txBytes,
		uint64_t priorPortBindings,
		const CustomHeader& ch) {
	RecordDynamicQpBindingImpl(
		switchId,
		outDev,
		candidateCount,
		queueBytes,
		txBytes,
		priorPortBindings,
		ch);
}

void SwitchNode::RecordFlowletDecisionStats(
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
		const CustomHeader& ch) {
	RecordFlowletDecisionImpl(
		switchId,
		outDev,
		candidateCount,
		queueBytes,
		txBytes,
		selectedScoreNs,
		previousScoreNs,
		flowletId,
		decisionTimeNs,
		switched,
		gapTriggered,
		byteTriggered,
		linkTriggered,
		ch);
}

void SwitchNode::RecordSourceFlowletDecisionStats(
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
		uint16_t dport) {
	RecordFlowletDecisionRawImpl(
		nodeId,
		outDev,
		candidateCount,
		queueBytes,
		txBytes,
		selectedScoreNs,
		previousScoreNs,
		flowletId,
		decisionTimeNs,
		switched,
		gapTriggered,
		byteTriggered,
		linkTriggered,
		sip,
		dip,
		sport,
		dport,
		true,
		pathQueueDelayNs,
		pathPropagationNs,
		pathReservedBytes,
		pathHops);
}

void SwitchNode::RecordSourceFlowletPacketStats(
		uint32_t nodeId,
		uint32_t outDev,
		uint32_t candidateCount,
		uint32_t sip,
		uint32_t dip,
		uint16_t sport,
		uint16_t dport,
		uint32_t packetBytes) {
	if (RouteChoiceOutputPath().empty()) {
		return;
	}
	StartRouteChoiceAutoDump();
	const RouteChoiceKey key{
		sip,
		dip,
		sport,
		dport,
		nodeId,
		0,
		outDev,
	};
	std::lock_guard<std::mutex> guard(RouteChoiceMutex());
	RouteChoiceStats& stats = RouteChoiceTable()[key];
	stats.packets++;
	stats.bytes += packetBytes;
	stats.nextHopCount = candidateCount;
	stats.nodeType = 0;
}

void SwitchNode::RecordPacketDlbReorderEvent(
		uint32_t packetBytes,
		uint64_t bufferedBytes,
		uint64_t drainedPackets,
		uint64_t drainedBytes,
		bool duplicate,
		bool nack) {
	if (!RoutingStatsEnabledImpl()) {
		return;
	}
	if (packetBytes > 0) {
		PacketDlbOutOfOrderPackets().fetch_add(1, std::memory_order_relaxed);
		PacketDlbOutOfOrderBytes().fetch_add(
			packetBytes, std::memory_order_relaxed);
	}
	if (drainedPackets > 0) {
		PacketDlbReorderDrainedPackets().fetch_add(
			drainedPackets, std::memory_order_relaxed);
		PacketDlbReorderDrainedBytes().fetch_add(
			drainedBytes, std::memory_order_relaxed);
	}
	uint64_t peak =
		PacketDlbReorderPeakBytes().load(std::memory_order_relaxed);
	while (bufferedBytes > peak &&
		!PacketDlbReorderPeakBytes().compare_exchange_weak(
			peak, bufferedBytes, std::memory_order_relaxed)) {
	}
	if (duplicate) {
		PacketDlbDuplicatePackets().fetch_add(1, std::memory_order_relaxed);
	}
	if (nack) {
		PacketDlbReorderNacks().fetch_add(1, std::memory_order_relaxed);
	}
}

void SwitchNode::RecordSourceQpBindingStats(
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
		uint32_t pathHops) {
	if (pathAware && pathHops > 0) {
		const size_t bucket = std::min<size_t>(
			pathHops, kPathLengthBuckets - 1);
		PathBindingCounts()[bucket].fetch_add(1, std::memory_order_relaxed);
		PathBindingBytes()[bucket].fetch_add(qpBytes, std::memory_order_relaxed);
	}
	if (dynamic) {
		RecordDynamicQpBindingRawImpl(
			nodeId,
			outDev,
			candidateCount,
			activeBytes,
			txBytes,
			activeQps,
			sip,
			dip,
			sport,
			dport,
			pathAware,
			pathScoreNs,
			pathQueueDelayNs,
			pathPropagationNs,
			pathReservedBytes,
			pathHops);
	}
	if (FlowletRoutingEnabled() || PacketDlbRoutingEnabled()) {
		return;
	}
	if (RouteChoiceOutputPath().empty()) {
		return;
	}
	StartRouteChoiceAutoDump();
	const RouteChoiceKey key{
		sip,
		dip,
		sport,
		dport,
		nodeId,
		0,
		outDev,
	};
	std::lock_guard<std::mutex> guard(RouteChoiceMutex());
	RouteChoiceStats& stats = RouteChoiceTable()[key];
	stats.packets++;
	stats.bytes += qpBytes;
	stats.nextHopCount = candidateCount;
	stats.nodeType = 0;
}

void SwitchNode::DumpRouteChoiceStats(const std::string& path) {
	DumpRouteChoiceStatsImpl(path);
}

void SwitchNode::PrintFlowletRoutingSummary() {
	static std::atomic<bool> pathPrinted{false};
	if (!pathPrinted.exchange(true)) {
		uint64_t totalBindings = 0;
		for (size_t bucket = 1; bucket < kPathLengthBuckets; ++bucket) {
			totalBindings += PathBindingCounts()[bucket].load(
				std::memory_order_relaxed);
		}
		if (totalBindings > 0) {
			std::cout << "[NS3 PATH SUMMARY]";
			for (size_t bucket = 1; bucket < kPathLengthBuckets; ++bucket) {
				const char* suffix = bucket == kPathLengthBuckets - 1
					? "plus"
					: "";
				std::cout << " links" << bucket << suffix << "_qps="
					<< PathBindingCounts()[bucket].load(
						std::memory_order_relaxed)
					<< " links" << bucket << suffix << "_bytes="
					<< PathBindingBytes()[bucket].load(
						std::memory_order_relaxed);
			}
			std::cout << std::endl;
		}
	}
	if (!FlowletRoutingEnabled() && !PacketDlbRoutingEnabled() &&
		!MultiQpPacketDlbRoutingEnabled()) {
		return;
	}
	static std::atomic<bool> printed{false};
	if (printed.exchange(true)) {
		return;
	}
	std::cout << (PacketDlbRoutingEnabled() || MultiQpPacketDlbRoutingEnabled()
				? "[NS3 PACKET DLB SUMMARY] decisions="
			: "[NS3 FLOWLET SUMMARY] decisions=")
		<< FlowletDecisionCount().load(std::memory_order_relaxed)
		<< " switches="
		<< FlowletSwitchCount().load(std::memory_order_relaxed)
		<< " gap_triggers="
		<< FlowletGapTriggerCount().load(std::memory_order_relaxed)
		<< " byte_triggers="
		<< FlowletByteTriggerCount().load(std::memory_order_relaxed)
		<< " link_triggers="
		<< FlowletLinkTriggerCount().load(std::memory_order_relaxed)
		<< " gap_ns=" << FlowletGapNs()
		<< " max_bytes=" << FlowletMaxBytes()
		<< " hysteresis_ns=" << FlowletHysteresisNs()
		<< " source_decisions="
		<< SourceFlowletDecisionCount().load(std::memory_order_relaxed)
		<< " source_switches="
		<< SourceFlowletSwitchCount().load(std::memory_order_relaxed)
		;
	if (PacketDlbRoutingEnabled() || MultiQpPacketDlbRoutingEnabled()) {
		std::cout << " reordered_packets="
			<< PacketDlbOutOfOrderPackets().load(std::memory_order_relaxed)
			<< " reordered_bytes="
			<< PacketDlbOutOfOrderBytes().load(std::memory_order_relaxed)
			<< " reorder_drained_packets="
			<< PacketDlbReorderDrainedPackets().load(
				std::memory_order_relaxed)
			<< " reorder_drained_bytes="
			<< PacketDlbReorderDrainedBytes().load(
				std::memory_order_relaxed)
			<< " reorder_peak_bytes="
			<< PacketDlbReorderPeakBytes().load(std::memory_order_relaxed)
			<< " duplicate_packets="
			<< PacketDlbDuplicatePackets().load(std::memory_order_relaxed)
			<< " reorder_nacks="
			<< PacketDlbReorderNacks().load(std::memory_order_relaxed);
	}
	std::cout << std::endl;
}

} /* namespace ns3 */
