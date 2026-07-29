#include <ns3/simulator.h>
#include <ns3/simple-seq-ts-header.h>
#include <ns3/udp-header.h>
#include <ns3/ipv4-header.h>
#include <ns3/node-list.h>
#include "ns3/ppp-header.h"
#include "ns3/boolean.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "ns3/data-rate.h"
#include "ns3/pointer.h"
#include "rdma-hw.h"
#include "ppp-header.h"
#include "qbb-header.h"
#include "cn-header.h"
#include "nvswitch-node.h"
#include "packet-dlb-tag.h"
#include "qbb-channel.h"
#include "switch-node.h"
#ifdef NS3_MTP
#include "ns3/mtp-interface.h"
#endif
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iostream>	// debug
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace ns3{

namespace {

constexpr uint32_t kMaxPathAwareHops = 32;
constexpr uint32_t kMaxPacketDlbPathsPerNic = 64;
constexpr uint64_t kP2rCapacityPinnedChunkMinBytes = 1ULL << 20;

uint32_t PacketDlbPathSampleWidth() {
	static const uint32_t width = []() {
		const char* value =
			std::getenv("AS_NS3_PACKET_DLB_PATH_SAMPLE_WIDTH");
		if (value == nullptr || value[0] == '\0') {
			value = std::getenv("AS_NS3_SPRAY_WIDTH");
		}
		if (value == nullptr || value[0] == '\0') {
			return uint32_t{4};
		}
		char* end = nullptr;
		const unsigned long parsed = std::strtoul(value, &end, 10);
		if (end == value || end == nullptr || *end != '\0' || parsed == 0) {
			return uint32_t{4};
		}
		return static_cast<uint32_t>(
			std::min<unsigned long>(parsed, kMaxPacketDlbPathsPerNic));
	}();
	return width;
}

bool PacketDlbBalancedThreeHopEnabled() {
	static const bool enabled = []() {
		const char* value =
			std::getenv("AS_NS3_PACKET_DLB_BALANCED_THREE_HOP");
		if (value == nullptr) {
			return false;
		}
		const std::string configured(value);
		return configured == "1" || configured == "true" ||
			configured == "TRUE" || configured == "on" ||
			configured == "ON";
	}();
	return enabled;
}

bool PacketDlbTwoFourHopEnabled() {
	static const bool enabled = []() {
		const char* value =
			std::getenv("AS_NS3_PACKET_DLB_TWO_FOUR_HOP");
		if (value == nullptr) {
			return false;
		}
		const std::string configured(value);
		return configured == "1" || configured == "true" ||
			configured == "TRUE" || configured == "on" ||
			configured == "ON";
	}();
	return enabled;
}

bool PacketDlbSelectiveCreditEnabled() {
	static const bool enabled = []() {
		const char* value =
			std::getenv("AS_NS3_PACKET_DLB_SELECTIVE_CREDIT");
		if (value == nullptr || value[0] == '\0') {
			return true;
		}
		const std::string configured(value);
		return configured != "0" && configured != "false" &&
			configured != "FALSE" && configured != "off" &&
			configured != "OFF";
	}();
	return enabled;
}

struct PathAwareCandidate {
	bool valid = false;
	long double queueDelayNs = 0;
	long double maxEdgeWorkNs = 0;
	uint64_t propagationNs = 0;
	uint64_t queueBytes = 0;
	uint64_t reservedBytes = 0;
	uint64_t bottleneckBps = std::numeric_limits<uint64_t>::max();
	std::vector<Ptr<QbbNetDevice>> hops;
};

Ptr<QbbNetDevice> GetPeerDevice(
	Ptr<QbbNetDevice> device,
	Ptr<QbbChannel>* channelOut);

std::unordered_map<uint64_t, uint64_t>& PathReservedBytes() {
	static std::unordered_map<uint64_t, uint64_t> reservations;
	return reservations;
}

std::unordered_map<uint64_t, uint64_t>& PacketDlbVirtualFinishNs() {
	static std::unordered_map<uint64_t, uint64_t> finishTimes;
	return finishTimes;
}

std::mutex& PathReservationMutex() {
	static std::mutex mutex;
	return mutex;
}

uint64_t PathEdgeKey(Ptr<QbbNetDevice> device) {
	return (static_cast<uint64_t>(device->GetNode()->GetId()) << 32) |
		static_cast<uint64_t>(device->GetIfIndex());
}

uint64_t PathSignature(const PathAwareCandidate& path) {
	uint64_t signature = 1469598103934665603ULL;
	for (const Ptr<QbbNetDevice>& device : path.hops) {
		signature ^= PathEdgeKey(device);
		signature *= 1099511628211ULL;
	}
	return signature;
}

int32_t PathDestinationNicIndex(const PathAwareCandidate& path) {
	if (path.hops.empty()) {
		return -1;
	}
	Ptr<QbbNetDevice> peer = GetPeerDevice(path.hops.back(), nullptr);
	if (peer == nullptr || peer->GetNode() == nullptr ||
		peer->GetNode()->GetNodeType() != 0) {
		return -1;
	}
	return static_cast<int32_t>(peer->GetIfIndex());
}

uint64_t SaturatingNs(long double value) {
	if (value <= 0) {
		return 0;
	}
	const long double maximum =
		static_cast<long double>(std::numeric_limits<uint64_t>::max());
	return value >= maximum
		? std::numeric_limits<uint64_t>::max()
		: static_cast<uint64_t>(std::llround(value));
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
	return lhs > std::numeric_limits<uint64_t>::max() - rhs
		? std::numeric_limits<uint64_t>::max()
		: lhs + rhs;
}

uint64_t SerializationNs(uint64_t bytes, uint64_t bitRate) {
	if (bytes == 0 || bitRate == 0) {
		return 0;
	}
	const long double value =
		static_cast<long double>(bytes) * 8.0L * 1000000000.0L /
		static_cast<long double>(bitRate);
	const long double maximum =
		static_cast<long double>(std::numeric_limits<uint64_t>::max());
	return value >= maximum
		? std::numeric_limits<uint64_t>::max()
		: static_cast<uint64_t>(std::ceil(value));
}

long double PathScoreNs(
		const PathAwareCandidate& path,
		uint64_t qpBytes,
		bool pipelinedReservations = false) {
	if (!path.valid || path.bottleneckBps == 0 ||
		path.bottleneckBps == std::numeric_limits<uint64_t>::max()) {
		return std::numeric_limits<long double>::infinity();
	}
	const long double serializationNs =
		static_cast<long double>(qpBytes) * 8.0L * 1000000000.0L /
		static_cast<long double>(path.bottleneckBps);
	return static_cast<long double>(path.propagationNs) +
		(pipelinedReservations ? path.maxEdgeWorkNs : path.queueDelayNs) +
		serializationNs;
}

Ptr<QbbNetDevice> GetPeerDevice(
		Ptr<QbbNetDevice> device,
		Ptr<QbbChannel>* channelOut) {
	if (device == nullptr) {
		return nullptr;
	}
	Ptr<QbbChannel> channel = DynamicCast<QbbChannel>(device->GetChannel());
	if (channel == nullptr || channel->GetNDevices() != 2) {
		return nullptr;
	}
	if (channelOut != nullptr) {
		*channelOut = channel;
	}
	for (uint32_t index = 0; index < channel->GetNDevices(); ++index) {
		Ptr<QbbNetDevice> candidate = channel->GetQbbDevice(index);
		if (candidate != device) {
			return candidate;
		}
	}
	return nullptr;
}

bool AppendPathHop(
		Ptr<QbbNetDevice> device,
		PathAwareCandidate* path,
		Ptr<QbbNetDevice>* peerDeviceOut = nullptr,
		bool recordHop = true) {
	if (device == nullptr || !device->IsLinkUp() || path == nullptr) {
		return false;
	}
	const uint64_t bitRate = device->GetDataRate().GetBitRate();
	if (bitRate == 0) {
		return false;
	}
	Ptr<QbbChannel> channel;
	Ptr<QbbNetDevice> peerDevice = GetPeerDevice(device, &channel);
	if (peerDevice == nullptr || channel == nullptr) {
		return false;
	}

	const uint64_t queueBytes = device->GetQueue() == nullptr
		? 0
		: device->GetQueue()->GetNBytesTotal();
	const auto reservation = PathReservedBytes().find(PathEdgeKey(device));
	const uint64_t reservedBytes = reservation == PathReservedBytes().end()
		? 0
		: reservation->second;
	const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
	const auto virtualFinish =
		PacketDlbVirtualFinishNs().find(PathEdgeKey(device));
	const uint64_t virtualWorkNs =
		virtualFinish == PacketDlbVirtualFinishNs().end() ||
			virtualFinish->second <= nowNs
			? 0
			: virtualFinish->second - nowNs;
	const long double virtualBytesValue =
		static_cast<long double>(virtualWorkNs) *
		static_cast<long double>(bitRate) / 8.0L / 1000000000.0L;
	const uint64_t virtualBytes = SaturatingNs(virtualBytesValue);
	path->valid = true;
	if (recordHop) {
		path->hops.push_back(device);
	}
	path->propagationNs += channel->GetDelay().GetNanoSeconds();
	path->queueBytes = SaturatingAdd(path->queueBytes, queueBytes);
	path->reservedBytes = SaturatingAdd(
		path->reservedBytes, SaturatingAdd(reservedBytes, virtualBytes));
	const long double queuedWorkNs =
		static_cast<long double>(SaturatingAdd(queueBytes, reservedBytes)) * 8.0L *
		1000000000.0L / static_cast<long double>(bitRate);
	const long double edgeWorkNs = std::max(
		queuedWorkNs, static_cast<long double>(virtualWorkNs));
	path->queueDelayNs += edgeWorkNs;
	path->maxEdgeWorkNs = std::max(path->maxEdgeWorkNs, edgeWorkNs);
	path->bottleneckBps = std::min(path->bottleneckBps, bitRate);
	if (peerDeviceOut != nullptr) {
		*peerDeviceOut = peerDevice;
	}
	return true;
}

std::vector<int> GetDualTableSourceCandidates(const RdmaHw& hw) {
	std::vector<int> candidates;
	for (uint32_t index = 0; index < hw.m_nic.size(); ++index) {
		Ptr<QbbNetDevice> device = hw.m_nic[index].dev;
		if (device == nullptr) {
			continue;
		}
		Ptr<QbbNetDevice> peerDevice = GetPeerDevice(device, nullptr);
		Ptr<Node> peerNode =
			peerDevice == nullptr ? nullptr : peerDevice->GetNode();
		if (peerNode != nullptr && peerNode->GetNodeType() == 1) {
			candidates.push_back(static_cast<int>(index));
		}
	}
	return candidates;
}

const std::vector<int>* GetRouteNextHops(Ptr<Node> node, uint32_t dip) {
	if (node->GetNodeType() == 1) {
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(node);
		return sw == nullptr ? nullptr : sw->GetRouteNextHops(dip);
	}
	if (node->GetNodeType() == 2) {
		Ptr<NVSwitchNode> sw = DynamicCast<NVSwitchNode>(node);
		return sw == nullptr ? nullptr : sw->GetRouteNextHops(dip);
	}
	return nullptr;
}

std::vector<Ptr<QbbNetDevice>> GetGpuFabricDevices(Ptr<Node> gpu);
Ptr<QbbNetDevice> FindDeviceToNode(Ptr<Node> node, uint32_t peerNodeId);

struct PacketDlbPathCacheKey {
	uint32_t sourceNode;
	uint32_t sourceIf;
	uint32_t destinationNode;
	uint32_t dip;

	bool operator==(const PacketDlbPathCacheKey& other) const {
		return sourceNode == other.sourceNode &&
			sourceIf == other.sourceIf &&
			destinationNode == other.destinationNode && dip == other.dip;
	}
};

struct PacketDlbPathCacheKeyHash {
	std::size_t operator()(const PacketDlbPathCacheKey& key) const {
		std::size_t hash = key.sourceNode;
		hash ^= static_cast<std::size_t>(key.sourceIf) + 0x9e3779b9U +
			(hash << 6) + (hash >> 2);
		hash ^= static_cast<std::size_t>(key.destinationNode) + 0x9e3779b9U +
			(hash << 6) + (hash >> 2);
		hash ^= static_cast<std::size_t>(key.dip) + 0x9e3779b9U +
			(hash << 6) + (hash >> 2);
		return hash;
	}
};

using PacketDlbPathTemplate = std::vector<Ptr<QbbNetDevice>>;
using PacketDlbPathTemplates = std::vector<PacketDlbPathTemplate>;

bool PacketDlbEndpointPairedPathAllowed(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		const PacketDlbPathTemplate& pathTemplate,
		bool* filterApplied);

uint64_t PacketDlbPathBaseRttNs(
		const PacketDlbPathTemplate& pathTemplate,
		uint64_t packetBytes) {
	uint64_t propagationNs = 0;
	uint64_t serializationNs = 0;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (device == nullptr || !device->IsLinkUp()) {
			return 0;
		}
		const uint64_t bitRate = device->GetDataRate().GetBitRate();
		if (bitRate == 0) {
			return 0;
		}
		Ptr<QbbChannel> channel =
			DynamicCast<QbbChannel>(device->GetChannel());
		if (channel == nullptr) {
			return 0;
		}
		propagationNs = SaturatingAdd(
			propagationNs, channel->GetDelay().GetNanoSeconds());
		serializationNs = SaturatingAdd(
			serializationNs, SerializationNs(packetBytes, bitRate));
	}
	return SaturatingAdd(
		SaturatingAdd(propagationNs, propagationNs),
		serializationNs);
}

struct PacketDlbPathStatsKey {
	uint32_t sourceGpu = 0;
	uint32_t destinationGpu = 0;
	uint32_t sourceNic = 0;
	int32_t destinationNic = -1;
	uint32_t pathHops = 0;
	uint64_t pathSignature = 0;

	bool operator<(const PacketDlbPathStatsKey& other) const {
		return std::tie(
			sourceGpu,
			destinationGpu,
			sourceNic,
			destinationNic,
			pathHops,
			pathSignature) <
			std::tie(
				other.sourceGpu,
				other.destinationGpu,
				other.sourceNic,
				other.destinationNic,
				other.pathHops,
				other.pathSignature);
	}
};

struct PacketDlbPathStats {
	std::string pathEdges;
	uint64_t consideredPackets = 0;
	uint64_t selectedPackets = 0;
	uint64_t selectedBytes = 0;
	uint64_t idleWhenConsidered = 0;
	uint64_t busyWhenConsidered = 0;
	uint64_t selectedIdlePackets = 0;
	uint64_t selectedBusyPackets = 0;
	uint64_t selectedBusyWithIdleAlternative = 0;
	uint64_t selectedWithLowerQueueAlternative = 0;
	uint64_t selectedWithShorterAlternative = 0;
	uint64_t candidatePathsSum = 0;
	uint64_t idleCandidatesSum = 0;
	uint64_t selectedQueueBytesSum = 0;
	uint64_t selectedReservedBytesSum = 0;
	uint64_t selectedScoreNsSum = 0;
};

struct PacketDlbConsideredPath {
	PacketDlbPathStatsKey key;
	const PacketDlbPathTemplate* pathTemplate = nullptr;
	std::string pathEdges;
	uint64_t queueBytes = 0;
	uint64_t reservedBytes = 0;
	uint64_t scoreNs = 0;

	bool IsIdle() const {
		return queueBytes == 0 && reservedBytes == 0;
	}

	uint64_t TotalQueuedBytes() const {
		return SaturatingAdd(queueBytes, reservedBytes);
	}
};

const std::string& PacketDlbPathStatsOutputPath() {
	static const std::string path = []() {
		const char* value =
			std::getenv("AS_NS3_PACKET_DLB_PATH_STATS_FILE");
		return value == nullptr ? std::string() : std::string(value);
	}();
	return path;
}

uint64_t PacketDlbPathStatsDumpIntervalMs() {
	static const uint64_t intervalMs = []() {
		const char* value =
			std::getenv("AS_NS3_ROUTE_CHOICE_DUMP_INTERVAL_MS");
		if (value == nullptr || value[0] == '\0') {
			return uint64_t{500};
		}
		char* end = nullptr;
		const unsigned long parsed = std::strtoul(value, &end, 10);
		return end == value || end == nullptr || *end != '\0' || parsed == 0
			? uint64_t{500}
			: static_cast<uint64_t>(parsed);
	}();
	return intervalMs;
}

std::map<PacketDlbPathStatsKey, PacketDlbPathStats>&
PacketDlbPathStatsTable() {
	static std::map<PacketDlbPathStatsKey, PacketDlbPathStats> table;
	return table;
}

std::mutex& PacketDlbPathStatsMutex() {
	static std::mutex mutex;
	return mutex;
}

uint64_t PacketDlbPathTemplateSignature(
		const PacketDlbPathTemplate& pathTemplate) {
	uint64_t signature = 1469598103934665603ULL;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		signature ^= PathEdgeKey(device);
		signature *= 1099511628211ULL;
	}
	return signature;
}

int32_t PacketDlbPathDestinationNic(
		const PacketDlbPathTemplate& pathTemplate) {
	if (pathTemplate.empty()) {
		return -1;
	}
	Ptr<QbbNetDevice> peer =
		GetPeerDevice(pathTemplate.back(), nullptr);
	if (peer == nullptr || peer->GetNode() == nullptr ||
		peer->GetNode()->GetNodeType() != 0) {
		return -1;
	}
	return static_cast<int32_t>(peer->GetIfIndex());
}

std::string PacketDlbPathEdges(
		const PacketDlbPathTemplate& pathTemplate) {
	std::ostringstream output;
	for (size_t index = 0; index < pathTemplate.size(); ++index) {
		Ptr<QbbNetDevice> device = pathTemplate[index];
		Ptr<QbbNetDevice> peer = GetPeerDevice(device, nullptr);
		if (index > 0) {
			output << '|';
		}
		output << device->GetNode()->GetId() << ':' << device->GetIfIndex();
		if (peer != nullptr && peer->GetNode() != nullptr) {
			output << '>' << peer->GetNode()->GetId()
				<< ':' << peer->GetIfIndex();
		}
	}
	return output.str();
}

void DumpPacketDlbPathStats(const std::string& path) {
	if (path.empty()) {
		return;
	}
	const std::string temporaryPath = path + ".tmp";
	FILE* output = fopen(temporaryPath.c_str(), "w");
	if (output == nullptr) {
		perror(temporaryPath.c_str());
		return;
	}
	fprintf(
		output,
		"src_gpu,dst_gpu,src_nic,dst_nic,path_hops,path_signature,"
		"path_edges,considered_packets,selected_packets,selected_bytes,"
		"idle_when_considered,busy_when_considered,selected_idle_packets,"
		"selected_busy_packets,selected_busy_with_idle_alternative,"
		"selected_with_lower_queue_alternative,"
		"selected_with_shorter_alternative,candidate_paths_sum,"
		"idle_candidates_sum,selected_queue_bytes_sum,"
		"selected_reserved_bytes_sum,selected_score_ns_sum\n");
	{
		std::lock_guard<std::mutex> guard(PacketDlbPathStatsMutex());
		for (const auto& item : PacketDlbPathStatsTable()) {
			const PacketDlbPathStatsKey& key = item.first;
			const PacketDlbPathStats& stats = item.second;
			fprintf(
				output,
				"%u,%u,%u,%d,%u,%lu,%s,%lu,%lu,%lu,%lu,%lu,%lu,"
				"%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu\n",
				key.sourceGpu,
				key.destinationGpu,
				key.sourceNic,
				key.destinationNic,
				key.pathHops,
				key.pathSignature,
				stats.pathEdges.c_str(),
				stats.consideredPackets,
				stats.selectedPackets,
				stats.selectedBytes,
				stats.idleWhenConsidered,
				stats.busyWhenConsidered,
				stats.selectedIdlePackets,
				stats.selectedBusyPackets,
				stats.selectedBusyWithIdleAlternative,
				stats.selectedWithLowerQueueAlternative,
				stats.selectedWithShorterAlternative,
				stats.candidatePathsSum,
				stats.idleCandidatesSum,
				stats.selectedQueueBytesSum,
				stats.selectedReservedBytesSum,
				stats.selectedScoreNsSum);
		}
	}
	fclose(output);
	if (std::rename(temporaryPath.c_str(), path.c_str()) != 0) {
		perror(path.c_str());
	}
}

std::atomic<bool>& PacketDlbPathStatsStopRequested() {
	static auto* requested = new std::atomic<bool>(false);
	return *requested;
}

std::condition_variable& PacketDlbPathStatsWakeup() {
	static auto* wakeup = new std::condition_variable;
	return *wakeup;
}

std::mutex& PacketDlbPathStatsWaitMutex() {
	static auto* mutex = new std::mutex;
	return *mutex;
}

std::thread& PacketDlbPathStatsThread() {
	static auto* thread = new std::thread;
	return *thread;
}

void StopPacketDlbPathStatsAutoDump() {
	PacketDlbPathStatsStopRequested().store(
		true, std::memory_order_release);
	PacketDlbPathStatsWakeup().notify_all();
	std::thread& thread = PacketDlbPathStatsThread();
	if (thread.joinable() &&
		thread.get_id() != std::this_thread::get_id()) {
		thread.join();
	}
}

void StartPacketDlbPathStatsAutoDump() {
	static std::once_flag once;
	std::call_once(once, []() {
		const std::string path = PacketDlbPathStatsOutputPath();
		if (path.empty()) {
			return;
		}
		// These objects must outlive the atexit hook that joins the worker.
		PacketDlbPathStatsTable();
		PacketDlbPathStatsMutex();
		PacketDlbPathStatsStopRequested().store(
			false, std::memory_order_release);
		if (std::atexit(StopPacketDlbPathStatsAutoDump) != 0) {
			return;
		}
		PacketDlbPathStatsThread() = std::thread([path]() {
			const auto interval = std::chrono::milliseconds(
				PacketDlbPathStatsDumpIntervalMs());
			std::unique_lock<std::mutex> lock(
				PacketDlbPathStatsWaitMutex());
			while (!PacketDlbPathStatsStopRequested().load(
					std::memory_order_acquire)) {
				const bool stopping =
					PacketDlbPathStatsWakeup().wait_for(
						lock,
						interval,
						[]() {
							return PacketDlbPathStatsStopRequested().load(
								std::memory_order_acquire);
						});
				if (stopping) {
					break;
				}
				lock.unlock();
				DumpPacketDlbPathStats(path);
				lock.lock();
			}
			lock.unlock();
			DumpPacketDlbPathStats(path);
		});
	});
}

void RecordPacketDlbPathDecision(
		const std::vector<PacketDlbConsideredPath>& consideredPaths,
		uint32_t selectedNic,
		const PacketDlbPathTemplate* selectedPathTemplate,
		uint64_t packetBytes) {
	if (consideredPaths.empty() || selectedPathTemplate == nullptr ||
		PacketDlbPathStatsOutputPath().empty()) {
		return;
	}
	StartPacketDlbPathStatsAutoDump();
	uint64_t idleCandidates = 0;
	uint64_t minimumQueuedBytes = std::numeric_limits<uint64_t>::max();
	uint32_t minimumHops = std::numeric_limits<uint32_t>::max();
	const PacketDlbConsideredPath* selectedPath = nullptr;
	for (const PacketDlbConsideredPath& path : consideredPaths) {
		idleCandidates += path.IsIdle() ? 1 : 0;
		minimumQueuedBytes =
			std::min(minimumQueuedBytes, path.TotalQueuedBytes());
		minimumHops = std::min(minimumHops, path.key.pathHops);
		if (path.key.sourceNic == selectedNic &&
			path.pathTemplate == selectedPathTemplate) {
			selectedPath = &path;
		}
	}

	std::lock_guard<std::mutex> guard(PacketDlbPathStatsMutex());
	for (const PacketDlbConsideredPath& path : consideredPaths) {
		PacketDlbPathStats& stats =
			PacketDlbPathStatsTable()[path.key];
		if (stats.pathEdges.empty()) {
			stats.pathEdges = path.pathEdges;
		}
		stats.consideredPackets++;
		stats.idleWhenConsidered += path.IsIdle() ? 1 : 0;
		stats.busyWhenConsidered += path.IsIdle() ? 0 : 1;
	}
	if (selectedPath == nullptr) {
		return;
	}

	PacketDlbPathStats& selectedStats =
		PacketDlbPathStatsTable()[selectedPath->key];
	const bool selectedIdle = selectedPath->IsIdle();
	const uint64_t idleAlternatives =
		idleCandidates - (selectedIdle ? 1 : 0);
	selectedStats.selectedPackets++;
	selectedStats.selectedBytes =
		SaturatingAdd(selectedStats.selectedBytes, packetBytes);
	selectedStats.selectedIdlePackets += selectedIdle ? 1 : 0;
	selectedStats.selectedBusyPackets += selectedIdle ? 0 : 1;
	selectedStats.selectedBusyWithIdleAlternative +=
		!selectedIdle && idleAlternatives > 0 ? 1 : 0;
	selectedStats.selectedWithLowerQueueAlternative +=
		selectedPath->TotalQueuedBytes() > minimumQueuedBytes ? 1 : 0;
	selectedStats.selectedWithShorterAlternative +=
		selectedPath->key.pathHops > minimumHops ? 1 : 0;
	selectedStats.candidatePathsSum += consideredPaths.size();
	selectedStats.idleCandidatesSum += idleCandidates;
	selectedStats.selectedQueueBytesSum = SaturatingAdd(
		selectedStats.selectedQueueBytesSum, selectedPath->queueBytes);
	selectedStats.selectedReservedBytesSum = SaturatingAdd(
		selectedStats.selectedReservedBytesSum,
		selectedPath->reservedBytes);
	selectedStats.selectedScoreNsSum = SaturatingAdd(
		selectedStats.selectedScoreNsSum, selectedPath->scoreNs);
}

std::unordered_map<
	PacketDlbPathCacheKey,
	PacketDlbPathTemplates,
	PacketDlbPathCacheKeyHash>& PacketDlbPathCache() {
	static std::unordered_map<
		PacketDlbPathCacheKey,
		PacketDlbPathTemplates,
		PacketDlbPathCacheKeyHash> cache;
	return cache;
}

std::mutex& PacketDlbPathCacheMutex() {
	static std::mutex mutex;
	return mutex;
}

bool SamePacketDlbPath(
		const PacketDlbPathTemplate& lhs,
		const PacketDlbPathTemplate& rhs) {
	if (lhs.size() != rhs.size()) {
		return false;
	}
	for (size_t index = 0; index < lhs.size(); ++index) {
		if (lhs[index] != rhs[index]) {
			return false;
		}
	}
	return true;
}

void AddPacketDlbPathTemplate(
		PacketDlbPathTemplate path,
		PacketDlbPathTemplates* paths) {
	if (paths == nullptr || path.empty() ||
		paths->size() >= kMaxPacketDlbPathsPerNic) {
		return;
	}
	for (const PacketDlbPathTemplate& existing : *paths) {
		if (SamePacketDlbPath(path, existing)) {
			return;
		}
	}
	paths->push_back(std::move(path));
}

void AddEndpointPairedPacketDlbPaths(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		PacketDlbPathTemplates* paths) {
	if (sourceDevice == nullptr || sourceDevice->GetNode() == nullptr ||
		paths == nullptr) {
		return;
	}
	Ptr<Node> destinationGpu = NodeList::GetNode(destinationNode);
	Ptr<QbbNetDevice> sourceSwitchIngress =
		GetPeerDevice(sourceDevice, nullptr);
	Ptr<Node> sourceSwitch = sourceSwitchIngress == nullptr
		? nullptr
		: sourceSwitchIngress->GetNode();
	if (sourceSwitch == nullptr || sourceSwitch->GetNodeType() != 1 ||
		destinationGpu == nullptr) {
		return;
	}

	const std::vector<Ptr<QbbNetDevice>> destinationDevices =
		GetGpuFabricDevices(destinationGpu);
	for (const Ptr<QbbNetDevice>& destinationDevice : destinationDevices) {
		Ptr<QbbNetDevice> destinationSwitchEgress =
			GetPeerDevice(destinationDevice, nullptr);
		Ptr<Node> destinationSwitch = destinationSwitchEgress == nullptr
			? nullptr
			: destinationSwitchEgress->GetNode();
		if (destinationSwitch == nullptr ||
			destinationSwitch->GetNodeType() != 1) {
			continue;
		}
		if (sourceSwitch->GetId() == destinationSwitch->GetId()) {
			AddPacketDlbPathTemplate(
				PacketDlbPathTemplate{
					sourceDevice,
					destinationSwitchEgress,
				},
				paths);
			continue;
		}

		Ptr<QbbNetDevice> directDevice = FindDeviceToNode(
			sourceSwitch, destinationSwitch->GetId());
		if (directDevice != nullptr) {
			AddPacketDlbPathTemplate(
				PacketDlbPathTemplate{
					sourceDevice,
					directDevice,
					destinationSwitchEgress,
				},
				paths);
			continue;
		}

		const uint32_t deviceCount = sourceSwitch->GetNDevices();
		for (uint32_t deviceIndex = 0;
			 deviceIndex < deviceCount &&
			 paths->size() < kMaxPacketDlbPathsPerNic;
			 ++deviceIndex) {
			Ptr<QbbNetDevice> firstDevice =
				DynamicCast<QbbNetDevice>(
					sourceSwitch->GetDevice(deviceIndex));
			Ptr<QbbNetDevice> middleIngress =
				GetPeerDevice(firstDevice, nullptr);
			Ptr<Node> middleSwitch = middleIngress == nullptr
				? nullptr
				: middleIngress->GetNode();
			if (middleSwitch == nullptr ||
				middleSwitch->GetNodeType() != 1 ||
				middleSwitch->GetId() == destinationSwitch->GetId()) {
				continue;
			}
			Ptr<QbbNetDevice> secondDevice = FindDeviceToNode(
				middleSwitch, destinationSwitch->GetId());
			if (secondDevice == nullptr) {
				continue;
			}
			AddPacketDlbPathTemplate(
				PacketDlbPathTemplate{
					sourceDevice,
					firstDevice,
					secondDevice,
					destinationSwitchEgress,
				},
				paths);
		}
	}
}

void EnumeratePacketDlbPaths(
		Ptr<QbbNetDevice> device,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t depth,
		std::unordered_set<uint32_t> visitedNodes,
		PacketDlbPathTemplate path,
		PacketDlbPathTemplates* paths) {
	if (device == nullptr || !device->IsLinkUp() || paths == nullptr ||
		depth >= kMaxPathAwareHops ||
		paths->size() >= kMaxPacketDlbPathsPerNic) {
		return;
	}
	Ptr<QbbNetDevice> peerDevice = GetPeerDevice(device, nullptr);
	if (peerDevice == nullptr || peerDevice->GetNode() == nullptr) {
		return;
	}
	path.push_back(device);
	Ptr<Node> nextNode = peerDevice->GetNode();
	if (nextNode->GetId() == destinationNode) {
		paths->push_back(std::move(path));
		return;
	}
	if (!visitedNodes.insert(nextNode->GetId()).second) {
		return;
	}

	const std::vector<int>* nextHops = GetRouteNextHops(nextNode, dip);
	if (nextHops == nullptr || nextHops->empty()) {
		return;
	}
	const uint32_t start =
		(dip ^ nextNode->GetId() ^ device->GetIfIndex()) %
		nextHops->size();
	for (uint32_t offset = 0;
		 offset < nextHops->size() &&
		 paths->size() < kMaxPacketDlbPathsPerNic;
		 ++offset) {
		const int nextHop = (*nextHops)[(start + offset) % nextHops->size()];
		if (nextHop < 0 ||
			static_cast<uint32_t>(nextHop) >= nextNode->GetNDevices()) {
			continue;
		}
		EnumeratePacketDlbPaths(
			DynamicCast<QbbNetDevice>(nextNode->GetDevice(nextHop)),
			destinationNode,
			dip,
			depth + 1,
			visitedNodes,
			path,
			paths);
	}
}

const PacketDlbPathTemplates& GetPacketDlbPathTemplates(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		uint32_t dip) {
	static const PacketDlbPathTemplates empty;
	if (sourceDevice == nullptr || sourceDevice->GetNode() == nullptr) {
		return empty;
	}
	std::lock_guard<std::mutex> cacheGuard(PacketDlbPathCacheMutex());
	const PacketDlbPathCacheKey key{
		sourceDevice->GetNode()->GetId(),
		sourceDevice->GetIfIndex(),
		destinationNode,
		dip,
	};
	auto found = PacketDlbPathCache().find(key);
	if (found != PacketDlbPathCache().end()) {
		return found->second;
	}

	PacketDlbPathTemplates paths;
	AddEndpointPairedPacketDlbPaths(
		sourceDevice, destinationNode, &paths);
	EnumeratePacketDlbPaths(
		sourceDevice,
		destinationNode,
		dip,
		0,
		std::unordered_set<uint32_t>{sourceDevice->GetNode()->GetId()},
		PacketDlbPathTemplate(),
		&paths);
	return PacketDlbPathCache().emplace(key, std::move(paths)).first->second;
}

bool PacketDlbPathMatchesConfiguredHopMode(
		const PacketDlbPathTemplate& pathTemplate) {
	if (PacketDlbBalancedThreeHopEnabled()) {
		return pathTemplate.size() == 3;
	}
	if (PacketDlbTwoFourHopEnabled()) {
		return pathTemplate.size() == 2 || pathTemplate.size() == 4;
	}
	return true;
}

struct PacketDlbAggregateTransport {
	uint64_t bitRate = 0;
	uint64_t windowBytes = 0;
	uint64_t maximumRttNs = 0;
	std::unordered_map<uint32_t, uint64_t> laneWindowBytes;
};

PacketDlbAggregateTransport GetPacketDlbAggregateTransport(
		const RdmaHw& hw,
		uint32_t destinationNode,
		uint32_t dip,
		uint64_t packetBytes) {
	PacketDlbAggregateTransport aggregate;
	for (const int candidateValue : GetDualTableSourceCandidates(hw)) {
		if (candidateValue < 0) {
			continue;
		}
		const uint32_t candidate =
			static_cast<uint32_t>(candidateValue);
		if (candidate >= hw.m_nic.size() ||
			hw.m_nic[candidate].dev == nullptr ||
			!hw.m_nic[candidate].dev->IsLinkUp()) {
			continue;
		}
		Ptr<QbbNetDevice> sourceDevice = hw.m_nic[candidate].dev;
		const PacketDlbPathTemplates& paths = GetPacketDlbPathTemplates(
			sourceDevice, destinationNode, dip);
		uint64_t laneRttNs = 0;
		for (const PacketDlbPathTemplate& pathTemplate : paths) {
			if (!PacketDlbPathMatchesConfiguredHopMode(pathTemplate)) {
				continue;
			}
			bool endpointFilterApplied = false;
			if (!PacketDlbEndpointPairedPathAllowed(
					sourceDevice,
					destinationNode,
					pathTemplate,
					&endpointFilterApplied)) {
				continue;
			}
			laneRttNs = std::max(
				laneRttNs,
				PacketDlbPathBaseRttNs(pathTemplate, packetBytes));
		}
		if (laneRttNs == 0) {
			for (const PacketDlbPathTemplate& pathTemplate : paths) {
				laneRttNs = std::max(
					laneRttNs,
					PacketDlbPathBaseRttNs(pathTemplate, packetBytes));
			}
		}
		if (laneRttNs == 0) {
			continue;
		}

		const uint64_t laneBitRate =
			sourceDevice->GetDataRate().GetBitRate();
		aggregate.bitRate = SaturatingAdd(
			aggregate.bitRate, laneBitRate);
		aggregate.maximumRttNs = std::max(
			aggregate.maximumRttNs, laneRttNs);
		const long double laneBdp =
			static_cast<long double>(laneRttNs) *
			static_cast<long double>(laneBitRate) /
			8.0L / 1000000000.0L;
		const uint64_t laneWindowBytes =
			SaturatingNs(std::ceil(laneBdp));
		aggregate.laneWindowBytes[candidate] = laneWindowBytes;
		aggregate.windowBytes = SaturatingAdd(
			aggregate.windowBytes, laneWindowBytes);
	}
	return aggregate;
}

bool EvaluatePacketDlbPath(
		const PacketDlbPathTemplate& pathTemplate,
		PathAwareCandidate* path) {
	if (path == nullptr || pathTemplate.empty()) {
		return false;
	}
	PathAwareCandidate candidate;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (!AppendPathHop(device, &candidate, nullptr, false)) {
			return false;
		}
	}
	*path = std::move(candidate);
	return true;
}

void ApplyActualPathWindow(
		RdmaHw* hw,
		Ptr<RdmaQueuePair> qp,
		const PacketDlbPathTemplate& pathTemplate,
		const PathAwareCandidate& path) {
	if (hw == nullptr || qp == nullptr ||
		qp->m_actualPathWindowInitialized || qp->m_win == 0 ||
		!path.valid || pathTemplate.empty() ||
		path.bottleneckBps == 0 ||
		path.bottleneckBps == std::numeric_limits<uint64_t>::max()) {
		return;
	}

	uint64_t serializationNs = 0;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (device == nullptr) {
			return;
		}
		const uint64_t bitRate = device->GetDataRate().GetBitRate();
		if (bitRate == 0) {
			return;
		}
		serializationNs = SaturatingAdd(
			serializationNs, SerializationNs(hw->m_mtu, bitRate));
	}
	const uint64_t actualRttNs = SaturatingAdd(
		SaturatingAdd(path.propagationNs, path.propagationNs),
		serializationNs);
	const long double actualBdp =
		static_cast<long double>(actualRttNs) *
		static_cast<long double>(path.bottleneckBps) /
		8.0L / 1000000000.0L;
	const long double maximumWindow =
		static_cast<long double>(std::numeric_limits<uint32_t>::max());
	const uint32_t actualWindowBytes = actualBdp >= maximumWindow
		? std::numeric_limits<uint32_t>::max()
		: static_cast<uint32_t>(std::ceil(actualBdp));

	if (SwitchNode::DisjointChunkRoutingEnabled()) {
		qp->SetWin(actualWindowBytes);
	} else {
		qp->SetWin(std::max(qp->m_win, actualWindowBytes));
	}
	qp->SetBaseRtt(std::max(qp->m_baseRtt, actualRttNs));
	qp->m_actualPathWindowInitialized = true;
	qp->m_actualPathWindowBytes = actualWindowBytes;
	qp->m_actualPathBaseRttNs = actualRttNs;
	qp->m_actualPathBottleneckBps = path.bottleneckBps;
}

bool SchedulePacketDlbPath(
		const std::vector<Ptr<QbbNetDevice>>& path,
		uint64_t packetBytes,
		uint64_t earliestStartNs,
		bool reserve,
		uint64_t* arrivalNs) {
	if (path.empty() || packetBytes == 0 || arrivalNs == nullptr) {
		return false;
	}
	const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
	uint64_t cursorNs = std::max(nowNs, earliestStartNs);
	for (const Ptr<QbbNetDevice>& device : path) {
		if (device == nullptr || !device->IsLinkUp()) {
			return false;
		}
		const uint64_t bitRate = device->GetDataRate().GetBitRate();
		if (bitRate == 0) {
			return false;
		}
		const uint64_t serviceNs = SerializationNs(packetBytes, bitRate);
		const uint64_t edgeKey = PathEdgeKey(device);
		const auto virtualFinish = PacketDlbVirtualFinishNs().find(edgeKey);
		uint64_t edgeReadyNs =
			virtualFinish == PacketDlbVirtualFinishNs().end()
				? nowNs
				: std::max(nowNs, virtualFinish->second);
		const uint64_t queueBytes = device->GetQueue() == nullptr
			? 0
			: device->GetQueue()->GetNBytesTotal();
		const auto reserved = PathReservedBytes().find(edgeKey);
		const uint64_t reservedBytes =
			reserved == PathReservedBytes().end() ? 0 : reserved->second;
		edgeReadyNs = std::max(
			edgeReadyNs,
			SaturatingAdd(
				nowNs,
				SerializationNs(
					SaturatingAdd(queueBytes, reservedBytes), bitRate)));
		const uint64_t startNs = std::max(cursorNs, edgeReadyNs);
		const uint64_t finishNs = SaturatingAdd(startNs, serviceNs);
		if (reserve) {
			PacketDlbVirtualFinishNs()[edgeKey] = finishNs;
		}
		Ptr<QbbChannel> channel =
			DynamicCast<QbbChannel>(device->GetChannel());
		const uint64_t propagationNs = channel == nullptr
			? 0
			: channel->GetDelay().GetNanoSeconds();
		cursorNs = SaturatingAdd(finishNs, propagationNs);
	}
	*arrivalNs = cursorNs;
	return true;
}

bool ProjectPacketDlbArrivalNs(
		const PacketDlbPathTemplate& pathTemplate,
		uint64_t packetBytes,
		uint64_t earliestStartNs,
		uint64_t* arrivalNs) {
	return SchedulePacketDlbPath(
		pathTemplate, packetBytes, earliestStartNs, false, arrivalNs);
}

uint64_t PacketDlbQpPathTailNs(
		Ptr<RdmaQueuePair> qp,
		const PacketDlbPathTemplate& pathTemplate,
		uint64_t packetBytes) {
	if (qp == nullptr || pathTemplate.empty() || packetBytes == 0) {
		return std::numeric_limits<uint64_t>::max();
	}
	uint64_t propagationNs = 0;
	uint64_t onePacketSerializationNs = 0;
	uint64_t slowestPacketSerializationNs = 0;
	uint64_t assignedEdgeTailNs = 0;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (device == nullptr || !device->IsLinkUp()) {
			return std::numeric_limits<uint64_t>::max();
		}
		const uint64_t bitRate = device->GetDataRate().GetBitRate();
		if (bitRate == 0) {
			return std::numeric_limits<uint64_t>::max();
		}
		Ptr<QbbChannel> channel =
			DynamicCast<QbbChannel>(device->GetChannel());
		if (channel == nullptr) {
			return std::numeric_limits<uint64_t>::max();
		}
		propagationNs = SaturatingAdd(
			propagationNs, channel->GetDelay().GetNanoSeconds());
		const uint64_t packetSerializationNs =
			SerializationNs(packetBytes, bitRate);
		onePacketSerializationNs = SaturatingAdd(
			onePacketSerializationNs, packetSerializationNs);
		slowestPacketSerializationNs = std::max(
			slowestPacketSerializationNs, packetSerializationNs);
		const auto assigned = qp->m_packetDlbAssignedEdgeBytes.find(
			PathEdgeKey(device));
		const uint64_t assignedBytes =
			assigned == qp->m_packetDlbAssignedEdgeBytes.end()
				? 0
				: assigned->second;
		assignedEdgeTailNs = std::max(
			assignedEdgeTailNs,
			SerializationNs(
				SaturatingAdd(assignedBytes, packetBytes), bitRate));
	}
	const uint64_t pipelineFillNs =
		onePacketSerializationNs >= slowestPacketSerializationNs
			? onePacketSerializationNs - slowestPacketSerializationNs
			: 0;
	return SaturatingAdd(
		SaturatingAdd(propagationNs, pipelineFillNs),
		assignedEdgeTailNs);
}

void RecordPacketDlbQpPathBytes(
		Ptr<RdmaQueuePair> qp,
		const PacketDlbPathTemplate& pathTemplate,
		uint64_t packetBytes) {
	if (qp == nullptr) {
		return;
	}
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (device == nullptr) {
			continue;
		}
		uint64_t& assignedBytes =
			qp->m_packetDlbAssignedEdgeBytes[PathEdgeKey(device)];
		assignedBytes = SaturatingAdd(assignedBytes, packetBytes);
	}
}

void ReservePacketDlbPath(
		const PathAwareCandidate& path,
		uint64_t packetBytes,
		uint64_t earliestStartNs) {
	uint64_t ignoredArrivalNs = 0;
	SchedulePacketDlbPath(
		path.hops, packetBytes, earliestStartNs, true, &ignoredArrivalNs);
}

uint32_t PacketDlbHash(uint32_t qpHash, uint64_t seq) {
	uint64_t mixed = seq + 0x9e3779b97f4a7c15ULL;
	mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
	mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
	mixed ^= mixed >> 31;
	return qpHash ^ static_cast<uint32_t>(mixed) ^
		static_cast<uint32_t>(mixed >> 32);
}

uint64_t PacketDlbLaneNextAvailNs(
		Ptr<RdmaQueuePair> qp,
		uint32_t nic,
		uint64_t nowNs) {
	if (qp == nullptr) {
		return nowNs;
	}
	const auto available = qp->m_packetDlbLaneNextAvail.find(nic);
	if (available == qp->m_packetDlbLaneNextAvail.end()) {
		return nowNs;
	}
	const int64_t availableNs = available->second.GetNanoSeconds();
	return availableNs <= 0
		? nowNs
		: std::max(nowNs, static_cast<uint64_t>(availableNs));
}

bool BuildBestPathFromDevice(
		Ptr<QbbNetDevice> device,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t qpHash,
		uint64_t qpBytes,
		uint32_t depth,
		std::unordered_set<uint32_t> visitedNodes,
		PathAwareCandidate* result) {
	if (device == nullptr || !device->IsLinkUp() || result == nullptr ||
		depth >= kMaxPathAwareHops) {
		return false;
	}

	PathAwareCandidate prefix;
	Ptr<QbbNetDevice> peerDevice;
	if (!AppendPathHop(device, &prefix, &peerDevice)) {
		return false;
	}

	Ptr<Node> nextNode = peerDevice->GetNode();
	if (nextNode == nullptr) {
		return false;
	}
	if (nextNode->GetId() == destinationNode) {
		*result = prefix;
		return true;
	}
	if (!visitedNodes.insert(nextNode->GetId()).second) {
		return false;
	}

	const std::vector<int>* nextHops = GetRouteNextHops(nextNode, dip);
	if (nextHops == nullptr || nextHops->empty()) {
		return false;
	}

	PathAwareCandidate best;
	long double bestScore = std::numeric_limits<long double>::infinity();
	const uint32_t start = (qpHash ^ nextNode->GetId()) % nextHops->size();
	for (uint32_t offset = 0; offset < nextHops->size(); ++offset) {
		const int nextHop = (*nextHops)[(start + offset) % nextHops->size()];
		if (nextHop < 0 ||
			static_cast<uint32_t>(nextHop) >= nextNode->GetNDevices()) {
			continue;
		}
		Ptr<QbbNetDevice> nextDevice =
			DynamicCast<QbbNetDevice>(nextNode->GetDevice(nextHop));
		PathAwareCandidate suffix;
		if (!BuildBestPathFromDevice(
				nextDevice,
				destinationNode,
				dip,
				qpHash,
				qpBytes,
				depth + 1,
				visitedNodes,
				&suffix)) {
			continue;
		}

		PathAwareCandidate combined = prefix;
		combined.hops.insert(
			combined.hops.end(), suffix.hops.begin(), suffix.hops.end());
		combined.propagationNs += suffix.propagationNs;
		combined.queueDelayNs += suffix.queueDelayNs;
		combined.maxEdgeWorkNs =
			std::max(combined.maxEdgeWorkNs, suffix.maxEdgeWorkNs);
		combined.queueBytes =
			SaturatingAdd(combined.queueBytes, suffix.queueBytes);
		combined.reservedBytes =
			SaturatingAdd(combined.reservedBytes, suffix.reservedBytes);
		combined.bottleneckBps =
			std::min(combined.bottleneckBps, suffix.bottleneckBps);
		const long double score = PathScoreNs(
			combined,
			qpBytes,
			SwitchNode::AdaptiveZcubeRoutingEnabled());
		if (score < bestScore) {
			best = std::move(combined);
			bestScore = score;
		}
	}

	if (!best.valid) {
		return false;
	}
	*result = std::move(best);
	return true;
}

std::vector<Ptr<QbbNetDevice>> GetGpuFabricDevices(Ptr<Node> gpu) {
	std::vector<Ptr<QbbNetDevice>> devices;
	if (gpu == nullptr || gpu->GetNodeType() != 0) {
		return devices;
	}
	for (uint32_t index = 0; index < gpu->GetNDevices(); ++index) {
		Ptr<QbbNetDevice> device =
			DynamicCast<QbbNetDevice>(gpu->GetDevice(index));
		Ptr<QbbNetDevice> peerDevice = GetPeerDevice(device, nullptr);
		Ptr<Node> peerNode =
			peerDevice == nullptr ? nullptr : peerDevice->GetNode();
		if (peerNode != nullptr && peerNode->GetNodeType() == 1) {
			devices.push_back(device);
		}
	}
	return devices;
}

Ptr<QbbNetDevice> FindDeviceToNode(Ptr<Node> node, uint32_t peerNodeId) {
	if (node == nullptr) {
		return nullptr;
	}
	for (uint32_t index = 0; index < node->GetNDevices(); ++index) {
		Ptr<QbbNetDevice> device =
			DynamicCast<QbbNetDevice>(node->GetDevice(index));
		Ptr<QbbNetDevice> peerDevice = GetPeerDevice(device, nullptr);
		Ptr<Node> peerNode =
			peerDevice == nullptr ? nullptr : peerDevice->GetNode();
		if (peerNode != nullptr && peerNode->GetId() == peerNodeId) {
			return device;
		}
	}
	return nullptr;
}

bool BuildCrossPairedDualTablePath(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNodeId,
		uint64_t qpBytes,
		PathAwareCandidate* result) {
	if (sourceDevice == nullptr || result == nullptr) {
		return false;
	}
	Ptr<Node> sourceGpu = sourceDevice->GetNode();
	Ptr<Node> destinationGpu = NodeList::GetNode(destinationNodeId);
	const std::vector<Ptr<QbbNetDevice>> sourceFabricDevices =
		GetGpuFabricDevices(sourceGpu);
	const std::vector<Ptr<QbbNetDevice>> destinationFabricDevices =
		GetGpuFabricDevices(destinationGpu);
	if (sourceFabricDevices.size() != 2 ||
		destinationFabricDevices.size() != 2) {
		return false;
	}

	Ptr<QbbNetDevice> sourceSwitchIngress =
		GetPeerDevice(sourceDevice, nullptr);
	Ptr<Node> sourceSwitch = sourceSwitchIngress == nullptr
		? nullptr
		: sourceSwitchIngress->GetNode();
	if (sourceSwitch == nullptr || sourceSwitch->GetNodeType() != 1) {
		return false;
	}

	PathAwareCandidate best;
	long double bestScore = std::numeric_limits<long double>::infinity();
	uint32_t crossPairCount = 0;
	for (const Ptr<QbbNetDevice>& destinationDevice :
		 destinationFabricDevices) {
		Ptr<QbbNetDevice> destinationSwitchEgress =
			GetPeerDevice(destinationDevice, nullptr);
		Ptr<Node> destinationSwitch = destinationSwitchEgress == nullptr
			? nullptr
			: destinationSwitchEgress->GetNode();
		if (destinationSwitch == nullptr ||
			destinationSwitch->GetNodeType() != 1 ||
			destinationSwitch->GetId() == sourceSwitch->GetId()) {
			continue;
		}

		Ptr<QbbNetDevice> interSwitchDevice = FindDeviceToNode(
			sourceSwitch, destinationSwitch->GetId());
		if (interSwitchDevice == nullptr) {
			continue;
		}

		PathAwareCandidate path;
		if (!AppendPathHop(sourceDevice, &path) ||
			!AppendPathHop(interSwitchDevice, &path) ||
			!AppendPathHop(destinationSwitchEgress, &path)) {
			continue;
		}
		++crossPairCount;
		const long double score = PathScoreNs(path, qpBytes);
		if (score < bestScore) {
			best = std::move(path);
			bestScore = score;
		}
	}

	// A Zcube endpoint has exactly one opposite-partition destination switch
	// for each source switch. Refuse ambiguous topologies and use SPF instead.
	if (crossPairCount != 1 || !best.valid) {
		return false;
	}
	*result = std::move(best);
	return true;
}

struct P2rEndpointDevices {
	Ptr<QbbNetDevice> group1;
	std::vector<Ptr<QbbNetDevice>> group2;

	bool valid() const {
		return group1 != nullptr && group2.size() == 2;
	}
};

uint32_t PeerNodeId(Ptr<QbbNetDevice> device) {
	Ptr<QbbNetDevice> peer = GetPeerDevice(device, nullptr);
	return peer == nullptr || peer->GetNode() == nullptr
		? std::numeric_limits<uint32_t>::max()
		: peer->GetNode()->GetId();
}

P2rEndpointDevices ClassifyP2rEndpointDevices(
		const std::vector<Ptr<QbbNetDevice>>& devices) {
	P2rEndpointDevices classified;
	if (devices.size() != 3) {
		return classified;
	}

	uint64_t highRate = 0;
	uint64_t lowRate = std::numeric_limits<uint64_t>::max();
	for (const Ptr<QbbNetDevice>& device : devices) {
		if (device == nullptr) {
			return P2rEndpointDevices{};
		}
		const uint64_t rate = device->GetDataRate().GetBitRate();
		highRate = std::max(highRate, rate);
		lowRate = std::min(lowRate, rate);
	}
	if (lowRate == 0 || highRate != lowRate * 2) {
		return P2rEndpointDevices{};
	}

	for (const Ptr<QbbNetDevice>& device : devices) {
		const uint64_t rate = device->GetDataRate().GetBitRate();
		if (rate == highRate) {
			if (classified.group1 != nullptr) {
				return P2rEndpointDevices{};
			}
			classified.group1 = device;
		} else if (rate == lowRate) {
			classified.group2.push_back(device);
		} else {
			return P2rEndpointDevices{};
		}
	}
	std::sort(
		classified.group2.begin(),
		classified.group2.end(),
		[](const Ptr<QbbNetDevice>& lhs,
		   const Ptr<QbbNetDevice>& rhs) {
			return PeerNodeId(lhs) < PeerNodeId(rhs);
		});
	return classified.valid() ? classified : P2rEndpointDevices{};
}

bool FindP2rPreferredSourceCandidate(
		const RdmaHw& hw,
		const std::vector<int>& candidates,
		uint32_t workerOrdinal,
		uint32_t* preferredCandidate) {
	if (preferredCandidate == nullptr || candidates.size() != 3) {
		return false;
	}
	std::vector<Ptr<QbbNetDevice>> devices;
	for (const int candidate : candidates) {
		if (candidate < 0 ||
			static_cast<uint32_t>(candidate) >= hw.m_nic.size() ||
			hw.m_nic[candidate].dev == nullptr) {
			return false;
		}
		devices.push_back(hw.m_nic[candidate].dev);
	}
	const P2rEndpointDevices classified =
		ClassifyP2rEndpointDevices(devices);
	if (!classified.valid()) {
		return false;
	}

	Ptr<QbbNetDevice> preferredDevice;
	if (workerOrdinal % 2 == 0) {
		preferredDevice = classified.group1;
	} else {
		const size_t replica =
			((workerOrdinal - 1) / 2) % classified.group2.size();
		preferredDevice = classified.group2[replica];
	}
	for (const int candidate : candidates) {
		if (hw.m_nic[candidate].dev == preferredDevice) {
			*preferredCandidate = static_cast<uint32_t>(candidate);
			return true;
		}
	}
	return false;
}

bool DevicesShareSwitch(
		Ptr<QbbNetDevice> lhs,
		Ptr<QbbNetDevice> rhs) {
	const uint32_t lhsPeer = PeerNodeId(lhs);
	return lhsPeer != std::numeric_limits<uint32_t>::max() &&
		lhsPeer == PeerNodeId(rhs);
}

struct PacketDlbEndpointPairingFilter {
	bool applicable = false;
	std::vector<int32_t> destinationNics;

	bool AllowsDestination(int32_t destinationNic) const {
		return std::find(
			destinationNics.begin(),
			destinationNics.end(),
			destinationNic) != destinationNics.end();
	}
};

uint32_t PacketDlbEndpointMinimumHops(
		Ptr<QbbNetDevice> sourceDevice,
		Ptr<QbbNetDevice> destinationDevice) {
	const uint32_t unreachable = std::numeric_limits<uint32_t>::max();
	if (sourceDevice == nullptr || destinationDevice == nullptr ||
		!sourceDevice->IsLinkUp() || !destinationDevice->IsLinkUp()) {
		return unreachable;
	}
	Ptr<QbbNetDevice> sourceSwitchIngress =
		GetPeerDevice(sourceDevice, nullptr);
	Ptr<QbbNetDevice> destinationSwitchEgress =
		GetPeerDevice(destinationDevice, nullptr);
	Ptr<Node> sourceSwitch = sourceSwitchIngress == nullptr
		? nullptr
		: sourceSwitchIngress->GetNode();
	Ptr<Node> destinationSwitch = destinationSwitchEgress == nullptr
		? nullptr
		: destinationSwitchEgress->GetNode();
	if (sourceSwitch == nullptr || destinationSwitch == nullptr ||
		sourceSwitch->GetNodeType() != 1 ||
		destinationSwitch->GetNodeType() != 1) {
		return unreachable;
	}
	if (sourceSwitch->GetId() == destinationSwitch->GetId()) {
		return 2;
	}

	std::deque<std::pair<Ptr<Node>, uint32_t>> frontier;
	std::unordered_set<uint32_t> visited{sourceSwitch->GetId()};
	frontier.emplace_back(sourceSwitch, 0);
	while (!frontier.empty()) {
		Ptr<Node> current = frontier.front().first;
		const uint32_t switchHops = frontier.front().second;
		frontier.pop_front();
		for (uint32_t deviceIndex = 0;
			 deviceIndex < current->GetNDevices();
			 ++deviceIndex) {
			Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(
				current->GetDevice(deviceIndex));
			if (device == nullptr || !device->IsLinkUp()) {
				continue;
			}
			Ptr<QbbNetDevice> peer = GetPeerDevice(device, nullptr);
			Ptr<Node> next = peer == nullptr ? nullptr : peer->GetNode();
			if (next == nullptr || next->GetNodeType() != 1) {
				continue;
			}
			const uint32_t nextSwitchHops = switchHops + 1;
			if (next->GetId() == destinationSwitch->GetId()) {
				return nextSwitchHops + 2;
			}
			if (visited.insert(next->GetId()).second) {
				frontier.emplace_back(next, nextSwitchHops);
			}
		}
	}
	return unreachable;
}

struct PacketDlbEndpointFlowEdge {
	size_t to = 0;
	size_t reverse = 0;
	uint64_t capacity = 0;
	int64_t cost = 0;
};

struct PacketDlbEndpointFlowHandle {
	size_t sourceIndex = 0;
	size_t destinationIndex = 0;
	size_t node = 0;
	size_t edge = 0;
	uint64_t initialCapacity = 0;
};

void AddPacketDlbEndpointFlowEdge(
		size_t from,
		size_t to,
		uint64_t capacity,
		int64_t cost,
		std::vector<std::vector<PacketDlbEndpointFlowEdge>>* graph) {
	if (graph == nullptr || from >= graph->size() || to >= graph->size()) {
		return;
	}
	const size_t forwardIndex = (*graph)[from].size();
	const size_t reverseIndex = (*graph)[to].size();
	(*graph)[from].push_back(PacketDlbEndpointFlowEdge{
		to, reverseIndex, capacity, cost});
	(*graph)[to].push_back(PacketDlbEndpointFlowEdge{
		from, forwardIndex, 0, -cost});
}

std::vector<PacketDlbEndpointPairingFilter>
BuildPacketDlbEndpointPairingFilters(
		const std::vector<Ptr<QbbNetDevice>>& sourceDevices,
		const std::vector<Ptr<QbbNetDevice>>& destinationDevices) {
	std::vector<PacketDlbEndpointPairingFilter> filters(
		sourceDevices.size());
	if (sourceDevices.size() < 2 || destinationDevices.size() < 2) {
		return filters;
	}

	uint64_t totalSourceRate = 0;
	uint64_t totalDestinationRate = 0;
	for (const Ptr<QbbNetDevice>& source : sourceDevices) {
		if (source == nullptr || source->GetDataRate().GetBitRate() == 0) {
			return filters;
		}
		totalSourceRate = SaturatingAdd(
			totalSourceRate, source->GetDataRate().GetBitRate());
	}
	for (const Ptr<QbbNetDevice>& destination : destinationDevices) {
		if (destination == nullptr ||
			destination->GetDataRate().GetBitRate() == 0) {
			return filters;
		}
		totalDestinationRate = SaturatingAdd(
			totalDestinationRate,
			destination->GetDataRate().GetBitRate());
	}
	if (totalSourceRate == 0 ||
		totalSourceRate != totalDestinationRate) {
		return filters;
	}

	const uint32_t unreachable = std::numeric_limits<uint32_t>::max();
	std::vector<std::vector<uint32_t>> minimumHops(
		sourceDevices.size(),
		std::vector<uint32_t>(destinationDevices.size(), unreachable));
	bool completeUniformMatrix = true;
	uint32_t firstHops = unreachable;
	for (size_t source = 0; source < sourceDevices.size(); ++source) {
		for (size_t destination = 0;
			 destination < destinationDevices.size();
			 ++destination) {
			const uint32_t hops = PacketDlbEndpointMinimumHops(
				sourceDevices[source], destinationDevices[destination]);
			minimumHops[source][destination] = hops;
			if (hops == unreachable) {
				completeUniformMatrix = false;
				continue;
			}
			if (firstHops == unreachable) {
				firstHops = hops;
			} else if (hops != firstHops) {
				completeUniformMatrix = false;
			}
		}
	}
	// When every endpoint pairing is structurally identical, retaining all
	// combinations gives the live ETA selector the most freedom.
	if (completeUniformMatrix) {
		return filters;
	}

	const size_t sourceBase = 1;
	const size_t destinationBase = sourceBase + sourceDevices.size();
	const size_t sink = destinationBase + destinationDevices.size();
	std::vector<std::vector<PacketDlbEndpointFlowEdge>> graph(sink + 1);
	for (size_t source = 0; source < sourceDevices.size(); ++source) {
		AddPacketDlbEndpointFlowEdge(
			0,
			sourceBase + source,
			sourceDevices[source]->GetDataRate().GetBitRate(),
			0,
			&graph);
	}
	for (size_t destination = 0;
		 destination < destinationDevices.size();
		 ++destination) {
		AddPacketDlbEndpointFlowEdge(
			destinationBase + destination,
			sink,
			destinationDevices[destination]->GetDataRate().GetBitRate(),
			0,
			&graph);
	}

	std::vector<PacketDlbEndpointFlowHandle> handles;
	constexpr int64_t kHopCostScale = 1024;
	for (size_t source = 0; source < sourceDevices.size(); ++source) {
		for (size_t destination = 0;
			 destination < destinationDevices.size();
			 ++destination) {
			const uint32_t hops = minimumHops[source][destination];
			if (hops == unreachable) {
				continue;
			}
			const uint64_t capacity = std::min(
				sourceDevices[source]->GetDataRate().GetBitRate(),
				destinationDevices[destination]->GetDataRate().GetBitRate());
			const int64_t directSwitchBonus = hops == 2 ? 1 : 0;
			const int64_t cost =
				static_cast<int64_t>(hops) * kHopCostScale -
				directSwitchBonus;
			const size_t node = sourceBase + source;
			const size_t edge = graph[node].size();
			AddPacketDlbEndpointFlowEdge(
				node,
				destinationBase + destination,
				capacity,
				cost,
				&graph);
			handles.push_back(PacketDlbEndpointFlowHandle{
				source, destination, node, edge, capacity});
		}
	}

	uint64_t transportedRate = 0;
	const int64_t infinity = std::numeric_limits<int64_t>::max() / 4;
	while (transportedRate < totalSourceRate) {
		std::vector<int64_t> distance(graph.size(), infinity);
		std::vector<int64_t> parentNode(graph.size(), -1);
		std::vector<int64_t> parentEdge(graph.size(), -1);
		distance[0] = 0;
		for (size_t iteration = 1;
			 iteration < graph.size();
			 ++iteration) {
			bool changed = false;
			for (size_t node = 0; node < graph.size(); ++node) {
				if (distance[node] == infinity) {
					continue;
				}
				for (size_t edge = 0;
					 edge < graph[node].size();
					 ++edge) {
					const PacketDlbEndpointFlowEdge& candidate =
						graph[node][edge];
					if (candidate.capacity == 0) {
						continue;
					}
					const int64_t nextDistance =
						distance[node] + candidate.cost;
					if (nextDistance >= distance[candidate.to]) {
						continue;
					}
					distance[candidate.to] = nextDistance;
					parentNode[candidate.to] =
						static_cast<int64_t>(node);
					parentEdge[candidate.to] =
						static_cast<int64_t>(edge);
					changed = true;
				}
			}
			if (!changed) {
				break;
			}
		}
		if (parentNode[sink] < 0) {
			return std::vector<PacketDlbEndpointPairingFilter>(
				sourceDevices.size());
		}

		uint64_t augment = totalSourceRate - transportedRate;
		for (size_t node = sink; node != 0;) {
			const size_t previous =
				static_cast<size_t>(parentNode[node]);
			const size_t edge =
				static_cast<size_t>(parentEdge[node]);
			augment = std::min(
				augment, graph[previous][edge].capacity);
			node = previous;
		}
		if (augment == 0) {
			return std::vector<PacketDlbEndpointPairingFilter>(
				sourceDevices.size());
		}
		for (size_t node = sink; node != 0;) {
			const size_t previous =
				static_cast<size_t>(parentNode[node]);
			const size_t edgeIndex =
				static_cast<size_t>(parentEdge[node]);
			PacketDlbEndpointFlowEdge& edge =
				graph[previous][edgeIndex];
			const size_t reverse = edge.reverse;
			edge.capacity -= augment;
			graph[node][reverse].capacity =
				SaturatingAdd(
					graph[node][reverse].capacity, augment);
			node = previous;
		}
		transportedRate = SaturatingAdd(transportedRate, augment);
	}

	for (const PacketDlbEndpointFlowHandle& handle : handles) {
		const PacketDlbEndpointFlowEdge& edge =
			graph[handle.node][handle.edge];
		if (edge.capacity == handle.initialCapacity) {
			continue;
		}
		const int32_t destinationNic = static_cast<int32_t>(
			destinationDevices[handle.destinationIndex]->GetIfIndex());
		std::vector<int32_t>& allowed =
			filters[handle.sourceIndex].destinationNics;
		if (std::find(
				allowed.begin(), allowed.end(), destinationNic) ==
				allowed.end()) {
			allowed.push_back(destinationNic);
		}
	}
	for (PacketDlbEndpointPairingFilter& filter : filters) {
		if (filter.destinationNics.empty()) {
			return std::vector<PacketDlbEndpointPairingFilter>(
				sourceDevices.size());
		}
		std::sort(
			filter.destinationNics.begin(),
			filter.destinationNics.end());
		filter.applicable = true;
	}
	return filters;
}

std::unordered_map<
	PacketDlbPathCacheKey,
	PacketDlbEndpointPairingFilter,
	PacketDlbPathCacheKeyHash>& PacketDlbEndpointPairingCache() {
	static std::unordered_map<
		PacketDlbPathCacheKey,
		PacketDlbEndpointPairingFilter,
		PacketDlbPathCacheKeyHash> cache;
	return cache;
}

std::mutex& PacketDlbEndpointPairingCacheMutex() {
	static std::mutex mutex;
	return mutex;
}

PacketDlbEndpointPairingFilter GetPacketDlbEndpointPairingFilter(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode) {
	PacketDlbEndpointPairingFilter none;
	if (sourceDevice == nullptr || sourceDevice->GetNode() == nullptr) {
		return none;
	}
	const PacketDlbPathCacheKey key{
		sourceDevice->GetNode()->GetId(),
		sourceDevice->GetIfIndex(),
		destinationNode,
		0,
	};
	std::lock_guard<std::mutex> cacheGuard(
		PacketDlbEndpointPairingCacheMutex());
	auto found = PacketDlbEndpointPairingCache().find(key);
	if (found != PacketDlbEndpointPairingCache().end()) {
		return found->second;
	}

	const std::vector<Ptr<QbbNetDevice>> sourceDevices =
		GetGpuFabricDevices(sourceDevice->GetNode());
	Ptr<Node> destinationGpu = NodeList::GetNode(destinationNode);
	const std::vector<Ptr<QbbNetDevice>> destinationDevices =
		destinationGpu == nullptr
			? std::vector<Ptr<QbbNetDevice>>{}
			: GetGpuFabricDevices(destinationGpu);
	const std::vector<PacketDlbEndpointPairingFilter> filters =
		BuildPacketDlbEndpointPairingFilters(
			sourceDevices, destinationDevices);
	for (size_t sourceIndex = 0;
		 sourceIndex < sourceDevices.size() &&
		 sourceIndex < filters.size();
		 ++sourceIndex) {
		const PacketDlbPathCacheKey sourceKey{
			sourceDevices[sourceIndex]->GetNode()->GetId(),
			sourceDevices[sourceIndex]->GetIfIndex(),
			destinationNode,
			0,
		};
		PacketDlbEndpointPairingCache()[sourceKey] =
			filters[sourceIndex];
	}
	found = PacketDlbEndpointPairingCache().find(key);
	if (found != PacketDlbEndpointPairingCache().end()) {
		return found->second;
	}
	PacketDlbEndpointPairingCache().emplace(key, none);
	return none;
}

struct SwitchPacketDlbEstimateKey {
	uint32_t sourceNode = 0;
	uint32_t sourceNic = 0;
	uint32_t destinationNode = 0;
	uint32_t dip = 0;
	uint32_t packetBytes = 0;

	bool operator==(const SwitchPacketDlbEstimateKey& other) const {
		return sourceNode == other.sourceNode &&
			sourceNic == other.sourceNic &&
			destinationNode == other.destinationNode &&
			dip == other.dip &&
			packetBytes == other.packetBytes;
	}
};

struct SwitchPacketDlbEstimateKeyHash {
	std::size_t operator()(
			const SwitchPacketDlbEstimateKey& key) const {
		std::size_t hash = key.sourceNode;
		auto combine = [&](uint32_t value) {
			hash ^= static_cast<std::size_t>(value) + 0x9e3779b9U +
				(hash << 6) + (hash >> 2);
		};
		combine(key.sourceNic);
		combine(key.destinationNode);
		combine(key.dip);
		combine(key.packetBytes);
		return hash;
	}
};

struct SwitchPacketDlbPathEstimate {
	bool valid = false;
	bool endpointPinned = false;
	int32_t destinationNic = -1;
	uint32_t hops = 0;
	uint64_t baseLatencyNs = 0;
	uint64_t propagationNs = 0;
};

std::unordered_map<
	SwitchPacketDlbEstimateKey,
	std::vector<SwitchPacketDlbPathEstimate>,
	SwitchPacketDlbEstimateKeyHash>&
SwitchPacketDlbEstimateCache() {
	static std::unordered_map<
		SwitchPacketDlbEstimateKey,
		std::vector<SwitchPacketDlbPathEstimate>,
		SwitchPacketDlbEstimateKeyHash> cache;
	return cache;
}

std::mutex& SwitchPacketDlbEstimateCacheMutex() {
	static std::mutex mutex;
	return mutex;
}

std::vector<SwitchPacketDlbPathEstimate>
BuildSwitchPacketDlbPathEstimates(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t packetBytes) {
	std::vector<SwitchPacketDlbPathEstimate> estimates;
	if (sourceDevice == nullptr || sourceDevice->GetNode() == nullptr ||
		packetBytes == 0) {
		return estimates;
	}
	const PacketDlbEndpointPairingFilter endpointPairing =
		GetPacketDlbEndpointPairingFilter(
			sourceDevice, destinationNode);
	const PacketDlbPathTemplates& paths = GetPacketDlbPathTemplates(
		sourceDevice, destinationNode, dip);
	const bool balancedThreeHop =
		PacketDlbBalancedThreeHopEnabled();
	const bool constrainHopMode =
		balancedThreeHop || PacketDlbTwoFourHopEnabled();
	std::map<int32_t, SwitchPacketDlbPathEstimate> bestByDestination;
	std::map<int32_t, SwitchPacketDlbPathEstimate> fallbackByDestination;
	auto retainBest = [](
			std::map<int32_t, SwitchPacketDlbPathEstimate>* candidates,
			int32_t destinationNic,
			const SwitchPacketDlbPathEstimate& estimate) {
		auto found = candidates->find(destinationNic);
		if (found == candidates->end() ||
			estimate.baseLatencyNs < found->second.baseLatencyNs ||
			(estimate.baseLatencyNs == found->second.baseLatencyNs &&
			 estimate.hops < found->second.hops)) {
			(*candidates)[destinationNic] = estimate;
		}
	};
	for (const PacketDlbPathTemplate& path : paths) {
		const int32_t destinationNic =
			PacketDlbPathDestinationNic(path);
		if (destinationNic < 0 ||
			(!balancedThreeHop && endpointPairing.applicable &&
			 !endpointPairing.AllowsDestination(destinationNic))) {
			continue;
		}

		uint64_t propagationNs = 0;
		uint64_t serializationNs = 0;
		bool valid = !path.empty();
		for (const Ptr<QbbNetDevice>& device : path) {
			if (device == nullptr || !device->IsLinkUp()) {
				valid = false;
				break;
			}
			const uint64_t bitRate =
				device->GetDataRate().GetBitRate();
			Ptr<QbbChannel> channel =
				DynamicCast<QbbChannel>(device->GetChannel());
			if (bitRate == 0 || channel == nullptr) {
				valid = false;
				break;
			}
			propagationNs = SaturatingAdd(
				propagationNs,
				static_cast<uint64_t>(
					channel->GetDelay().GetNanoSeconds()));
			serializationNs = SaturatingAdd(
				serializationNs,
				SerializationNs(packetBytes, bitRate));
		}
		if (!valid) {
			continue;
		}

		SwitchPacketDlbPathEstimate estimate;
		estimate.valid = true;
		estimate.endpointPinned =
			balancedThreeHop || endpointPairing.applicable;
		estimate.destinationNic = destinationNic;
		estimate.hops = static_cast<uint32_t>(path.size());
		estimate.baseLatencyNs = SaturatingAdd(
			propagationNs, serializationNs);
		estimate.propagationNs = propagationNs;
		retainBest(&fallbackByDestination, destinationNic, estimate);
		if (!constrainHopMode ||
			PacketDlbPathMatchesConfiguredHopMode(path)) {
			retainBest(&bestByDestination, destinationNic, estimate);
		}
	}
	if (bestByDestination.empty()) {
		bestByDestination = std::move(fallbackByDestination);
	}
	for (const auto& item : bestByDestination) {
		estimates.push_back(item.second);
	}
	return estimates;
}

const std::vector<SwitchPacketDlbPathEstimate>&
GetSwitchPacketDlbPathEstimates(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t packetBytes) {
	static const std::vector<SwitchPacketDlbPathEstimate> empty;
	if (sourceDevice == nullptr || sourceDevice->GetNode() == nullptr) {
		return empty;
	}
	const SwitchPacketDlbEstimateKey key{
		sourceDevice->GetNode()->GetId(),
		sourceDevice->GetIfIndex(),
		destinationNode,
		dip,
		packetBytes,
	};
	{
		std::lock_guard<std::mutex> guard(
			SwitchPacketDlbEstimateCacheMutex());
		auto found = SwitchPacketDlbEstimateCache().find(key);
		if (found != SwitchPacketDlbEstimateCache().end()) {
			return found->second;
		}
	}
	std::vector<SwitchPacketDlbPathEstimate> estimates =
		BuildSwitchPacketDlbPathEstimates(
			sourceDevice,
			destinationNode,
			dip,
			packetBytes);
	std::lock_guard<std::mutex> guard(
		SwitchPacketDlbEstimateCacheMutex());
	return SwitchPacketDlbEstimateCache()
		.emplace(key, std::move(estimates))
		.first->second;
}

SwitchPacketDlbPathEstimate SelectSwitchPacketDlbPathEstimate(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t packetBytes,
		uint32_t packetHash) {
	const std::vector<SwitchPacketDlbPathEstimate>& estimates =
		GetSwitchPacketDlbPathEstimates(
			sourceDevice,
			destinationNode,
			dip,
			packetBytes);
	uint64_t bestLatencyNs = std::numeric_limits<uint64_t>::max();
	std::vector<const SwitchPacketDlbPathEstimate*> best;
	for (const SwitchPacketDlbPathEstimate& estimate : estimates) {
		if (!estimate.valid) {
			continue;
		}
		if (estimate.baseLatencyNs < bestLatencyNs) {
			bestLatencyNs = estimate.baseLatencyNs;
			best.clear();
		}
		if (estimate.baseLatencyNs == bestLatencyNs) {
			best.push_back(&estimate);
		}
	}
	if (best.empty()) {
		return SwitchPacketDlbPathEstimate{};
	}
	return *best[packetHash % best.size()];
}

bool PacketDlbEndpointPairedPathAllowed(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		const PacketDlbPathTemplate& pathTemplate,
		bool* filterApplied) {
	const PacketDlbEndpointPairingFilter filter =
		GetPacketDlbEndpointPairingFilter(
			sourceDevice, destinationNode);
	if (filterApplied != nullptr) {
		*filterApplied = filter.applicable;
	}
	if (!filter.applicable) {
		return true;
	}
	return filter.AllowsDestination(
		PacketDlbPathDestinationNic(pathTemplate));
}

bool BuildP2rEndpointPath(
		Ptr<QbbNetDevice> sourceDevice,
		Ptr<QbbNetDevice> destinationDevice,
		uint32_t qpHash,
		uint64_t qpBytes,
		PathAwareCandidate* result) {
	if (sourceDevice == nullptr || destinationDevice == nullptr ||
		result == nullptr) {
		return false;
	}
	Ptr<QbbNetDevice> sourceSwitchIngress =
		GetPeerDevice(sourceDevice, nullptr);
	Ptr<QbbNetDevice> destinationSwitchEgress =
		GetPeerDevice(destinationDevice, nullptr);
	Ptr<Node> sourceSwitch = sourceSwitchIngress == nullptr
		? nullptr
		: sourceSwitchIngress->GetNode();
	Ptr<Node> destinationSwitch = destinationSwitchEgress == nullptr
		? nullptr
		: destinationSwitchEgress->GetNode();
	if (sourceSwitch == nullptr || destinationSwitch == nullptr ||
		sourceSwitch->GetNodeType() != 1 ||
		destinationSwitch->GetNodeType() != 1) {
		return false;
	}

	PathAwareCandidate best;
	long double bestScore = std::numeric_limits<long double>::infinity();
	auto consider = [&](PathAwareCandidate path) {
		if (!path.valid) {
			return;
		}
		const long double score = PathScoreNs(path, qpBytes, true);
		if (score < bestScore) {
			best = std::move(path);
			bestScore = score;
		}
	};

	if (sourceSwitch->GetId() == destinationSwitch->GetId()) {
		PathAwareCandidate direct;
		if (AppendPathHop(sourceDevice, &direct) &&
			AppendPathHop(destinationSwitchEgress, &direct)) {
			consider(std::move(direct));
		}
	} else {
		Ptr<QbbNetDevice> interSwitchDevice = FindDeviceToNode(
			sourceSwitch, destinationSwitch->GetId());
		if (interSwitchDevice != nullptr) {
			PathAwareCandidate cross;
			if (AppendPathHop(sourceDevice, &cross) &&
				AppendPathHop(interSwitchDevice, &cross) &&
				AppendPathHop(destinationSwitchEgress, &cross)) {
				consider(std::move(cross));
			}
		} else {
			const uint32_t deviceCount = sourceSwitch->GetNDevices();
			const uint32_t deviceStart = deviceCount == 0
				? 0
				: (qpHash ^ sourceSwitch->GetId() ^
				   destinationSwitch->GetId()) %
					deviceCount;
			for (uint32_t offset = 0; offset < deviceCount; ++offset) {
				const uint32_t deviceIndex =
					(deviceStart + offset) % deviceCount;
				Ptr<QbbNetDevice> firstInterSwitchDevice =
					DynamicCast<QbbNetDevice>(
						sourceSwitch->GetDevice(deviceIndex));
				Ptr<QbbNetDevice> middleIngress =
					GetPeerDevice(firstInterSwitchDevice, nullptr);
				Ptr<Node> middleSwitch = middleIngress == nullptr
					? nullptr
					: middleIngress->GetNode();
				if (middleSwitch == nullptr ||
					middleSwitch->GetNodeType() != 1 ||
					middleSwitch->GetId() ==
						destinationSwitch->GetId()) {
					continue;
				}
				Ptr<QbbNetDevice> secondInterSwitchDevice =
					FindDeviceToNode(
						middleSwitch, destinationSwitch->GetId());
				if (secondInterSwitchDevice == nullptr) {
					continue;
				}
				PathAwareCandidate relay;
				if (AppendPathHop(sourceDevice, &relay) &&
					AppendPathHop(firstInterSwitchDevice, &relay) &&
					AppendPathHop(secondInterSwitchDevice, &relay) &&
					AppendPathHop(destinationSwitchEgress, &relay)) {
					consider(std::move(relay));
				}
			}
		}
	}

	if (!best.valid) {
		return false;
	}
	*result = std::move(best);
	return true;
}

bool BuildP2rDisjointPath(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNodeId,
		uint32_t qpHash,
		uint64_t qpBytes,
		uint32_t sourceNicOrdinalHint,
		PathAwareCandidate* result) {
	if (sourceDevice == nullptr || result == nullptr) {
		return false;
	}
	const P2rEndpointDevices source = ClassifyP2rEndpointDevices(
		GetGpuFabricDevices(sourceDevice->GetNode()));
	const P2rEndpointDevices destination = ClassifyP2rEndpointDevices(
		GetGpuFabricDevices(NodeList::GetNode(destinationNodeId)));
	if (!source.valid() || !destination.valid()) {
		return false;
	}

	const bool sharedGroup1 =
		DevicesShareSwitch(source.group1, destination.group1);
	bool sharedGroup2 = false;
	for (const Ptr<QbbNetDevice>& sourceGroup2 : source.group2) {
		for (const Ptr<QbbNetDevice>& destinationGroup2 :
			 destination.group2) {
			sharedGroup2 = sharedGroup2 ||
				DevicesShareSwitch(sourceGroup2, destinationGroup2);
		}
	}
	const bool sameRolePairing = sharedGroup1 || sharedGroup2;

	Ptr<QbbNetDevice> destinationDevice;
	if (sourceDevice == source.group1) {
		if (sameRolePairing) {
			destinationDevice = destination.group1;
		} else {
			const uint32_t replica =
				sourceNicOrdinalHint ==
					std::numeric_limits<uint32_t>::max()
				? qpHash % destination.group2.size()
				: (sourceNicOrdinalHint / 2) %
					destination.group2.size();
			destinationDevice = destination.group2[replica];
		}
	} else {
		auto sourceReplica = std::find(
			source.group2.begin(), source.group2.end(), sourceDevice);
		if (sourceReplica == source.group2.end()) {
			return false;
		}
		if (sameRolePairing) {
			const size_t replica = static_cast<size_t>(
				std::distance(source.group2.begin(), sourceReplica));
			destinationDevice = destination.group2[replica];
		} else {
			destinationDevice = destination.group1;
		}
	}

	return BuildP2rEndpointPath(
		sourceDevice,
		destinationDevice,
		qpHash,
		qpBytes,
		result);
}

bool BuildAdaptiveDualTablePath(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNodeId,
		uint32_t qpHash,
		uint64_t qpBytes,
		uint32_t sourceNicOrdinalHint,
		PathAwareCandidate* result) {
	if (sourceDevice == nullptr || result == nullptr) {
		return false;
	}
	Ptr<Node> sourceGpu = sourceDevice->GetNode();
	Ptr<Node> destinationGpu = NodeList::GetNode(destinationNodeId);
	const std::vector<Ptr<QbbNetDevice>> sourceFabricDevices =
		GetGpuFabricDevices(sourceGpu);
	const std::vector<Ptr<QbbNetDevice>> destinationFabricDevices =
		GetGpuFabricDevices(destinationGpu);
	if (SwitchNode::DisjointChunkRoutingEnabled() &&
		sourceFabricDevices.size() == 3 &&
		destinationFabricDevices.size() == 3) {
		return BuildP2rDisjointPath(
			sourceDevice,
			destinationNodeId,
			qpHash,
			qpBytes,
			sourceNicOrdinalHint,
			result);
	}
	if (sourceFabricDevices.size() != 2 ||
		destinationFabricDevices.size() != 2) {
		return false;
	}

	Ptr<QbbNetDevice> sourceSwitchIngress =
		GetPeerDevice(sourceDevice, nullptr);
	Ptr<Node> sourceSwitch = sourceSwitchIngress == nullptr
		? nullptr
		: sourceSwitchIngress->GetNode();
	if (sourceSwitch == nullptr || sourceSwitch->GetNodeType() != 1) {
		return false;
	}

	PathAwareCandidate best;
	PathAwareCandidate bestCanonical;
	PathAwareCandidate bestThreeHop;
	long double bestScore = std::numeric_limits<long double>::infinity();
	long double bestCanonicalScore =
		std::numeric_limits<long double>::infinity();
	long double bestThreeHopScore =
		std::numeric_limits<long double>::infinity();
	bool pairHasTwoHop = false;
	for (const Ptr<QbbNetDevice>& sourceFabricDevice :
		 sourceFabricDevices) {
		Ptr<QbbNetDevice> sourceIngress =
			GetPeerDevice(sourceFabricDevice, nullptr);
		Ptr<Node> candidateSourceSwitch = sourceIngress == nullptr
			? nullptr
			: sourceIngress->GetNode();
		if (candidateSourceSwitch == nullptr) {
			continue;
		}
		for (const Ptr<QbbNetDevice>& destinationFabricDevice :
			 destinationFabricDevices) {
			Ptr<QbbNetDevice> destinationEgress =
				GetPeerDevice(destinationFabricDevice, nullptr);
			Ptr<Node> candidateDestinationSwitch =
				destinationEgress == nullptr
				? nullptr
				: destinationEgress->GetNode();
			if (candidateDestinationSwitch != nullptr &&
				candidateSourceSwitch->GetId() ==
					candidateDestinationSwitch->GetId()) {
				pairHasTwoHop = true;
				break;
			}
		}
		if (pairHasTwoHop) {
			break;
		}
	}
	auto consider = [&](PathAwareCandidate path) {
		if (!path.valid) {
			return;
		}
		const long double score = PathScoreNs(path, qpBytes, true);
		if (score < bestScore) {
			best = path;
			bestScore = score;
		}
		// In Zcube the canonical endpoint pairings are either direct (two
		// host-facing links) or same-partition via a relay (four links). The
		// odd three-link path is a useful failure fallback, but selecting it
		// merely to balance one endpoint can sacrifice both path families.
		if (path.hops.size() % 2 == 0 && score < bestCanonicalScore) {
			bestCanonical = path;
			bestCanonicalScore = score;
		}
		if (path.hops.size() == 3 && score < bestThreeHopScore) {
			bestThreeHop = std::move(path);
			bestThreeHopScore = score;
		}
	};

	const uint32_t destinationStart =
		qpHash % destinationFabricDevices.size();
	for (uint32_t destinationOffset = 0;
		 destinationOffset < destinationFabricDevices.size();
		 ++destinationOffset) {
		const Ptr<QbbNetDevice>& destinationDevice =
			destinationFabricDevices[
				(destinationStart + destinationOffset) %
				destinationFabricDevices.size()];
		Ptr<QbbNetDevice> destinationSwitchEgress =
			GetPeerDevice(destinationDevice, nullptr);
		Ptr<Node> destinationSwitch = destinationSwitchEgress == nullptr
			? nullptr
			: destinationSwitchEgress->GetNode();
		if (destinationSwitch == nullptr ||
			destinationSwitch->GetNodeType() != 1) {
			continue;
		}

		if (destinationSwitch->GetId() == sourceSwitch->GetId()) {
			PathAwareCandidate direct;
			if (AppendPathHop(sourceDevice, &direct) &&
				AppendPathHop(destinationSwitchEgress, &direct)) {
				consider(std::move(direct));
			}
			continue;
		}

		Ptr<QbbNetDevice> interSwitchDevice = FindDeviceToNode(
			sourceSwitch, destinationSwitch->GetId());
		if (interSwitchDevice != nullptr) {
			PathAwareCandidate cross;
			if (AppendPathHop(sourceDevice, &cross) &&
				AppendPathHop(interSwitchDevice, &cross) &&
				AppendPathHop(destinationSwitchEgress, &cross)) {
				consider(std::move(cross));
			}
			continue;
		}

		// Same-partition Zcube switches need one opposite-partition relay.
		// Rotate the scan by QP hash so equal-cost relay choices do not all
		// start from the same physical link.
		const uint32_t deviceCount = sourceSwitch->GetNDevices();
		const uint32_t deviceStart = deviceCount == 0
			? 0
			: (qpHash ^ sourceSwitch->GetId() ^ destinationSwitch->GetId()) %
				deviceCount;
		for (uint32_t offset = 0; offset < deviceCount; ++offset) {
			const uint32_t deviceIndex =
				(deviceStart + offset) % deviceCount;
			Ptr<QbbNetDevice> firstInterSwitchDevice =
				DynamicCast<QbbNetDevice>(sourceSwitch->GetDevice(deviceIndex));
			Ptr<QbbNetDevice> middleIngress =
				GetPeerDevice(firstInterSwitchDevice, nullptr);
			Ptr<Node> middleSwitch = middleIngress == nullptr
				? nullptr
				: middleIngress->GetNode();
			if (middleSwitch == nullptr || middleSwitch->GetNodeType() != 1 ||
				middleSwitch->GetId() == destinationSwitch->GetId()) {
				continue;
			}
			Ptr<QbbNetDevice> secondInterSwitchDevice = FindDeviceToNode(
				middleSwitch, destinationSwitch->GetId());
			if (secondInterSwitchDevice == nullptr) {
				continue;
			}

			PathAwareCandidate longPath;
			if (AppendPathHop(sourceDevice, &longPath) &&
				AppendPathHop(firstInterSwitchDevice, &longPath) &&
				AppendPathHop(secondInterSwitchDevice, &longPath) &&
				AppendPathHop(destinationSwitchEgress, &longPath)) {
				consider(std::move(longPath));
			}
		}
	}

	if (SwitchNode::DisjointChunkRoutingEnabled()) {
		if (pairHasTwoHop && bestCanonical.valid) {
			*result = std::move(bestCanonical);
			return true;
		}
		if (!pairHasTwoHop && bestThreeHop.valid) {
			*result = std::move(bestThreeHop);
			return true;
		}
	}
	if (SwitchNode::DynamicChunkRoutingEnabled() && bestCanonical.valid) {
		*result = std::move(bestCanonical);
		return true;
	}
	if (!best.valid) {
		return false;
	}
	*result = std::move(best);
	return true;
}

bool BuildPolicyPathFromDevice(
		Ptr<QbbNetDevice> device,
		uint32_t destinationNode,
		uint32_t dip,
		uint32_t qpHash,
		uint64_t qpBytes,
		bool crossPairedDualTable,
		PathAwareCandidate* result,
		uint32_t sourceNicOrdinalHint =
			std::numeric_limits<uint32_t>::max()) {
	if (crossPairedDualTable) {
		if (SwitchNode::AdaptiveZcubeRoutingEnabled() &&
			BuildAdaptiveDualTablePath(
				device,
				destinationNode,
				qpHash,
				qpBytes,
				sourceNicOrdinalHint,
				result)) {
			return true;
		}
		if (!SwitchNode::AdaptiveZcubeRoutingEnabled() &&
			BuildCrossPairedDualTablePath(
				device, destinationNode, qpBytes, result)) {
			return true;
		}
	}
	std::unordered_set<uint32_t> visitedNodes{device->GetNode()->GetId()};
	return BuildBestPathFromDevice(
		device,
		destinationNode,
		dip,
		qpHash,
		qpBytes,
		0,
		std::move(visitedNodes),
		result);
}

void UnbindPacketDlbSwitchRoutes(
		Ptr<RdmaQueuePair> qp,
		uint64_t seq,
		const std::vector<uint32_t>& switchIds) {
	if (qp == nullptr) {
		return;
	}
	for (const uint32_t switchId : switchIds) {
		Ptr<Node> node = NodeList::GetNode(switchId);
		if (node == nullptr || node->GetNodeType() != 1) {
			continue;
		}
		Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(node);
		if (sw != nullptr) {
			sw->UnbindPacketDlbRoute(
				qp->sip.Get(),
				qp->dip.Get(),
				qp->sport,
				qp->dport,
				seq);
		}
	}
}

void CancelPreparedPacketDlbRoute(Ptr<RdmaQueuePair> qp) {
	if (qp == nullptr || !qp->m_packetDlbPrepared) {
		return;
	}
	UnbindPacketDlbSwitchRoutes(
		qp,
		qp->m_packetDlbPreparedSeq,
		qp->m_packetDlbBoundSwitches);
	qp->m_packetDlbBoundSwitches.clear();
	qp->m_packetDlbPrepared = false;
	qp->m_packetDlbPreparedNicIdx = -1;
}

void CommitPreparedPacketDlbRoute(Ptr<RdmaQueuePair> qp) {
	if (qp == nullptr || !qp->m_packetDlbPrepared) {
		return;
	}
	auto existing = qp->m_packetDlbOutstandingRoutes.find(
		qp->m_packetDlbPreparedSeq);
	if (existing != qp->m_packetDlbOutstandingRoutes.end()) {
		UnbindPacketDlbSwitchRoutes(
			qp, existing->first, existing->second);
		qp->m_packetDlbOutstandingRoutes.erase(existing);
	}
	qp->m_packetDlbOutstandingRoutes.emplace(
		qp->m_packetDlbPreparedSeq,
		std::move(qp->m_packetDlbBoundSwitches));
	qp->m_packetDlbBoundSwitches.clear();
	qp->m_packetDlbPrepared = false;
	qp->m_packetDlbPreparedNicIdx = -1;
}

void DiscardAcknowledgedPacketDlbRoutes(
		Ptr<RdmaQueuePair> qp,
		uint64_t acknowledgedSeq) {
	if (qp == nullptr) {
		return;
	}
	auto route = qp->m_packetDlbOutstandingRoutes.begin();
	while (route != qp->m_packetDlbOutstandingRoutes.end() &&
		route->first < acknowledgedSeq) {
		route = qp->m_packetDlbOutstandingRoutes.erase(route);
	}
}

void CancelOutstandingPacketDlbRoutes(Ptr<RdmaQueuePair> qp) {
	if (qp == nullptr) {
		return;
	}
	CancelPreparedPacketDlbRoute(qp);
	for (const auto& route : qp->m_packetDlbOutstandingRoutes) {
		UnbindPacketDlbSwitchRoutes(qp, route.first, route.second);
	}
	qp->m_packetDlbOutstandingRoutes.clear();
}

void BindPacketDlbPath(
		Ptr<RdmaQueuePair> qp,
		const PathAwareCandidate& path,
		uint64_t seq) {
	if (qp == nullptr) {
		return;
	}
	qp->m_packetDlbBoundSwitches.clear();
	for (const Ptr<QbbNetDevice>& device : path.hops) {
		if (device == nullptr || device->GetNode() == nullptr ||
			device->GetNode()->GetNodeType() != 1) {
			continue;
		}
		Ptr<SwitchNode> sw =
			DynamicCast<SwitchNode>(device->GetNode());
		if (sw == nullptr) {
			continue;
		}
		sw->BindPacketDlbRoute(
			qp->sip.Get(),
			qp->dip.Get(),
			qp->sport,
			qp->dport,
			seq,
			device->GetIfIndex());
		qp->m_packetDlbBoundSwitches.push_back(
			device->GetNode()->GetId());
	}
}

void BindPathAwareRoute(
		Ptr<RdmaQueuePair> qp,
		const PathAwareCandidate& path,
		uint64_t reservationBytes) {
	for (const Ptr<QbbNetDevice>& device : path.hops) {
		const uint64_t edge = PathEdgeKey(device);
		PathReservedBytes()[edge] += reservationBytes;
		qp->m_pathReservationEdges.push_back(edge);

		Ptr<Node> node = device->GetNode();
		if (node->GetNodeType() == 1) {
			DynamicCast<SwitchNode>(node)->BindPathAwareQpRoute(
				qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport,
				device->GetIfIndex());
		} else if (node->GetNodeType() == 2) {
			DynamicCast<NVSwitchNode>(node)->BindPathAwareQpRoute(
				qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport,
				device->GetIfIndex());
		}
	}
	qp->m_pathReservationBytes = reservationBytes;
}

void ReleasePathAwareReservationBytesLocked(Ptr<RdmaQueuePair> qp) {
	if (qp == nullptr || qp->m_pathReservationEdges.empty()) {
		return;
	}
	for (const uint64_t edge : qp->m_pathReservationEdges) {
		auto reservation = PathReservedBytes().find(edge);
		if (reservation != PathReservedBytes().end()) {
			if (reservation->second <= qp->m_pathReservationBytes) {
				PathReservedBytes().erase(reservation);
			} else {
				reservation->second -= qp->m_pathReservationBytes;
			}
		}
	}
	qp->m_pathReservationBytes = 0;
}

void ReleasePathAwareRouteLocked(Ptr<RdmaQueuePair> qp) {
	if (qp == nullptr || qp->m_pathReservationEdges.empty()) {
		return;
	}
	ReleasePathAwareReservationBytesLocked(qp);
	for (const uint64_t edge : qp->m_pathReservationEdges) {

		const uint32_t nodeId = static_cast<uint32_t>(edge >> 32);
		Ptr<Node> node = NodeList::GetNode(nodeId);
		if (node == nullptr) {
			continue;
		}
		if (node->GetNodeType() == 1) {
			DynamicCast<SwitchNode>(node)->UnbindPathAwareQpRoute(
				qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport);
		} else if (node->GetNodeType() == 2) {
			DynamicCast<NVSwitchNode>(node)->UnbindPathAwareQpRoute(
				qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport);
		}
	}
	qp->m_pathReservationEdges.clear();
}

void ReleasePathAwareRoute(Ptr<RdmaQueuePair> qp) {
	std::lock_guard<std::mutex> guard(PathReservationMutex());
	ReleasePathAwareRouteLocked(qp);
}

void RebindPathAwareRoute(
		Ptr<RdmaQueuePair> qp,
		const PathAwareCandidate& path,
		uint64_t reservationBytes) {
	std::lock_guard<std::mutex> guard(PathReservationMutex());
	ReleasePathAwareRouteLocked(qp);
	BindPathAwareRoute(qp, path, reservationBytes);
}

uint32_t SelectSwitchPacketDlbTxNic(
		RdmaHw* hw,
		Ptr<RdmaQueuePair> qp,
		uint32_t currentNic) {
	if (!qp->m_packetDlbCandidatesInitialized) {
		qp->m_packetDlbCandidates = GetDualTableSourceCandidates(*hw);
		if (qp->m_packetDlbCandidates.empty()) {
			auto routes = hw->m_rtTable.find(qp->dip.Get());
			if (routes != hw->m_rtTable.end()) {
				qp->m_packetDlbCandidates = routes->second;
			}
		}
		qp->m_packetDlbCandidatesInitialized = true;
	}
	const std::vector<int>& candidates = qp->m_packetDlbCandidates;
	if (candidates.size() <= 1) {
		return currentNic;
	}
	if (qp->m_packetDlbPrepared) {
		CancelPreparedPacketDlbRoute(qp);
	}

	const uint64_t packetBytes = std::min(
		static_cast<uint64_t>(hw->m_mtu), qp->GetBytesLeft());
	if (packetBytes == 0) {
		return currentNic;
	}
	const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
	const uint64_t seq = qp->snd_nxt;
	const uint32_t packetHash = PacketDlbHash(qp->GetHash(), seq);
	const uint32_t start = packetHash % candidates.size();
	const uint32_t destinationNode = (qp->dip.Get() >> 8) & 0xffff;
	std::vector<SwitchPacketDlbPathEstimate> pathEstimates(
		hw->m_nic.size());
	uint64_t minimumPathLatencyNs =
		std::numeric_limits<uint64_t>::max();
	uint32_t minimumPathHops =
		std::numeric_limits<uint32_t>::max();
	for (const int candidateValue : candidates) {
		if (candidateValue < 0) {
			continue;
		}
		const uint32_t candidate =
			static_cast<uint32_t>(candidateValue);
		if (candidate >= hw->m_nic.size() ||
			hw->m_nic[candidate].dev == nullptr ||
			!hw->m_nic[candidate].dev->IsLinkUp()) {
			continue;
		}
		pathEstimates[candidate] =
			SelectSwitchPacketDlbPathEstimate(
				hw->m_nic[candidate].dev,
				destinationNode,
				qp->dip.Get(),
				static_cast<uint32_t>(packetBytes),
				packetHash);
		if (pathEstimates[candidate].valid) {
			minimumPathLatencyNs = std::min(
				minimumPathLatencyNs,
				pathEstimates[candidate].baseLatencyNs);
			minimumPathHops = std::min(
				minimumPathHops,
				pathEstimates[candidate].hops);
		}
	}

	uint32_t selected = std::numeric_limits<uint32_t>::max();
	uint32_t viableCandidates = 0;
	uint64_t selectedScoreNs = std::numeric_limits<uint64_t>::max();
	uint64_t selectedLaneReadyNs = nowNs;
	uint64_t selectedQueueBytes = 0;
	uint64_t selectedQueueDelayNs = 0;
	uint64_t selectedPropagationNs = 0;
	uint32_t selectedPathHops = 0;
	int32_t selectedDestinationNic = -1;
	bool selectedEndpointPinned = false;
	uint64_t currentScoreNs = std::numeric_limits<uint64_t>::max();
	auto evaluateCandidates = [&](bool allowPaused) {
		for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
			const int candidateValue =
				candidates[(start + offset) % candidates.size()];
			if (candidateValue < 0) {
				continue;
			}
			const uint32_t candidate =
				static_cast<uint32_t>(candidateValue);
			if (candidate >= hw->m_nic.size()) {
				continue;
			}
			Ptr<QbbNetDevice> device = hw->m_nic[candidate].dev;
			if (device == nullptr || !device->IsLinkUp() ||
				(!allowPaused && device->IsPriorityPaused(qp->m_pg))) {
				continue;
			}
			const uint64_t bitRate = device->GetDataRate().GetBitRate();
			if (bitRate == 0) {
				continue;
			}
			const uint64_t laneReadyNs =
				PacketDlbLaneNextAvailNs(qp, candidate, nowNs);
			const uint64_t queueBytes = device->GetQueue() == nullptr
				? 0
				: device->GetQueue()->GetNBytesTotal();
			const uint64_t queuedWorkNs = SaturatingAdd(
				device->GetTxRemainingNs(),
				SerializationNs(queueBytes, bitRate));
			const uint64_t physicalReadyNs =
				SaturatingAdd(nowNs, queuedWorkNs);
			const uint64_t readyNs =
				std::max(laneReadyNs, physicalReadyNs);
			const SwitchPacketDlbPathEstimate pathEstimate =
				pathEstimates[candidate];
			Ptr<QbbChannel> channel =
				DynamicCast<QbbChannel>(device->GetChannel());
			const uint64_t firstHopPropagationNs =
				channel == nullptr
				? 0
				: static_cast<uint64_t>(
					channel->GetDelay().GetNanoSeconds());
			uint64_t baseLatencyNs = SaturatingAdd(
				SerializationNs(packetBytes, bitRate),
				firstHopPropagationNs);
			if (pathEstimate.valid &&
				minimumPathLatencyNs !=
					std::numeric_limits<uint64_t>::max() &&
				pathEstimate.baseLatencyNs >
					minimumPathLatencyNs) {
				const uint64_t extraPathLatencyNs =
					pathEstimate.baseLatencyNs -
					minimumPathLatencyNs;
				const uint64_t remainingBytes = std::max(
					packetBytes, qp->GetBytesLeft());
				const long double amortizedPenaltyNs =
					static_cast<long double>(
						extraPathLatencyNs) *
					static_cast<long double>(packetBytes) /
					static_cast<long double>(remainingBytes);
				baseLatencyNs = SaturatingAdd(
					baseLatencyNs,
					SaturatingNs(
						std::ceil(amortizedPenaltyNs)));
			}
			const uint64_t queueDelayNs = readyNs - nowNs;
			// Adapt UGAL-L's local-congestion-by-distance comparison to
			// independent source NICs by using path stretch. A 3+3 pair has
			// equal cost, while the 4-hop side of a 2+4 pair pays 2x.
			uint64_t hopWeightedQueueDelayNs = queueDelayNs;
			if (pathEstimate.valid && minimumPathHops !=
					std::numeric_limits<uint32_t>::max() &&
				minimumPathHops > 0) {
				const long double pathStretch =
					static_cast<long double>(pathEstimate.hops) /
					static_cast<long double>(minimumPathHops);
				hopWeightedQueueDelayNs = SaturatingNs(std::ceil(
					static_cast<long double>(queueDelayNs) *
					pathStretch));
			}
			const uint64_t scoreNs = SaturatingAdd(
				hopWeightedQueueDelayNs, baseLatencyNs);
			++viableCandidates;
			if (candidate == currentNic) {
				currentScoreNs = scoreNs;
			}
			if (scoreNs < selectedScoreNs) {
				selected = candidate;
				selectedScoreNs = scoreNs;
				selectedLaneReadyNs = laneReadyNs;
				selectedQueueBytes = queueBytes;
				selectedQueueDelayNs = queueDelayNs;
				selectedPropagationNs = pathEstimate.valid
					? pathEstimate.propagationNs
					: firstHopPropagationNs;
				selectedPathHops = pathEstimate.hops;
				selectedDestinationNic =
					pathEstimate.destinationNic;
				selectedEndpointPinned =
					pathEstimate.endpointPinned;
			}
		}
	};
	evaluateCandidates(false);
	if (selected == std::numeric_limits<uint32_t>::max()) {
		viableCandidates = 0;
		evaluateCandidates(true);
	}
	if (selected == std::numeric_limits<uint32_t>::max()) {
		return currentNic;
	}

	if (qp->m_initialSelectedNicIdx < 0) {
		qp->m_initialSelectedNicIdx = static_cast<int32_t>(selected);
	} else if (qp->m_selectedNicIdx >= 0 &&
		qp->m_selectedNicIdx != static_cast<int32_t>(selected)) {
		++qp->m_nicReassignments;
	}
	qp->m_selectedNicIdx = static_cast<int32_t>(selected);
	if (selectedEndpointPinned && selectedDestinationNic >= 0) {
		qp->m_selectedDestinationNicIdx =
			selectedDestinationNic;
	} else {
		const PacketDlbEndpointPairingFilter endpointPairing =
			GetPacketDlbEndpointPairingFilter(
				hw->m_nic[selected].dev, destinationNode);
		qp->m_selectedDestinationNicIdx =
			endpointPairing.applicable &&
				!endpointPairing.destinationNics.empty()
			? endpointPairing.destinationNics[
				packetHash %
					endpointPairing.destinationNics.size()]
			: -1;
	}
	qp->m_bindCandidateCount = viableCandidates;
	qp->m_bindPathHops = selectedPathHops;
	qp->m_bindPathScoreNs = selectedScoreNs;
	qp->m_bindPathQueueDelayNs = selectedQueueDelayNs;
	qp->m_bindPathPropagationNs = selectedPropagationNs;
	qp->m_bindPathReservedBytes = 0;
	qp->m_bindPathSignature = 0;
	qp->m_nextAvail =
		NanoSeconds(static_cast<int64_t>(selectedLaneReadyNs));

	SwitchNode::RecordSourceFlowletDecisionStats(
		hw->m_node->GetId(),
		selected,
		viableCandidates,
		selectedQueueBytes,
		selected < hw->tx_bytes.size() ? hw->tx_bytes[selected] : 0,
		selectedScoreNs,
		currentScoreNs == std::numeric_limits<uint64_t>::max()
			? 0
			: currentScoreNs,
		selectedQueueDelayNs,
		selectedPropagationNs,
		0,
		selectedPathHops,
		hw->m_mtu == 0 ? 0 : seq / hw->m_mtu,
		nowNs,
		selected != currentNic,
		false,
		true,
		false,
		qp->sip.Get(),
		qp->dip.Get(),
		qp->sport,
		qp->dport);
	return selected;
}

uint32_t SelectPacketDlbTxNic(
		RdmaHw* hw,
		Ptr<RdmaQueuePair> qp,
		uint32_t currentNic,
		bool fixedSourceNic = false) {
	if (!fixedSourceNic && SwitchNode::SwitchPacketDlbRoutingEnabled()) {
		return SelectSwitchPacketDlbTxNic(hw, qp, currentNic);
	}
	std::vector<int> fixedCandidates;
	if (fixedSourceNic) {
		fixedCandidates.push_back(static_cast<int>(currentNic));
	} else if (!qp->m_packetDlbCandidatesInitialized) {
		qp->m_packetDlbCandidates =
			GetDualTableSourceCandidates(*hw);
	}
	if (!fixedSourceNic && !qp->m_packetDlbCandidatesInitialized &&
		qp->m_packetDlbCandidates.empty()) {
		auto routes = hw->m_rtTable.find(qp->dip.Get());
		if (routes != hw->m_rtTable.end()) {
			qp->m_packetDlbCandidates = routes->second;
		}
	}
	if (!fixedSourceNic) {
		qp->m_packetDlbCandidatesInitialized = true;
	}
	const std::vector<int>& candidates = fixedSourceNic
		? fixedCandidates
		: qp->m_packetDlbCandidates;
	if (candidates.empty() || (!fixedSourceNic && candidates.size() <= 1)) {
		return currentNic;
	}
	auto isLiveDevice = [&](uint32_t candidate) {
		return candidate < hw->m_nic.size() &&
			hw->m_nic[candidate].dev != nullptr &&
			hw->m_nic[candidate].dev->IsLinkUp();
	};
	auto isLiveCandidate = [&](uint32_t candidate) {
		return isLiveDevice(candidate) &&
			std::find(
				candidates.begin(),
				candidates.end(),
				static_cast<int>(candidate)) != candidates.end() &&
			!qp->IsPacketDlbLaneWinBound(candidate);
	};

	if (qp->m_packetDlbPrepared &&
		qp->m_packetDlbPreparedSeq == qp->snd_nxt &&
		qp->m_packetDlbPreparedNicIdx >= 0 &&
		isLiveCandidate(
			static_cast<uint32_t>(qp->m_packetDlbPreparedNicIdx))) {
		return static_cast<uint32_t>(qp->m_packetDlbPreparedNicIdx);
	}
	if (qp->m_packetDlbPrepared) {
		CancelPreparedPacketDlbRoute(qp);
	}

	const uint64_t packetBytes = std::min(
		static_cast<uint64_t>(hw->m_mtu), qp->GetBytesLeft());
	if (packetBytes == 0) {
		return currentNic;
	}
	const uint64_t seq = qp->snd_nxt;
	const uint32_t packetHash = PacketDlbHash(qp->GetHash(), seq);
	const uint32_t destinationNode = (qp->dip.Get() >> 8) & 0xffff;
	const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
	const bool currentLive = isLiveCandidate(currentNic);
	const bool workConservingBulk =
		!fixedSourceNic &&
		qp->GetBytesLeft() >
			packetBytes * std::max<size_t>(size_t{1}, candidates.size());

	uint32_t selected = std::numeric_limits<uint32_t>::max();
	uint32_t viableCandidates = 0;
	uint64_t selectedLaneReadyNs = nowNs;
	bool selectedLaneReadyNow = false;
	PathAwareCandidate selectedPath;
	const PacketDlbPathTemplate* selectedPathTemplate = nullptr;
	long double selectedScore = std::numeric_limits<long double>::infinity();
	long double currentScore = std::numeric_limits<long double>::infinity();
	long double selectedCompletionScore =
		std::numeric_limits<long double>::infinity();
	uint64_t selectedTieRank = std::numeric_limits<uint64_t>::max();
	thread_local std::vector<PacketDlbConsideredPath> consideredPaths;
	consideredPaths.clear();
	const bool recordPathStats =
		!PacketDlbPathStatsOutputPath().empty();
	{
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		auto evaluateCandidates = [&](bool threeHopOnly, bool twoFourHopOnly) {
			const uint32_t start = packetHash % candidates.size();
			for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
				const int candidateValue =
					candidates[(start + offset) % candidates.size()];
				if (candidateValue < 0) {
					continue;
				}
				const uint32_t candidate =
					static_cast<uint32_t>(candidateValue);
				if (!isLiveDevice(candidate)) {
					continue;
				}
				const uint64_t laneReadyNs =
					PacketDlbLaneNextAvailNs(qp, candidate, nowNs);
				const bool laneReadyNow = laneReadyNs <= nowNs;

				const PacketDlbPathTemplates& pathTemplates =
					GetPacketDlbPathTemplates(
						hw->m_nic[candidate].dev,
						destinationNode,
						qp->dip.Get());
				const PacketDlbEndpointPairingFilter endpointPairing =
					GetPacketDlbEndpointPairingFilter(
						hw->m_nic[candidate].dev,
						destinationNode);
				bool sourceReachable = false;
				long double sourceBestScore =
					std::numeric_limits<long double>::infinity();
				const uint32_t pathStart = pathTemplates.empty()
					? 0
					: packetHash % pathTemplates.size();
				const uint32_t pathSampleLimit = std::min<uint32_t>(
					static_cast<uint32_t>(pathTemplates.size()),
					PacketDlbPathSampleWidth());
				const uint32_t pathScanLimit =
					(threeHopOnly || twoFourHopOnly ||
					 endpointPairing.applicable)
					? static_cast<uint32_t>(pathTemplates.size())
					: pathSampleLimit;
				uint32_t sampledPaths = 0;
				for (uint32_t pathOffset = 0;
					 pathOffset < pathScanLimit;
					 ++pathOffset) {
					const PacketDlbPathTemplate& pathTemplate =
						pathTemplates[
							(pathStart + pathOffset) % pathTemplates.size()];
					if (threeHopOnly && pathTemplate.size() != 3) {
						continue;
					}
					if (twoFourHopOnly && pathTemplate.size() != 2 &&
						pathTemplate.size() != 4) {
						continue;
					}
					if (endpointPairing.applicable &&
						!endpointPairing.AllowsDestination(
							PacketDlbPathDestinationNic(pathTemplate))) {
						continue;
					}
					if (sampledPaths >= pathSampleLimit) {
						break;
					}
					++sampledPaths;
					PathAwareCandidate path;
					if (!EvaluatePacketDlbPath(pathTemplate, &path)) {
						continue;
					}
					uint64_t projectedArrivalNs = 0;
					if (!ProjectPacketDlbArrivalNs(
							pathTemplate,
							packetBytes,
							laneReadyNs,
							&projectedArrivalNs)) {
						continue;
					}
					sourceReachable = true;
					const uint64_t arrivalDelayNs =
						projectedArrivalNs >= nowNs
							? projectedArrivalNs - nowNs
							: 0;
					const uint64_t qpPathTailNs =
						PacketDlbQpPathTailNs(
							qp, pathTemplate, packetBytes);
					const uint64_t laneWaitNs =
						laneReadyNs >= nowNs
							? laneReadyNs - nowNs
							: 0;
					const uint64_t laneAwareQpPathTailNs =
						SaturatingAdd(laneWaitNs, qpPathTailNs);
					// The live ETA reacts to shared congestion, while the
					// QP-local edge ledger supplies enough lookahead to open a
					// slower disjoint lane before the short NIC self-serializes
					// every packet. The ledger dies with the QP, so traffic
					// from earlier collectives cannot bias this decision.
					const long double score =
						static_cast<long double>(
							std::max(
								arrivalDelayNs,
								laneAwareQpPathTailNs));
					if (!std::isfinite(score)) {
						continue;
					}
					if (recordPathStats) {
						consideredPaths.push_back(PacketDlbConsideredPath{
							PacketDlbPathStatsKey{
								hw->m_node->GetId(),
								destinationNode,
								candidate,
								PacketDlbPathDestinationNic(pathTemplate),
								static_cast<uint32_t>(pathTemplate.size()),
								PacketDlbPathTemplateSignature(pathTemplate),
							},
							&pathTemplate,
							PacketDlbPathEdges(pathTemplate),
							path.queueBytes,
							path.reservedBytes,
							SaturatingNs(score),
						});
					}
					sourceBestScore = std::min(sourceBestScore, score);
					uint64_t tieRank =
						PacketDlbPathTemplateSignature(pathTemplate) ^
						(static_cast<uint64_t>(packetHash) << 32) ^
						static_cast<uint64_t>(packetHash);
					tieRank = (tieRank ^ (tieRank >> 30)) *
						0xbf58476d1ce4e5b9ULL;
					tieRank = (tieRank ^ (tieRank >> 27)) *
						0x94d049bb133111ebULL;
					tieRank ^= tieRank >> 31;
					bool better =
						selected ==
							std::numeric_limits<uint32_t>::max();
					if (!better && workConservingBulk &&
						laneReadyNow != selectedLaneReadyNow) {
						better = laneReadyNow;
					} else if (!better &&
						(!workConservingBulk ||
						 laneReadyNow == selectedLaneReadyNow)) {
						better =
							score < selectedCompletionScore ||
							(score == selectedCompletionScore &&
							 tieRank < selectedTieRank);
					}
					if (better) {
						selectedCompletionScore = score;
						selectedTieRank = tieRank;
						selected = candidate;
						selectedLaneReadyNs = laneReadyNs;
						selectedLaneReadyNow = laneReadyNow;
						selectedPath = std::move(path);
						selectedPathTemplate = &pathTemplate;
						selectedScore = score;
					}
				}
				if (sourceReachable) {
					++viableCandidates;
					if (candidate == currentNic) {
						currentScore = sourceBestScore;
					}
				}
			}
		};

		const bool preferBalancedThreeHop =
			PacketDlbBalancedThreeHopEnabled();
		const bool preferTwoFourHop =
			PacketDlbTwoFourHopEnabled();
		NS_ABORT_MSG_IF(
			preferBalancedThreeHop && preferTwoFourHop,
			"Packet DLB three-hop-only and two-four-hop-only modes "
			"cannot both be enabled");
		if (preferBalancedThreeHop) {
			evaluateCandidates(true, false);
		} else if (preferTwoFourHop) {
			evaluateCandidates(false, true);
		}
		if (selected == std::numeric_limits<uint32_t>::max()) {
			if (preferBalancedThreeHop || preferTwoFourHop) {
				viableCandidates = 0;
				currentScore = std::numeric_limits<long double>::infinity();
				consideredPaths.clear();
			}
			evaluateCandidates(false, false);
		}

		if (selected != std::numeric_limits<uint32_t>::max() &&
			selectedPath.valid && selectedPathTemplate != nullptr) {
			if (preferBalancedThreeHop &&
				selectedPathTemplate->size() == 3) {
				ApplyActualPathWindow(
					hw, qp, *selectedPathTemplate, selectedPath);
			}
			RecordPacketDlbQpPathBytes(
				qp, *selectedPathTemplate, packetBytes);
			selectedPath.hops.assign(
				selectedPathTemplate->begin(), selectedPathTemplate->end());
			ReservePacketDlbPath(
				selectedPath, packetBytes, selectedLaneReadyNs);
			BindPacketDlbPath(qp, selectedPath, seq);
		}
	}

	if (selected == std::numeric_limits<uint32_t>::max()) {
		const uint32_t start = packetHash % candidates.size();
		uint64_t fallbackReadyNs = std::numeric_limits<uint64_t>::max();
		for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
			const int candidateValue =
				candidates[(start + offset) % candidates.size()];
			if (candidateValue < 0) {
				continue;
			}
			const uint32_t candidate =
				static_cast<uint32_t>(candidateValue);
			if (isLiveCandidate(candidate)) {
				++viableCandidates;
				const uint64_t laneReadyNs =
					PacketDlbLaneNextAvailNs(qp, candidate, nowNs);
				if (selected == std::numeric_limits<uint32_t>::max() ||
					laneReadyNs < fallbackReadyNs) {
					selected = candidate;
					selectedLaneReadyNs = laneReadyNs;
					selectedLaneReadyNow = laneReadyNs <= nowNs;
					fallbackReadyNs = laneReadyNs;
				}
			}
		}
		NS_ASSERT_MSG(
			selected != std::numeric_limits<uint32_t>::max(),
			"Packet DLB found no live source NIC");
		selectedScore = 0;
	}

	if (qp->m_initialSelectedNicIdx < 0) {
		qp->m_initialSelectedNicIdx = static_cast<int32_t>(selected);
	} else if (qp->m_selectedNicIdx >= 0 &&
		qp->m_selectedNicIdx != static_cast<int32_t>(selected)) {
		++qp->m_nicReassignments;
	}
	qp->m_selectedNicIdx = static_cast<int32_t>(selected);
	qp->m_selectedDestinationNicIdx = selectedPath.valid
		? PathDestinationNicIndex(selectedPath)
		: -1;
	qp->m_bindCandidateCount = viableCandidates;
	qp->m_bindPathHops =
		static_cast<uint32_t>(selectedPath.hops.size());
	qp->m_bindPathScoreNs = SaturatingNs(selectedScore);
	qp->m_bindPathQueueDelayNs =
		SaturatingNs(selectedPath.maxEdgeWorkNs);
	qp->m_bindPathPropagationNs = selectedPath.propagationNs;
	qp->m_bindPathReservedBytes = selectedPath.reservedBytes;
	qp->m_bindPathSignature =
		selectedPath.valid ? PathSignature(selectedPath) : 0;
	qp->m_packetDlbPrepared = true;
	qp->m_packetDlbPreparedSeq = seq;
	qp->m_packetDlbPreparedNicIdx = static_cast<int32_t>(selected);
	if (!fixedSourceNic) {
		qp->m_nextAvail = NanoSeconds(
			static_cast<int64_t>(selectedLaneReadyNs));
	}
	if (recordPathStats) {
		RecordPacketDlbPathDecision(
			consideredPaths,
			selected,
			selectedPathTemplate,
			packetBytes);
	}

	const uint64_t selectedQueueBytes = SaturatingAdd(
		selectedPath.queueBytes, selectedPath.reservedBytes);
	SwitchNode::RecordSourceFlowletDecisionStats(
		hw->m_node->GetId(),
		selected,
		viableCandidates,
		selectedQueueBytes,
		selected < hw->tx_bytes.size() ? hw->tx_bytes[selected] : 0,
		SaturatingNs(selectedScore),
		std::isfinite(currentScore) ? SaturatingNs(currentScore) : 0,
		SaturatingNs(selectedPath.maxEdgeWorkNs),
		selectedPath.propagationNs,
		selectedPath.reservedBytes,
		static_cast<uint32_t>(selectedPath.hops.size()),
		hw->m_mtu == 0 ? 0 : seq / hw->m_mtu,
		nowNs,
		selected != currentNic,
		false,
		true,
		!currentLive,
		qp->sip.Get(),
		qp->dip.Get(),
		qp->sport,
		qp->dport);
	return selected;
}

}  // namespace

TypeId RdmaHw::GetTypeId (void)
{
	static TypeId tid = TypeId ("ns3::RdmaHw")
		.SetParent<Object> ()
		.AddAttribute("MinRate",
				"Minimum rate of a throttled flow",
				DataRateValue(DataRate("100Mb/s")),
				MakeDataRateAccessor(&RdmaHw::m_minRate),
				MakeDataRateChecker())
		.AddAttribute("Mtu",
				"Mtu.",
				UintegerValue(1000),
				MakeUintegerAccessor(&RdmaHw::m_mtu),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute ("CcMode",
				"which mode of DCQCN is running",
				UintegerValue(0),
				MakeUintegerAccessor(&RdmaHw::m_cc_mode),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("NACKGenerationInterval",
				"The NACK Generation interval",
				DoubleValue(500.0),
				MakeDoubleAccessor(&RdmaHw::m_nack_interval),
				MakeDoubleChecker<double>())
		.AddAttribute("L2ChunkSize",
				"Layer 2 chunk size. Disable chunk mode if equals to 0.",
				UintegerValue(0),
				MakeUintegerAccessor(&RdmaHw::m_chunk),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("L2AckInterval",
				"Layer 2 Ack intervals. Disable ack if equals to 0.",
				UintegerValue(0),
				MakeUintegerAccessor(&RdmaHw::m_ack_interval),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("L2BackToZero",
				"Layer 2 go back to zero transmission.",
				BooleanValue(false),
				MakeBooleanAccessor(&RdmaHw::m_backto0),
				MakeBooleanChecker())
		.AddAttribute("EwmaGain",
				"Control gain parameter which determines the level of rate decrease",
				DoubleValue(1.0 / 16),
				MakeDoubleAccessor(&RdmaHw::m_g),
				MakeDoubleChecker<double>())
		.AddAttribute ("RateOnFirstCnp",
				"the fraction of rate on first CNP",
				DoubleValue(1.0),
				MakeDoubleAccessor(&RdmaHw::m_rateOnFirstCNP),
				MakeDoubleChecker<double> ())
		.AddAttribute("ClampTargetRate",
				"Clamp target rate.",
				BooleanValue(false),
				MakeBooleanAccessor(&RdmaHw::m_EcnClampTgtRate),
				MakeBooleanChecker())
		.AddAttribute("RPTimer",
				"The rate increase timer at RP in microseconds",
				DoubleValue(1500.0),
				MakeDoubleAccessor(&RdmaHw::m_rpgTimeReset),
				MakeDoubleChecker<double>())
		.AddAttribute("RateDecreaseInterval",
				"The interval of rate decrease check",
				DoubleValue(4.0),
				MakeDoubleAccessor(&RdmaHw::m_rateDecreaseInterval),
				MakeDoubleChecker<double>())
		.AddAttribute("FastRecoveryTimes",
				"The rate increase timer at RP",
				UintegerValue(5),
				MakeUintegerAccessor(&RdmaHw::m_rpgThreshold),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("AlphaResumInterval",
				"The interval of resuming alpha",
				DoubleValue(55.0),
				MakeDoubleAccessor(&RdmaHw::m_alpha_resume_interval),
				MakeDoubleChecker<double>())
		.AddAttribute("RateAI",
				"Rate increment unit in AI period",
				DataRateValue(DataRate("5Mb/s")),
				MakeDataRateAccessor(&RdmaHw::m_rai),
				MakeDataRateChecker())
		.AddAttribute("RateHAI",
				"Rate increment unit in hyperactive AI period",
				DataRateValue(DataRate("50Mb/s")),
				MakeDataRateAccessor(&RdmaHw::m_rhai),
				MakeDataRateChecker())
		.AddAttribute("VarWin",
				"Use variable window size or not",
				BooleanValue(false),
				MakeBooleanAccessor(&RdmaHw::m_var_win),
				MakeBooleanChecker())
		.AddAttribute("FastReact",
				"Fast React to congestion feedback",
				BooleanValue(true),
				MakeBooleanAccessor(&RdmaHw::m_fast_react),
				MakeBooleanChecker())
		.AddAttribute("MiThresh",
				"Threshold of number of consecutive AI before MI",
				UintegerValue(5),
				MakeUintegerAccessor(&RdmaHw::m_miThresh),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("TargetUtil",
				"The Target Utilization of the bottleneck bandwidth, by default 95%",
				DoubleValue(0.95),
				MakeDoubleAccessor(&RdmaHw::m_targetUtil),
				MakeDoubleChecker<double>())
		.AddAttribute("UtilHigh",
				"The upper bound of Target Utilization of the bottleneck bandwidth, by default 98%",
				DoubleValue(0.98),
				MakeDoubleAccessor(&RdmaHw::m_utilHigh),
				MakeDoubleChecker<double>())
		.AddAttribute("RateBound",
				"Bound packet sending by rate, for test only",
				BooleanValue(true),
				MakeBooleanAccessor(&RdmaHw::m_rateBound),
				MakeBooleanChecker())
		.AddAttribute("MultiRate",
				"Maintain multiple rates in HPCC",
				BooleanValue(true),
				MakeBooleanAccessor(&RdmaHw::m_multipleRate),
				MakeBooleanChecker())
		.AddAttribute("SampleFeedback",
				"Whether sample feedback or not",
				BooleanValue(false),
				MakeBooleanAccessor(&RdmaHw::m_sampleFeedback),
				MakeBooleanChecker())
		.AddAttribute("TimelyAlpha",
				"Alpha of TIMELY",
				DoubleValue(0.875),
				MakeDoubleAccessor(&RdmaHw::m_tmly_alpha),
				MakeDoubleChecker<double>())
		.AddAttribute("TimelyBeta",
				"Beta of TIMELY",
				DoubleValue(0.8),
				MakeDoubleAccessor(&RdmaHw::m_tmly_beta),
				MakeDoubleChecker<double>())
		.AddAttribute("TimelyTLow",
				"TLow of TIMELY (ns)",
				UintegerValue(50000),
				MakeUintegerAccessor(&RdmaHw::m_tmly_TLow),
				MakeUintegerChecker<uint64_t>())
		.AddAttribute("TimelyTHigh",
				"THigh of TIMELY (ns)",
				UintegerValue(500000),
				MakeUintegerAccessor(&RdmaHw::m_tmly_THigh),
				MakeUintegerChecker<uint64_t>())
		.AddAttribute("TimelyMinRtt",
				"MinRtt of TIMELY (ns)",
				UintegerValue(20000),
				MakeUintegerAccessor(&RdmaHw::m_tmly_minRtt),
				MakeUintegerChecker<uint64_t>())
		.AddAttribute("DctcpRateAI",
				"DCTCP's Rate increment unit in AI period",
				DataRateValue(DataRate("1000Mb/s")),
				MakeDataRateAccessor(&RdmaHw::m_dctcp_rai),
				MakeDataRateChecker())
		.AddAttribute("PintSmplThresh",
				"PINT's sampling threshold in rand()%65536",
				UintegerValue(65536),
				MakeUintegerAccessor(&RdmaHw::pint_smpl_thresh),
				MakeUintegerChecker<uint32_t>())
		.AddAttribute("GPUsPerServer",
				"the number of gpus in a server, used for routing",
				UintegerValue(1),
				MakeUintegerAccessor(&RdmaHw::m_gpus_per_server),
				MakeUintegerChecker<uint32_t>())	
		.AddAttribute("TotalPauseTimes",
				"The number of pause times to simulate PFC pause due to PCIe",
				UintegerValue(0),
				MakeUintegerAccessor(&RdmaHw::m_total_pause_times),
				MakeUintegerChecker<uint64_t>())
		.AddAttribute("NVLS_enable",
				"NVLS enable info",
				UintegerValue(0),
				MakeUintegerAccessor(&RdmaHw::nvls_enable),
				MakeUintegerChecker<uint32_t>());
		;
	return tid;
}

RdmaHw::RdmaHw(){
}

void RdmaHw::enable_nvls() {
	nvls_enable = 1;
}

void RdmaHw::disable_nvls() {
	nvls_enable = 0;
}

void RdmaHw::add_nvswitch(uint32_t nvswitch_id) {
	nvswitch_set.insert(nvswitch_id);
}

void RdmaHw::SetNode(Ptr<Node> node){
	m_node = node;
}
void RdmaHw::Setup(QpCompleteCallback cb,SendCompleteCallback send_cb){
	tx_bytes.resize(m_nic.size());
	last_tx_bytes.resize(m_nic.size());
	for (uint32_t i = 0; i < m_nic.size(); i++){
		tx_bytes[i] = 0;
		last_tx_bytes[i] = 0;
		Ptr<QbbNetDevice> dev = m_nic[i].dev;
		if (dev == NULL)
			continue;
		// share data with NIC
		dev->m_rdmaEQ->m_qpGrp = m_nic[i].qpGrp;
		// setup callback
		dev->m_rdmaReceiveCb = MakeCallback(&RdmaHw::Receive, this);
        dev->m_rdmaSentCb = MakeCallback(&RdmaHw::SendPacketComplete, this);
		dev->m_rdmaLinkDownCb = MakeCallback(&RdmaHw::SetLinkDown, this);
		dev->m_rdmaPktSent = MakeCallback(&RdmaHw::PktSent, this);
		dev->m_rdmaSelectTxNic = MakeCallback(&RdmaHw::SelectTxNic, this);
		dev->m_rdmaUpdateTxBytes = MakeCallback(&RdmaHw::UpdateTxBytes, this);
		// config NIC
		dev->m_rdmaEQ->m_rdmaGetNxtPkt = MakeCallback(&RdmaHw::GetNxtPacket, this);
	}
	// setup qp complete callback
	m_qpCompleteCallback = cb;
    m_sendCompleteCallback = send_cb;
}
uint32_t ip_to_node_id(Ipv4Address ip) { return (ip.Get() >> 8) & 0xffff; }
uint32_t RdmaHw::GetNicIdxOfQp(Ptr<RdmaQueuePair> qp){
	uint32_t src = qp->m_src;
	uint32_t dst = qp->m_dest;
	const bool sameServer =
		src / m_gpus_per_server == dst / m_gpus_per_server;
	std::vector<int> dualTableCandidates;
	std::vector<int>* candidates = nullptr;
	if (sameServer || m_rtTable_nxthop_nvswitch.count(qp->dip.Get()) != 0) {
		auto routes = m_rtTable_nxthop_nvswitch.find(qp->dip.Get());
		if (routes != m_rtTable_nxthop_nvswitch.end()) {
			candidates = &routes->second;
		}
	} else if (SwitchNode::DualTableRoutingEnabled() ||
		SwitchNode::MultiQpPacketDlbRoutingEnabled()) {
		// A dual-table route is shortest only after its physical source NIC is
		// fixed. Do not filter the second NIC through the global source SPF.
		dualTableCandidates = GetDualTableSourceCandidates(*this);
		if (!dualTableCandidates.empty()) {
			candidates = &dualTableCandidates;
		}
	} else {
		auto routes = m_rtTable.find(qp->dip.Get());
		if (routes != m_rtTable.end()) {
			candidates = &routes->second;
		}
	}

	NS_ASSERT_MSG(
		candidates != nullptr && !candidates->empty(),
		"We assume at least one NIC is alive");
	auto &v = *candidates;
	if (qp->m_selectedNicIdx >= 0) {
		const int selected = qp->m_selectedNicIdx;
		if (std::find(v.begin(), v.end(), selected) != v.end() &&
			static_cast<uint32_t>(selected) < m_nic.size() &&
			m_nic[selected].dev != nullptr &&
			m_nic[selected].dev->IsLinkUp()) {
			return static_cast<uint32_t>(selected);
		}
		if (SwitchNode::PacketDlbRoutingEnabled() ||
			SwitchNode::MultiQpPacketDlbRoutingEnabled()) {
			CancelOutstandingPacketDlbRoutes(qp);
		}
		ReleasePathAwareRoute(qp);
		qp->m_selectedNicIdx = -1;
	}

	const bool pathAwarePolicy =
		SwitchNode::PathAwareQpRoutingEnabled() &&
		!SwitchNode::PacketDlbRoutingEnabled() &&
		src / m_gpus_per_server != dst / m_gpus_per_server;
	const bool dynamic =
		SwitchNode::DynamicQpRoutingEnabled() &&
		!SwitchNode::PacketDlbRoutingEnabled() && v.size() > 1;
	uint32_t selected = v[qp->GetHash() % v.size()];
	uint32_t viableCandidates = static_cast<uint32_t>(v.size());
	uint64_t selectedActiveBytes = 0;
	uint64_t selectedActiveQps = 0;
	uint64_t selectedTxBytes =
		selected < tx_bytes.size() ? tx_bytes[selected] : 0;
	bool usedPathAware = false;
	bool usedStaticFallback = false;
	uint64_t selectedPathScoreNs = 0;
	uint64_t selectedPathQueueDelayNs = 0;
	uint64_t selectedPathPropagationNs = 0;
	uint64_t selectedPathReservedBytes = 0;
	uint32_t selectedPathHops = 0;
	uint64_t selectedPathSignature = 0;
	int32_t selectedDestinationNic = -1;
	const bool fixedSourceNic =
		SwitchNode::MultiQpPacketDlbRoutingEnabled() &&
		qp->m_sourceNicOrdinalHint != std::numeric_limits<uint32_t>::max();

	if (fixedSourceNic) {
		selected = std::numeric_limits<uint32_t>::max();
		viableCandidates = 0;
		const uint32_t start = qp->m_sourceNicOrdinalHint % v.size();
		for (uint32_t offset = 0; offset < v.size(); ++offset) {
			const int candidate = v[(start + offset) % v.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= m_nic.size() ||
				m_nic[candidate].dev == nullptr ||
				!m_nic[candidate].dev->IsLinkUp()) {
				continue;
			}
			++viableCandidates;
			if (selected == std::numeric_limits<uint32_t>::max()) {
				selected = static_cast<uint32_t>(candidate);
				qp->m_sourceNicHintFallback = offset != 0;
			}
		}
		NS_ASSERT_MSG(
			selected != std::numeric_limits<uint32_t>::max(),
			"Multi-QP packet DLB found no live source NIC");
		selectedTxBytes =
			selected < tx_bytes.size() ? tx_bytes[selected] : 0;
	}

	if (pathAwarePolicy) {
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		uint32_t pathSelected = std::numeric_limits<uint32_t>::max();
		PathAwareCandidate bestPath;
		long double bestScore = std::numeric_limits<long double>::infinity();
		viableCandidates = 0;
		const bool hasSourceNicHint =
			SwitchNode::DynamicChunkRoutingEnabled() &&
			qp->m_sourceNicOrdinalHint !=
				std::numeric_limits<uint32_t>::max();
		uint32_t start = hasSourceNicHint
			? qp->m_sourceNicOrdinalHint % v.size()
			: qp->GetHash() % v.size();
		uint32_t preferredP2rCandidate = 0;
		const bool adaptiveP2rSourceSelection =
			hasSourceNicHint &&
			SwitchNode::DisjointChunkRoutingEnabled() &&
			FindP2rPreferredSourceCandidate(
				*this,
				v,
				qp->m_sourceNicOrdinalHint,
				&preferredP2rCandidate);
			if (adaptiveP2rSourceSelection) {
				const auto preferred = std::find(
					v.begin(),
					v.end(),
					static_cast<int>(preferredP2rCandidate));
			if (preferred != v.end()) {
				start = static_cast<uint32_t>(
						std::distance(v.begin(), preferred));
				}
			}
			const bool capacityPinnedP2rSource =
				adaptiveP2rSourceSelection &&
				qp->GetInitialSize() >= kP2rCapacityPinnedChunkMinBytes;
			for (uint32_t offset = 0; offset < v.size(); ++offset) {
				// Large P2R chunks use worker ordinals as 4:2:2 capacity
				// tokens. Once the preferred family is reachable, keep that
				// token pinned while the selected family still chooses its
				// best internal path dynamically.
				if (capacityPinnedP2rSource &&
					pathSelected != std::numeric_limits<uint32_t>::max()) {
					break;
				}
				const int candidate = v[(start + offset) % v.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= m_nic.size() ||
				m_nic[candidate].dev == nullptr ||
				!m_nic[candidate].dev->IsLinkUp()) {
				continue;
			}
			PathAwareCandidate path;
			if (!BuildPolicyPathFromDevice(
					m_nic[candidate].dev,
					ip_to_node_id(qp->dip),
					qp->dip.Get(),
					qp->GetHash(),
					qp->GetInitialSize(),
					SwitchNode::DualTableRoutingEnabled(),
					&path,
					qp->m_sourceNicOrdinalHint)) {
				continue;
			}
			++viableCandidates;
			const bool pipelinedReservations =
				SwitchNode::AdaptiveZcubeRoutingEnabled();
			const long double score = PathScoreNs(
				path,
				qp->GetInitialSize(),
				pipelinedReservations);
			if (score < bestScore) {
				pathSelected = static_cast<uint32_t>(candidate);
				bestPath = std::move(path);
				bestScore = score;
			}
			if (hasSourceNicHint && !adaptiveP2rSourceSelection) {
				qp->m_sourceNicHintFallback = offset != 0;
				break;
			}
		}

		if (pathSelected != std::numeric_limits<uint32_t>::max()) {
			selected = pathSelected;
			if (adaptiveP2rSourceSelection) {
				qp->m_sourceNicHintFallback =
					selected != preferredP2rCandidate;
			}
			if (SwitchNode::DisjointChunkRoutingEnabled() &&
				m_nic[selected].dev != nullptr) {
				const P2rEndpointDevices p2r =
					ClassifyP2rEndpointDevices(
						GetGpuFabricDevices(m_nic[selected].dev->GetNode()));
				if (p2r.valid()) {
					const uint32_t pathFamilyParallelism =
						std::max<uint32_t>(
							1, qp->m_sourcePathParallelism);
					qp->m_sourcePathParallelism =
						m_nic[selected].dev == p2r.group1
							? pathFamilyParallelism
							: std::max<uint32_t>(
								1, pathFamilyParallelism / 2);
				}
			}
			selectedPathScoreNs = SaturatingNs(bestScore);
			selectedPathQueueDelayNs = SaturatingNs(
				SwitchNode::AdaptiveZcubeRoutingEnabled()
					? bestPath.maxEdgeWorkNs
					: bestPath.queueDelayNs);
			selectedPathPropagationNs = bestPath.propagationNs;
			selectedPathReservedBytes = bestPath.reservedBytes;
			selectedPathHops = static_cast<uint32_t>(bestPath.hops.size());
			selectedPathSignature = PathSignature(bestPath);
			selectedDestinationNic = PathDestinationNicIndex(bestPath);
			if (SwitchNode::DisjointChunkRoutingEnabled()) {
				ApplyActualPathWindow(this, qp, bestPath.hops, bestPath);
			}
			BindPathAwareRoute(qp, bestPath, qp->GetBytesLeft());
			usedPathAware = true;
		}
	}

	if (!usedPathAware && pathAwarePolicy &&
		SwitchNode::DualTableRoutingEnabled()) {
		// If the conditional path search cannot reach the destination (for
		// example after a link failure), fall back only to a live source NIC
		// from the original global SPF table. Picking an arbitrary live first
		// hop can bind the QP to a path whose downstream segment is broken.
		auto fallbackRoutes = m_rtTable.find(qp->dip.Get());
		if (fallbackRoutes != m_rtTable.end() && !fallbackRoutes->second.empty()) {
			const std::vector<int>& fallback = fallbackRoutes->second;
			const uint32_t start = qp->GetHash() % fallback.size();
			viableCandidates = 0;
			for (uint32_t offset = 0; offset < fallback.size(); ++offset) {
				const int candidate = fallback[(start + offset) % fallback.size()];
				if (candidate < 0 ||
					static_cast<uint32_t>(candidate) >= m_nic.size() ||
					m_nic[candidate].dev == nullptr ||
					!m_nic[candidate].dev->IsLinkUp()) {
					continue;
				}
				++viableCandidates;
				if (!usedStaticFallback) {
					selected = static_cast<uint32_t>(candidate);
					usedStaticFallback = true;
				}
			}
		}
		NS_ASSERT_MSG(
			usedStaticFallback,
			"Dual-table routing found no reachable source NIC");
	}

	if (!usedPathAware && !usedStaticFallback && dynamic) {
		selected = std::numeric_limits<uint32_t>::max();
		viableCandidates = 0;
		uint64_t bestActiveBytes = std::numeric_limits<uint64_t>::max();
		uint64_t bestActiveQps = std::numeric_limits<uint64_t>::max();
		uint64_t bestTxBytes = std::numeric_limits<uint64_t>::max();
		const uint32_t start = qp->GetHash() % v.size();
		for (uint32_t offset = 0; offset < v.size(); ++offset) {
			const int candidate = v[(start + offset) % v.size()];
			if (candidate < 0 ||
				static_cast<uint32_t>(candidate) >= m_nic.size() ||
				m_nic[candidate].dev == nullptr ||
				!m_nic[candidate].dev->IsLinkUp()) {
				continue;
			}
			++viableCandidates;
			uint64_t activeBytes = 0;
			uint64_t activeQps = 0;
			Ptr<RdmaQueuePairGroup> group = m_nic[candidate].qpGrp;
			if (group != nullptr) {
				for (uint32_t index = 0; index < group->GetN(); ++index) {
					Ptr<RdmaQueuePair> activeQp = group->Get(index);
					const uint64_t outstanding =
						activeQp->GetBytesLeft() + activeQp->GetOnTheFly();
					if (outstanding > 0) {
						activeBytes += outstanding;
						++activeQps;
					}
				}
			}
			const uint64_t sentBytes =
				static_cast<uint32_t>(candidate) < tx_bytes.size()
					? tx_bytes[candidate]
					: 0;
			const bool better =
				activeBytes < bestActiveBytes ||
				(activeBytes == bestActiveBytes && activeQps < bestActiveQps) ||
				(activeBytes == bestActiveBytes && activeQps == bestActiveQps &&
				 sentBytes < bestTxBytes);
			if (better) {
				selected = static_cast<uint32_t>(candidate);
				bestActiveBytes = activeBytes;
				bestActiveQps = activeQps;
				bestTxBytes = sentBytes;
			}
		}
		NS_ASSERT_MSG(
			selected != std::numeric_limits<uint32_t>::max(),
			"Dynamic QP routing found no live NIC");
		selectedActiveBytes = bestActiveBytes;
		selectedActiveQps = bestActiveQps;
		selectedTxBytes = bestTxBytes;
	}

	if (usedPathAware) {
		Ptr<RdmaQueuePairGroup> group = m_nic[selected].qpGrp;
		if (group != nullptr) {
			for (uint32_t index = 0; index < group->GetN(); ++index) {
				Ptr<RdmaQueuePair> activeQp = group->Get(index);
				const uint64_t outstanding =
					activeQp->GetBytesLeft() + activeQp->GetOnTheFly();
				if (outstanding > 0) {
					selectedActiveBytes += outstanding;
					++selectedActiveQps;
				}
			}
		}
		selectedTxBytes = selected < tx_bytes.size() ? tx_bytes[selected] : 0;
	}

	if (qp->m_initialSelectedNicIdx < 0) {
		qp->m_initialSelectedNicIdx = static_cast<int32_t>(selected);
	} else if (qp->m_selectedNicIdx >= 0 &&
		qp->m_selectedNicIdx != static_cast<int32_t>(selected)) {
		++qp->m_nicReassignments;
	}
	qp->m_selectedNicIdx = static_cast<int32_t>(selected);
	qp->m_selectedDestinationNicIdx = selectedDestinationNic;
	qp->m_bindCandidateCount = viableCandidates;
	qp->m_bindPathHops = selectedPathHops;
	qp->m_bindPathScoreNs = selectedPathScoreNs;
	qp->m_bindPathQueueDelayNs = selectedPathQueueDelayNs;
	qp->m_bindPathPropagationNs = selectedPathPropagationNs;
	qp->m_bindPathReservedBytes = selectedPathReservedBytes;
	qp->m_bindPathSignature = selectedPathSignature;
	SwitchNode::RecordSourceQpBindingStats(
		fixedSourceNic || dynamic || usedPathAware,
		usedPathAware,
		src,
		selected,
		viableCandidates,
		selectedActiveBytes,
		selectedTxBytes,
		selectedActiveQps,
		qp->sip.Get(),
		qp->dip.Get(),
		qp->sport,
		qp->dport,
		qp->GetInitialSize(),
		selectedPathScoreNs,
		selectedPathQueueDelayNs,
		selectedPathPropagationNs,
		selectedPathReservedBytes,
		selectedPathHops);
	return selected;
}

uint32_t RdmaHw::SelectTxNic(
		Ptr<RdmaQueuePair> qp,
		uint32_t currentNic) {
	if (qp == nullptr || m_node == nullptr || m_node->GetNodeType() != 0 ||
		qp->m_src / m_gpus_per_server == qp->m_dest / m_gpus_per_server) {
		return currentNic;
	}
	if (SwitchNode::PacketDlbRoutingEnabled()) {
		return SelectPacketDlbTxNic(this, qp, currentNic);
	}
	if (SwitchNode::MultiQpPacketDlbRoutingEnabled()) {
		return SelectPacketDlbTxNic(this, qp, currentNic, true);
	}
	if (!SwitchNode::FlowletRoutingEnabled()) {
		return currentNic;
	}

	std::vector<int> dualTableCandidates;
	const std::vector<int>* candidateTable = nullptr;
	if (SwitchNode::DualTableRoutingEnabled()) {
		dualTableCandidates = GetDualTableSourceCandidates(*this);
		candidateTable = &dualTableCandidates;
	} else {
		auto routes = m_rtTable.find(qp->dip.Get());
		if (routes != m_rtTable.end()) {
			candidateTable = &routes->second;
		}
	}
	if (candidateTable == nullptr || candidateTable->size() <= 1) {
		return currentNic;
	}
	const std::vector<int>& candidates = *candidateTable;
	auto isLiveCandidate = [&](uint32_t candidate) {
		return std::find(
			candidates.begin(), candidates.end(), static_cast<int>(candidate)) !=
				candidates.end() &&
			candidate < m_nic.size() && m_nic[candidate].dev != nullptr &&
			m_nic[candidate].dev->IsLinkUp();
	};

	const bool currentLive = isLiveCandidate(currentNic);
	if (qp->m_sourceFlowletDecisionPending && currentLive) {
		return currentNic;
	}
	const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
	const uint64_t gapNs = SwitchNode::FlowletGapNs();
	const uint64_t maxBytes = SwitchNode::FlowletMaxBytes();
	const bool firstDecision = !qp->m_sourceFlowletInitialized;
	const bool gapTriggered =
		qp->m_sourceFlowletInitialized && qp->m_sourcePacketSent &&
		nowNs >= qp->m_sourceLastPacketNs &&
		nowNs - qp->m_sourceLastPacketNs >= gapNs;
	const bool byteTriggered =
		qp->m_sourceFlowletInitialized && maxBytes > 0 &&
		qp->snd_nxt >= qp->m_sourceNextByteBoundary;
	const bool linkTriggered = !currentLive;
	if (!firstDecision && !gapTriggered && !byteTriggered && !linkTriggered) {
		return currentNic;
	}
	if (firstDecision && currentLive) {
		qp->m_sourceFlowletInitialized = true;
		qp->m_sourceFlowletDecisionPending = true;
		qp->m_sourceFlowletId = 0;
		if (maxBytes > 0) {
			const uint64_t remainder = qp->snd_nxt % maxBytes;
			const uint64_t increment = maxBytes - remainder;
			qp->m_sourceNextByteBoundary =
				qp->snd_nxt > std::numeric_limits<uint64_t>::max() - increment
					? std::numeric_limits<uint64_t>::max()
					: qp->snd_nxt + increment;
		}
		return currentNic;
	}
	// A source-NIC migration is reorder-safe only after the previous flowlet
	// has been acknowledged. Link failure is the exception because staying on
	// the failed rail cannot make progress.
	if (!firstDecision && !linkTriggered && qp->GetOnTheFly() > 0) {
		return currentNic;
	}
	if (SwitchNode::DualTableRoutingEnabled()) {
		// Do not charge the QP's old reservation to itself while comparing the
		// two conditional shortest-path tables at a safe flowlet boundary.
		ReleasePathAwareRoute(qp);
	}

	const uint64_t flowletId = firstDecision
		? 0
		: (qp->m_sourceFlowletId == std::numeric_limits<uint64_t>::max()
			? qp->m_sourceFlowletId
			: qp->m_sourceFlowletId + 1);
	const uint64_t bytesLeft = qp->GetBytesLeft();
	const uint64_t scoreBytes = maxBytes > 0
		? std::min(maxBytes, bytesLeft)
		: std::min(static_cast<uint64_t>(m_mtu), bytesLeft);
	const uint32_t flowletHash = qp->GetHash() ^
		static_cast<uint32_t>(flowletId * 0x9e3779b97f4a7c15ULL);

	uint32_t selected = std::numeric_limits<uint32_t>::max();
	uint32_t viableCandidates = 0;
	PathAwareCandidate selectedPath;
	PathAwareCandidate currentPath;
	long double selectedScore = std::numeric_limits<long double>::infinity();
	long double currentScore = std::numeric_limits<long double>::infinity();
	{
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		const uint32_t start =
			(flowletHash + static_cast<uint32_t>(flowletId)) % candidates.size();
		for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
			const int candidateValue =
				candidates[(start + offset) % candidates.size()];
			if (candidateValue < 0) {
				continue;
			}
			const uint32_t candidate = static_cast<uint32_t>(candidateValue);
			if (!isLiveCandidate(candidate)) {
				continue;
			}
			PathAwareCandidate path;
			if (!BuildPolicyPathFromDevice(
					m_nic[candidate].dev,
					ip_to_node_id(qp->dip),
					qp->dip.Get(),
					flowletHash,
					scoreBytes,
					SwitchNode::DualTableRoutingEnabled(),
					&path)) {
				continue;
			}
			++viableCandidates;
			const long double score = PathScoreNs(path, scoreBytes);
			if (candidate == currentNic) {
				currentPath = path;
				currentScore = score;
			}
			if (score < selectedScore) {
				selected = candidate;
				selectedPath = std::move(path);
				selectedScore = score;
			}
		}
	}

	if (selected == std::numeric_limits<uint32_t>::max()) {
		// Drop back to a source NIC from the normal global SPF table. This
		// leaves downstream forwarding to the existing ECMP tables and avoids
		// choosing a merely link-up dual-table NIC with no reachable suffix.
		auto fallbackRoutes = m_rtTable.find(qp->dip.Get());
		if (fallbackRoutes != m_rtTable.end() && !fallbackRoutes->second.empty()) {
			const std::vector<int>& fallback = fallbackRoutes->second;
			const uint32_t start = flowletHash % fallback.size();
			viableCandidates = 0;
			for (uint32_t offset = 0; offset < fallback.size(); ++offset) {
				const int candidateValue =
					fallback[(start + offset) % fallback.size()];
				if (candidateValue < 0) {
					continue;
				}
				const uint32_t candidate =
					static_cast<uint32_t>(candidateValue);
				if (candidate >= m_nic.size() ||
					m_nic[candidate].dev == nullptr ||
					!m_nic[candidate].dev->IsLinkUp()) {
					continue;
				}
				++viableCandidates;
				if (selected == std::numeric_limits<uint32_t>::max() ||
					candidate == currentNic) {
					selected = candidate;
				}
			}
		}
		NS_ASSERT_MSG(
			selected != std::numeric_limits<uint32_t>::max(),
			"Dual-table routing found no reachable source NIC");
		selectedScore = 0;
	} else if (selected != currentNic && currentPath.valid &&
		selectedScore +
			static_cast<long double>(SwitchNode::FlowletHysteresisNs()) >=
			currentScore) {
		selected = currentNic;
		selectedPath = currentPath;
		selectedScore = currentScore;
	}

	if (SwitchNode::DualTableRoutingEnabled()) {
		if (selectedPath.valid) {
			RebindPathAwareRoute(
				qp, selectedPath, qp->GetBytesLeft());
		} else if (selected != currentNic) {
			ReleasePathAwareRoute(qp);
		}
	}

	if (qp->m_initialSelectedNicIdx < 0) {
		qp->m_initialSelectedNicIdx = static_cast<int32_t>(selected);
	} else if (qp->m_selectedNicIdx >= 0 &&
		qp->m_selectedNicIdx != static_cast<int32_t>(selected)) {
		++qp->m_nicReassignments;
	}
	qp->m_selectedNicIdx = static_cast<int32_t>(selected);
	qp->m_selectedDestinationNicIdx = selectedPath.valid
		? PathDestinationNicIndex(selectedPath)
		: -1;
	qp->m_bindCandidateCount = viableCandidates;
	qp->m_bindPathHops = static_cast<uint32_t>(selectedPath.hops.size());
	qp->m_bindPathScoreNs = SaturatingNs(selectedScore);
	qp->m_bindPathQueueDelayNs = SaturatingNs(
		SwitchNode::AdaptiveZcubeRoutingEnabled()
			? selectedPath.maxEdgeWorkNs
			: selectedPath.queueDelayNs);
	qp->m_bindPathPropagationNs = selectedPath.propagationNs;
	qp->m_bindPathReservedBytes = selectedPath.reservedBytes;
	qp->m_bindPathSignature = selectedPath.valid
		? PathSignature(selectedPath)
		: 0;
	qp->m_sourceFlowletInitialized = true;
	qp->m_sourceFlowletDecisionPending = true;
	qp->m_sourceFlowletId = flowletId;
	if (maxBytes > 0) {
		const uint64_t remainder = qp->snd_nxt % maxBytes;
		const uint64_t increment = maxBytes - remainder;
		qp->m_sourceNextByteBoundary =
			qp->snd_nxt > std::numeric_limits<uint64_t>::max() - increment
				? std::numeric_limits<uint64_t>::max()
				: qp->snd_nxt + increment;
	}

	const bool switched = selected != currentNic;
	const uint64_t selectedQueueBytes =
		selectedPath.queueBytes >
			std::numeric_limits<uint64_t>::max() - selectedPath.reservedBytes
			? std::numeric_limits<uint64_t>::max()
			: selectedPath.queueBytes + selectedPath.reservedBytes;
	SwitchNode::RecordSourceFlowletDecisionStats(
		m_node->GetId(),
		selected,
		viableCandidates,
		selectedQueueBytes,
		selected < tx_bytes.size() ? tx_bytes[selected] : 0,
		SaturatingNs(selectedScore),
		std::isfinite(currentScore) ? SaturatingNs(currentScore) : 0,
		SaturatingNs(selectedPath.queueDelayNs),
		selectedPath.propagationNs,
		selectedPath.reservedBytes,
		static_cast<uint32_t>(selectedPath.hops.size()),
		flowletId,
		nowNs,
		switched,
		gapTriggered,
		byteTriggered,
		linkTriggered,
		qp->sip.Get(),
		qp->dip.Get(),
		qp->sport,
		qp->dport);
	return selected;
}

uint64_t RdmaHw::GetQpKey(uint32_t dip, uint16_t sport, uint16_t pg){
	return ((uint64_t)dip << 32) | ((uint64_t)sport << 16) | (uint64_t)pg;
}
Ptr<RdmaQueuePair> RdmaHw::GetQp(uint32_t dip, uint16_t sport, uint16_t pg){
	uint64_t key = GetQpKey(dip, sport, pg);
	auto it = m_qpMap.find(key);
	if (it != m_qpMap.end())
		return it->second;
	return NULL;
}
void RdmaHw::AddQueuePair(uint32_t src, uint32_t dest, uint64_t tag, uint64_t size, uint16_t pg, Ipv4Address sip, Ipv4Address dip, uint16_t sport, uint16_t dport, uint32_t win, uint64_t baseRtt, uint32_t sourceNicOrdinalHint, uint32_t sourcePathParallelism, Callback<void> notifyAppFinish, Callback<void> notifyAppSent){
	// create qp
	Ptr<RdmaQueuePair> qp = CreateObject<RdmaQueuePair>(pg, sip, dip, sport, dport);
	qp->SetSrc(src);
	qp->SetDest(dest);
	qp->SetTag(tag);
	qp->SetSize(size);
	qp->SetInitialSize(size);
	qp->m_sourceNicOrdinalHint = sourceNicOrdinalHint;
	qp->m_sourcePathParallelism = std::max<uint32_t>(1, sourcePathParallelism);
	const bool packetDlbCrossServer =
		SwitchNode::PacketDlbRoutingEnabled() &&
		src / m_gpus_per_server != dest / m_gpus_per_server;
	qp->SetPacketDlbSelectiveCredit(
		packetDlbCrossServer &&
		SwitchNode::SwitchPacketDlbRoutingEnabled() &&
		PacketDlbSelectiveCreditEnabled());
	PacketDlbAggregateTransport packetDlbTransport;
	uint32_t effectiveWin = win;
	uint64_t effectiveBaseRtt = baseRtt;
	if (SwitchNode::PacketDlbRoutingEnabled() &&
		src / m_gpus_per_server != dest / m_gpus_per_server) {
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		packetDlbTransport = GetPacketDlbAggregateTransport(
			*this, dest, dip.Get(), m_mtu);
		if (win > 0 && packetDlbTransport.windowBytes > 0) {
			const uint32_t aggregateWin =
				packetDlbTransport.windowBytes >=
					std::numeric_limits<uint32_t>::max()
					? std::numeric_limits<uint32_t>::max()
					: static_cast<uint32_t>(
						packetDlbTransport.windowBytes);
			effectiveWin = std::max(effectiveWin, aggregateWin);
		}
		effectiveBaseRtt = std::max(
			effectiveBaseRtt, packetDlbTransport.maximumRttNs);
	}
	for (const auto& lane : packetDlbTransport.laneWindowBytes) {
		qp->SetPacketDlbLaneWindow(lane.first, lane.second);
	}
	qp->SetWin(effectiveWin);
	qp->SetBaseRtt(effectiveBaseRtt);
	qp->SetVarWin(m_var_win);
	qp->SetAppNotifyCallback(notifyAppFinish);
	qp->SetAppSentCallback(notifyAppSent);
	// add qp
	uint32_t nic_idx = GetNicIdxOfQp(qp);

	// std::cout << "src is: " << src << ", dst is: " << dest <<  ", nic_idx: " << nic_idx << ", and the m_nic size is: " << m_nic.size() << std::endl;
	// Assign the qp to specific qbbnetdevice
	m_nic[nic_idx].qpGrp->AddQp(qp);
	uint64_t key = GetQpKey(dip.Get(), sport, pg);
	m_qpMap[key] = qp;
	qp_cnp[key] = 0;
	last_qp_cnp[key] = 0;
	last_qp_rate[key] = 0;

	// set init variables
	DataRate m_bps = m_nic[nic_idx].dev->GetDataRate();
	if (packetDlbTransport.bitRate > 0) {
		m_bps = DataRate(packetDlbTransport.bitRate);
	}
	qp->m_rate = m_bps;
	qp->m_max_rate = m_bps;
	if (m_cc_mode == 1){
		qp->mlx.m_targetRate = m_bps;
	}else if (m_cc_mode == 3){
		qp->hp.m_curRate = m_bps;
		if (m_multipleRate){
			for (uint32_t i = 0; i < IntHeader::maxHop; i++)
				qp->hp.hopState[i].Rc = m_bps;
		}
	}else if (m_cc_mode == 7){
		qp->tmly.m_curRate = m_bps;
	}else if (m_cc_mode == 10){
		qp->hpccPint.m_curRate = m_bps;
	}
	// NVLS settings
	if(nvls_enable == 1) qp->nvls_enable = 1;
	else qp->nvls_enable = 0;
	// Notify Nic
	m_nic[nic_idx].dev->NewQp(qp);
}

void RdmaHw::DeleteQueuePair(Ptr<RdmaQueuePair> qp){
	CancelOutstandingPacketDlbRoutes(qp);
	ReleasePathAwareRoute(qp);
	// remove qp from the m_qpMap
	uint64_t key = GetQpKey(qp->dip.Get(), qp->sport, qp->m_pg);
	m_qpMap.erase(key);
	qp_cnp.erase(key);
	last_qp_cnp.erase(key);
	last_qp_rate.erase(key);
}

void RdmaHw::ReleasePathReservationBytes(Ptr<RdmaQueuePair> qp) {
	std::lock_guard<std::mutex> guard(PathReservationMutex());
	ReleasePathAwareReservationBytesLocked(qp);
}

Ptr<RdmaRxQueuePair> RdmaHw::GetRxQp(uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport, uint16_t pg, bool create){
    uint64_t key = ((uint64_t)dip << 32) | ((uint64_t)pg << 16) | (uint64_t)dport;
    #ifdef NS3_MTP
    MtpInterface::explicitCriticalSection cs;
    #endif
    auto it = m_rxQpMap.find(key);
    if (it != m_rxQpMap.end()){
        #ifdef NS3_MTP
        cs.ExitSection();
        #endif
        return it->second;
    }
    if (create){
        // create new rx qp
        Ptr<RdmaRxQueuePair> q = CreateObject<RdmaRxQueuePair>();
        // init the qp
        q->sip = sip;
        q->dip = dip;
        q->sport = sport;
        q->dport = dport;
        q->m_ecn_source.qIndex = pg;
        // store in map
        m_rxQpMap[key] = q;
        #ifdef NS3_MTP
        cs.ExitSection();
        #endif
        return q;
    }
    #ifdef NS3_MTP
    cs.ExitSection();
    #endif
    return NULL;
}

uint32_t RdmaHw::GetNicIdxOfRxQp(Ptr<RdmaRxQueuePair> q){
	// BUG就出现在这里了，首先要判断m_rtTable[q->dip]是否存在，若不存在就去判断m_rtTable_nxthop_nvswitch是否存在，如果都不存在，那么就输出错误
	// auto &v = m_rtTable[q->dip];

	if(m_rtTable.count(q->dip) != 0) {
		auto &v = m_rtTable[q->dip];
		if(v.size() > 0)
			return v[q->GetHash() % v.size()];
		else 
			NS_ASSERT_MSG(false, "We assume at least one NIC is alive");
	} else if(m_rtTable_nxthop_nvswitch.count(q->dip) != 0) {
		auto &v = m_rtTable_nxthop_nvswitch[q->dip];
		if(v.size() > 0)
			return v[q->GetHash() % v.size()];
		else 
			NS_ASSERT_MSG(false, "We assume at least one NIC is alive");
	} else {
		NS_ASSERT_MSG(false, "We assume at least one NIC is alive");
	}
	NS_ASSERT_MSG(false, "We assume at least one NIC is alive");
	
}
void RdmaHw::DeleteRxQp(uint32_t dip, uint16_t pg, uint16_t dport){
	uint64_t key = ((uint64_t)dip << 32) | ((uint64_t)pg << 16) | (uint64_t)dport;
	m_rxQpMap.erase(key);
}

int RdmaHw::SendPacketComplete(Ptr<Packet> p, CustomHeader &ch)
{
	uint16_t qIndex = ch.udp.pg;
	uint16_t port = ch.udp.sport;
	uint32_t seq = ch.udp.seq;
	// uint8_t cnp = (ch.flags >> qbbHeader::FLAG_CNP) & 1;
	// int i;
	Ptr<RdmaQueuePair> qp = GetQp(ch.dip, port, qIndex);
	if (qp == NULL)
	{
		return 0;
	}
	uint32_t nic_idx = GetNicIdxOfQp(qp);
	Ptr<QbbNetDevice> dev = m_nic[nic_idx].dev;
	SendComplete(qp);
}

void RdmaHw::SendComplete(Ptr<RdmaQueuePair> qp)
{
	NS_ASSERT(!m_sendCompleteCallback.IsNull());

	m_sendCompleteCallback(qp);
}

int RdmaHw::ReceiveUdp(Ptr<Packet> p, CustomHeader &ch){
	uint8_t ecnbits = ch.GetIpv4EcnBits();
	
	uint32_t payload_size = p->GetSize() - ch.GetSerializedSize();
	// TODO find corresponding rx queue pair
	Ptr<RdmaRxQueuePair> rxQp = GetRxQp(ch.dip, ch.sip, ch.udp.dport, ch.udp.sport, ch.udp.pg, true);
	if (ecnbits != 0){
		rxQp->m_ecn_source.ecnbits |= ecnbits;
		rxQp->m_ecn_source.qfb++;
	}
	rxQp->m_ecn_source.total++;
	rxQp->m_milestone_rx = m_ack_interval;

	const uint32_t sourceNode = (ch.sip >> 8) & 0xffff;
	const uint32_t destinationNode = (ch.dip >> 8) & 0xffff;
	const bool crossServer =
		sourceNode / m_gpus_per_server !=
		destinationNode / m_gpus_per_server;
	const bool selectiveCredit =
		PacketDlbSelectiveCreditEnabled() &&
		crossServer &&
		SwitchNode::SwitchPacketDlbRoutingEnabled();
	const uint64_t receivedBytesBefore =
		rxQp->m_packetDlbUniqueReceivedBytes;
	const bool arrivedOutOfOrder =
		ch.udp.seq > rxQp->ReceiverNextExpectedSeq;
	int x = ReceiverCheckSeq(ch.udp.seq, rxQp, payload_size);
	const uint64_t receivedBytesAfter =
		rxQp->m_packetDlbUniqueReceivedBytes;
	const uint64_t selectiveCreditBytes =
		receivedBytesAfter >= receivedBytesBefore
			? receivedBytesAfter - receivedBytesBefore
			: 0;
	if (x == 1 || x == 2 ||
		(selectiveCredit && selectiveCreditBytes > 0)){
		// Cumulative sequence progress and selective delivery credit are
		// intentionally independent.
		qbbHeader seqh;
		seqh.SetSeq(rxQp->ReceiverNextExpectedSeq);
		seqh.SetPG(ch.udp.pg);
		seqh.SetSport(ch.udp.dport);
		seqh.SetDport(ch.udp.sport);
		seqh.SetIntHeader(ch.udp.ih);
		if (ecnbits)
			seqh.SetCnp();

		Ptr<Packet> newp = Create<Packet>(std::max(60-14-20-(int)seqh.GetSerializedSize(), 0));
		if (selectiveCredit) {
			newp->AddPacketTag(PacketDlbCreditTag(
				receivedBytesAfter,
				ch.udp.seq,
				payload_size));
		}
		newp->AddHeader(seqh);

		Ipv4Header head;	// Prepare IPv4 header
		head.SetDestination(Ipv4Address(ch.sip));
		head.SetSource(Ipv4Address(ch.dip));
		head.SetProtocol(x == 2 ? 0xFD : 0xFC); //ack=0xFC nack=0xFD
		head.SetTtl(64);
		head.SetPayloadSize(newp->GetSize());
		head.SetIdentification(rxQp->m_ipid++);
		// GPU receives the packet and generate ACK with NVLS tag
		if(ch.m_tos == 4) head.SetTos(4);

		newp->AddHeader(head);
		AddHeader(newp, 0x800);	// Attach PPP header
		uint32_t sip = ch.sip;
		uint32_t sid = (sip >> 8) & 0xffff;
		uint32_t dip = ch.dip;
		uint32_t did = (dip >> 8) & 0xffff;
		// send
		uint32_t nic_idx = GetNicIdxOfRxQp(rxQp);
		m_nic[nic_idx].dev->RdmaEnqueueHighPrioQ(newp);
		// 发送给目标 NVSwitch 的报文
		if(did == m_node->GetId() && m_node->GetNodeType() == 2 && ch.m_tos == 4) m_nic[nic_idx].dev->SwitchAsHostSend();
		else m_nic[nic_idx].dev->TriggerTransmit();
			if (selectiveCredit && selectiveCreditBytes > 0) {
				SwitchNode::RecordPacketDlbSelectiveCredit(
				static_cast<uint32_t>(std::min<uint64_t>(
					selectiveCreditBytes,
					std::numeric_limits<uint32_t>::max())),
				arrivedOutOfOrder);
		}
	}
	return 0;
}

int RdmaHw::ReceiveCnp(Ptr<Packet> p, CustomHeader &ch){
	// QCN on NIC
	// This is a Congestion signal
	// Then, extract data from the congestion packet.
	// We assume, without verify, the packet is destinated to me
	uint32_t qIndex = ch.cnp.qIndex;
	if (qIndex == 1){		//DCTCP
		return 0;
	}
	uint16_t udpport = ch.cnp.fid; // corresponds to the sport
	uint8_t ecnbits = ch.cnp.ecnBits;
	uint16_t qfb = ch.cnp.qfb;
	uint16_t total = ch.cnp.total;

	uint32_t i;
	// get qp
	Ptr<RdmaQueuePair> qp = GetQp(ch.sip, udpport, qIndex);
	if (qp == NULL)
		std::cout << "ERROR: QCN NIC cannot find the flow\n";
	// get nic
	uint32_t nic_idx = GetNicIdxOfQp(qp);
	Ptr<QbbNetDevice> dev = m_nic[nic_idx].dev;

	if (qp->m_rate == 0)			//lazy initialization	
	{
		qp->m_rate = dev->GetDataRate();
		if (m_cc_mode == 1){
			qp->mlx.m_targetRate = dev->GetDataRate();
		}else if (m_cc_mode == 3){
			qp->hp.m_curRate = dev->GetDataRate();
			if (m_multipleRate){
				for (uint32_t i = 0; i < IntHeader::maxHop; i++)
					qp->hp.hopState[i].Rc = dev->GetDataRate();
			}
		}else if (m_cc_mode == 7){
			qp->tmly.m_curRate = dev->GetDataRate();
		}else if (m_cc_mode == 10){
			qp->hpccPint.m_curRate = dev->GetDataRate();
		}
	}
	return 0;
}

int RdmaHw::ReceiveAck(Ptr<Packet> p, CustomHeader &ch){
	uint16_t qIndex = ch.ack.pg;
	uint16_t port = ch.ack.dport;
	uint64_t seq = ch.ack.seq;
	uint8_t cnp = (ch.ack.flags >> qbbHeader::FLAG_CNP) & 1;


	int i;
	Ptr<RdmaQueuePair> qp = GetQp(ch.sip, port, qIndex);
	if (qp == NULL){
		return 0;
	}

	uint32_t nic_idx = GetNicIdxOfQp(qp);
	Ptr<QbbNetDevice> dev = m_nic[nic_idx].dev;
	if (m_ack_interval == 0)
		std::cout << "ERROR: shouldn't receive ack\n";
	else {
		if (qp->m_packetDlbSelectiveCredit && p != nullptr) {
			PacketDlbCreditTag creditTag;
			if (p->PeekPacketTag(creditTag)) {
				qp->AcknowledgeDelivered(
					creditTag.GetDeliveredBytes(),
					creditTag.GetReceivedSeq(),
					creditTag.GetReceivedBytes());
			}
		}
		if (!m_backto0){
			qp->Acknowledge(seq);
		}else {
			uint64_t goback_seq = seq / m_chunk * m_chunk;
			qp->Acknowledge(goback_seq);
		}
			if (SwitchNode::PacketDlbRoutingEnabled() ||
				SwitchNode::MultiQpPacketDlbRoutingEnabled()) {
				DiscardAcknowledgedPacketDlbRoutes(qp, qp->snd_una);
			}
		if (qp->IsFinished()){
			QpComplete(qp);
		}
	}
	if (ch.l3Prot == 0xFD) // NACK
		RecoverQueue(qp);

	// handle cnp
	if (cnp){
		uint64_t key = GetQpKey(qp->dip.Get(), qp->sport, qp->m_pg);
		qp_cnp[key]++; // update for the number of cnp this qp has received
		++qp->m_cnpCount;
		if (m_cc_mode == 1){ // mlx version
			cnp_received_mlx(qp);
		} 
	}

	if (m_cc_mode == 3){
		HandleAckHp(qp, p, ch);
	}else if (m_cc_mode == 7){
		HandleAckTimely(qp, p, ch);
	}else if (m_cc_mode == 8){
		HandleAckDctcp(qp, p, ch);
	}else if (m_cc_mode == 10){
		HandleAckHpPint(qp, p, ch);
	}
	uint32_t sip = ch.sip;
	uint32_t sid = (sip >> 8) & 0xffff;
	uint32_t dip = ch.dip;
	uint32_t did = (dip >> 8) & 0xffff;
	// ACK may advance the on-the-fly window, allowing more packets to send
	if(did == m_node->GetId() && m_node->GetNodeType() == 2) m_nic[nic_idx].dev->SwitchAsHostSend();
	else m_nic[nic_idx].dev->TriggerTransmit();
	//std:://cout << "ack triggere transmitted\n";
	return 0;
}

int RdmaHw::Receive(Ptr<Packet> p, CustomHeader &ch){
	if (ch.l3Prot == 0x11){ // UDP
		ReceiveUdp(p, ch);
	}else if (ch.l3Prot == 0xFF){ // CNP
		ReceiveCnp(p, ch);
	}else if (ch.l3Prot == 0xFD){ // NACK
		ReceiveAck(p, ch);
	}else if (ch.l3Prot == 0xFC){ // ACK
		ReceiveAck(p, ch);
	}
	return 0;
}

int RdmaHw::ReceiverCheckSeq(uint64_t seq, Ptr<RdmaRxQueuePair> q, uint32_t size){
	uint64_t expected = q->ReceiverNextExpectedSeq;
	if (SwitchNode::PacketDlbRoutingEnabled() ||
		SwitchNode::MultiQpPacketDlbRoutingEnabled()) {
		const bool selectiveCredit =
			PacketDlbSelectiveCreditEnabled();
		if (seq < expected) {
			SwitchNode::RecordPacketDlbReorderEvent(
				0, q->m_reorderBufferedBytes, 0, 0, true, false);
			return 3;
		}
		if (seq > expected) {
			const auto inserted =
				q->m_reorderSegments.emplace(seq, size);
			if (!inserted.second) {
				SwitchNode::RecordPacketDlbReorderEvent(
					0, q->m_reorderBufferedBytes, 0, 0, true, false);
				return 4;
			}
			if (selectiveCredit) {
				q->m_packetDlbUniqueReceivedBytes = SaturatingAdd(
					q->m_packetDlbUniqueReceivedBytes, size);
			}
			q->m_reorderBufferedBytes = SaturatingAdd(
				q->m_reorderBufferedBytes, size);
			q->m_reorderPeakBytes = std::max(
				q->m_reorderPeakBytes, q->m_reorderBufferedBytes);

			bool nack = false;
			if (q->m_nackTimer == Time(0)) {
				q->m_nackTimer =
					Simulator::Now() + MicroSeconds(m_nack_interval);
				q->m_lastNACK = expected;
			} else if (Simulator::Now() >= q->m_nackTimer &&
				q->m_lastNACK == expected) {
				q->m_nackTimer =
					Simulator::Now() + MicroSeconds(m_nack_interval);
				nack = true;
			}
			SwitchNode::RecordPacketDlbReorderEvent(
				size,
				q->m_reorderBufferedBytes,
				0,
				0,
				false,
				nack);
			return nack ? 2 : 5;
		}

		if (selectiveCredit) {
			q->m_packetDlbUniqueReceivedBytes = SaturatingAdd(
				q->m_packetDlbUniqueReceivedBytes, size);
		}
		q->ReceiverNextExpectedSeq = SaturatingAdd(expected, size);
		uint64_t drainedPackets = 0;
		uint64_t drainedBytes = 0;
		while (!q->m_reorderSegments.empty()) {
			auto next = q->m_reorderSegments.begin();
			if (next->first < q->ReceiverNextExpectedSeq) {
				q->m_reorderBufferedBytes =
					next->second >= q->m_reorderBufferedBytes
						? 0
						: q->m_reorderBufferedBytes - next->second;
				q->m_reorderSegments.erase(next);
				continue;
			}
			if (next->first != q->ReceiverNextExpectedSeq) {
				break;
			}
			q->ReceiverNextExpectedSeq = SaturatingAdd(
				q->ReceiverNextExpectedSeq, next->second);
			drainedBytes = SaturatingAdd(drainedBytes, next->second);
			++drainedPackets;
			q->m_reorderSegments.erase(next);
		}
		q->m_reorderBufferedBytes =
			drainedBytes >= q->m_reorderBufferedBytes
				? 0
				: q->m_reorderBufferedBytes - drainedBytes;
		if (q->m_reorderSegments.empty()) {
			q->m_nackTimer = Time(0);
		} else {
			q->m_nackTimer =
				Simulator::Now() + MicroSeconds(m_nack_interval);
			q->m_lastNACK = q->ReceiverNextExpectedSeq;
		}
		if (drainedPackets > 0) {
			SwitchNode::RecordPacketDlbReorderEvent(
				0,
				q->m_reorderBufferedBytes,
				drainedPackets,
				drainedBytes,
				false,
				false);
			return 1;
		}
		if (q->ReceiverNextExpectedSeq >= q->m_milestone_rx) {
			q->m_milestone_rx += m_ack_interval;
			return 1;
		}
		if (q->ReceiverNextExpectedSeq % m_chunk == 0) {
			return 1;
		}
		return 5;
	}

	if (seq == expected){
		q->ReceiverNextExpectedSeq = expected + size;
		if (q->ReceiverNextExpectedSeq >= q->m_milestone_rx){
			q->m_milestone_rx += m_ack_interval;
			return 1; //Generate ACK
		}else if (q->ReceiverNextExpectedSeq % m_chunk == 0){
			return 1;
		}else {
			return 5;
		}
	} else if (seq > expected) {
		// Generate NACK
		if (Simulator::Now() >= q->m_nackTimer || q->m_lastNACK != expected){
			q->m_nackTimer = Simulator::Now() + MicroSeconds(m_nack_interval);
			q->m_lastNACK = expected;
			if (m_backto0){
				q->ReceiverNextExpectedSeq = q->ReceiverNextExpectedSeq / m_chunk*m_chunk;
			}
			return 2;
		}else
			return 4;
	}else {
		// Duplicate. 
		return 3;
	}
}
void RdmaHw::AddHeader (Ptr<Packet> p, uint16_t protocolNumber){
	PppHeader ppp;
	ppp.SetProtocol (EtherToPpp (protocolNumber));
	p->AddHeader (ppp);
}
uint16_t RdmaHw::EtherToPpp (uint16_t proto){
	switch(proto){
		case 0x0800: return 0x0021;   //IPv4
		case 0x86DD: return 0x0057;   //IPv6
		default: NS_ASSERT_MSG (false, "PPP Protocol number not defined!");
	}
	return 0;
}

void RdmaHw::RecoverQueue(Ptr<RdmaQueuePair> qp){
	CancelOutstandingPacketDlbRoutes(qp);
	qp->snd_nxt = qp->snd_una;
}

void RdmaHw::QpComplete(Ptr<RdmaQueuePair> qp){
	NS_ASSERT(!m_qpCompleteCallback.IsNull());
	if (m_cc_mode == 1){
		Simulator::Cancel(qp->mlx.m_eventUpdateAlpha);
		Simulator::Cancel(qp->mlx.m_eventDecreaseRate);
		Simulator::Cancel(qp->mlx.m_rpTimer);
	}

	// This callback will log info
	// It may also delete the rxQp on the receiver
	m_qpCompleteCallback(qp);

	qp->m_notifyAppFinish();

	// delete the qp
	DeleteQueuePair(qp);
}

void RdmaHw::SetLinkDown(Ptr<QbbNetDevice> dev){
	printf("RdmaHw: node:%u a link down\n", m_node->GetId());
}

void RdmaHw::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx, bool is_nvswitch){
	uint32_t dip = dstAddr.Get();
	if(is_nvswitch == false)
		m_rtTable[dip].push_back(intf_idx);
	else {
		m_rtTable_nxthop_nvswitch[dip].push_back(intf_idx);
	}
}

void RdmaHw::ClearTable(){
	m_rtTable.clear();
	m_rtTable_nxthop_nvswitch.clear();
}

void RdmaHw::RedistributeQp(){
	// clear old qpGrp
	for (uint32_t i = 0; i < m_nic.size(); i++){
		if (m_nic[i].dev == NULL)
			continue;
		m_nic[i].qpGrp->Clear();
	}

	// redistribute qp
	for (auto &it : m_qpMap){
		Ptr<RdmaQueuePair> qp = it.second;
		uint32_t nic_idx = GetNicIdxOfQp(qp);
		m_nic[nic_idx].qpGrp->AddQp(qp);
		// Notify Nic
		m_nic[nic_idx].dev->ReassignedQp(qp);
	}
}

Ptr<Packet> RdmaHw::GetNxtPacket(Ptr<RdmaQueuePair> qp){
	uint64_t payload_size = qp->GetBytesLeft();
	if ((uint64_t)m_mtu < payload_size)
		payload_size = m_mtu;
	Ptr<Packet> p = Create<Packet> ((uint32_t)payload_size);
	// add SimpleSeqTsHeader
	SimpleSeqTsHeader seqTs;
	seqTs.SetSeq (qp->snd_nxt);
	seqTs.SetPG (qp->m_pg);
	p->AddHeader (seqTs);
	// add udp header
	UdpHeader udpHeader;
	udpHeader.SetDestinationPort (qp->dport);
	udpHeader.SetSourcePort (qp->sport);
	p->AddHeader (udpHeader);
	// add ipv4 header
	Ipv4Header ipHeader;
	ipHeader.SetSource (qp->sip);
	ipHeader.SetDestination (qp->dip);
	ipHeader.SetProtocol (0x11);
	ipHeader.SetPayloadSize (p->GetSize());
	ipHeader.SetTtl (64);
	// nvls <-> ToS, ToS = 1 -> NVLS enable
	if(qp->nvls_enable == 1) ipHeader.SetTos (4);
	else ipHeader.SetTos (0);
	ipHeader.SetIdentification (qp->m_ipid);
	p->AddHeader(ipHeader);
	// add ppp header
		PppHeader ppp;
		ppp.SetProtocol (0x0021); // EtherToPpp(0x800), see point-to-point-net-device.cc
		p->AddHeader (ppp);
		if (SwitchNode::SwitchPacketDlbRoutingEnabled() &&
			qp->m_selectedDestinationNicIdx >= 0) {
			p->AddPacketTag(PacketDlbEndpointTag(
				static_cast<uint32_t>(
					qp->m_selectedDestinationNicIdx)));
		}

		// update state
	if (qp->m_packetDlbSelectiveCredit &&
		qp->m_selectedNicIdx >= 0) {
		qp->RecordPacketDlbSend(
			qp->snd_nxt,
			static_cast<uint32_t>(payload_size),
			static_cast<uint32_t>(qp->m_selectedNicIdx));
	}
	qp->snd_nxt += payload_size;
	// std::cout << "current snd_nxt is: " << qp->snd_nxt << ", the window is: " << qp->m_win << std::endl;
	qp->m_ipid++;

	// return
	return p;
}

void RdmaHw::PktSent(
		Ptr<RdmaQueuePair> qp,
		Ptr<Packet> pkt,
		Time interframeGap,
		uint32_t sentNic){
	qp->lastPktSize = pkt->GetSize();
	const bool packetDlb = SwitchNode::PacketDlbRoutingEnabled();
	const bool fixedSourcePacketDlb =
		SwitchNode::MultiQpPacketDlbRoutingEnabled();
	const bool packetPathDlb = packetDlb || fixedSourcePacketDlb;
	const bool sameServer =
		qp->m_src / m_gpus_per_server ==
		qp->m_dest / m_gpus_per_server;
	if ((SwitchNode::FlowletRoutingEnabled() || packetPathDlb) &&
		m_node != nullptr &&
		m_node->GetNodeType() == 0) {
		if (packetPathDlb) {
			CommitPreparedPacketDlbRoute(qp);
		} else {
			qp->m_sourcePacketSent = true;
			qp->m_sourceFlowletDecisionPending = false;
			qp->m_sourceLastPacketNs = Simulator::Now().GetNanoSeconds();
		}
		if (qp->m_selectedNicIdx >= 0) {
			uint32_t candidateCount = 0;
			if (!sameServer && SwitchNode::DualTableRoutingEnabled()) {
				candidateCount = static_cast<uint32_t>(
					GetDualTableSourceCandidates(*this).size());
			} else {
				const auto& routeTable = sameServer
					? m_rtTable_nxthop_nvswitch
					: m_rtTable;
				auto routes = routeTable.find(qp->dip.Get());
				if (routes != routeTable.end()) {
					candidateCount =
						static_cast<uint32_t>(routes->second.size());
				}
			}
			SwitchNode::RecordSourceFlowletPacketStats(
				m_node->GetId(),
				static_cast<uint32_t>(qp->m_selectedNicIdx),
				candidateCount,
				qp->sip.Get(),
				qp->dip.Get(),
				qp->sport,
				qp->dport,
				pkt->GetSize());
		}
	}
	UpdateNextAvail(qp, interframeGap, pkt->GetSize(), sentNic);
	if (!packetDlb || sameServer || qp->GetBytesLeft() == 0 ||
		qp->IsWinBound()) {
		return;
	}

	NS_ASSERT_MSG(
		sentNic < m_nic.size() && m_nic[sentNic].dev != nullptr &&
			m_nic[sentNic].qpGrp != nullptr,
		"Packet DLB sent a packet through an invalid source NIC");
	const uint32_t nextNic = SelectPacketDlbTxNic(this, qp, sentNic);
	if (nextNic == sentNic) {
		return;
	}
	NS_ASSERT_MSG(
		nextNic < m_nic.size() && m_nic[nextNic].dev != nullptr &&
			m_nic[nextNic].dev->IsLinkUp() &&
			m_nic[nextNic].qpGrp != nullptr,
		"Packet DLB selected an unavailable source NIC for its next packet");
	NS_ASSERT_MSG(
		m_nic[sentNic].qpGrp->RemoveQp(qp),
		"Packet DLB QP is missing from its current NIC queue");
	m_nic[nextNic].qpGrp->AddQp(qp);
	Simulator::ScheduleNow(
		&QbbNetDevice::ReassignedQp, m_nic[nextNic].dev, qp);
}

void RdmaHw::UpdateNextAvail(
		Ptr<RdmaQueuePair> qp,
		Time interframeGap,
		uint32_t pkt_size,
		uint32_t sentNic){
	const bool packetDlbMultiLane =
		SwitchNode::PacketDlbRoutingEnabled() &&
		qp->m_src / m_gpus_per_server != qp->m_dest / m_gpus_per_server &&
		sentNic < m_nic.size() && m_nic[sentNic].dev != nullptr;
	if (packetDlbMultiLane) {
		const uint64_t physicalBitRate =
			m_nic[sentNic].dev->GetDataRate().GetBitRate();
		uint64_t effectiveBitRate = physicalBitRate;
		if (m_rateBound && qp->m_max_rate.GetBitRate() > 0) {
			const uint64_t aggregateRate = std::min(
				qp->m_rate.GetBitRate(), qp->m_max_rate.GetBitRate());
			const long double scaledRate =
				static_cast<long double>(physicalBitRate) *
				static_cast<long double>(aggregateRate) /
				static_cast<long double>(qp->m_max_rate.GetBitRate());
			effectiveBitRate = std::max<uint64_t>(
				1,
				std::min<uint64_t>(
					physicalBitRate,
					static_cast<uint64_t>(std::ceil(scaledRate))));
		}
		const Time sendingTime =
			interframeGap +
			DataRate(effectiveBitRate).CalculateBytesTxTime(pkt_size);
		const Time laneNextAvail = Simulator::Now() + sendingTime;
		Time& storedNextAvail =
			qp->m_packetDlbLaneNextAvail[sentNic];
		storedNextAvail = std::max(storedNextAvail, laneNextAvail);
		qp->m_nextAvail = storedNextAvail;
		return;
	}

	Time sendingTime;
	if (m_rateBound)
		sendingTime = interframeGap + qp->m_rate.CalculateBytesTxTime(pkt_size);
	else
		sendingTime = interframeGap + qp->m_max_rate.CalculateBytesTxTime(pkt_size);
	qp->m_nextAvail = Simulator::Now() + sendingTime;
}

void RdmaHw::ChangeRate(Ptr<RdmaQueuePair> qp, DataRate new_rate){
	if (SwitchNode::PacketDlbRoutingEnabled() &&
		qp->m_src / m_gpus_per_server != qp->m_dest / m_gpus_per_server) {
		qp->m_rate = new_rate;
		const uint32_t nic_idx = GetNicIdxOfQp(qp);
		const uint64_t nowNs = Simulator::Now().GetNanoSeconds();
		const uint64_t laneReadyNs =
			PacketDlbLaneNextAvailNs(qp, nic_idx, nowNs);
		qp->m_nextAvail =
			NanoSeconds(static_cast<int64_t>(laneReadyNs));
		m_nic[nic_idx].dev->UpdateNextAvail(qp->m_nextAvail);
		return;
	}

	#if 1
	Time sendingTime = qp->m_rate.CalculateBytesTxTime(qp->lastPktSize);
	Time new_sendintTime = new_rate.CalculateBytesTxTime(qp->lastPktSize);
	qp->m_nextAvail = qp->m_nextAvail + new_sendintTime - sendingTime;
	// update nic's next avail event
	uint32_t nic_idx = GetNicIdxOfQp(qp);
	m_nic[nic_idx].dev->UpdateNextAvail(qp->m_nextAvail);
	#endif

	// change to new rate
	qp->m_rate = new_rate;
}
/**
 * when nic send a packet, update the bytes it has sent
*/
void RdmaHw::UpdateTxBytes(uint32_t port_id, uint64_t bytes){
	tx_bytes[port_id] += bytes;
}
/**
 * output format:
 * time, host_id, port_id, bandwidth
*/
void RdmaHw::PrintHostBW(FILE* bw_output, uint32_t bw_mon_interval){
	for(int i = 0; i < m_nic.size(); ++i){
		if(tx_bytes[i] == last_tx_bytes[i]){
			continue;
		}
		double bw = (tx_bytes[i] - last_tx_bytes[i]) * 8 * 1e6 / (bw_mon_interval); // bit/s
		bw = bw*1.0 / 1e9; // Gbps
		fprintf(bw_output, "%lu, %u, %u, %f\n", Simulator::Now().GetTimeStep(), m_node->GetId(), i, bw);
		fflush(bw_output);
		last_tx_bytes[i] = tx_bytes[i];
	}
}
/**
 * output format:
 * time, src, dst, sport, dport, size, rate
*/
void RdmaHw::PrintQPRate(FILE* rate_output){
	std::unordered_map<uint64_t, Ptr<RdmaQueuePair>>::iterator it = m_qpMap.begin();
	for(; it != m_qpMap.end(); it++){
		Ptr<RdmaQueuePair> qp = it->second;
		uint64_t key = it->first;
		if(qp->m_rate.GetBitRate() == last_qp_rate[key]){
			continue;
		}
		fprintf(rate_output, "%lu, %u, %u, %u, %u, %u, %u\n", Simulator::Now().GetTimeStep(), qp->m_src, qp->m_dest, qp->sport, qp->dport, qp->m_size, qp->m_rate.GetBitRate());
		fflush(rate_output);
		last_qp_rate[key] = qp->m_rate.GetBitRate();
	}
}
/**
 * output format:
 * time, src, dst, sport, dport, size, cnp_number
*/
void RdmaHw::PrintQPCnpNumber(FILE* cnp_output){
	std::unordered_map<uint64_t, Ptr<RdmaQueuePair>>::iterator it = m_qpMap.begin();
	for(; it != m_qpMap.end(); it++){
		Ptr<RdmaQueuePair> qp = it->second;
		uint64_t key = it->first;
		if(qp_cnp[key] != last_qp_cnp[key]){
			fprintf(cnp_output, "%lu, %u, %u, %u, %u, %u, %u\n", Simulator::Now().GetTimeStep(), qp->m_src, qp->m_dest, qp->sport, qp->dport, qp->m_size, qp_cnp[key]);
			fflush(cnp_output);
			last_qp_cnp[key] = qp_cnp[key];
		}
	}
}
#define PRINT_LOG 0
/******************************
 * Mellanox's version of DCQCN
 *****************************/
void RdmaHw::UpdateAlphaMlx(Ptr<RdmaQueuePair> q){
	#if PRINT_LOG
	//printf("%lu alpha update: %08x %08x %u %u %.6lf->", Simulator::Now().GetTimeStep(), q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->mlx.m_alpha);
	#endif
	if (q->mlx.m_alpha_cnp_arrived){
		q->mlx.m_alpha = (1 - m_g)*q->mlx.m_alpha + m_g; 	//binary feedback
	}else {
		q->mlx.m_alpha = (1 - m_g)*q->mlx.m_alpha; 	//binary feedback
	}
	#if PRINT_LOG
	//printf("%.6lf\n", q->mlx.m_alpha);
	#endif
	q->mlx.m_alpha_cnp_arrived = false; // clear the CNP_arrived bit
	ScheduleUpdateAlphaMlx(q);
}
void RdmaHw::ScheduleUpdateAlphaMlx(Ptr<RdmaQueuePair> q){
	q->mlx.m_eventUpdateAlpha = Simulator::Schedule(MicroSeconds(m_alpha_resume_interval), &RdmaHw::UpdateAlphaMlx, this, q);
}

void RdmaHw::cnp_received_mlx(Ptr<RdmaQueuePair> q){
	q->mlx.m_alpha_cnp_arrived = true; // set CNP_arrived bit for alpha update
	q->mlx.m_decrease_cnp_arrived = true; // set CNP_arrived bit for rate decrease
	if (q->mlx.m_first_cnp){
		// init alpha
		q->mlx.m_alpha = 1;
		q->mlx.m_alpha_cnp_arrived = false;
		// schedule alpha update
		ScheduleUpdateAlphaMlx(q);
		// schedule rate decrease
		ScheduleDecreaseRateMlx(q, 1); // add 1 ns to make sure rate decrease is after alpha update
		// set rate on first CNP
		q->mlx.m_targetRate = q->m_rate = m_rateOnFirstCNP * q->m_rate;
		q->mlx.m_first_cnp = false;
	}
}

void RdmaHw::CheckRateDecreaseMlx(Ptr<RdmaQueuePair> q){
	ScheduleDecreaseRateMlx(q, 0);
	if (q->mlx.m_decrease_cnp_arrived){
		#if PRINT_LOG
		printf("%lu rate dec: %08x %08x %u %u (%0.3lf %.3lf)->", Simulator::Now().GetTimeStep(), q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
		#endif
		bool clamp = true;
		if (!m_EcnClampTgtRate){
			if (q->mlx.m_rpTimeStage == 0)
				clamp = false;
		}
		if (clamp)
			q->mlx.m_targetRate = q->m_rate;
		q->m_rate = std::max(m_minRate, q->m_rate * (1 - q->mlx.m_alpha / 2));
		// reset rate increase related things
		q->mlx.m_rpTimeStage = 0;
		q->mlx.m_decrease_cnp_arrived = false;
		Simulator::Cancel(q->mlx.m_rpTimer);
		q->mlx.m_rpTimer = Simulator::Schedule(MicroSeconds(m_rpgTimeReset), &RdmaHw::RateIncEventTimerMlx, this, q);
		#if PRINT_LOG
		printf("(%.3lf %.3lf)\n", q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
		#endif
	}
}
void RdmaHw::ScheduleDecreaseRateMlx(Ptr<RdmaQueuePair> q, uint32_t delta){
	q->mlx.m_eventDecreaseRate = Simulator::Schedule(MicroSeconds(m_rateDecreaseInterval) + NanoSeconds(delta), &RdmaHw::CheckRateDecreaseMlx, this, q);
}

void RdmaHw::RateIncEventTimerMlx(Ptr<RdmaQueuePair> q){
	q->mlx.m_rpTimer = Simulator::Schedule(MicroSeconds(m_rpgTimeReset), &RdmaHw::RateIncEventTimerMlx, this, q);
	RateIncEventMlx(q);
	q->mlx.m_rpTimeStage++;
}
void RdmaHw::RateIncEventMlx(Ptr<RdmaQueuePair> q){
	// check which increase phase: fast recovery, active increase, hyper increase
	if (q->mlx.m_rpTimeStage < m_rpgThreshold){ // fast recovery
		FastRecoveryMlx(q);
	}else if (q->mlx.m_rpTimeStage == m_rpgThreshold){ // active increase
		ActiveIncreaseMlx(q);
	}else { // hyper increase
		HyperIncreaseMlx(q);
	}
}

void RdmaHw::FastRecoveryMlx(Ptr<RdmaQueuePair> q){
	#if PRINT_LOG
	printf("%lu fast recovery: %08x %08x %u %u (%0.3lf %.3lf)->", Simulator::Now().GetTimeStep(), q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
	q->m_rate = (q->m_rate / 2) + (q->mlx.m_targetRate / 2);
	#if PRINT_LOG
	printf("(%.3lf %.3lf)\n", q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
}
void RdmaHw::ActiveIncreaseMlx(Ptr<RdmaQueuePair> q){
	#if PRINT_LOG
	printf("%lu active inc: %08x %08x %u %u (%0.3lf %.3lf)->", Simulator::Now().GetTimeStep(), q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
	// get NIC
	uint32_t nic_idx = GetNicIdxOfQp(q);
	Ptr<QbbNetDevice> dev = m_nic[nic_idx].dev;
	// increate rate
	q->mlx.m_targetRate += m_rai;
	if (q->mlx.m_targetRate > dev->GetDataRate())
		q->mlx.m_targetRate = dev->GetDataRate();
	q->m_rate = (q->m_rate / 2) + (q->mlx.m_targetRate / 2);
	#if PRINT_LOG
	printf("(%.3lf %.3lf)\n", q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
}
void RdmaHw::HyperIncreaseMlx(Ptr<RdmaQueuePair> q){
	#if PRINT_LOG
	printf("%lu hyper inc: %08x %08x %u %u (%0.3lf %.3lf)->", Simulator::Now().GetTimeStep(), q->sip.Get(), q->dip.Get(), q->sport, q->dport, q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
	// get NIC
	uint32_t nic_idx = GetNicIdxOfQp(q);
	Ptr<QbbNetDevice> dev = m_nic[nic_idx].dev;
	// increate rate
	q->mlx.m_targetRate += m_rhai;
	if (q->mlx.m_targetRate > dev->GetDataRate())
		q->mlx.m_targetRate = dev->GetDataRate();
	q->m_rate = (q->m_rate / 2) + (q->mlx.m_targetRate / 2);
	#if PRINT_LOG
	printf("(%.3lf %.3lf)\n", q->mlx.m_targetRate.GetBitRate() * 1e-9, q->m_rate.GetBitRate() * 1e-9);
	#endif
}

/***********************
 * High Precision CC
 ***********************/
void RdmaHw::HandleAckHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
	uint64_t ack_seq = ch.ack.seq;
	// update rate
	if (ack_seq > qp->hp.m_lastUpdateSeq){ // if full RTT feedback is ready, do full update
		UpdateRateHp(qp, p, ch, false);
	}else{ // do fast react
		FastReactHp(qp, p, ch);
	}
}

void RdmaHw::UpdateRateHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool fast_react){
	uint64_t next_seq = qp->snd_nxt;
	bool print = !fast_react || true;
	if (qp->hp.m_lastUpdateSeq == 0){ // first RTT
		qp->hp.m_lastUpdateSeq = next_seq;
		// store INT
		IntHeader &ih = ch.ack.ih;
		NS_ASSERT(ih.nhop <= IntHeader::maxHop);
		for (uint32_t i = 0; i < ih.nhop; i++)
			qp->hp.hop[i] = ih.hop[i];
		#if PRINT_LOG
		if (print){
			printf("%lu %s %08x %08x %u %u [%u,%u,%u]", Simulator::Now().GetTimeStep(), fast_react? "fast" : "update", qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport, qp->hp.m_lastUpdateSeq, ch.ack.seq, next_seq);
			for (uint32_t i = 0; i < ih.nhop; i++)
				printf(" %u %lu %lu", ih.hop[i].GetQlen(), ih.hop[i].GetBytes(), ih.hop[i].GetTime());
			printf("\n");
		}
		#endif
	}else {
		// check packet INT
		IntHeader &ih = ch.ack.ih;
		if (ih.nhop <= IntHeader::maxHop){
			double max_c = 0;
			bool inStable = false;
			#if PRINT_LOG
			if (print)
				printf("%lu %s %08x %08x %u %u [%u,%u,%u]", Simulator::Now().GetTimeStep(), fast_react? "fast" : "update", qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport, qp->hp.m_lastUpdateSeq, ch.ack.seq, next_seq);
			#endif
			// check each hop
			double U = 0;
			uint64_t dt = 0;
			bool updated[IntHeader::maxHop] = {false}, updated_any = false;
			NS_ASSERT(ih.nhop <= IntHeader::maxHop);
			for (uint32_t i = 0; i < ih.nhop; i++){
				if (m_sampleFeedback){
					if (ih.hop[i].GetQlen() == 0 && fast_react)
						continue;
				}
				updated[i] = updated_any = true;
				#if PRINT_LOG
				if (print)
					printf(" %u(%u) %lu(%lu) %lu(%lu)", ih.hop[i].GetQlen(), qp->hp.hop[i].GetQlen(), ih.hop[i].GetBytes(), qp->hp.hop[i].GetBytes(), ih.hop[i].GetTime(), qp->hp.hop[i].GetTime());
				#endif
				uint64_t tau = ih.hop[i].GetTimeDelta(qp->hp.hop[i]);;
				double duration = tau * 1e-9;
				double txRate = (ih.hop[i].GetBytesDelta(qp->hp.hop[i])) * 8 / duration;
				double u = txRate / ih.hop[i].GetLineRate() + (double)std::min(ih.hop[i].GetQlen(), qp->hp.hop[i].GetQlen()) * qp->m_max_rate.GetBitRate() / ih.hop[i].GetLineRate() /qp->m_win;
				#if PRINT_LOG
				if (print)
					printf(" %.3lf %.3lf", txRate, u);
				#endif
				if (!m_multipleRate){
					// for aggregate (single R)
					if (u > U){
						U = u;
						dt = tau;
					}
				}else {
					// for per hop (per hop R)
					if (tau > qp->m_baseRtt)
						tau = qp->m_baseRtt;
					qp->hp.hopState[i].u = (qp->hp.hopState[i].u * (qp->m_baseRtt - tau) + u * tau) / double(qp->m_baseRtt);
				}
				qp->hp.hop[i] = ih.hop[i];
			}

			DataRate new_rate;
			int32_t new_incStage;
			DataRate new_rate_per_hop[IntHeader::maxHop];
			int32_t new_incStage_per_hop[IntHeader::maxHop];
			if (!m_multipleRate){
				// for aggregate (single R)
				if (updated_any){
					if (dt > qp->m_baseRtt)
						dt = qp->m_baseRtt;
					qp->hp.u = (qp->hp.u * (qp->m_baseRtt - dt) + U * dt) / double(qp->m_baseRtt);
					max_c = qp->hp.u / m_targetUtil;

					if (max_c >= 1 || qp->hp.m_incStage >= m_miThresh){
						new_rate = qp->hp.m_curRate / max_c + m_rai;
						new_incStage = 0;
					}else{
						new_rate = qp->hp.m_curRate + m_rai;
						new_incStage = qp->hp.m_incStage+1;
					}
					if (new_rate < m_minRate)
						new_rate = m_minRate;
					if (new_rate > qp->m_max_rate)
						new_rate = qp->m_max_rate;
					#if PRINT_LOG
					if (print)
						printf(" u=%.6lf U=%.3lf dt=%u max_c=%.3lf", qp->hp.u, U, dt, max_c);
					#endif
					#if PRINT_LOG
					if (print)
						printf(" rate:%.3lf->%.3lf\n", qp->hp.m_curRate.GetBitRate()*1e-9, new_rate.GetBitRate()*1e-9);
					#endif
				}
			}else{
				// for per hop (per hop R)
				new_rate = qp->m_max_rate;
				for (uint32_t i = 0; i < ih.nhop; i++){
					if (updated[i]){
						double c = qp->hp.hopState[i].u / m_targetUtil;
						if (c >= 1 || qp->hp.hopState[i].incStage >= m_miThresh){
							new_rate_per_hop[i] = qp->hp.hopState[i].Rc / c + m_rai;
							new_incStage_per_hop[i] = 0;
						}else{
							new_rate_per_hop[i] = qp->hp.hopState[i].Rc + m_rai;
							new_incStage_per_hop[i] = qp->hp.hopState[i].incStage+1;
						}
						// bound rate
						if (new_rate_per_hop[i] < m_minRate)
							new_rate_per_hop[i] = m_minRate;
						if (new_rate_per_hop[i] > qp->m_max_rate)
							new_rate_per_hop[i] = qp->m_max_rate;
						// find min new_rate
						if (new_rate_per_hop[i] < new_rate)
							new_rate = new_rate_per_hop[i];
						#if PRINT_LOG
						if (print)
							printf(" [%u]u=%.6lf c=%.3lf", i, qp->hp.hopState[i].u, c);
						#endif
						#if PRINT_LOG
						if (print)
							printf(" %.3lf->%.3lf", qp->hp.hopState[i].Rc.GetBitRate()*1e-9, new_rate.GetBitRate()*1e-9);
						#endif
					}else{
						if (qp->hp.hopState[i].Rc < new_rate)
							new_rate = qp->hp.hopState[i].Rc;
					}
				}
				#if PRINT_LOG
				printf("\n");
				#endif
			}
			if (updated_any)
				ChangeRate(qp, new_rate);
			if (!fast_react){
				if (updated_any){
					qp->hp.m_curRate = new_rate;
					qp->hp.m_incStage = new_incStage;
				}
				if (m_multipleRate){
					// for per hop (per hop R)
					for (uint32_t i = 0; i < ih.nhop; i++){
						if (updated[i]){
							qp->hp.hopState[i].Rc = new_rate_per_hop[i];
							qp->hp.hopState[i].incStage = new_incStage_per_hop[i];
						}
					}
				}
			}
		}
		if (!fast_react){
			if (next_seq > qp->hp.m_lastUpdateSeq)
				qp->hp.m_lastUpdateSeq = next_seq; //+ rand() % 2 * m_mtu;
		}
	}
}

void RdmaHw::FastReactHp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
	if (m_fast_react)
		UpdateRateHp(qp, p, ch, true);
}

/**********************
 * TIMELY
 *********************/
void RdmaHw::HandleAckTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
	uint64_t ack_seq = ch.ack.seq;
	// update rate
	if (ack_seq > qp->tmly.m_lastUpdateSeq){ // if full RTT feedback is ready, do full update
		UpdateRateTimely(qp, p, ch, false);
	}else{ // do fast react
		FastReactTimely(qp, p, ch);
	}
}
void RdmaHw::UpdateRateTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool us){
	uint64_t next_seq = qp->snd_nxt;
	uint64_t rtt = Simulator::Now().GetTimeStep() - ch.ack.ih.ts;
	bool print = !us;
	if (qp->tmly.m_lastUpdateSeq != 0){ // not first RTT
		int64_t new_rtt_diff = (int64_t)rtt - (int64_t)qp->tmly.lastRtt;
		double rtt_diff = (1 - m_tmly_alpha) * qp->tmly.rttDiff + m_tmly_alpha * new_rtt_diff;
		double gradient = rtt_diff / m_tmly_minRtt;
		bool inc = false;
		double c = 0;
		#if PRINT_LOG
		if (print)
			printf("%lu node:%u rtt:%lu rttDiff:%.0lf gradient:%.3lf rate:%.3lf", Simulator::Now().GetTimeStep(), m_node->GetId(), rtt, rtt_diff, gradient, qp->tmly.m_curRate.GetBitRate() * 1e-9);
		#endif
		if (rtt < m_tmly_TLow){
			inc = true;
		}else if (rtt > m_tmly_THigh){
			c = 1 - m_tmly_beta * (1 - (double)m_tmly_THigh / rtt);
			inc = false;
		}else if (gradient <= 0){
			inc = true;
		}else{
			c = 1 - m_tmly_beta * gradient;
			if (c < 0)
				c = 0;
			inc = false;
		}
		if (inc){
			if (qp->tmly.m_incStage < 5){
				qp->m_rate = qp->tmly.m_curRate + m_rai;
			}else{
				qp->m_rate = qp->tmly.m_curRate + m_rhai;
			}
			if (qp->m_rate > qp->m_max_rate)
				qp->m_rate = qp->m_max_rate;
			if (!us){
				qp->tmly.m_curRate = qp->m_rate;
				qp->tmly.m_incStage++;
				qp->tmly.rttDiff = rtt_diff;
			}
		}else{
			qp->m_rate = std::max(m_minRate, qp->tmly.m_curRate * c);
			if (!us){
				qp->tmly.m_curRate = qp->m_rate;
				qp->tmly.m_incStage = 0;
				qp->tmly.rttDiff = rtt_diff;
			}
		}
		#if PRINT_LOG
		if (print){
			printf(" %c %.3lf\n", inc? '^':'v', qp->m_rate.GetBitRate() * 1e-9);
		}
		#endif
	}
	if (!us && next_seq > qp->tmly.m_lastUpdateSeq){
		qp->tmly.m_lastUpdateSeq = next_seq;
		// update
		qp->tmly.lastRtt = rtt;
	}
}
void RdmaHw::FastReactTimely(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
}

/**********************
 * DCTCP
 *********************/
void RdmaHw::HandleAckDctcp(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
	uint64_t ack_seq = ch.ack.seq;
	uint8_t cnp = (ch.ack.flags >> qbbHeader::FLAG_CNP) & 1;
	bool new_batch = false;

	// update alpha
	qp->dctcp.m_ecnCnt += (cnp > 0);
	if (ack_seq > qp->dctcp.m_lastUpdateSeq){ // if full RTT feedback is ready, do alpha update
		#if PRINT_LOG
		printf("%lu %s %08x %08x %u %u [%u,%u,%u] %.3lf->", Simulator::Now().GetTimeStep(), "alpha", qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport, qp->dctcp.m_lastUpdateSeq, ch.ack.seq, qp->snd_nxt, qp->dctcp.m_alpha);
		#endif
		new_batch = true;
		if (qp->dctcp.m_lastUpdateSeq == 0){ // first RTT
			qp->dctcp.m_lastUpdateSeq = qp->snd_nxt;
			qp->dctcp.m_batchSizeOfAlpha = qp->snd_nxt / m_mtu + 1;
		}else {
			double frac = std::min(1.0, double(qp->dctcp.m_ecnCnt) / qp->dctcp.m_batchSizeOfAlpha);
			qp->dctcp.m_alpha = (1 - m_g) * qp->dctcp.m_alpha + m_g * frac;
			qp->dctcp.m_lastUpdateSeq = qp->snd_nxt;
			qp->dctcp.m_ecnCnt = 0;
			qp->dctcp.m_batchSizeOfAlpha = (qp->snd_nxt - ack_seq) / m_mtu + 1;
			#if PRINT_LOG
			printf("%.3lf F:%.3lf", qp->dctcp.m_alpha, frac);
			#endif
		}
		#if PRINT_LOG
		printf("\n");
		#endif
	}

	// check cwr exit
	if (qp->dctcp.m_caState == 1){
		if (ack_seq > qp->dctcp.m_highSeq)
			qp->dctcp.m_caState = 0;
	}

	// check if need to reduce rate: ECN and not in CWR
	if (cnp && qp->dctcp.m_caState == 0){
		#if PRINT_LOG
		printf("%lu %s %08x %08x %u %u %.3lf->", Simulator::Now().GetTimeStep(), "rate", qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport, qp->m_rate.GetBitRate()*1e-9);
		#endif
		qp->m_rate = std::max(m_minRate, qp->m_rate * (1 - qp->dctcp.m_alpha / 2));
		#if PRINT_LOG
		printf("%.3lf\n", qp->m_rate.GetBitRate() * 1e-9);
		#endif
		qp->dctcp.m_caState = 1;
		qp->dctcp.m_highSeq = qp->snd_nxt;
	}

	// additive inc
	if (qp->dctcp.m_caState == 0 && new_batch)
		qp->m_rate = std::min(qp->m_max_rate, qp->m_rate + m_dctcp_rai);
}

/*********************
 * HPCC-PINT
 ********************/
void RdmaHw::SetPintSmplThresh(double p){
       pint_smpl_thresh = (uint32_t)(65536 * p);
}
void RdmaHw::HandleAckHpPint(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch){
       uint64_t ack_seq = ch.ack.seq;
       if (rand() % 65536 >= pint_smpl_thresh)
               return;
       // update rate
       if (ack_seq > qp->hpccPint.m_lastUpdateSeq){ // if full RTT feedback is ready, do full update
               UpdateRateHpPint(qp, p, ch, false);
       }else{ // do fast react
               UpdateRateHpPint(qp, p, ch, true);
       }
}

void RdmaHw::UpdateRateHpPint(Ptr<RdmaQueuePair> qp, Ptr<Packet> p, CustomHeader &ch, bool fast_react){
       uint64_t next_seq = qp->snd_nxt;
       if (qp->hpccPint.m_lastUpdateSeq == 0){ // first RTT
               qp->hpccPint.m_lastUpdateSeq = next_seq;
       }else {
               // check packet INT
               IntHeader &ih = ch.ack.ih;
               double U = Pint::decode_u(ih.GetPower());

               DataRate new_rate;
               int32_t new_incStage;
               double max_c = U / m_targetUtil;

               if (max_c >= 1 || qp->hpccPint.m_incStage >= m_miThresh){
                       new_rate = qp->hpccPint.m_curRate / max_c + m_rai;
                       new_incStage = 0;
               }else{
                       new_rate = qp->hpccPint.m_curRate + m_rai;
                       new_incStage = qp->hpccPint.m_incStage+1;
               }
               if (new_rate < m_minRate)
                       new_rate = m_minRate;
               if (new_rate > qp->m_max_rate)
                       new_rate = qp->m_max_rate;
               ChangeRate(qp, new_rate);
               if (!fast_react){
                       qp->hpccPint.m_curRate = new_rate;
                       qp->hpccPint.m_incStage = new_incStage;
               }
               if (!fast_react){
                       if (next_seq > qp->hpccPint.m_lastUpdateSeq)
                               qp->hpccPint.m_lastUpdateSeq = next_seq; //+ rand() % 2 * m_mtu;
               }
       }
}

}
