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
#include "qbb-channel.h"
#include "switch-node.h"
#ifdef NS3_MTP
#include "ns3/mtp-interface.h"
#endif
#include <algorithm>
#include <cmath>
#include <iostream>	// debug
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ns3{

namespace {

constexpr uint32_t kMaxPathAwareHops = 32;
constexpr uint32_t kMaxPacketDlbPathsPerNic = 64;

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
bool HasLivePacketDlbPath(
	Ptr<QbbNetDevice> sourceDevice,
	uint32_t destinationNode,
	uint32_t dip);

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
		Ptr<QbbNetDevice>* peerDeviceOut = nullptr) {
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
	path->hops.push_back(device);
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

uint64_t GetPacketDlbAggregateSourceBitRate(
		const RdmaHw& hw,
		uint32_t destinationNode,
		uint32_t dip) {
	uint64_t aggregateBitRate = 0;
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
		if (!HasLivePacketDlbPath(
				hw.m_nic[candidate].dev, destinationNode, dip)) {
			continue;
		}
		aggregateBitRate = SaturatingAdd(
			aggregateBitRate,
			hw.m_nic[candidate].dev->GetDataRate().GetBitRate());
	}
	return aggregateBitRate;
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

bool HasLivePacketDlbPath(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNode,
		uint32_t dip) {
	const PacketDlbPathTemplates& paths = GetPacketDlbPathTemplates(
		sourceDevice, destinationNode, dip);
	for (const PacketDlbPathTemplate& path : paths) {
		if (std::all_of(
				path.begin(),
				path.end(),
				[](const Ptr<QbbNetDevice>& device) {
					return device != nullptr && device->IsLinkUp();
				})) {
			return true;
		}
	}
	return false;
}

bool EvaluatePacketDlbPath(
		const PacketDlbPathTemplate& pathTemplate,
		PathAwareCandidate* path) {
	if (path == nullptr || pathTemplate.empty()) {
		return false;
	}
	PathAwareCandidate candidate;
	for (const Ptr<QbbNetDevice>& device : pathTemplate) {
		if (!AppendPathHop(device, &candidate)) {
			return false;
		}
	}
	*path = std::move(candidate);
	return true;
}

void ReservePacketDlbPath(
		const PathAwareCandidate& path,
		uint64_t packetBytes) {
	uint64_t cursorNs = Simulator::Now().GetNanoSeconds();
	for (const Ptr<QbbNetDevice>& device : path.hops) {
		if (device == nullptr) {
			continue;
		}
		const uint64_t bitRate = device->GetDataRate().GetBitRate();
		const uint64_t serviceNs = SerializationNs(packetBytes, bitRate);
		uint64_t& finishNs =
			PacketDlbVirtualFinishNs()[PathEdgeKey(device)];
		const uint64_t startNs = std::max(cursorNs, finishNs);
		finishNs = SaturatingAdd(startNs, serviceNs);
		Ptr<QbbChannel> channel =
			DynamicCast<QbbChannel>(device->GetChannel());
		const uint64_t propagationNs = channel == nullptr
			? 0
			: channel->GetDelay().GetNanoSeconds();
		cursorNs = SaturatingAdd(finishNs, propagationNs);
	}
}

uint32_t PacketDlbHash(uint32_t qpHash, uint64_t seq) {
	uint64_t mixed = seq + 0x9e3779b97f4a7c15ULL;
	mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
	mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
	mixed ^= mixed >> 31;
	return qpHash ^ static_cast<uint32_t>(mixed) ^
		static_cast<uint32_t>(mixed >> 32);
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

bool BuildAdaptiveDualTablePath(
		Ptr<QbbNetDevice> sourceDevice,
		uint32_t destinationNodeId,
		uint32_t qpHash,
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
	PathAwareCandidate bestCanonical;
	long double bestScore = std::numeric_limits<long double>::infinity();
	long double bestCanonicalScore =
		std::numeric_limits<long double>::infinity();
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
			bestCanonical = std::move(path);
			bestCanonicalScore = score;
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
		PathAwareCandidate* result) {
	if (crossPairedDualTable) {
		if (SwitchNode::AdaptiveZcubeRoutingEnabled() &&
			BuildAdaptiveDualTablePath(
				device,
				destinationNode,
				qpHash,
				qpBytes,
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

uint32_t SelectPacketDlbTxNic(
		RdmaHw* hw,
		Ptr<RdmaQueuePair> qp,
		uint32_t currentNic) {
	std::vector<int> fabricCandidates = GetDualTableSourceCandidates(*hw);
	const std::vector<int>* candidateTable = &fabricCandidates;
	if (fabricCandidates.empty()) {
		auto routes = hw->m_rtTable.find(qp->dip.Get());
		if (routes == hw->m_rtTable.end()) {
			return currentNic;
		}
		candidateTable = &routes->second;
	}
	if (candidateTable->size() <= 1) {
		return currentNic;
	}
	const std::vector<int>& candidates = *candidateTable;
	auto isLiveCandidate = [&](uint32_t candidate) {
		return std::find(
			candidates.begin(),
			candidates.end(),
			static_cast<int>(candidate)) != candidates.end() &&
			candidate < hw->m_nic.size() &&
			hw->m_nic[candidate].dev != nullptr &&
			hw->m_nic[candidate].dev->IsLinkUp();
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
	std::vector<long double> sourceServiceNs(
		hw->m_nic.size(), std::numeric_limits<long double>::infinity());
	long double minimumSourceServiceNs =
		std::numeric_limits<long double>::infinity();
	for (const int candidateValue : candidates) {
		if (candidateValue < 0) {
			continue;
		}
		const uint32_t candidate =
			static_cast<uint32_t>(candidateValue);
		if (!isLiveCandidate(candidate)) {
			continue;
		}
		const uint64_t bitRate =
			hw->m_nic[candidate].dev->GetDataRate().GetBitRate();
		if (bitRate == 0) {
			continue;
		}
		const uint64_t sentBytes = candidate < hw->tx_bytes.size()
			? hw->tx_bytes[candidate]
			: 0;
		sourceServiceNs[candidate] =
			static_cast<long double>(sentBytes) * 8.0L * 1000000000.0L /
			static_cast<long double>(bitRate);
		minimumSourceServiceNs = std::min(
			minimumSourceServiceNs, sourceServiceNs[candidate]);
	}
	if (!std::isfinite(minimumSourceServiceNs)) {
		minimumSourceServiceNs = 0;
	}

	uint32_t selected = std::numeric_limits<uint32_t>::max();
	uint32_t viableCandidates = 0;
	PathAwareCandidate selectedPath;
	long double selectedScore = std::numeric_limits<long double>::infinity();
	long double currentScore = std::numeric_limits<long double>::infinity();
	{
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		const uint32_t start = packetHash % candidates.size();
		for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
			const int candidateValue =
				candidates[(start + offset) % candidates.size()];
			if (candidateValue < 0) {
				continue;
			}
			const uint32_t candidate =
				static_cast<uint32_t>(candidateValue);
			if (!isLiveCandidate(candidate)) {
				continue;
			}

			const PacketDlbPathTemplates& pathTemplates =
				GetPacketDlbPathTemplates(
					hw->m_nic[candidate].dev,
					destinationNode,
					qp->dip.Get());
			bool sourceReachable = false;
			long double sourceBestScore =
				std::numeric_limits<long double>::infinity();
			const uint32_t pathStart = pathTemplates.empty()
				? 0
				: packetHash % pathTemplates.size();
			for (uint32_t pathOffset = 0;
				 pathOffset < pathTemplates.size();
				 ++pathOffset) {
				const PacketDlbPathTemplate& pathTemplate =
					pathTemplates[
						(pathStart + pathOffset) % pathTemplates.size()];
				PathAwareCandidate path;
				if (!EvaluatePacketDlbPath(pathTemplate, &path)) {
					continue;
				}
				sourceReachable = true;
				const long double serviceSkewNs =
					std::isfinite(sourceServiceNs[candidate])
						? sourceServiceNs[candidate] -
							minimumSourceServiceNs
						: 0;
				const long double score =
					PathScoreNs(path, packetBytes, true) +
					serviceSkewNs;
				sourceBestScore = std::min(sourceBestScore, score);
				if (score < selectedScore) {
					selected = candidate;
					selectedPath = std::move(path);
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

		if (selected != std::numeric_limits<uint32_t>::max() &&
			selectedPath.valid) {
			ReservePacketDlbPath(selectedPath, packetBytes);
			BindPacketDlbPath(qp, selectedPath, seq);
		}
	}

	if (selected == std::numeric_limits<uint32_t>::max()) {
		const uint32_t start = packetHash % candidates.size();
		for (uint32_t offset = 0; offset < candidates.size(); ++offset) {
			const int candidateValue =
				candidates[(start + offset) % candidates.size()];
			if (candidateValue < 0) {
				continue;
			}
			const uint32_t candidate =
				static_cast<uint32_t>(candidateValue);
			if (isLiveCandidate(candidate)) {
				selected = candidate;
				++viableCandidates;
				break;
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
	} else if (SwitchNode::DualTableRoutingEnabled()) {
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
		if (SwitchNode::PacketDlbRoutingEnabled()) {
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
		const uint32_t start = hasSourceNicHint
			? qp->m_sourceNicOrdinalHint % v.size()
			: qp->GetHash() % v.size();
		for (uint32_t offset = 0; offset < v.size(); ++offset) {
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
					&path)) {
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
			if (hasSourceNicHint) {
				qp->m_sourceNicHintFallback = offset != 0;
				break;
			}
		}

		if (pathSelected != std::numeric_limits<uint32_t>::max()) {
			selected = pathSelected;
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
		dynamic || usedPathAware,
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
void RdmaHw::AddQueuePair(uint32_t src, uint32_t dest, uint64_t tag, uint64_t size, uint16_t pg, Ipv4Address sip, Ipv4Address dip, uint16_t sport, uint16_t dport, uint32_t win, uint64_t baseRtt, uint32_t sourceNicOrdinalHint, Callback<void> notifyAppFinish, Callback<void> notifyAppSent){
	// create qp
	Ptr<RdmaQueuePair> qp = CreateObject<RdmaQueuePair>(pg, sip, dip, sport, dport);
	qp->SetSrc(src);
	qp->SetDest(dest);
	qp->SetTag(tag);
	qp->SetSize(size);
	qp->SetInitialSize(size);
	qp->m_sourceNicOrdinalHint = sourceNicOrdinalHint;
	uint64_t packetDlbAggregateBitRate = 0;
	uint32_t effectiveWin = win;
	if (SwitchNode::PacketDlbRoutingEnabled() &&
		src / m_gpus_per_server != dest / m_gpus_per_server) {
		std::lock_guard<std::mutex> guard(PathReservationMutex());
		packetDlbAggregateBitRate = GetPacketDlbAggregateSourceBitRate(
			*this, dest, dip.Get());
		if (win > 0 && baseRtt > 0 && packetDlbAggregateBitRate > 0) {
			const long double aggregateBdp =
				static_cast<long double>(baseRtt) *
				static_cast<long double>(packetDlbAggregateBitRate) /
				8.0L / 1000000000.0L;
			const long double maximumWindow =
				static_cast<long double>(
					std::numeric_limits<uint32_t>::max());
			const uint32_t aggregateWin = aggregateBdp >= maximumWindow
				? std::numeric_limits<uint32_t>::max()
				: static_cast<uint32_t>(std::ceil(aggregateBdp));
			effectiveWin = std::max(effectiveWin, aggregateWin);
		}
	}
	qp->SetWin(effectiveWin);
	qp->SetBaseRtt(baseRtt);
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
	if (packetDlbAggregateBitRate > 0) {
		m_bps = DataRate(packetDlbAggregateBitRate);
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

	int x = ReceiverCheckSeq(ch.udp.seq, rxQp, payload_size);
	if (x == 1 || x == 2){ //generate ACK or NACK
		qbbHeader seqh;
		seqh.SetSeq(rxQp->ReceiverNextExpectedSeq);
		seqh.SetPG(ch.udp.pg);
		seqh.SetSport(ch.udp.dport);
		seqh.SetDport(ch.udp.sport);
		seqh.SetIntHeader(ch.udp.ih);
		if (ecnbits)
			seqh.SetCnp();

		Ptr<Packet> newp = Create<Packet>(std::max(60-14-20-(int)seqh.GetSerializedSize(), 0));
		newp->AddHeader(seqh);

		Ipv4Header head;	// Prepare IPv4 header
		head.SetDestination(Ipv4Address(ch.sip));
		head.SetSource(Ipv4Address(ch.dip));
		head.SetProtocol(x == 1 ? 0xFC : 0xFD); //ack=0xFC nack=0xFD
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
		if (!m_backto0){
			qp->Acknowledge(seq);
		}else {
			uint64_t goback_seq = seq / m_chunk * m_chunk;
			qp->Acknowledge(goback_seq);
		}
		if (SwitchNode::PacketDlbRoutingEnabled()) {
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
	if (SwitchNode::PacketDlbRoutingEnabled()) {
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

	// update state
	qp->snd_nxt += payload_size;
	// std::cout << "current snd_nxt is: " << qp->snd_nxt << ", the window is: " << qp->m_win << std::endl;
	qp->m_ipid++;

	// return
	return p;
}

void RdmaHw::PktSent(Ptr<RdmaQueuePair> qp, Ptr<Packet> pkt, Time interframeGap){
	qp->lastPktSize = pkt->GetSize();
	const bool packetDlb = SwitchNode::PacketDlbRoutingEnabled();
	const int32_t sentNicIdx = qp->m_selectedNicIdx;
	if ((SwitchNode::FlowletRoutingEnabled() || packetDlb) &&
		m_node != nullptr &&
		m_node->GetNodeType() == 0) {
		if (packetDlb) {
			CommitPreparedPacketDlbRoute(qp);
		} else {
			qp->m_sourcePacketSent = true;
			qp->m_sourceFlowletDecisionPending = false;
			qp->m_sourceLastPacketNs = Simulator::Now().GetNanoSeconds();
		}
		if (qp->m_selectedNicIdx >= 0) {
			uint32_t candidateCount = 0;
			const bool sameServer =
				qp->m_src / m_gpus_per_server ==
				qp->m_dest / m_gpus_per_server;
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
	UpdateNextAvail(qp, interframeGap, pkt->GetSize());
	if (!packetDlb || sentNicIdx < 0 || qp->GetBytesLeft() == 0 ||
		qp->IsWinBound()) {
		return;
	}

	const uint32_t sentNic = static_cast<uint32_t>(sentNicIdx);
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

void RdmaHw::UpdateNextAvail(Ptr<RdmaQueuePair> qp, Time interframeGap, uint32_t pkt_size){
	Time sendingTime;
	if (m_rateBound)
		sendingTime = interframeGap + qp->m_rate.CalculateBytesTxTime(pkt_size);
	else
		sendingTime = interframeGap + qp->m_max_rate.CalculateBytesTxTime(pkt_size);
	qp->m_nextAvail = Simulator::Now() + sendingTime;
}

void RdmaHw::ChangeRate(Ptr<RdmaQueuePair> qp, DataRate new_rate){
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
