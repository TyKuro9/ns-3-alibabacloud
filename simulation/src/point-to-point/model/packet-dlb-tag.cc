#include "packet-dlb-tag.h"

#include "ns3/log.h"

#include <limits>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("PacketDlbEndpointTag");
NS_OBJECT_ENSURE_REGISTERED(PacketDlbEndpointTag);
NS_OBJECT_ENSURE_REGISTERED(PacketDlbCreditTag);

TypeId PacketDlbEndpointTag::GetTypeId() {
	static TypeId typeId = TypeId("ns3::PacketDlbEndpointTag")
		.SetParent<Tag>()
		.SetGroupName("PointToPoint")
		.AddConstructor<PacketDlbEndpointTag>();
	return typeId;
}

TypeId PacketDlbEndpointTag::GetInstanceTypeId() const {
	return GetTypeId();
}

uint32_t PacketDlbEndpointTag::GetSerializedSize() const {
	return 4;
}

void PacketDlbEndpointTag::Serialize(TagBuffer buffer) const {
	buffer.WriteU32(m_destinationNic);
}

void PacketDlbEndpointTag::Deserialize(TagBuffer buffer) {
	m_destinationNic = buffer.ReadU32();
}

void PacketDlbEndpointTag::Print(std::ostream& stream) const {
	stream << "destinationNic=" << m_destinationNic;
}

PacketDlbEndpointTag::PacketDlbEndpointTag()
	: m_destinationNic(std::numeric_limits<uint32_t>::max()) {}

PacketDlbEndpointTag::PacketDlbEndpointTag(uint32_t destinationNic)
	: m_destinationNic(destinationNic) {}

uint32_t PacketDlbEndpointTag::GetDestinationNic() const {
	return m_destinationNic;
}

TypeId PacketDlbCreditTag::GetTypeId() {
	static TypeId typeId = TypeId("ns3::PacketDlbCreditTag")
		.SetParent<Tag>()
		.SetGroupName("PointToPoint")
		.AddConstructor<PacketDlbCreditTag>();
	return typeId;
}

TypeId PacketDlbCreditTag::GetInstanceTypeId() const {
	return GetTypeId();
}

uint32_t PacketDlbCreditTag::GetSerializedSize() const {
	return 20;
}

void PacketDlbCreditTag::Serialize(TagBuffer buffer) const {
	buffer.WriteU64(m_deliveredBytes);
	buffer.WriteU64(m_receivedSeq);
	buffer.WriteU32(m_receivedBytes);
}

void PacketDlbCreditTag::Deserialize(TagBuffer buffer) {
	m_deliveredBytes = buffer.ReadU64();
	m_receivedSeq = buffer.ReadU64();
	m_receivedBytes = buffer.ReadU32();
}

void PacketDlbCreditTag::Print(std::ostream& stream) const {
	stream << "deliveredBytes=" << m_deliveredBytes
		<< " receivedSeq=" << m_receivedSeq
		<< " receivedBytes=" << m_receivedBytes;
}

PacketDlbCreditTag::PacketDlbCreditTag()
	: m_deliveredBytes(0),
	  m_receivedSeq(0),
	  m_receivedBytes(0) {}

PacketDlbCreditTag::PacketDlbCreditTag(
		uint64_t deliveredBytes,
		uint64_t receivedSeq,
		uint32_t receivedBytes)
	: m_deliveredBytes(deliveredBytes),
	  m_receivedSeq(receivedSeq),
	  m_receivedBytes(receivedBytes) {}

uint64_t PacketDlbCreditTag::GetDeliveredBytes() const {
	return m_deliveredBytes;
}

uint64_t PacketDlbCreditTag::GetReceivedSeq() const {
	return m_receivedSeq;
}

uint32_t PacketDlbCreditTag::GetReceivedBytes() const {
	return m_receivedBytes;
}

}  // namespace ns3
