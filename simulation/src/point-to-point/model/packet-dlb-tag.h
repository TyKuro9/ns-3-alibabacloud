#ifndef PACKET_DLB_TAG_H
#define PACKET_DLB_TAG_H

#include "ns3/tag.h"

#include <cstdint>

namespace ns3 {

class PacketDlbEndpointTag : public Tag {
public:
	static TypeId GetTypeId();
	TypeId GetInstanceTypeId() const override;
	uint32_t GetSerializedSize() const override;
	void Serialize(TagBuffer buffer) const override;
	void Deserialize(TagBuffer buffer) override;
	void Print(std::ostream& stream) const override;

	PacketDlbEndpointTag();
	explicit PacketDlbEndpointTag(uint32_t destinationNic);

	uint32_t GetDestinationNic() const;

private:
	uint32_t m_destinationNic;
};

class PacketDlbCreditTag : public Tag {
public:
	static TypeId GetTypeId();
	TypeId GetInstanceTypeId() const override;
	uint32_t GetSerializedSize() const override;
	void Serialize(TagBuffer buffer) const override;
	void Deserialize(TagBuffer buffer) override;
	void Print(std::ostream& stream) const override;

	PacketDlbCreditTag();
	PacketDlbCreditTag(
		uint64_t deliveredBytes,
		uint64_t receivedSeq,
		uint32_t receivedBytes);

	uint64_t GetDeliveredBytes() const;
	uint64_t GetReceivedSeq() const;
	uint32_t GetReceivedBytes() const;

private:
	uint64_t m_deliveredBytes;
	uint64_t m_receivedSeq;
	uint32_t m_receivedBytes;
};

}  // namespace ns3

#endif  // PACKET_DLB_TAG_H
