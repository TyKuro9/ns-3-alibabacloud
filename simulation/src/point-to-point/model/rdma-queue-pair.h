#ifndef RDMA_QUEUE_PAIR_H
#define RDMA_QUEUE_PAIR_H

#include <ns3/object.h>
#include <ns3/packet.h>
#include <ns3/ipv4-address.h>
#include <ns3/data-rate.h>
#include <ns3/event-id.h>
#include <ns3/custom-header.h>
#include <ns3/int-header.h>
#include <cstdint>
#include <map>
#include <vector>

namespace ns3 {

class RdmaQueuePair : public Object {
public:
	Time startTime;
	Ipv4Address sip, dip;
	uint16_t sport, dport;
	uint64_t m_size, m_init_size, m_tag;
	uint32_t m_src, m_dest;
	int32_t m_selectedNicIdx;
	int32_t m_initialSelectedNicIdx;
	int32_t m_selectedDestinationNicIdx;
	uint32_t m_sourceNicOrdinalHint;
	uint32_t m_sourcePathParallelism;
	bool m_sourceNicHintFallback;
	uint32_t m_bindCandidateCount;
	uint32_t m_bindPathHops;
	uint64_t m_bindPathScoreNs;
	uint64_t m_bindPathQueueDelayNs;
	uint64_t m_bindPathPropagationNs;
	uint64_t m_bindPathReservedBytes;
	uint64_t m_bindPathSignature;
	uint64_t m_nicReassignments;
	uint64_t m_cnpCount;
	bool m_sourceFlowletInitialized;
	bool m_sourceFlowletDecisionPending;
	bool m_sourcePacketSent;
	uint64_t m_sourceLastPacketNs;
	uint64_t m_sourceNextByteBoundary;
	uint64_t m_sourceFlowletId;
	bool m_packetDlbPrepared;
	uint64_t m_packetDlbPreparedSeq;
	int32_t m_packetDlbPreparedNicIdx;
	bool m_packetDlbCandidatesInitialized;
	bool m_actualPathWindowInitialized;
	uint32_t m_actualPathWindowBytes;
	uint64_t m_actualPathBaseRttNs;
	std::vector<int> m_packetDlbCandidates;
	std::vector<uint32_t> m_packetDlbBoundSwitches;
	std::map<uint64_t, std::vector<uint32_t>>
		m_packetDlbOutstandingRoutes;
	std::vector<uint64_t> m_pathReservationEdges;
	uint64_t m_pathReservationBytes;
	uint64_t snd_nxt, snd_una; // next seq to send, the highest unacked seq
	uint16_t m_pg;
	uint16_t m_ipid;
	uint32_t m_win; // bound of on-the-fly packets
	uint64_t m_baseRtt; // base RTT of this qp
	DataRate m_max_rate; // max rate
	bool m_var_win; // variable window size
	Time m_nextAvail;	//< Soonest time of next send
	uint32_t wp; // current window of packets
	uint32_t lastPktSize;
	Callback<void> m_notifyAppFinish;
	Callback<void> m_notifyAppSent;
	/******************************
	 * runtime states
	 *****************************/
	uint32_t nvls_enable;
	DataRate m_rate;	//< Current rate
	struct {
		DataRate m_targetRate;	//< Target rate
		EventId m_eventUpdateAlpha;
		double m_alpha;
		bool m_alpha_cnp_arrived; // indicate if CNP arrived in the last slot
		bool m_first_cnp; // indicate if the current CNP is the first CNP
		EventId m_eventDecreaseRate;
		bool m_decrease_cnp_arrived; // indicate if CNP arrived in the last slot
		uint32_t m_rpTimeStage;
		EventId m_rpTimer;
	} mlx;
	struct {
		uint64_t m_lastUpdateSeq;
		DataRate m_curRate;
		IntHop hop[IntHeader::maxHop];
		uint32_t keep[IntHeader::maxHop];
		uint32_t m_incStage;
		double m_lastGap;
		double u;
		struct {
			double u;
			DataRate Rc;
			uint32_t incStage;
		}hopState[IntHeader::maxHop];
	} hp;
	struct{
		uint64_t m_lastUpdateSeq;
		DataRate m_curRate;
		uint32_t m_incStage;
		uint64_t lastRtt;
		double rttDiff;
	} tmly;
	struct{
		uint64_t m_lastUpdateSeq;
		uint32_t m_caState;
		uint64_t m_highSeq; // when to exit cwr
		double m_alpha;
		uint32_t m_ecnCnt;
		uint32_t m_batchSizeOfAlpha;
	} dctcp;
	struct{
		uint64_t m_lastUpdateSeq;
		DataRate m_curRate;
		uint32_t m_incStage;
	}hpccPint;

	/***********
	 * methods
	 **********/
	static TypeId GetTypeId (void);
	RdmaQueuePair(uint16_t pg, Ipv4Address _sip, Ipv4Address _dip, uint16_t _sport, uint16_t _dport);
	void SetSize(uint64_t size);
	void SetWin(uint32_t win);
	void SetBaseRtt(uint64_t baseRtt);
	void SetVarWin(bool v);
	void SetAppNotifyCallback(Callback<void> notifyAppFinish);
	void SetAppSentCallback(Callback<void> notifyAppSent);

	uint64_t GetBytesLeft();
	uint64_t GetInitialSize();
	uint32_t GetSrc();
	uint32_t GetDest();
	uint64_t GetTag();
	void SetTag(uint64_t tag);void SetSrc(uint32_t src);void SetDest(uint32_t dest);void SetInitialSize(uint64_t size);
	uint32_t GetHash(void);
	void Acknowledge(uint64_t ack);
	uint64_t GetOnTheFly();
	bool IsWinBound();
	uint64_t GetWin(); // window size calculated from m_rate
	bool IsFinished();
	uint64_t HpGetCurWin(); // window size calculated from hp.m_curRate, used by HPCC
};

class RdmaRxQueuePair : public Object { // Rx side queue pair
public:
	struct ECNAccount{
		uint16_t qIndex;
		uint8_t ecnbits;
		uint16_t qfb;
		uint16_t total;

		ECNAccount() { memset(this, 0, sizeof(ECNAccount));}
	};
	ECNAccount m_ecn_source;
	uint32_t sip, dip;
	uint16_t sport, dport;
	uint16_t m_ipid;
	uint64_t ReceiverNextExpectedSeq;
	std::map<uint64_t, uint32_t> m_reorderSegments;
	uint64_t m_reorderBufferedBytes;
	uint64_t m_reorderPeakBytes;
	Time m_nackTimer;
	int32_t m_milestone_rx;
	uint64_t m_lastNACK;
	EventId QcnTimerEvent; // if destroy this rxQp, remember to cancel this timer

	static TypeId GetTypeId (void);
	RdmaRxQueuePair();
	uint32_t GetHash(void);
};

class RdmaQueuePairGroup : public Object {
public:
	std::vector<Ptr<RdmaQueuePair> > m_qps;
	//std::vector<Ptr<RdmaRxQueuePair> > m_rxQps;

	static TypeId GetTypeId (void);
	RdmaQueuePairGroup(void);
	uint32_t GetN(void);
	Ptr<RdmaQueuePair> Get(uint32_t idx);
	Ptr<RdmaQueuePair> operator[](uint32_t idx);
	void AddQp(Ptr<RdmaQueuePair> qp);
	bool RemoveQp(Ptr<RdmaQueuePair> qp);
	//void AddRxQp(Ptr<RdmaRxQueuePair> rxQp);
	void Clear(void);
};

}

#endif /* RDMA_QUEUE_PAIR_H */
