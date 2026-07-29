/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2009 INRIA
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Author: Mathieu Lacage <mathieu.lacage@sophia.inria.fr>
 */

#include "ns3/test.h"
#include "ns3/drop-tail-queue.h"
#include "ns3/simulator.h"
#include "ns3/point-to-point-net-device.h"
#include "ns3/point-to-point-channel.h"
#include "ns3/net-device-queue-interface.h"
#include "ns3/packet-dlb-tag.h"
#include "ns3/rdma-queue-pair.h"

#include <string>

using namespace ns3;

/**
 * \brief Test class for PointToPoint model
 *
 * It tries to send one packet from one NetDevice to another, over a
 * PointToPointChannel.
 */
class PointToPointTest : public TestCase
{
public:
  /**
   * \brief Create the test
   */
  PointToPointTest ();

  /**
   * \brief Run the test
   */
  virtual void DoRun (void);

private:
  
  Ptr<const Packet> m_recvdPacket; //!< received packet
  /**
   * \brief Send one packet to the device specified
   *
   * \param device NetDevice to send to.
   * \param buffer Payload content of the packet.
   * \param size Size of the payload.
   */
  void SendOnePacket (Ptr<PointToPointNetDevice> device, uint8_t const *buffer, uint32_t size);
  /**
   * \brief Callback function which sets the recvdPacket parameter
   *
   * \param dev The receiving device.
   * \param pkt The received packet.
   * \param mode The protocol mode used.
   * \param sender The sender address.
   * 
   * \return A boolean indicating packet handled properly.
   */
  bool RxPacket (Ptr<NetDevice> dev, Ptr<const Packet> pkt, uint16_t mode, const Address &sender);
};

PointToPointTest::PointToPointTest ()
  : TestCase ("PointToPoint")
{
}

void
PointToPointTest::SendOnePacket (Ptr<PointToPointNetDevice> device, uint8_t const *buffer, uint32_t size)
{
  Ptr<Packet> p = Create<Packet> (buffer, size);
  device->Send (p, device->GetBroadcast (), 0x800);
}

bool
PointToPointTest::RxPacket (Ptr<NetDevice> dev, Ptr<const Packet> pkt, uint16_t mode, const Address &sender)
{
  m_recvdPacket = pkt;
  return true;
}


void
PointToPointTest::DoRun (void)
{
  Ptr<Node> a = CreateObject<Node> ();
  Ptr<Node> b = CreateObject<Node> ();
  Ptr<PointToPointNetDevice> devA = CreateObject<PointToPointNetDevice> ();
  Ptr<PointToPointNetDevice> devB = CreateObject<PointToPointNetDevice> ();
  Ptr<PointToPointChannel> channel = CreateObject<PointToPointChannel> ();

  devA->Attach (channel);
  devA->SetAddress (Mac48Address::Allocate ());
  devA->SetQueue (CreateObject<DropTailQueue<Packet> > ());
  devB->Attach (channel);
  devB->SetAddress (Mac48Address::Allocate ());
  devB->SetQueue (CreateObject<DropTailQueue<Packet> > ());

  a->AddDevice (devA);
  b->AddDevice (devB);
  
  devB->SetReceiveCallback (MakeCallback (&PointToPointTest::RxPacket,
                                          this));
  uint8_t txBuffer [] = "\"Can you tell me where my country lies?\" \\ said the unifaun to his true love's eyes. \\ \"It lies with me!\" cried the Queen of Maybe \\ - for her merchandise, he traded in his prize.";
  size_t txBufferSize = sizeof(txBuffer);
  
  Simulator::Schedule (Seconds (1.0), &PointToPointTest::SendOnePacket, this, devA, txBuffer, txBufferSize);

  Simulator::Run ();

  NS_TEST_EXPECT_MSG_EQ (m_recvdPacket->GetSize (), txBufferSize, "trivial");

  uint8_t rxBuffer [1500]; // As large as the P2P MTU size, assuming that the user didn't change it.
  
  m_recvdPacket->CopyData (rxBuffer, txBufferSize);
  NS_TEST_EXPECT_MSG_EQ (memcmp (rxBuffer, txBuffer, txBufferSize), 0, "trivial");
  
  Simulator::Destroy ();
}

class PacketDlbSelectiveCreditTest : public TestCase
{
public:
  PacketDlbSelectiveCreditTest ()
    : TestCase ("Packet DLB selective credit releases the send window without completing the QP")
  {
  }

  void DoRun () override
  {
    Ptr<RdmaQueuePair> qp = CreateObject<RdmaQueuePair> (
        3,
        Ipv4Address ("10.0.0.1"),
        Ipv4Address ("10.0.0.2"),
        1000,
        2000);
    qp->SetSize (36000);
    qp->SetWin (18000);
    qp->snd_nxt = 18000;

    NS_TEST_EXPECT_MSG_EQ (qp->IsWinBound (), true,
                           "the cumulative window should initially be full");

    qp->SetPacketDlbSelectiveCredit (true);
    qp->SetPacketDlbLaneWindow (0, 9000);
    qp->SetPacketDlbLaneWindow (1, 9000);
    qp->RecordPacketDlbSend (0, 9000, 0);
    qp->RecordPacketDlbSend (9000, 9000, 1);
    NS_TEST_EXPECT_MSG_EQ (qp->IsWinBound (), true,
                           "all lane windows should initially be full");
    qp->AcknowledgeDelivered (9000, 9000, 9000);

    NS_TEST_EXPECT_MSG_EQ (qp->GetOnTheFly (), 18000,
                           "selective credit must not advance cumulative ACK state");
    NS_TEST_EXPECT_MSG_EQ (qp->GetWindowOnTheFly (), 9000,
                           "one independently delivered packet should return its credit");
    NS_TEST_EXPECT_MSG_EQ (qp->IsWinBound (), false,
                           "returned credit should permit another packet");
    NS_TEST_EXPECT_MSG_EQ (qp->IsFinished (), false,
                           "selective credit must not complete the QP");

    qp->snd_nxt = 36000;
    qp->AcknowledgeDelivered (36000, 0, 9000);
    NS_TEST_EXPECT_MSG_EQ (qp->IsFinished (), false,
                           "all delivery credit still must not replace cumulative ACK");
    qp->Acknowledge (36000);
    NS_TEST_EXPECT_MSG_EQ (qp->IsFinished (), true,
                           "cumulative ACK should retain completion authority");

    Ptr<Packet> packet = Create<Packet> ();
    packet->AddPacketTag (PacketDlbCreditTag (123456, 9000, 9000));
    PacketDlbCreditTag decoded;
    NS_TEST_EXPECT_MSG_EQ (packet->PeekPacketTag (decoded), true,
                           "selective credit tag should survive packet metadata handling");
    NS_TEST_EXPECT_MSG_EQ (decoded.GetDeliveredBytes (), 123456,
                           "selective credit tag should preserve its byte counter");
    NS_TEST_EXPECT_MSG_EQ (decoded.GetReceivedSeq (), 9000,
                           "selective credit tag should preserve packet sequence");
    NS_TEST_EXPECT_MSG_EQ (decoded.GetReceivedBytes (), 9000,
                           "selective credit tag should preserve packet size");
  }
};

/**
 * \brief TestSuite for PointToPoint module
 */
class PointToPointTestSuite : public TestSuite
{
public:
  /**
   * \brief Constructor
   */
  PointToPointTestSuite ();
};

PointToPointTestSuite::PointToPointTestSuite ()
  : TestSuite ("devices-point-to-point", UNIT)
{
  AddTestCase (new PointToPointTest, TestCase::QUICK);
  AddTestCase (new PacketDlbSelectiveCreditTest, TestCase::QUICK);
}

static PointToPointTestSuite g_pointToPointTestSuite; //!< The testsuite
