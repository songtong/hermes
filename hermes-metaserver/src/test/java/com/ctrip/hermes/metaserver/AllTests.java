package com.ctrip.hermes.metaserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.metaserver.assign.AssignBalancerTest;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolderTest;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolderTest;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerLeaseAllocatorTest;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.consumer.ActiveConsumerListTest;
import com.ctrip.hermes.metaserver.consumer.ConsumerAssignmentHolderTest;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolderTest;
import com.ctrip.hermes.metaserver.consumer.LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.consumer.NonOrderedConsumeConsumerLeaseAllocatorTest;
import com.ctrip.hermes.metaserver.consumer.OrderedConsumeConsumerLeaseAllocatorTest;

@RunWith(Suite.class)
@SuiteClasses({ //
// MetaServerBrokerAssignmentTest.class, //
// MetaServerAssignmentTest.class,//
// MetaServerBaseMetaChangeTest.class, //
// MetaServerBrokerLeaseTest.class,//
// MetaServerBrokerLeaseChangedTest.class,//
// MetaServerConsumerLeaseTest.class,//
// // MetaServerConsumerLeaseChangeTest.class,//
// MetaServerLeadershipTest.class,//
LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest.class,//
      AssignBalancerTest.class,//
      DefaultBrokerPartitionAssigningStrategyTest.class, //
      DefaultBrokerLeaseAllocatorTest.class, //
      BrokerLeaseHolderTest.class, //
      BrokerAssignmentHolderTest.class, //
      ActiveConsumerListTest.class, //
      ConsumerAssignmentHolderTest.class, //
      NonOrderedConsumeConsumerLeaseAllocatorTest.class, //
      OrderedConsumeConsumerLeaseAllocatorTest.class, //
      ConsumerLeaseHolderTest.class, //
// add test classes here

})
public class AllTests {
	//
}
