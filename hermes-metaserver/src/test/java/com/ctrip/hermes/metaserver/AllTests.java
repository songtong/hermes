package com.ctrip.hermes.metaserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.metaserver.assign.AssignBalancerTest;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerLeaseAllocatorTest;
import com.ctrip.hermes.metaserver.broker.DefaultBrokerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.consumer.LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerAssignmentTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerBaseMetaChangeTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerBrokerAssignmentTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerBrokerLeaseTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerConsumerLeaseTest;
import com.ctrip.hermes.metaserver.fulltest.MetaServerLeadershipTest;

@RunWith(Suite.class)
@SuiteClasses({ //
MetaServerBrokerAssignmentTest.class, //
      MetaServerAssignmentTest.class,//
      MetaServerBaseMetaChangeTest.class, //
      MetaServerBrokerLeaseTest.class,//
      // MetaServerBrokerLeaseChangedTest.class,//
      MetaServerConsumerLeaseTest.class,//
      // MetaServerConsumerLeaseChangeTest.class,//
      MetaServerLeadershipTest.class,//
      LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategyTest.class,//
      AssignBalancerTest.class,//
      DefaultBrokerPartitionAssigningStrategyTest.class, //
      DefaultBrokerLeaseAllocatorTest.class //
// add test classes here

})
public class AllTests {
	//
}
