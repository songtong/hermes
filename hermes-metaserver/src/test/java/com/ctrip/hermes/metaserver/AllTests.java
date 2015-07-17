package com.ctrip.hermes.metaserver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.metaserver.fulltest.*;

@RunWith(Suite.class)
@SuiteClasses({
		  MetaServerBrokerAssignmentTest.class,
		  MetaServerAssignmentTest.class,
//		  MetaServerBaseMetaChangeTest.class,
//		  MetaServerBrokerLeaseTest.class,
//		  MetaServerConsumerLeaseTest.class,
		  MetaServerLeadershipTest.class
// add test classes here

})
public class AllTests {
//
}
