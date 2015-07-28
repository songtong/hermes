package com.ctrip.hermes.broker;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.broker.ack.internal.DefaultAckHolderTest;
import com.ctrip.hermes.broker.integration.ProduceTest;

@RunWith(Suite.class)
@SuiteClasses({//
DefaultAckHolderTest.class,//
ProduceTest.class,//
// add test classes here

})
public class AllTests {

}
