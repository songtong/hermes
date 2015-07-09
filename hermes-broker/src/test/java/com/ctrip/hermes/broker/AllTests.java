package com.ctrip.hermes.broker;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.broker.ack.internal.DefaultAckHolderTest;

@RunWith(Suite.class)
@SuiteClasses({//
DefaultAckHolderTest.class,//
// add test classes here

})
public class AllTests {

}
