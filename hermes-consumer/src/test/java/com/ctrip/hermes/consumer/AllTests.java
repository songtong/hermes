package com.ctrip.hermes.consumer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.consumer.integration.ConsumerIntegrationTest;

@RunWith(Suite.class)
@SuiteClasses({ //
ConsumerIntegrationTest.class, //
})
public class AllTests {

}
