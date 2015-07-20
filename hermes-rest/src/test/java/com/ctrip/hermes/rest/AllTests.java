package com.ctrip.hermes.rest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.rest.resource.KafkaTopicsResourceTest;

@RunWith(Suite.class)
@SuiteClasses({ 
	KafkaTopicsResourceTest.class, 
	RestIntegrationTest.class,

})
public class AllTests {

}
