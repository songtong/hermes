package com.ctrip.hermes.meta;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.meta.rest.CodecResourceTest;
import com.ctrip.hermes.meta.rest.MetaResourceTest;
import com.ctrip.hermes.meta.rest.SchemaResourceTest;
import com.ctrip.hermes.meta.rest.TopicResourceTest;

@RunWith(Suite.class)
@SuiteClasses({ CodecResourceTest.class, MetaResourceTest.class, SchemaResourceTest.class, TopicResourceTest.class })
public class AllTests {

}
