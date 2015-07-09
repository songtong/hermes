package com.ctrip.hermes.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.core.message.retry.FrequencySpecifiedRetryPolicyTest;
import com.ctrip.hermes.core.message.retry.RetryPolicyFactoryTest;
import com.ctrip.hermes.core.transport.netty.MagicTest;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodecTest;

@RunWith(Suite.class)
@SuiteClasses({//
FrequencySpecifiedRetryPolicyTest.class,//
      RetryPolicyFactoryTest.class, //
      MagicTest.class, //
      HermesPrimitiveCodecTest.class, //
// add test classes here

})
public class AllTests {

}
