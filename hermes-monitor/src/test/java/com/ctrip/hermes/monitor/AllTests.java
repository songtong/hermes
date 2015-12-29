package com.ctrip.hermes.monitor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.monitor.checker.CatBasedCheckerTest;
import com.ctrip.hermes.monitor.checker.ConsumeDelayCheckerTest;
import com.ctrip.hermes.monitor.checker.ProduceTransportFailedRatioCheckerTest;
import com.ctrip.hermes.monitor.checker.ProduceFailureCheckerTest;
import com.ctrip.hermes.monitor.checker.ProduceLatencyCheckerTest;

@RunWith(Suite.class)
@SuiteClasses({ //
CatBasedCheckerTest.class,//
      ProduceLatencyCheckerTest.class,//
      ConsumeDelayCheckerTest.class,//
      ProduceFailureCheckerTest.class,//
      ProduceTransportFailedRatioCheckerTest.class,//

})
public class AllTests {

}
