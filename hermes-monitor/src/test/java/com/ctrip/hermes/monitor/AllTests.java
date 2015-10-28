package com.ctrip.hermes.monitor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.monitor.checker.CatTransactionCrossReportBasedCheckerTest;
import com.ctrip.hermes.monitor.checker.ConsumeDelayCheckerTest;
import com.ctrip.hermes.monitor.checker.ProduceLatencyCheckerTest;

@RunWith(Suite.class)
@SuiteClasses({ //
CatTransactionCrossReportBasedCheckerTest.class,//
      ProduceLatencyCheckerTest.class,//
      ConsumeDelayCheckerTest.class,//

})
public class AllTests {

}
