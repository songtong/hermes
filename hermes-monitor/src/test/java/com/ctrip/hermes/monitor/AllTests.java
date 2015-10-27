package com.ctrip.hermes.monitor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.monitor.checker.CatTransactionCrossReportBasedCheckerTest;

@RunWith(Suite.class)
@SuiteClasses({ //
CatTransactionCrossReportBasedCheckerTest.class,//

})
public class AllTests {

}
