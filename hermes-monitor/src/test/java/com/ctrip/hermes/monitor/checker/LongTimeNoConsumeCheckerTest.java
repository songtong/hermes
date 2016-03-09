package com.ctrip.hermes.monitor.checker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.monitor.checker.mysql.LongTimeNoConsumeChecker;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class LongTimeNoConsumeCheckerTest extends BaseCheckerTest {

	@Component("MockLongTimeNoConsumeChecker")
	public static class MockLongTimeNoConsumeChecker extends LongTimeNoConsumeChecker {
	}

	@Autowired
	@Qualifier("MockLongTimeNoConsumeChecker")
	MockLongTimeNoConsumeChecker m_checker;

	@Test
	public void testParseLimits() throws Exception {
		String includeStr = "{ \".*\" : {\".*\": 30}, \"a.b.c\" : {\"group2\": 100} }";
		String excludeStr = "{\"a.b.c\":[\"group1\"], \"c.d.e\":[]}";
		Meta meta = DefaultSaxParser.parse(loadTestData("testParseLimits"));
		System.out.println(m_checker.parseLimits(meta, includeStr, excludeStr));
	}
}
