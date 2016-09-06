package com.ctrip.hermes.portal;

import org.junit.Test;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;

/**
 * @author marsqing
 *
 *         Jun 6, 2016 2:01:32 PM
 */
public class ApolloTest {
	@Test
	public void test() {
		Config config = ConfigService.getAppConfig();
		String someKey = "meta.zk.connectionString";
		String someDefaultValue = "xx";
		System.out
		      .println(String.format("Value for key %s is %s", someKey, config.getProperty(someKey, someDefaultValue)));
	}
}
