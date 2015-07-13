package com.ctrip.hermes.core.schedule;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ExponentialSchedulePolicyTest {
	@Test
	public void testFail() {
		ExponentialSchedulePolicy policy = new ExponentialSchedulePolicy(10, 80);
		Assert.assertEquals(10, policy.fail(false));
		Assert.assertEquals(20, policy.fail(false));
		Assert.assertEquals(40, policy.fail(false));
		Assert.assertEquals(80, policy.fail(false));
		for (int i = 0; i < 100; i++) {
			Assert.assertEquals(80, policy.fail(false));
		}
	}

	@Test
	public void testFailAndSuccess() {
		ExponentialSchedulePolicy policy = new ExponentialSchedulePolicy(10, 80);
		Assert.assertEquals(10, policy.fail(false));
		Assert.assertEquals(20, policy.fail(false));
		Assert.assertEquals(40, policy.fail(false));
		policy.succeess();
		Assert.assertEquals(10, policy.fail(false));
		Assert.assertEquals(20, policy.fail(false));
		Assert.assertEquals(40, policy.fail(false));
		Assert.assertEquals(80, policy.fail(false));
		for (int i = 0; i < 100; i++) {
			Assert.assertEquals(80, policy.fail(false));
		}
		policy.succeess();
		Assert.assertEquals(10, policy.fail(false));
		Assert.assertEquals(20, policy.fail(false));
		Assert.assertEquals(40, policy.fail(false));
		Assert.assertEquals(80, policy.fail(false));
	}
}
