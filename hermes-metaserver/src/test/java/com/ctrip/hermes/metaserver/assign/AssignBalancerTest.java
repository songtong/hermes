package com.ctrip.hermes.metaserver.assign;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

public class AssignBalancerTest {

	@Test
	public void testPointToAvg() {
		AssignBalancer<Integer> a = new AssignBalancer<>(6, 2, Collections.<Integer> emptyList());

		assertEquals(3, a.distanceToAvg(0));
		assertEquals(2, a.distanceToAvg(1));
		assertEquals(1, a.distanceToAvg(2));
		assertEquals(0, a.distanceToAvg(3));
		assertEquals(-1, a.distanceToAvg(4));
		assertEquals(-2, a.distanceToAvg(5));
		assertEquals(-3, a.distanceToAvg(6));

		a = new AssignBalancer<>(7, 2, Collections.<Integer> emptyList());

		assertEquals(3, a.distanceToAvg(0));
		assertEquals(2, a.distanceToAvg(1));
		assertEquals(1, a.distanceToAvg(2));
		assertEquals(0, a.distanceToAvg(3));
		assertEquals(0, a.distanceToAvg(4));
		assertEquals(-1, a.distanceToAvg(5));
		assertEquals(-2, a.distanceToAvg(6));
		assertEquals(-3, a.distanceToAvg(7));
	}
}
