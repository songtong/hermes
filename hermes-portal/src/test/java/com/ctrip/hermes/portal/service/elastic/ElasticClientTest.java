package com.ctrip.hermes.portal.service.elastic;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class ElasticClientTest extends ComponentTestCase {

	private ElasticClient ec;

	@Before
	public void init() {
		ec = lookup(ElasticClient.class);
	}

	@Test
	public void testGetProducers() {
		System.out.println(ec.getLastWeekProducers("order_new"));
	}

	@Test
	public void testGetLastMinuteCount() {
		System.out.println(ec.getBrokerReceived());
	}

}
