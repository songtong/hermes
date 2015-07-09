package com.ctrip.hermes.portal.dal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.portal.StartPortal;

public class HermesPortalDaoTest extends StartPortal {

	private HermesPortalDao m_dao;

	@Before
	public void init() {
		m_dao = lookup(HermesPortalDao.class);
	}

	@After
	public void stop() throws Exception {
		stopServer();
	}

	@Test
	public void testGetDelay() throws Exception {
		Tpp tpp = new Tpp("order_new", 0, true);
		System.out.println(m_dao.getDelayTime(tpp.getTopic(), tpp.getPartition(), 1));
	}

}
