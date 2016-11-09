package com.ctrip.hermes.portal.service.zookeeperMigration;

import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

public class ZookeeperMigrationServiceTest extends ComponentTestCase {

	@Test
	public void test() throws DalException, Exception {
		ZookeeperMigrationService service = lookup(ZookeeperMigrationService.class);
		service.initializeZkFromBaseMeta(5);

		System.out.println("Initialize done");
	}
}
