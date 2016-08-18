package com.ctrip.hermes.metaservice.service;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.model.Idc;

public class IdcServiceTest extends ComponentTestCase {

	@Test
	public void testDelete() throws Exception {
		IdcService idcService = lookup(IdcService.class);
		idcService.deleteIdc(1);
	}

	@Test
	public void test() throws Exception {
		IdcService idcService = lookup(IdcService.class);
		// Idc idc = new Idc("JQ");
		// idc.setEnabled(true);
		// idc.setPrimary(false);
		// idcService.addIdc(idc);
		// System.out.println(idcService.listIdcs());
		// try {
		// idc.setId("OY");
		// idc.setPrimary(false);
		// idcService.addIdc(idc);
		// } catch (Exception e) {
		// System.out.println("Failed to add OY." + e.getMessage());
		// }
		Idc idc = new Idc();
		idc.setName("aaaaaaaa");
		idc.setEnabled(true);
		idc.setPrimary(true);
		idcService.addIdc(idc);
		System.out.println("After add an idc");
		System.out.println(idcService.listIdcs());

		idcService.disableIdc(1);
		System.out.println("After disable 1");
		System.out.println(idcService.listIdcs());

		idcService.enableIdc(1);
		System.out.println("After enable 1");
		System.out.println(idcService.listIdcs());

		try {
			idcService.switchPrimary(2);
			System.out.println("After switch to primary to 2");
			System.out.println(idcService.listIdcs());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		idcService.forceSwitchPrimary(2);
		System.out.println("After force switch to primary to 1");
		System.out.println(idcService.listIdcs());

		idcService.deleteIdc(3);
		System.out.println("After delete 3");
		System.out.println(idcService.listIdcs());
	}

}
