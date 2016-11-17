package com.ctrip.hermes.admin.core.service;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.admin.core.service.KVService;
import com.ctrip.hermes.admin.core.service.KVService.Tag;

public class KVServiceTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String key = "hermes_key";
		KVService kvService = lookup(KVService.class);
		kvService.setKeyValue(key, "value");
		kvService.setKeyValue(key, "tag-value", Tag.CHECKER);
		System.out.println(kvService.getValue(key));
		System.out.println(kvService.getValue(key, Tag.CHECKER));
		System.out.println(kvService.list());
		System.out.println(kvService.list(Tag.CHECKER));
	}
}
