package com.ctrip.hermes.metaservice.service;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.metaservice.service.KVService.Tag;

public class KVServiceTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String key = "hermes_key";
		KVService kvService = lookup(KVService.class);
		kvService.setKeyValue(key, "value");
		kvService.setKeyValue(key, "tag-value", Tag.ALARM);
		System.out.println(kvService.getValue(key));
		System.out.println(kvService.getValue(key, Tag.ALARM));
		System.out.println(kvService.list());
		System.out.println(kvService.list(Tag.ALARM));
	}
}
