package com.ctrip.hermes.storage;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.storage.TopicStorageService;
import com.ctrip.hermes.core.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.meta.entity.Topic;

public class TopicStorageServiceTest extends ComponentTestCase {
	MetaService metaService;
	TopicStorageService service;

	@Before
	public void before() {

		service = lookup(TopicStorageService.class);
		metaService = lookup(MetaService.class);
	}

	@Test
	public void createByDSAndTopicName() {
		Topic topic = metaService.findTopic("cmessage_fws");
		try {
			service.createNewTopic("ds0", "order_new");
		} catch (TopicAlreadyExistsException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void createByDatabaseNameandTopic() {
		Topic topic = metaService.findTopic("cmessage_fws");
		try {
			service.createNewTopic("fxhermesshard01db", topic);
		} catch (TopicAlreadyExistsException e) {
			e.printStackTrace();
		}
	}
}
