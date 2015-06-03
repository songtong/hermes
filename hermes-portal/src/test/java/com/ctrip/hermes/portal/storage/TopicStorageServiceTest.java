package com.ctrip.hermes.portal.storage;

import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.service.DefaultMetaServiceWrapper;
import com.ctrip.hermes.portal.service.MetaServiceWrapper;
import com.ctrip.hermes.portal.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.portal.service.storage.TopicStorageService;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.portal.service.storage.exception.TopicIsNullException;

public class TopicStorageServiceTest extends ComponentTestCase {
	MetaServiceWrapper metaService;
	TopicStorageService service;

	@Before
	public void before(){

//		defineComponent(StorageHandler.class, MockStorageHandler.class);
		service = lookup(DefaultTopicStorageService.class, DefaultTopicStorageService.ID);
		metaService = lookup(MetaServiceWrapper.class, DefaultMetaServiceWrapper.ID);
	}

	@Test
	public void devTest() {
//		Topic topic = metaService.findTopicByName("cmessage_fws");
//		try {
//			service.createNewTopic("ds0", "order_new");
//		} catch (TopicAlreadyExistsException | StorageHandleErrorException e) {
//			e.printStackTrace();
//		}
	}

	@Test
	public void createNewTopic() throws Exception {
//		Topic topic = metaService.findTopicByName("cmessage_fws");

		service.initTopicStorage(buildTopic());
	}

	@Test(expected = TopicIsNullException.class)
	public void createNullTopic() throws TopicAlreadyExistsException, StorageHandleErrorException, TopicIsNullException {
		service.initTopicStorage(null);
	}

	@Test
	public void deleteTopic() throws Exception {
		service.dropTopicStorage(buildTopic());
	}

	@Test
	public void addPartiitionsStorage() throws Exception {
		service.addPartitionStorage(buildTopic(), buildPartition());
	}

	@Test
	public void delPartitionStorage() throws Exception {
		service.delPartitionStorage(buildTopic(), buildPartition());
	}

	@Test
	public void addConsumerGroup() throws Exception {
		service.addConsumerStorage(buildTopic(), buildGroup());
	}

	@Test
	public void delConsumerGroup() throws Exception {
		service.delConsumerStorage(buildTopic(), buildGroup());
	}

	private ConsumerGroup buildGroup() {
		ConsumerGroup group = new ConsumerGroup();
		group.setId(907);
		group.setName("OnlyForTest");
		group.setAppIds("543216");
		return group;
	}


	private Partition buildPartition() {
		return null;
	}

	private Topic buildTopic() {
		return metaService.findTopicByName("cmessage_fws");
	}

}
