package com.ctrip.hermes.portal.storage;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.DefaultPortalMetaService;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTopic;

public class TopicStorageServiceTest extends ComponentTestCase {
	PortalMetaService metaService;
	
	TopicService topicService;

	TopicStorageService service;

	@Before
	public void before() throws Exception {
		String zkMode = System.getProperty("zkMode");
		if (!"real".equalsIgnoreCase(zkMode)) {
			@SuppressWarnings("resource")
			TestingServer m_zkServer = new TestingServer(2181);
			System.out.println("Starting zk with fake mode, connection string is " + m_zkServer.getConnectString());
		}

		// defineComponent(StorageHandler.class, MockStorageHandler.class);
		service = lookup(TopicStorageService.class, DefaultTopicStorageService.ID);
		metaService = lookup(PortalMetaService.class, DefaultPortalMetaService.ID);
		topicService = lookup(TopicService.class);
	}

	@Test
	public void createNewTopic() throws Exception {

		service.initTopicStorage(buildTopic());
	}

	@Test
	public void deleteTopic() throws Exception {
		service.dropTopicStorage(buildTopic());
	}

	@Test
	public void addPartitionsStorage() throws Exception {
		Topic topic = buildTopic();

		service.addPartitionStorage(topic, topic.getPartitions().get(0));
	}

	@Test
	public void delPartitionStorage() throws Exception {
		Topic topic = buildTopic();

		service.delPartitionStorage(topic, topic.getPartitions().get(0));
	}

	@Test
	public void addConsumerGroup() throws Exception {
		service.addConsumerStorage(buildTopic(), buildGroup());
	}

	@Test
	public void delConsumerGroup() throws Exception {
		service.delConsumerStorage(buildTopic(), buildGroup());
	}

	@Test
	public void showStorageTopic() throws Exception {
		StorageTopic storageTopic = service.getTopicStorage(buildTopic());
		System.out.println(storageTopic.toString());
	}

	private ConsumerGroup buildGroup() {
		ConsumerGroup group = new ConsumerGroup();
		group.setId(907);
		group.setName("OnlyForTest");
		group.setAppIds("543216");
		return group;
	}

	private Topic buildTopic() {
		return topicService.findTopicEntityByName("cmessage_fws");
	}

	@Test(expected = TopicIsNullException.class)
	public void createNullTopic() throws TopicAlreadyExistsException, StorageHandleErrorException, TopicIsNullException {
		System.out.println("Try to Create Null Topic.");
		service.initTopicStorage(null);
	}

}
