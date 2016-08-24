package com.ctrip.hermes.portal.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.EntityToModelConverter;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicAlreadyExistsException;
import com.ctrip.hermes.metaservice.service.storage.exception.TopicIsNullException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTopic;
import com.ctrip.hermes.portal.service.meta.DefaultPortalMetaService;
import com.ctrip.hermes.portal.service.meta.PortalMetaService;

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
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		service.initTopicStorage(topicModel, partitions);
	}

	@Test
	public void deleteTopic() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		service.dropTopicStorage(topicModel, partitions, null);
	}

	@Test
	public void addPartitionsStorage() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		service.addPartitionStorage(topicModel, partitions.get(0));
	}

	@Test
	public void delPartitionStorage() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		List<com.ctrip.hermes.metaservice.model.ConsumerGroup> consumerGroups = new ArrayList<>();
		for (ConsumerGroup cg : topicEntity.getConsumerGroups()) {
			consumerGroups.add(EntityToModelConverter.convert(cg));
		}
		service.delPartitionStorage(topicModel, partitions.get(0), consumerGroups);
	}

	@Test
	public void addConsumerGroup() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter
		      .convert(buildGroup());
		service.addConsumerStorage(topicModel, partitions, consumerGroupModel);
	}

	@Test
	public void delConsumerGroup() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		com.ctrip.hermes.metaservice.model.ConsumerGroup consumerGroupModel = EntityToModelConverter
		      .convert(buildGroup());
		service.delConsumerStorage(topicModel, partitions, consumerGroupModel);
	}

	@Test
	public void showStorageTopic() throws Exception {
		Topic topicEntity = buildTopic();
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
		List<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (Partition p : topicEntity.getPartitions()) {
			partitions.add(EntityToModelConverter.convert(p));
		}
		StorageTopic storageTopic = service.getTopicStorage(topicModel, partitions);
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
		service.initTopicStorage(null, null);
	}

}
