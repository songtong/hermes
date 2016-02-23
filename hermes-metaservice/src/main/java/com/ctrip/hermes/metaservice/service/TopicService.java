package com.ctrip.hermes.metaservice.service;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.EntityToModelConverter;
import com.ctrip.hermes.metaservice.converter.EntityToViewConverter;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.converter.ModelToViewConverter;
import com.ctrip.hermes.metaservice.converter.ViewToModelConverter;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedProducerDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;
import com.ctrip.hermes.metaservice.view.SchemaView;
import com.ctrip.hermes.metaservice.view.TopicView;

@Named
public class TopicService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicService.class);

	@Inject
	private TransactionManager tm;

	@Inject
	private SchemaService m_schemaService;

	@Inject
	private TopicStorageService m_topicStorageService;

	@Inject
	private CachedTopicDao m_topicDao;

	@Inject
	private CachedPartitionDao m_partitionDao;

	@Inject
	private CachedConsumerGroupDao m_consumerGroupDao;

	@Inject
	private CachedProducerDao m_producerDao;

	@Inject
	private CodecService m_codecService;

	@Inject
	private ZookeeperService m_zookeeperService;

	@Inject
	private StorageService m_datasourceService;

	/**
	 * 
	 * @param topicName
	 * @param partition
	 */
	public TopicView addPartitionsForTopic(String topicName, List<Partition> partitions) throws Exception {
		Topic topic = findTopicEntityByName(topicName);
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));

		int partitionId = 0;
		for (Partition p : topic.getPartitions()) {
			if (p.getId() != null && p.getId() >= partitionId) {
				partitionId = p.getId() + 1;
			}
		}

		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topic);

		for (Partition partition : partitions) {
			com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter.convert(partition);
			if (partition.getId() < 0) {
				partition.setId(partitionId++);
				partitionModel.setTopicId(topic.getId());
				m_partitionDao.insert(partitionModel);

				if (Storage.MYSQL.equals(topic.getStorageType())) {
					Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> consumerGroupModels = m_consumerGroupDao
					      .findByTopic(topic.getId(), false);
					if (!m_topicStorageService.addPartitionForTopic(topicModel, partitionModel, consumerGroupModels)) {
						partitionId--;
						m_logger.error("Add new topic partition failed, please try later.");
						throw new RuntimeException("Add new topic partition failed, please try later.");
					}

				}
			} else {
				partitionModel.setTId(topic.getId());
				partitionModel.setPId(partition.getId());
				partitionModel.setTopicId(topic.getId());
				partitionModel.setId(partition.getId());
				m_partitionDao.updateByTopicAndPartition(partitionModel, PartitionEntity.UPDATESET_FULL);
			}
		}

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			topic.getPartitions().addAll(partitions);
			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
			m_zookeeperService.ensureBrokerLeaseZkPath(topic);
		}

		TopicView topicView = EntityToViewConverter.convert(topic);
		fillTopicView(topicView);
		return topicView;
	}

	public void addPartitionStorage(String ds, String table, int span) throws StorageHandleErrorException {
		m_topicStorageService.addPartitionStorage(ds, table, span);
	}

	/**
	 * @param topicEntity
	 * @return
	 * @throws DalException
	 */
	public TopicView createTopic(TopicView topicView) throws Exception {
		tm.startTransaction("fxhermesmetadb");
		com.ctrip.hermes.metaservice.model.Topic topicModel = ViewToModelConverter.convert(topicView);
		Collection<com.ctrip.hermes.metaservice.model.Partition> partitionModels = new ArrayList<>();
		try {
			topicModel.setCreateTime(new Date(System.currentTimeMillis()));
			m_topicDao.insert(topicModel);
			topicView.setId(topicModel.getId());

			int partitionId = 0;
			for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topicView.getPartitions()) {
				com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter
						.convert(partitionEntity);
				partitionModel.setId(partitionId++);
				partitionModel.setTopicId(topicModel.getId());
				partitionEntity.setId(partitionModel.getId());
				m_partitionDao.insert(partitionModel);
				partitionModels.add(partitionModel);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			m_logger.warn("create topic failed", e);
			tm.rollbackTransaction();
			throw e;
		}
		if (Storage.MYSQL.equals(topicModel.getStorageType())) {
			if (!m_topicStorageService.initTopicStorage(topicModel, partitionModels)) {
				m_logger.error("Init topic storage failed, please try later.");
				throw new RuntimeException("Init topic storage failed, please try later.");
			}

			Topic topicEntity = findTopicEntityById(topicModel.getId());
			m_zookeeperService.ensureBrokerLeaseZkPath(topicEntity);
		}

		return topicView;
	}

	/**
	 * @param name
	 * @throws DalException
	 */
	public void deleteTopic(String name) throws Exception {
		Topic topic = findTopicEntityByName(name);
		if (topic == null)
			return;
		Collection<com.ctrip.hermes.metaservice.model.Partition> partitions = new ArrayList<>();
		for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topic.getPartitions()) {
			com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter
					.convert(partitionEntity);
			partitionModel.setTopicId(topic.getId());
			partitionModel.setTId(topic.getId());
			m_partitionDao.deleteByTopicId(partitionModel);
			partitions.add(partitionModel);
		}
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> consumerGroups = new ArrayList<>();
		for (com.ctrip.hermes.meta.entity.ConsumerGroup cgEntity : topic.getConsumerGroups()) {
			com.ctrip.hermes.metaservice.model.ConsumerGroup cgModel = EntityToModelConverter.convert(cgEntity);
			m_consumerGroupDao.deleteByPK(cgModel);
			consumerGroups.add(cgModel);
		}
		for (com.ctrip.hermes.meta.entity.Producer producerEntity : topic.getProducers()) {
			com.ctrip.hermes.metaservice.model.Producer producerModel = EntityToModelConverter.convert(producerEntity);
			m_producerDao.deleteByPK(producerModel);
		}

		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topic);
		m_topicDao.deleteByPK(topicModel);
		// Remove related schemas
		if (topic.getId() != null && topic.getId() > 0) {
			try {
				m_schemaService.deleteSchemas(topic);
			} catch (Throwable e) {
				m_logger.error(String.format("delete schema failed for topic: %s", topic.getName()), e);
			}
		}

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			try {
				m_topicStorageService.dropTopicStorage(topicModel, partitions, consumerGroups);
				m_zookeeperService.deleteConsumerLeaseTopicParentZkPath(topic.getName());
				m_zookeeperService.deleteBrokerLeaseTopicParentZkPath(topic.getName());
				m_zookeeperService.deleteMetaServerAssignmentZkPath(topic.getName());
			} catch (Exception e) {
				if (e instanceof StorageHandleErrorException) {
					m_logger.warn("Delete topic tables failed", e);
				} else {
					throw new RuntimeException("Delete topic failed.", e);
				}
			}
		}
	}

	public void delPartitionStorage(String ds, String table) throws StorageHandleErrorException {
		m_topicStorageService.delPartitionStorage(ds, table);
	}

	public Topic findTopicEntityById(long id) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByPK(id);
			Topic entity = ModelToEntityConverter.convert(model);
			return fillTopicEntity(entity);
		} catch (DalException e) {
			m_logger.warn("findTopicById failed", e);
		}
		return null;
	}

	public Topic findTopicEntityByName(String topic) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByName(topic);
			Topic topicEntity = ModelToEntityConverter.convert(model);
			return fillTopicEntity(topicEntity);
		} catch (DalException e) {
			m_logger.warn("findTopicByName failed, name: " + topic, e);
		}
		return null;
	}

	public TopicView findTopicViewByName(String topicName) {
		try {
			com.ctrip.hermes.metaservice.model.Topic topicModel = this.m_topicDao.findByName(topicName);
			TopicView topicView = ModelToViewConverter.convert(topicModel);
			return fillTopicView(topicView);
		} catch (Exception e) {
			m_logger.warn("findTopicViewByName failed, name: " + topicName, e);
		}
		return null;
	}

	public List<TopicView> findTopicViews(String pattern) {
		List<TopicView> filtered = new ArrayList<TopicView>();

		for (TopicView topic : getTopicViews().values()) {
			if (Pattern.matches(pattern, topic.getName())) {
				filtered.add(topic);
			}
		}

		Collections.sort(filtered, new Comparator<TopicView>() {
			@Override
			public int compare(TopicView o1, TopicView o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		return filtered;
	}

	public Map<String, TopicView> getTopicViews() {
		Map<String, TopicView> result = new HashMap<String, TopicView>();
		try {
			List<TopicView> topics = findTopicViews(true);
			for (TopicView t : topics) {
				result.put(t.getName(), t);
			}
		} catch (Exception e) {
			m_logger.warn("get topics failed", e);
		}
		return result;
	}

	public List<String> getTopicNames() {
		List<String> result = new ArrayList<String>();
		try {
			Collection<com.ctrip.hermes.metaservice.model.Topic> topicModels = m_topicDao.list(false);
			for (com.ctrip.hermes.metaservice.model.Topic topicModel : topicModels) {
				result.add(topicModel.getName());
			}
		} catch (Exception e) {
			m_logger.warn("getTopicNames failed", e);
		}
		return result;
	}

	public Map<String, Topic> getTopicEntities() {
		Map<String, Topic> result = new HashMap<String, Topic>();
		try {
			List<Topic> topics = findTopicEntities(true);
			for (Topic t : topics) {
				result.put(t.getName(), t);
			}
		} catch (Exception e) {
			m_logger.warn("get topics failed", e);
		}
		return result;
	}

	public Integer queryStorageSize(String ds) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageSize(ds);
	}

	public Integer queryStorageSize(String ds, String table) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageSize(ds, table);
	}

	public List<StoragePartition> queryStorageTablePartitions(String ds, String table)
			throws StorageHandleErrorException {
		return m_topicStorageService.queryTablePartitions(ds, table);
	}

	public List<StorageTable> queryStorageTables(String ds) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageTables(ds);
	}

	/**
	 * @param topic
	 * @return
	 * @throws Exception
	 */
	public TopicView updateTopic(TopicView topicView) throws Exception {
		com.ctrip.hermes.metaservice.model.Topic topicModel = this.m_topicDao.findByName(topicView.getName());

		topicModel.setAckTimeoutSeconds(topicView.getAckTimeoutSeconds());
		topicModel.setCodecType(topicView.getCodecType());
		topicModel.setConsumerRetryPolicy(topicView.getConsumerRetryPolicy());
		topicModel.setOwner1(topicView.getOwner1());
		topicModel.setOwner2(topicView.getOwner2());
		topicModel.setPhone1(topicView.getPhone1());
		topicModel.setPhone2(topicView.getPhone2());
		topicModel.setDescription(topicView.getDescription());
		topicModel.setEndpointType(topicView.getEndpointType());
		topicModel.setStatus(topicView.getStatus());

		List<Partition> partitions = new ArrayList<>();
		for (Partition partition : topicView.getPartitions()) {
			partitions.add(partition);
		}
		addPartitionsForTopic(topicModel.getName(), partitions);
		m_topicDao.updateByPK(topicModel, TopicEntity.UPDATESET_FULL);

		if (Storage.MYSQL.equals(topicView.getStorageType())) {
			Topic topicEntity = findTopicEntityById(topicModel.getId());
			m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
			m_zookeeperService.ensureBrokerLeaseZkPath(topicEntity);
		}

		return ModelToViewConverter.convert(topicModel);
	}

	public List<TopicView> findTopicViews(boolean isFillDetail) throws DalException, IOException, RestClientException {
		Collection<com.ctrip.hermes.metaservice.model.Topic> models = m_topicDao.list(false);
		List<TopicView> views = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Topic model : models) {
			TopicView view = ModelToViewConverter.convert(model);
			if (isFillDetail) {
				views.add(fillTopicView(view));
			} else {
				views.add(view);
			}
		}
		return views;
	}

	public List<com.ctrip.hermes.meta.entity.Topic> findTopicEntities(boolean isFillDetail) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Topic> models = m_topicDao.list(true);
		Map<Long, Collection<ConsumerGroup>> consumers = consumerListToMap(m_consumerGroupDao.list(true));
		Map<Long, Collection<com.ctrip.hermes.metaservice.model.Partition>> partitions = partitionListToMap(
				m_partitionDao.list(true));
		List<com.ctrip.hermes.meta.entity.Topic> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Topic model : models) {
			if (isFillDetail) {
				Topic topic = ModelToEntityConverter.convert(model);
				addConsumerGroups4Topic(topic, consumers.get(topic.getId()));
				addPartitions4Topic(topic, partitions.get(topic.getId()));
				entities.add(topic);
			} else {
				entities.add(ModelToEntityConverter.convert(model));
			}
		}
		return entities;
	}

	private void addConsumerGroups4Topic(Topic topic, Collection<ConsumerGroup> consumers) {
		if (consumers != null) {
			for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : consumers) {
				com.ctrip.hermes.meta.entity.ConsumerGroup entity = ModelToEntityConverter.convert(model);
				topic.addConsumerGroup(entity);
			}
		}
	}

	private void addPartitions4Topic(Topic topic, Collection<com.ctrip.hermes.metaservice.model.Partition> partitions) {
		if (partitions != null) {
			for (com.ctrip.hermes.metaservice.model.Partition model : partitions) {
				com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
				topic.addPartition(entity);
			}
		}
	}

	private void addPartitions4TopicView(TopicView topicView,
			Collection<com.ctrip.hermes.metaservice.model.Partition> partitions) {
		if (partitions != null) {
			for (com.ctrip.hermes.metaservice.model.Partition model : partitions) {
				com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
				topicView.getPartitions().add(entity);
			}
		}
	}

	private Map<Long, Collection<com.ctrip.hermes.metaservice.model.Partition>> partitionListToMap(
			Collection<com.ctrip.hermes.metaservice.model.Partition> collection) {
		Map<Long, Collection<com.ctrip.hermes.metaservice.model.Partition>> map = new HashMap<>();
		if (collection != null) {
			for (com.ctrip.hermes.metaservice.model.Partition partition : collection) {
				Collection<com.ctrip.hermes.metaservice.model.Partition> list = map.get(partition.getTopicId());
				if (list == null) {
					list = new ArrayList<>();
					map.put(partition.getTopicId(), list);
				}
				list.add(partition);
			}
		}
		return map;
	}

	private Map<Long, Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup>> consumerListToMap(
			Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> collection) {
		Map<Long, Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup>> map = new HashMap<>();
		if (collection != null) {
			for (com.ctrip.hermes.metaservice.model.ConsumerGroup consumer : collection) {
				Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> list = map.get(consumer.getTopicId());
				if (list == null) {
					list = new ArrayList<>();
					map.put(consumer.getTopicId(), list);
				}
				list.add(consumer);
			}
		}
		return map;
	}

	protected Topic fillTopicEntity(Topic topicEntity) throws DalException {
		addConsumerGroups4Topic(topicEntity, m_consumerGroupDao.findByTopic(topicEntity.getId(), false));
		addPartitions4Topic(topicEntity, m_partitionDao.findByTopic(topicEntity.getId(), false));
		return topicEntity;
	}

	private TopicView fillTopicView(TopicView topicView) throws DalException, IOException, RestClientException {
		// Fill Storage
		Storage storage = m_datasourceService.getStorages().get(topicView.getStorageType());
		topicView.setStorage(storage);

		// Fill Schema
		if (topicView.getSchemaId() != null) {
			SchemaView schemaView;
			schemaView = m_schemaService.getSchemaView(topicView.getSchemaId());
			topicView.setSchema(schemaView);
		}

		// Fill Codec
		Codec codec = m_codecService.getCodecs().get(topicView.getCodecType());
		topicView.setCodec(codec);

		// Fill Partitions
		addPartitions4TopicView(topicView, m_partitionDao.findByTopic(topicView.getId(), false));

		return topicView;

	}
}
