package com.ctrip.hermes.metaservice.service;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedProducerDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;

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
	private ZookeeperService m_zookeeperService;

	/**
	 * 
	 * @param topicName
	 * @param partition
	 */
	public Topic addPartitionsForTopic(String topicName, List<Partition> partitions) throws Exception {
		Topic topic = findTopicByName(topicName);
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));

		int partitionId = 0;
		for (Partition p : topic.getPartitions()) {
			if (p.getId() != null && p.getId() >= partitionId) {
				partitionId = p.getId() + 1;
			}
		}

		for (Partition partition : partitions) {
			partition.setId(partitionId++);
			com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter.convert(partition);
			partitionModel.setTopicId(topic.getId());
			m_partitionDao.insert(partitionModel);

			if (Storage.MYSQL.equals(topic.getStorageType())) {
				if (!m_topicStorageService.addPartitionForTopic(topic, partition)) {
					partitionId--;
					m_logger.error("Add new topic partition failed, please try later.");
					throw new RuntimeException("Add new topic partition failed, please try later.");
				}

			}
		}

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
			m_zookeeperService.ensureBrokerLeaseZkPath(topic);
		}

		return topic;
	}

	public void addPartitionStorage(String ds, String table, int span) throws StorageHandleErrorException {
		m_topicStorageService.addPartitionStorage(ds, table, span);
	}

	
	/**
	 * @param topicEntity
	 * @return
	 * @throws DalException
	 */
	public Topic createTopic(Topic topicEntity) throws Exception {
		tm.startTransaction("fxhermesmetadb");
		try {
			topicEntity.setCreateTime(new Date(System.currentTimeMillis()));
			com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
			m_topicDao.insert(topicModel);
			topicEntity.setId(topicModel.getId());

			int partitionId = 0;
			for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topicEntity.getPartitions()) {
				com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter
				      .convert(partitionEntity);
				partitionModel.setId(partitionId++);
				partitionModel.setTopicId(topicModel.getId());
				partitionEntity.setId(partitionModel.getId());
				m_partitionDao.insert(partitionModel);
			}
			tm.commitTransaction();
		} catch (Exception e) {
			m_logger.warn("create topic failed", e);
			tm.rollbackTransaction();
			throw e;
		}
		if (Storage.MYSQL.equals(topicEntity.getStorageType())) {
			if (!m_topicStorageService.initTopicStorage(topicEntity)) {
				m_logger.error("Init topic storage failed, please try later.");
				throw new RuntimeException("Init topic storage failed, please try later.");
			}

			m_zookeeperService.ensureConsumerLeaseZkPath(topicEntity);
			m_zookeeperService.ensureBrokerLeaseZkPath(topicEntity);
		}

		return topicEntity;
	}

	/**
	 * @param name
	 * @throws DalException
	 */
	public void deleteTopic(String name) throws Exception {
		Topic topic = findTopicByName(name);
		if (topic == null)
			return;
		for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topic.getPartitions()) {
			com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter.convert(partitionEntity);
			partitionModel.setTopicId(topic.getId());
			m_partitionDao.deleteByTopicId(partitionModel);
		}
		for (com.ctrip.hermes.meta.entity.ConsumerGroup cgEntity : topic.getConsumerGroups()) {
			com.ctrip.hermes.metaservice.model.ConsumerGroup cgModel = EntityToModelConverter.convert(cgEntity);
			m_consumerGroupDao.deleteByPK(cgModel);
		}
		for (com.ctrip.hermes.meta.entity.Producer producerEntity : topic.getProducers()) {
			com.ctrip.hermes.metaservice.model.Producer producerModel = EntityToModelConverter.convert(producerEntity);
			m_producerDao.deleteByPK(producerModel);
		}

		com.ctrip.hermes.metaservice.model.Topic proto = EntityToModelConverter.convert(topic);
		m_topicDao.deleteByPK(proto);
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
				m_topicStorageService.dropTopicStorage(topic);
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

	public Topic findTopicById(long id) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByPK(id);
			return fillTopic(model);
		} catch (DalException e) {
			m_logger.warn("findTopicById failed", e);
		}
		return null;
	}

	public Topic findTopicByName(String topic) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByName(topic);
			return fillTopic(model);
		} catch (DalException e) {
			m_logger.warn("findTopicByName failed, name: " + topic, e);
		}
		return null;
	}

	public List<Topic> findTopics(String pattern) {
		List<Topic> filtered = new ArrayList<Topic>();

		for (Topic topic : getTopics().values()) {
			if (Pattern.matches(pattern, topic.getName())) {
				filtered.add(topic);
			}
		}

		Collections.sort(filtered, new Comparator<Topic>() {
			@Override
			public int compare(Topic o1, Topic o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});

		return filtered;
	}

	public Map<String, Topic> getTopics() {
		Map<String, Topic> result = new HashMap<String, Topic>();
		try {
			List<Topic> topics = findTopics(true);
			for (Topic t : topics) {
				result.put(t.getName(), t);
			}
		} catch (DalException e) {
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
	public Topic updateTopic(Topic topic) throws Exception {
		Topic originTopic = findTopicByName(topic.getName());

		originTopic.setAckTimeoutSeconds(topic.getAckTimeoutSeconds());
		originTopic.setCodecType(topic.getCodecType());
		originTopic.setConsumerRetryPolicy(topic.getConsumerRetryPolicy());
		originTopic.setCreateBy(topic.getCreateBy());
		originTopic.setDescription(topic.getDescription());
		originTopic.setEndpointType(topic.getEndpointType());
		originTopic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		originTopic.setStatus(topic.getStatus());

		List<Partition> partitions = new ArrayList<>();
		for (Partition partition : topic.getPartitions()) {
			if (partition.getId() == -1) {
				partitions.add(partition);
			}
		}
		addPartitionsForTopic(originTopic.getName(), partitions);
		com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(originTopic);
		m_topicDao.updateByPK(topicModel, TopicEntity.UPDATESET_FULL);

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
			m_zookeeperService.ensureBrokerLeaseZkPath(topic);
		}

		return originTopic;
	}

	public List<com.ctrip.hermes.meta.entity.Topic> findTopics(boolean isFillDetail) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Topic> models = m_topicDao.list();
		List<com.ctrip.hermes.meta.entity.Topic> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Topic model : models) {
			if (isFillDetail) {
				entities.add(fillTopic(model));
			} else {
				entities.add(ModelToEntityConverter.convert(model));
			}
		}
		return entities;
	}

	protected com.ctrip.hermes.meta.entity.Topic fillTopic(com.ctrip.hermes.metaservice.model.Topic topicModel)
	      throws DalException {
		com.ctrip.hermes.meta.entity.Topic topicEntity = ModelToEntityConverter.convert(topicModel);
		Collection<com.ctrip.hermes.metaservice.model.ConsumerGroup> cgModels = m_consumerGroupDao.findByTopic(topicModel
		      .getId());
		for (com.ctrip.hermes.metaservice.model.ConsumerGroup model : cgModels) {
			com.ctrip.hermes.meta.entity.ConsumerGroup entity = ModelToEntityConverter.convert(model);
			topicEntity.addConsumerGroup(entity);
		}
		List<com.ctrip.hermes.metaservice.model.Partition> partitionModels = m_partitionDao.findByTopic(topicModel
		      .getId());
		for (com.ctrip.hermes.metaservice.model.Partition model : partitionModels) {
			com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
			topicEntity.addPartition(entity);
		}
		// List<com.ctrip.hermes.meta.entity.Producer> producers = findProducers(model);
		// for (com.ctrip.hermes.meta.entity.Producer p : producers) {
		// entity.addProducer(p);
		// }
		return topicEntity;
	}
	// @Override
	// public List<com.ctrip.hermes.meta.entity.Producer> findProducers(com.ctrip.hermes.metaservice.model.Topic topicModel)
	// throws DalException {
	// List<Producer> models = m_producerDao.findByTopic(topicModel.getId());
	// List<com.ctrip.hermes.meta.entity.Producer> entities = new ArrayList<>();
	// for (Producer model : models) {
	// com.ctrip.hermes.meta.entity.Producer entity = ModelToEntityConverter.convert(model);
	// entities.add(entity);
	// }
	// return entities;
	// }
}

class ZKStringSerializer implements ZkSerializer {

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		if (bytes == null)
			return null;
		else
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
	}

	@Override
	public byte[] serialize(Object data) throws ZkMarshallingError {
		byte[] bytes = null;
		try {
			bytes = data.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError(e);
		}
		return bytes;
	}

}