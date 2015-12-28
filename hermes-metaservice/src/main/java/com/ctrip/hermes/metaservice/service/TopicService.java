package com.ctrip.hermes.metaservice.service;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.*;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.ctrip.hermes.metaservice.service.storage.TopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

@Named
public class TopicService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicService.class);

	@Inject
	private PortalMetaService m_metaService;

	@Inject
	private SchemaService m_schemaService;

	@Inject
	private TopicStorageService m_topicStorageService;

	@Inject
	private TopicDao m_topicDao;

	@Inject
	private PartitionDao m_partitionDao;

	@Inject
	private ConsumerGroupDao m_consumerGroupDao;

	@Inject
	private ProducerDao m_producerDao;

	@Inject
	private ZookeeperService m_zookeeperService;

	private List<String> validKafkaConfigKeys = new ArrayList<String>();

	public static final int DEFAULT_KAFKA_PARTITIONS = 3;

	public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 2;

	public TopicService() {
		validKafkaConfigKeys.add("segment.index.bytes");
		validKafkaConfigKeys.add("segment.jitter.ms");
		validKafkaConfigKeys.add("min.cleanable.dirty.ratio");
		validKafkaConfigKeys.add("retention.bytes");
		validKafkaConfigKeys.add("file.delete.delay.ms");
		validKafkaConfigKeys.add("flush.ms");
		validKafkaConfigKeys.add("cleanup.policy");
		validKafkaConfigKeys.add("unclean.leader.election.enable");
		validKafkaConfigKeys.add("flush.messages");
		validKafkaConfigKeys.add("retention.ms");
		validKafkaConfigKeys.add("min.insync.replicas");
		validKafkaConfigKeys.add("delete.retention.ms");
		validKafkaConfigKeys.add("index.interval.bytes");
		validKafkaConfigKeys.add("segment.bytes");
		validKafkaConfigKeys.add("segment.ms");
	}

	/**
	 * @param topicEntity
	 * @return
	 * @throws DalException
	 */
	public Topic createTopic(Topic topicEntity) throws Exception {
		topicEntity.setCreateTime(new Date(System.currentTimeMillis()));
		topicEntity.setLastModifiedTime(new Date());
		long maxTopicId = 0;
		for (Topic topic2 : m_metaService.findTopics()) {
			if (topic2.getId() != null && topic2.getId() > maxTopicId) {
				maxTopicId = topic2.getId();
			}
		}
		topicEntity.setId(maxTopicId + 1);

		int partitionId = 0;
		for (Partition partition : topicEntity.getPartitions()) {
			partition.setId(partitionId++);
		}

		com.ctrip.hermes.metaservice.model.Topic topicModel = new com.ctrip.hermes.metaservice.model.Topic();
		if (topicEntity.getId() != null)
			topicModel.setId(topicEntity.getId());
		topicModel.setName(topicEntity.getName());
		if (topicEntity.getPartitionCount() != null)
			topicModel.setPartitionCount(topicEntity.getPartitionCount());
		topicModel.setStorageType(topicEntity.getStorageType());
		topicModel.setDescription(topicEntity.getDescription());
		topicModel.setStatus(topicEntity.getStatus());
		topicModel.setCreateTime(topicEntity.getCreateTime());
		if (topicEntity.getSchemaId() != null)
			topicModel.setSchemaId(topicEntity.getSchemaId());
		topicModel.setConsumerRetryPolicy(topicEntity.getConsumerRetryPolicy());
		topicModel.setCreateBy(topicEntity.getCreateBy());
		topicModel.setEndpointType(topicEntity.getEndpointType());
		topicModel.setAckTimeoutSeconds(topicEntity.getAckTimeoutSeconds());
		topicModel.setCodecType(topicEntity.getCodecType());
		topicModel.setOtherInfo(topicEntity.getOtherInfo());
		topicModel.setStoragePartitionSize(topicEntity.getStoragePartitionCount());
		topicModel.setResendPartitionSize(topicEntity.getResendPartitionSize());
		topicModel.setStoragePartitionCount(topicEntity.getStoragePartitionCount());
		topicModel.setProperties(JSON.toJSONString(topicEntity.getProperties()));
		topicModel.setPriorityMessageEnabled(topicEntity.isPriorityMessageEnabled());
		topicModel.setMetaId(m_metaService.getMetaEntity().getId());
		m_topicDao.insert(topicModel);

		for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topicEntity.getPartitions()) {
			com.ctrip.hermes.metaservice.model.Partition partitionModel = new com.ctrip.hermes.metaservice.model.Partition();
			partitionModel.setId(partitionEntity.getId());
			partitionModel.setEndpointId(partitionEntity.getEndpoint());
			partitionModel.setReadDatasourceId(partitionEntity.getReadDatasource());
			partitionModel.setWriteDatasourceId(partitionEntity.getWriteDatasource());
			partitionModel.setTopicId(topicModel.getId());
			m_partitionDao.insert(partitionModel);
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
	 * @param topic
	 */
	public void createTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.findPartitionsByTopic(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic.getName());
		if (targetStorage == null) {
			return;
		}

		String zkConnect = null;
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					if ("zookeeper.connect".equals(prop.getValue().getName())) {
						zkConnect = prop.getValue().getValue();
						break;
					}
				}
			}
		}

		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = DEFAULT_KAFKA_PARTITIONS;
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		int replication = DEFAULT_KAFKA_REPLICATION_FACTOR;
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if ("replication-factor".equals(prop.getName())) {
				replication = Integer.parseInt(prop.getValue());
			} else if ("partitions".equals(prop.getName())) {
				partition = Integer.parseInt(prop.getValue());
			} else if (validKafkaConfigKeys.contains(prop.getName())) {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}

		m_logger.info("create topic in kafka, topic {}, partition {}, replication {}, prop {}", topic.getName(),
		      partition, replication, topicProp);
		AdminUtils.createTopic(zkUtils, topic.getName(), partition, replication, topicProp);
	}

	/**
	 * @param topic
	 */
	public void deleteTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.findPartitionsByTopic(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic.getName());
		if (targetStorage == null) {
			return;
		}

		String zkConnect = null;
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					if ("zookeeper.connect".equals(prop.getValue().getName())) {
						zkConnect = prop.getValue().getValue();
						break;
					}
				}
			}
		}

		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		zkClient.setZkSerializer(new ZKStringSerializer());

		m_logger.info("delete topic in kafka, topic {}", topic.getName());
		AdminUtils.deleteTopic(zkUtils, topic.getName());
	}

	/**
	 * @param topic
	 */
	public void configTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.findPartitionsByTopic(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorageByTopic(topic.getName());
		if (targetStorage == null) {
			return;
		}

		String zkConnect = null;
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (consumerDatasource.equals(datasource.getId())) {
				Map<String, Property> properties = datasource.getProperties();
				for (Map.Entry<String, Property> prop : properties.entrySet()) {
					if ("zookeeper.connect".equals(prop.getValue().getName())) {
						zkConnect = prop.getValue().getValue();
						break;
					}
				}
			}
		}

		ZkClient zkClient = new ZkClient(new ZkConnection(zkConnect));
		zkClient.setZkSerializer(new ZKStringSerializer());
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if (validKafkaConfigKeys.contains(prop.getName())) {
				topicProp.setProperty(prop.getName(), prop.getValue());
			}
		}

		m_logger.info("config topic in kafka, topic {}, prop {}", topic.getName(), topicProp);
		AdminUtils.changeTopicConfig(zkUtils, topic.getName(), topicProp);
	}

	/**
	 * @param name
	 * @throws DalException
	 */
	public void deleteTopic(String name) throws Exception {
		Topic topic = m_metaService.findTopicByName(name);
		if (topic == null)
			return;
		for (com.ctrip.hermes.meta.entity.Partition partitionEntity : topic.getPartitions()) {
			com.ctrip.hermes.metaservice.model.Partition partitionModel = new com.ctrip.hermes.metaservice.model.Partition();
			partitionModel.setId(partitionEntity.getId());
			m_partitionDao.deleteByPK(partitionModel);
		}
		for (com.ctrip.hermes.meta.entity.ConsumerGroup cgEntity : topic.getConsumerGroups()) {
			com.ctrip.hermes.metaservice.model.ConsumerGroup cgModel = new com.ctrip.hermes.metaservice.model.ConsumerGroup();
			cgModel.setId(cgEntity.getId());
			m_consumerGroupDao.deleteByPK(cgModel);
		}
		for (com.ctrip.hermes.meta.entity.Producer producerEntity : topic.getProducers()) {
			com.ctrip.hermes.metaservice.model.Producer producerModel = new com.ctrip.hermes.metaservice.model.Producer();
			producerModel.setAppId(producerEntity.getAppId());
			m_producerDao.deleteByPK(producerModel);
		}

		com.ctrip.hermes.metaservice.model.Topic proto = new com.ctrip.hermes.metaservice.model.Topic();
		proto.setId(topic.getId());
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

	public List<Topic> findTopics(String pattern) {
		List<Topic> filtered = new ArrayList<Topic>();

		for (Topic topic : m_metaService.getTopics().values()) {
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

	public Topic findTopicById(long topicId) {
		return m_metaService.findTopicById(topicId);
	}

	public Topic findTopicByName(String topicName) {
		return m_metaService.findTopicByName(topicName);
	}

	/**
	 * @param topic
	 * @return
	 * @throws Exception
	 */
	public Topic updateTopic(Topic topic) throws Exception {
		Topic originTopic = m_metaService.findTopicByName(topic.getName());

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
		com.ctrip.hermes.metaservice.model.Topic topicModel = m_topicDao
		      .findByPK(topic.getId(), TopicEntity.READSET_FULL);
		if (originTopic.getId() != null)
			topicModel.setId(originTopic.getId());
		topicModel.setName(originTopic.getName());
		if (originTopic.getPartitionCount() != null)
			topicModel.setPartitionCount(originTopic.getPartitionCount());
		topicModel.setStorageType(originTopic.getStorageType());
		topicModel.setDescription(originTopic.getDescription());
		topicModel.setStatus(originTopic.getStatus());
		topicModel.setCreateTime(originTopic.getCreateTime());
		if (originTopic.getSchemaId() != null)
			topicModel.setSchemaId(originTopic.getSchemaId());
		topicModel.setConsumerRetryPolicy(originTopic.getConsumerRetryPolicy());
		topicModel.setCreateBy(originTopic.getCreateBy());
		topicModel.setEndpointType(originTopic.getEndpointType());
		topicModel.setAckTimeoutSeconds(originTopic.getAckTimeoutSeconds());
		topicModel.setCodecType(originTopic.getCodecType());
		topicModel.setOtherInfo(originTopic.getOtherInfo());
		topicModel.setStoragePartitionSize(originTopic.getStoragePartitionCount());
		topicModel.setResendPartitionSize(originTopic.getResendPartitionSize());
		topicModel.setStoragePartitionCount(originTopic.getStoragePartitionCount());
		topicModel.setProperties(JSON.toJSONString(originTopic.getProperties()));
		topicModel.setPriorityMessageEnabled(originTopic.isPriorityMessageEnabled());
		topicModel.setMetaId(m_metaService.getMetaEntity().getId());
		m_topicDao.updateByPK(topicModel, TopicEntity.UPDATESET_FULL);

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
			m_zookeeperService.ensureBrokerLeaseZkPath(topic);
		}

		return originTopic;
	}

	public Integer queryStorageSize(String ds) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageSize(ds);
	}

	public Integer queryStorageSize(String ds, String table) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageSize(ds, table);
	}

	public List<StorageTable> queryStorageTables(String ds) throws StorageHandleErrorException {
		return m_topicStorageService.queryStorageTables(ds);
	}

	public List<StoragePartition> queryStorageTablePartitions(String ds, String table)
	      throws StorageHandleErrorException {
		return m_topicStorageService.queryTablePartitions(ds, table);
	}

	/**
	 * 
	 * @param topicName
	 * @param partition
	 */
	public Topic addPartitionsForTopic(String topicName, List<Partition> partitions) throws Exception {
		Topic topic = m_metaService.findTopicByName(topicName);

		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));

		int partitionId = 0;
		for (Partition p : topic.getPartitions()) {
			if (p.getId() != null && p.getId() > partitionId) {
				partitionId = p.getId();
			}
		}

		for (Partition partition : partitions) {
			partition.setId(partitionId++);
			com.ctrip.hermes.metaservice.model.Partition partitionModel = new com.ctrip.hermes.metaservice.model.Partition();
			partitionModel.setId(partition.getId());
			partitionModel.setEndpointId(partition.getEndpoint());
			partitionModel.setReadDatasourceId(partition.getReadDatasource());
			partitionModel.setWriteDatasourceId(partition.getWriteDatasource());
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

	public void delPartitionStorage(String ds, String table) throws StorageHandleErrorException {
		m_topicStorageService.delPartitionStorage(ds, table);
	}

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