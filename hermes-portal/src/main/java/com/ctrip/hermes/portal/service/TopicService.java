package com.ctrip.hermes.portal.service;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.MetaServiceWrapper;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.portal.service.storage.TopicStorageService;

@Named
public class TopicService {

	private static final Logger m_logger = LoggerFactory.getLogger(TopicService.class);

	@Inject
	private MetaServiceWrapper m_metaService;

	@Inject
	private SchemaService m_schemaService;

	@Inject
	private TopicStorageService m_topicStorageService;

	@Inject
	private ZookeeperService m_zookeeperService;

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public Topic createTopic(Topic topic) throws Exception {
		Meta meta = m_metaService.getMeta();
		topic.setCreateTime(new Date(System.currentTimeMillis()));
		topic.setLastModifiedTime(topic.getCreateTime());
		long maxTopicId = 0;
		for (Topic topic2 : meta.getTopics().values()) {
			if (topic2.getId() != null && topic2.getId() > maxTopicId) {
				maxTopicId = topic2.getId();
			}
		}
		topic.setId(maxTopicId + 1);

		int partitionId = 0;
		for (Partition partition : topic.getPartitions()) {
			partition.setId(partitionId++);
		}

		meta.addTopic(topic);

		if (Endpoint.BROKER.equals(topic.getEndpointType())) {
			if (!m_topicStorageService.initTopicStorage(topic)) {
				throw new RuntimeException("Init topic storage failed, please try later.");
			}

			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
		}

		if (!m_metaService.updateMeta(meta)) {
			throw new RuntimeException("Update meta failed, please try later");
		}

		return topic;
	}

	/**
	 * 
	 * @param topic
	 */
	public void createTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.getPartitions(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic.getName());
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

		ZkClient zkClient = new ZkClient(zkConnect);
		zkClient.setZkSerializer(new ZKStringSerializer());
		int partition = 1;
		int replication = 1;
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if ("replication-factor".equals(prop.getName())) {
				replication = Integer.parseInt(prop.getValue());
			} else if ("partitions".equals(prop.getName())) {
				partition = Integer.parseInt(prop.getValue());
			} else if ("retention.ms".equals(prop.getName())) {
				topicProp.setProperty("retention.ms", prop.getValue());
			} else if ("retention.bytes".equals(prop.getName())) {
				topicProp.setProperty("retention.bytes", prop.getValue());
			}
		}

		m_logger.debug("create topic in kafka, topic {}, partition {}, replication {}, prop {}", topic.getName(),
		      partition, replication, topicProp);
		AdminUtils.createTopic(zkClient, topic.getName(), partition, replication, topicProp);
	}

	/**
	 * 
	 * @param topic
	 */
	public void deleteTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.getPartitions(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic.getName());
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

		ZkClient zkClient = new ZkClient(zkConnect);
		zkClient.setZkSerializer(new ZKStringSerializer());

		m_logger.debug("delete topic in kafka, topic {}", topic.getName());
		AdminUtils.deleteTopic(zkClient, topic.getName());
	}

	/**
	 * 
	 * @param topic
	 */
	public void configTopicInKafka(Topic topic) {
		List<Partition> partitions = m_metaService.getPartitions(topic.getName());
		if (partitions == null || partitions.size() < 1) {
			return;
		}

		String consumerDatasource = partitions.get(0).getReadDatasource();
		Storage targetStorage = m_metaService.findStorage(topic.getName());
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

		ZkClient zkClient = new ZkClient(zkConnect);
		zkClient.setZkSerializer(new ZKStringSerializer());
		Properties topicProp = new Properties();
		for (Property prop : topic.getProperties()) {
			if ("retention.ms".equals(prop.getName())) {
				topicProp.setProperty("retention.ms", prop.getValue());
			} else if ("retention.bytes".equals(prop.getName())) {
				topicProp.setProperty("retention.bytes", prop.getValue());
			}
		}

		m_logger.debug("config topic in kafka, topic {}, prop {}", topic.getName(), topicProp);
		AdminUtils.changeTopicConfig(zkClient, topic.getName(), topicProp);
	}

	/**
	 * 
	 * @param name
	 * @throws DalException
	 */
	public void deleteTopic(String name) throws Exception {
		Meta meta = m_metaService.getMeta();
		Topic topic = meta.findTopic(name);
		if (topic == null)
			return;
		meta.removeTopic(name);
		// Remove related schemas
		m_schemaService.deleteSchemas(topic);
		m_topicStorageService.dropTopicStorage(topic);
		m_zookeeperService.deleteConsumerLeaseZkPath(topic);
		m_metaService.updateMeta(meta);
	}

	public Storage findStorage(String topic) {
		return m_metaService.findStorage(topic);
	}

	public List<Topic> findTopics(String pattern) {
		return m_metaService.findTopicsByPattern(pattern);
	}

	public Topic getTopic(long topicId) {
		return m_metaService.findTopic(topicId);
	}

	public Topic getTopic(String topic) {
		return m_metaService.findTopicByName(topic);
	}

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public Topic updateTopic(Topic topic) throws DalException {
		Meta meta = m_metaService.getMeta();
		meta.removeTopic(topic.getName());
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		meta.addTopic(topic);
		if (Endpoint.BROKER.equals(topic.getEndpointType())) {
			m_zookeeperService.ensureConsumerLeaseZkPath(topic);
		}

		m_metaService.updateMeta(meta);
		return topic;
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