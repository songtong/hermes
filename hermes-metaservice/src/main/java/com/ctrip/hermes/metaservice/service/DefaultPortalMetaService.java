package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Codes;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.model.DatasourceEntity;
import com.ctrip.hermes.metaservice.model.MetaEntity;
import com.ctrip.hermes.metaservice.model.TopicEntity;

@Named(type = PortalMetaService.class, value = DefaultPortalMetaService.ID)
public class DefaultPortalMetaService extends DefaultMetaService implements PortalMetaService, Initializable {
	public static final String ID = "portal-meta-service";

	protected static final Logger logger = LoggerFactory.getLogger(DefaultPortalMetaService.class);

	@Inject
	private ZookeeperService m_zookeeperService;

	@Override
	public synchronized Meta buildNewMeta() throws DalException {
		Meta metaEntity = previewNewMeta();
		com.ctrip.hermes.metaservice.model.Meta metaModel = EntityToModelConverter.convert(metaEntity);
		m_metaDao.insert(metaModel);
		refreshMeta();
		try {
			m_zookeeperService.updateZkBaseMetaVersion(this.getMetaEntity().getVersion());
		} catch (Exception e) {
			m_logger.warn("update zk base meta failed", e);
		}
		return metaEntity;
	}

	@Override
	public synchronized Meta previewNewMeta() throws DalException {
		com.ctrip.hermes.metaservice.model.Meta metaModel = m_metaDao.findLatest(MetaEntity.READSET_FULL);
		metaModel.setVersion(metaModel.getVersion() + 1);
		Meta metaEntity = new Meta();
		metaEntity.setVersion(metaModel.getVersion());
		List<com.ctrip.hermes.meta.entity.App> apps = findApps();
		for (com.ctrip.hermes.meta.entity.App entity : apps) {
			metaEntity.addApp(entity);
		}
		List<com.ctrip.hermes.meta.entity.Codec> codecs = findCodecs();
		for (com.ctrip.hermes.meta.entity.Codec entity : codecs) {
			metaEntity.addCodec(entity);
		}
		List<com.ctrip.hermes.meta.entity.Endpoint> endpoints = findEndpoints();
		for (com.ctrip.hermes.meta.entity.Endpoint entity : endpoints) {
			metaEntity.addEndpoint(entity);
		}
		List<com.ctrip.hermes.meta.entity.Server> servers = findServers();
		for (com.ctrip.hermes.meta.entity.Server entity : servers) {
			metaEntity.addServer(entity);
		}
		List<com.ctrip.hermes.meta.entity.Storage> storages = findStorages();
		for (com.ctrip.hermes.meta.entity.Storage entity : storages) {
			metaEntity.addStorage(entity);
		}
		List<com.ctrip.hermes.meta.entity.Topic> topics = findTopics();
		for (com.ctrip.hermes.meta.entity.Topic entity : topics) {
			metaEntity.addTopic(entity);
		}
		metaModel.setValue(JSON.toJSONString(metaEntity));
		return metaEntity;
	}

	@Override
	public Map<String, Topic> getTopics() {
		Map<String, Topic> result = new HashMap<String, Topic>();
		try {
			List<Topic> topics = findTopics();
			for (Topic t : topics) {
				result.put(t.getName(), t);
			}
		} catch (DalException e) {
			logger.warn("get topics failed", e);
		}
		return result;
	}

	@Override
	public Topic findTopicById(long id) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByPK(id, TopicEntity.READSET_FULL);
			return fillTopic(model);

		} catch (DalException e) {
			logger.warn("findTopicById failed", e);
		}
		return null;
	}

	@Override
	public Topic findTopicByName(String topic) {
		try {
			com.ctrip.hermes.metaservice.model.Topic model = this.m_topicDao.findByName(topic, TopicEntity.READSET_FULL);
			return fillTopic(model);
		} catch (DalException e) {
			logger.warn("findTopicById failed", e);
		}
		return null;
	}

	@Override
	public Map<String, Codec> getCodecs() {
		Map<String, Codec> result = new HashMap<String, Codec>();
		try {
			for (Codec codec : findCodecs()) {
				result.put(codec.getType(), codec);
			}
		} catch (DalException e) {
			logger.warn("getCodecs failed", e);
		}
		return result;
	}

	@Override
	public Codec findCodecByType(String type) {
		return getCodecs().get(type);
	}

	@Override
	public Codec findCodecByTopic(String topicName) {
		Topic topic = findTopicByName(topicName);
		return topic != null ? findCodecByType(topic.getCodecType()) : null;
	}

	@Override
	public Map<String, Endpoint> getEndpoints() {
		Map<String, Endpoint> result = new HashMap<String, Endpoint>();
		try {
			for (Endpoint e : findEndpoints()) {
				result.put(e.getId(), e);
			}
		} catch (DalException e) {
			logger.warn("getEndpoints failed", e);
		}
		return result;
	}

	@Override
	public synchronized void addEndpoint(Endpoint endpoint) throws Exception {
		com.ctrip.hermes.metaservice.model.Endpoint proto = EntityToModelConverter.convert(endpoint);
		m_endpointDao.insert(proto);
		logger.info("Add Endpoint: {} done.", endpoint);
	}

	public Map<String, Storage> getStorages() {
		Map<String, Storage> result = new HashMap<>();
		try {
			List<Storage> storages = findStorages();
			for (Storage s : storages) {
				result.put(s.getType(), s);
			}
		} catch (DalException e) {
			logger.warn("getStorages failed", e);
		}
		return result;
	}

	@Override
	public Map<String, Datasource> getDatasources() {
		Map<String, Datasource> idMap = new HashMap<>();
		List<Datasource> dss = new ArrayList<>();

		for (Storage storage : getStorages().values()) {
			dss.addAll(storage.getDatasources());
		}

		for (Datasource ds : dss) {
			if (idMap.containsKey(ds.getId())) {
				logger.warn("Duplicated Datasource: key {}, Datasource: {}", ds.getId(), ds.toString());
			}
			idMap.put(ds.getId(), ds);
		}
		return idMap;
	}

	@Override
	public Storage findStorageByTopic(String topicName) {
		Topic topic = findTopicByName(topicName);
		try {
			List<Storage> storages = findStorages();
			for (Storage s : storages) {
				if (s.getType().equals(topic.getStorageType()))
					return s;
			}
		} catch (Exception e) {
			logger.warn("findStorageByTopic failed", e);
		}
		return null;
	}

	@Override
	public List<Partition> findPartitionsByTopic(String topicName) {
		Topic topic = findTopicByName(topicName);
		return topic.getPartitions();
	}

	@Override
	public Datasource findDatasource(String storageType, String datasourceId) {
		List<Datasource> datasources = findDatasources(storageType);
		for (Datasource d : datasources) {
			if (d.getId().equals(datasourceId)) {
				Property p = d.getProperties().get("password");
				if (p != null && p.getValue().startsWith("~{") && p.getValue().endsWith("}")) {
					p.setValue(Codes.forDecode().decode(p.getValue().substring(2, p.getValue().length() - 1)));
					d.getProperties().put("password", p);
				}
				return d;
			}
		}
		return null;
	}

	@Override
	public void deleteEndpoint(String endpointId) throws Exception {
		com.ctrip.hermes.metaservice.model.Endpoint proto = new com.ctrip.hermes.metaservice.model.Endpoint();
		proto.setId(endpointId);
		m_endpointDao.deleteByPK(proto);
		logger.info("Delete Endpoint: id:{} done.", endpointId);
	}

	@Override
	public void addDatasource(Datasource datasource, String dsType) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource proto = EntityToModelConverter.convert(datasource);
		proto.setId(datasource.getId());
		proto.setStorageType(dsType);
		m_datasourceDao.insert(proto);
		logger.info("Add Datasource: DS: {} done.", datasource);
	}

	@Override
	public void deleteDatasource(String id, String dsType) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource proto = new com.ctrip.hermes.metaservice.model.Datasource();
		proto.setId(id);
		m_datasourceDao.deleteByPK(proto);
		logger.info("Delete Datasource: type:{}, id:{} done. updating Meta.", dsType, id);
	}

	@Override
	public void updateDatasource(Datasource dsEntity) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource dsModel = m_datasourceDao.findByPK(dsEntity.getId(),
		      DatasourceEntity.READSET_FULL);
		dsModel.setProperties(JSON.toJSONString(dsEntity.getProperties()));
		m_datasourceDao.updateByPK(dsModel, DatasourceEntity.UPDATESET_FULL);
	}

	private void syncMetaFromDB() {
		try {
			if (isMetaUpdated()) {
				m_zookeeperService.updateZkBaseMetaVersion(this.getMetaEntity().getVersion());
			}
		} catch (Exception e) {
			m_logger.warn("Update meta from db failed, maybe update base meta version in zk failed.", e);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		syncMetaFromDB();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("UpdateMetaUseDB", true))
		      .scheduleWithFixedDelay(new Runnable() {
			      @Override
			      public void run() {
				      syncMetaFromDB();
			      }
		      }, 1, 1, TimeUnit.MINUTES); // sync from db with interval: 1 mins
	}

	@Override
	public Partition findPartition(String topicName, int partitionId) {
		List<Partition> partitions = findPartitionsByTopic(topicName);
		for (Partition partition : partitions) {
			if (partitionId == partition.getId()) {
				return partition;
			}
		}
		return null;
	}

	@Override
	public List<Datasource> findDatasources(String storageType) {
		List<Datasource> result = new ArrayList<>();
		try {
			List<com.ctrip.hermes.metaservice.model.Datasource> datasources = m_datasourceDao.findByStorageType(
			      storageType, DatasourceEntity.READSET_FULL);
			for (com.ctrip.hermes.metaservice.model.Datasource d : datasources) {
				result.add(ModelToEntityConverter.convert(d));
			}
		} catch (Exception e) {
			logger.warn("findDatasources failed", e);
		}
		return result;
	}

	@Override
	public List<ConsumerGroup> findConsumersByTopic(String topicName) {
		Topic topic = findTopicByName(topicName);
		return topic.getConsumerGroups();
	}

	@Override
	public String getZookeeperList() {
		List<Storage> storages = new ArrayList<>();
		try {
			storages = findStorages();
		} catch (DalException e) {
			logger.warn("findStorages failed", e);
		}
		for (Storage storage : storages) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("zookeeper.connect".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

	@Override
	public String getKafkaBrokerList() {
		List<Storage> storages = new ArrayList<>();
		try {
			storages = findStorages();
		} catch (DalException e) {
			logger.warn("findStorages failed", e);
		}
		for (Storage storage : storages) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("bootstrap.servers".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

}
