package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.dal.CachedAppDao;
import com.ctrip.hermes.metaservice.dal.CachedCodecDao;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;
import com.ctrip.hermes.metaservice.dal.CachedServerDao;
import com.ctrip.hermes.metaservice.dal.CachedStorageDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.model.App;
import com.ctrip.hermes.metaservice.model.Codec;
import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.model.Datasource;
import com.ctrip.hermes.metaservice.model.DatasourceEntity;
import com.ctrip.hermes.metaservice.model.Endpoint;
import com.ctrip.hermes.metaservice.model.Meta;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.MetaEntity;
import com.ctrip.hermes.metaservice.model.Partition;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.ctrip.hermes.metaservice.model.Producer;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.ProducerEntity;
import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.Storage;
import com.ctrip.hermes.metaservice.model.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService {
	protected static final Logger m_logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	protected MetaDao m_metaDao;

	@Inject
	protected CachedAppDao m_appDao;

	@Inject
	protected CachedCodecDao m_codecDao;

	@Inject
	protected CachedEndpointDao m_endpointDao;

	@Inject
	protected CachedServerDao m_serverDao;

	@Inject
	protected CachedStorageDao m_storageDao;

	@Inject
	protected CachedTopicDao m_topicDao;

	@Inject
	protected CachedConsumerGroupDao m_consumerGroupDao;

	@Inject
	protected PartitionDao m_partitionDao;

	@Inject
	protected ProducerDao m_producerDao;

	@Inject
	protected CachedDatasourceDao m_datasourceDao;

	private Meta m_metaModel;

	private com.ctrip.hermes.meta.entity.Meta m_metaEntity;

	@Override
	public List<com.ctrip.hermes.meta.entity.App> findApps() throws DalException {
		Collection<App> models = m_appDao.list();
		List<com.ctrip.hermes.meta.entity.App> entities = new ArrayList<>();
		for (App model : models) {
			com.ctrip.hermes.meta.entity.App entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Codec> findCodecs() throws DalException {
		Collection<Codec> models = m_codecDao.list();
		List<com.ctrip.hermes.meta.entity.Codec> entities = new ArrayList<>();
		for (Codec model : models) {
			com.ctrip.hermes.meta.entity.Codec entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.ConsumerGroup> findConsumerGroups(
	      com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException {
		List<ConsumerGroup> models = m_consumerGroupDao.findByTopicId(topicModel.getId(),
		      ConsumerGroupEntity.READSET_FULL);
		List<com.ctrip.hermes.meta.entity.ConsumerGroup> entities = new ArrayList<>();
		for (ConsumerGroup model : models) {
			com.ctrip.hermes.meta.entity.ConsumerGroup entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Datasource> findDatasources(
	      com.ctrip.hermes.metaservice.model.Storage storageModel) throws DalException {
		List<Datasource> models = m_datasourceDao
		      .findByStorageType(storageModel.getType(), DatasourceEntity.READSET_FULL);
		List<com.ctrip.hermes.meta.entity.Datasource> entities = new ArrayList<>();
		for (Datasource model : models) {
			com.ctrip.hermes.meta.entity.Datasource entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Endpoint> findEndpoints() throws DalException {
		Collection<Endpoint> models = m_endpointDao.list();
		List<com.ctrip.hermes.meta.entity.Endpoint> entities = new ArrayList<>();
		for (Endpoint model : models) {
			com.ctrip.hermes.meta.entity.Endpoint entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public com.ctrip.hermes.meta.entity.Meta refreshMeta() throws DalException {
		try {
			m_metaModel = m_metaDao.findLatest(MetaEntity.READSET_FULL);
			m_metaEntity = ModelToEntityConverter.convert(m_metaModel);
		} catch (DalNotFoundException e) {
			m_logger.warn("find Latest Meta failed", e);
			m_metaEntity = new com.ctrip.hermes.meta.entity.Meta();
			m_metaEntity.addStorage(new com.ctrip.hermes.meta.entity.Storage(com.ctrip.hermes.meta.entity.Storage.MYSQL));
			m_metaEntity.addStorage(new com.ctrip.hermes.meta.entity.Storage(com.ctrip.hermes.meta.entity.Storage.KAFKA));
			m_metaEntity.setVersion(0L);
		}
		return m_metaEntity;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Partition> findPartitions(
	      com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException {
		List<Partition> models = m_partitionDao.findByTopicId(topicModel.getId(), PartitionEntity.READSET_FULL);
		List<com.ctrip.hermes.meta.entity.Partition> entities = new ArrayList<>();
		for (Partition model : models) {
			com.ctrip.hermes.meta.entity.Partition entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Producer> findProducers(com.ctrip.hermes.metaservice.model.Topic topicModel)
	      throws DalException {
		List<Producer> models = m_producerDao.findByTopicId(topicModel.getId(), ProducerEntity.READSET_FULL);
		List<com.ctrip.hermes.meta.entity.Producer> entities = new ArrayList<>();
		for (Producer model : models) {
			com.ctrip.hermes.meta.entity.Producer entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Server> findServers() throws DalException {
		Collection<Server> models = m_serverDao.list();
		List<com.ctrip.hermes.meta.entity.Server> entities = new ArrayList<>();
		for (Server model : models) {
			com.ctrip.hermes.meta.entity.Server entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Storage> findStorages() throws DalException {
		Collection<Storage> models = m_storageDao.list();
		List<com.ctrip.hermes.meta.entity.Storage> entities = new ArrayList<>();
		for (Storage model : models) {
			entities.add(fillStorage(model));
		}
		return entities;
	}

	@Override
	public com.ctrip.hermes.meta.entity.Storage getStorage(String type) throws DalException {
		List<com.ctrip.hermes.meta.entity.Storage> storages = findStorages();
		for (com.ctrip.hermes.meta.entity.Storage s : storages) {
			if (s.getType().equals(type)) {
				return s;
			}
		}
		return new com.ctrip.hermes.meta.entity.Storage();
	}

	protected com.ctrip.hermes.meta.entity.Storage fillStorage(Storage model) throws DalException {
		com.ctrip.hermes.meta.entity.Storage entity = ModelToEntityConverter.convert(model);
		List<com.ctrip.hermes.meta.entity.Datasource> datasources = findDatasources(model);
		for (com.ctrip.hermes.meta.entity.Datasource ds : datasources) {
			entity.addDatasource(ds);
		}
		return entity;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Topic> findTopics() throws DalException {
		Collection<Topic> models = m_topicDao.list();
		List<com.ctrip.hermes.meta.entity.Topic> entities = new ArrayList<>();
		for (Topic model : models) {
			entities.add(fillTopic(model));
		}
		return entities;
	}

	protected com.ctrip.hermes.meta.entity.Topic fillTopic(Topic model) throws DalException {
		com.ctrip.hermes.meta.entity.Topic entity = ModelToEntityConverter.convert(model);
		List<com.ctrip.hermes.meta.entity.ConsumerGroup> consumerGroups = findConsumerGroups(model);
		for (com.ctrip.hermes.meta.entity.ConsumerGroup cg : consumerGroups) {
			entity.addConsumerGroup(cg);
		}
		List<com.ctrip.hermes.meta.entity.Partition> partitions = findPartitions(model);
		for (com.ctrip.hermes.meta.entity.Partition p : partitions) {
			entity.addPartition(p);
		}
		List<com.ctrip.hermes.meta.entity.Producer> producers = findProducers(model);
		for (com.ctrip.hermes.meta.entity.Producer p : producers) {
			entity.addProducer(p);
		}
		return entity;
	}

	public com.ctrip.hermes.meta.entity.Meta getMetaEntity() {
		if (m_metaModel == null || m_metaEntity == null) {
			try {
				refreshMeta();
			} catch (DalException e) {
				m_logger.warn("get meta entity failed", e);
				return new com.ctrip.hermes.meta.entity.Meta();
			}
		}
		return m_metaEntity;
	}

	protected boolean isMetaUpdated() {
		try {
			Meta latestMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
			if (m_metaModel == null || latestMeta.getVersion() > m_metaModel.getVersion()) {
				m_metaModel = latestMeta;
				m_metaEntity = ModelToEntityConverter.convert(m_metaModel);
				return true;
			}
		} catch (DalException e) {
			m_logger.warn("Update meta from db failed.", e);
		}
		return false;
	}
}
