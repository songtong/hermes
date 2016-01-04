package com.ctrip.hermes.metaservice.service;

import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.model.AppDao;
import com.ctrip.hermes.metaservice.model.AppEntity;
import com.ctrip.hermes.metaservice.model.CodecDao;
import com.ctrip.hermes.metaservice.model.CodecEntity;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.model.DatasourceDao;
import com.ctrip.hermes.metaservice.model.DatasourceEntity;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.ProducerEntity;
import com.ctrip.hermes.metaservice.model.ServerDao;
import com.ctrip.hermes.metaservice.model.ServerEntity;
import com.ctrip.hermes.metaservice.model.StorageDao;
import com.ctrip.hermes.metaservice.model.StorageEntity;
import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;

@Named
public class MetaRefactor {

	@Inject
	private TransactionManager tm;

	@Inject
	private MetaDao metaDao;

	@Inject
	private ServerDao serverDao;

	@Inject
	private StorageDao storageDao;

	@Inject
	private DatasourceDao datasourceDao;

	@Inject
	private EndpointDao endpointDao;

	@Inject
	private AppDao appDao;

	@Inject
	private CodecDao codecDao;

	@Inject
	private TopicDao topicDao;

	@Inject
	private PartitionDao partitionDao;

	@Inject
	private ProducerDao producerDao;

	@Inject
	private ConsumerGroupDao consumerGroupDao;

	public void restore() throws Exception {
		for (com.ctrip.hermes.metaservice.model.Topic model : topicDao.list(TopicEntity.READSET_FULL)) {
			for (com.ctrip.hermes.metaservice.model.ConsumerGroup cg : consumerGroupDao.findByTopicId(model.getId(),
			      ConsumerGroupEntity.READSET_FULL)) {
				consumerGroupDao.deleteByPK(cg);
			}
			for (com.ctrip.hermes.metaservice.model.Producer producer : producerDao.findByTopicId(model.getId(),
			      ProducerEntity.READSET_FULL)) {
				producerDao.deleteByPK(producer);
			}
			for (com.ctrip.hermes.metaservice.model.Partition partition : partitionDao.findByTopicId(model.getId(),
			      PartitionEntity.READSET_FULL)) {
				partitionDao.deleteByPK(partition);
			}
			topicDao.deleteByPK(model);
		}

		for (com.ctrip.hermes.metaservice.model.Codec model : codecDao.list(CodecEntity.READSET_FULL)) {
			codecDao.deleteByPK(model);
		}

		for (com.ctrip.hermes.metaservice.model.App model : appDao.list(AppEntity.READSET_FULL)) {
			appDao.deleteByPK(model);
		}

		for (com.ctrip.hermes.metaservice.model.Endpoint model : endpointDao.list(EndpointEntity.READSET_FULL)) {
			endpointDao.deleteByPK(model);
		}

		for (com.ctrip.hermes.metaservice.model.Storage model : storageDao.list(StorageEntity.READSET_FULL)) {
			for (com.ctrip.hermes.metaservice.model.Datasource ds : datasourceDao.findByStorageType(model.getType(),
			      DatasourceEntity.READSET_FULL)) {
				datasourceDao.deleteByPK(ds);
			}
			storageDao.deleteByPK(model);
		}

		for (com.ctrip.hermes.metaservice.model.Server model : serverDao.list(ServerEntity.READSET_FULL)) {
			serverDao.deleteByPK(model);
		}
	}

	public void refactor() throws Exception {
		tm.startTransaction("fxhermesmetadb");
		try {
			com.ctrip.hermes.metaservice.model.Meta metaModel = metaDao
			      .findLatest(com.ctrip.hermes.metaservice.model.MetaEntity.READSET_FULL);
			com.ctrip.hermes.meta.entity.Meta metaEntity = JSON.parseObject(metaModel.getValue(),
			      com.ctrip.hermes.meta.entity.Meta.class);
			metaModel.setVersion(metaEntity.getVersion());
			metaDao.insert(metaModel);

			for (com.ctrip.hermes.meta.entity.Server entity : metaEntity.getServers().values()) {
				com.ctrip.hermes.metaservice.model.Server serverModel = EntityToModelConverter.convert(entity);
				serverDao.insert(serverModel);
				System.out.println(serverModel);
			}

			for (com.ctrip.hermes.meta.entity.Storage entity : metaEntity.getStorages().values()) {
				com.ctrip.hermes.metaservice.model.Storage storageModel = EntityToModelConverter.convert(entity);
				storageDao.insert(storageModel);
				System.out.println(storageModel);

				for (com.ctrip.hermes.meta.entity.Datasource dsEntity : entity.getDatasources()) {
					com.ctrip.hermes.metaservice.model.Datasource dsModel = EntityToModelConverter.convert(dsEntity);
					dsModel.setStorageType(storageModel.getType());
					datasourceDao.insert(dsModel);
					System.out.println(dsModel);
				}
			}

			for (com.ctrip.hermes.meta.entity.Endpoint entity : metaEntity.getEndpoints().values()) {
				com.ctrip.hermes.metaservice.model.Endpoint endpointModel = EntityToModelConverter.convert(entity);
				endpointDao.insert(endpointModel);
				System.out.println(endpointModel);
			}

			for (com.ctrip.hermes.meta.entity.App entity : metaEntity.getApps().values()) {
				com.ctrip.hermes.metaservice.model.App appModel = EntityToModelConverter.convert(entity);
				appDao.insert(appModel);
				System.out.println(appModel);
			}

			for (com.ctrip.hermes.meta.entity.Codec entity : metaEntity.getCodecs().values()) {
				com.ctrip.hermes.metaservice.model.Codec codecModel = EntityToModelConverter.convert(entity);
				codecDao.insert(codecModel);
				System.out.println(codecModel);
			}

			for (com.ctrip.hermes.meta.entity.Topic topicEntity : metaEntity.getTopics().values()) {
				com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
				topicDao.insert(topicModel);
				System.out.println(topicModel);

				for (com.ctrip.hermes.meta.entity.Partition entity : topicEntity.getPartitions()) {
					com.ctrip.hermes.metaservice.model.Partition partitionModel = EntityToModelConverter.convert(entity);
					partitionModel.setTopicId(topicModel.getId());
					partitionDao.insert(partitionModel);
					System.out.println(partitionModel);
				}

				for (com.ctrip.hermes.meta.entity.Producer entity : topicEntity.getProducers()) {
					com.ctrip.hermes.metaservice.model.Producer producerModel = EntityToModelConverter.convert(entity);
					producerModel.setTopicId(topicModel.getId());
					producerDao.insert(producerModel);
					System.out.println(producerModel);
				}

				for (com.ctrip.hermes.meta.entity.ConsumerGroup entity : topicEntity.getConsumerGroups()) {
					com.ctrip.hermes.metaservice.model.ConsumerGroup model = EntityToModelConverter.convert(entity);
					model.setTopicId(topicModel.getId());
					consumerGroupDao.insert(model);
					System.out.println(model);
				}
				tm.commitTransaction();
			}
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
	}
}
