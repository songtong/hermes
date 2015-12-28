package com.ctrip.hermes.metaservice.model;

import java.io.IOException;

import org.junit.Test;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.metaservice.service.EntityToModelConverter;

public class MetaImporter extends ComponentTestCase {

	@Test
	public void importMeta() throws IOException, DalException {
		MetaDao metaDao = lookup(MetaDao.class);
		com.ctrip.hermes.metaservice.model.Meta metaModel = metaDao
		      .findLatest(com.ctrip.hermes.metaservice.model.MetaEntity.READSET_FULL);
		com.ctrip.hermes.meta.entity.Meta metaEntity = JSON.parseObject(metaModel.getValue(),
		      com.ctrip.hermes.meta.entity.Meta.class);
		metaModel.setVersion(metaEntity.getVersion());
		metaDao.updateByPK(metaModel, com.ctrip.hermes.metaservice.model.MetaEntity.UPDATESET_FULL);

		ServerDao serverDao = lookup(ServerDao.class);

		for (com.ctrip.hermes.meta.entity.Server entity : metaEntity.getServers().values()) {
			com.ctrip.hermes.metaservice.model.Server serverModel = EntityToModelConverter.convert(entity);
			serverModel.setMetaId(metaModel.getId());
			serverDao.insert(serverModel);
			System.out.println(serverModel);
		}

		StorageDao storageDao = lookup(StorageDao.class);
		DatasourceDao datasourceDao = lookup(DatasourceDao.class);

		for (com.ctrip.hermes.meta.entity.Storage entity : metaEntity.getStorages().values()) {
			com.ctrip.hermes.metaservice.model.Storage storageModel = EntityToModelConverter.convert(entity);
			storageModel.setMetaId(metaModel.getId());
			storageDao.insert(storageModel);
			System.out.println(storageModel);

			for (com.ctrip.hermes.meta.entity.Datasource dsEntity : entity.getDatasources()) {
				com.ctrip.hermes.metaservice.model.Datasource dsModel = EntityToModelConverter.convert(dsEntity);
				dsModel.setStorageType(storageModel.getType());
				datasourceDao.insert(dsModel);
				System.out.println(dsModel);
			}
		}

		EndpointDao endpointDao = lookup(EndpointDao.class);

		for (com.ctrip.hermes.meta.entity.Endpoint entity : metaEntity.getEndpoints().values()) {
			com.ctrip.hermes.metaservice.model.Endpoint endpointModel = EntityToModelConverter.convert(entity);
			endpointModel.setMetaId(metaModel.getId());
			endpointDao.insert(endpointModel);
			System.out.println(endpointModel);
		}

		AppDao appDao = lookup(AppDao.class);

		for (com.ctrip.hermes.meta.entity.App entity : metaEntity.getApps().values()) {
			com.ctrip.hermes.metaservice.model.App appModel = EntityToModelConverter.convert(entity);
			appModel.setMetaId(metaModel.getId());
			appDao.insert(appModel);
			System.out.println(appModel);
		}

		CodecDao codecDao = lookup(CodecDao.class);

		for (com.ctrip.hermes.meta.entity.Codec entity : metaEntity.getCodecs().values()) {
			com.ctrip.hermes.metaservice.model.Codec codecModel = EntityToModelConverter.convert(entity);
			codecModel.setMetaId(metaModel.getId());
			codecDao.insert(codecModel);
			System.out.println(codecModel);
		}

		TopicDao topicDao = lookup(TopicDao.class);
		PartitionDao partitionDao = lookup(PartitionDao.class);
		ProducerDao producerDao = lookup(ProducerDao.class);
		ConsumerGroupDao consumerGroupDao = lookup(ConsumerGroupDao.class);

		for (com.ctrip.hermes.meta.entity.Topic topicEntity : metaEntity.getTopics().values()) {
			com.ctrip.hermes.metaservice.model.Topic topicModel = EntityToModelConverter.convert(topicEntity);
			topicModel.setMetaId(metaModel.getId());
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
		}
	}

}
