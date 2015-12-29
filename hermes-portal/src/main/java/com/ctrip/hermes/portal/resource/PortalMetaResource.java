package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.model.AppDao;
import com.ctrip.hermes.metaservice.model.CodecDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.DatasourceDao;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.PartitionDao;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.ServerDao;
import com.ctrip.hermes.metaservice.model.StorageDao;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.service.EntityToModelConverter;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PortalMetaResource {

	private static final Logger logger = LoggerFactory.getLogger(PortalMetaResource.class);

	private PortalMetaService metaService = PlexusComponentLocator.lookup(PortalMetaService.class);

	@GET
	@Path("refresh")
	public Response refreshMeta() {
		Meta meta = null;
		try {
			meta = metaService.refreshMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.warn("refresh meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@GET
	@Path("preview")
	public Response previewNewMeta() {
		Meta meta = null;
		try {
			meta = metaService.previewNewMeta();
		} catch (DalException e) {
			logger.warn("preview meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	@Path("build")
	public Response buildNewMeta() {
		Meta meta = null;
		try {
			meta = metaService.buildNewMeta();
		} catch (DalException e) {
			logger.warn("build meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@POST
	@Path("refactor")
	public Response refactorMeta() {
		try {
			MetaDao metaDao = PlexusComponentLocator.lookup(MetaDao.class);
			com.ctrip.hermes.metaservice.model.Meta metaModel = metaDao
			      .findLatest(com.ctrip.hermes.metaservice.model.MetaEntity.READSET_FULL);
			com.ctrip.hermes.meta.entity.Meta metaEntity = JSON.parseObject(metaModel.getValue(),
			      com.ctrip.hermes.meta.entity.Meta.class);
			metaModel.setVersion(metaEntity.getVersion());
			metaDao.updateByPK(metaModel, com.ctrip.hermes.metaservice.model.MetaEntity.UPDATESET_FULL);

			ServerDao serverDao = PlexusComponentLocator.lookup(ServerDao.class);

			for (com.ctrip.hermes.meta.entity.Server entity : metaEntity.getServers().values()) {
				com.ctrip.hermes.metaservice.model.Server serverModel = EntityToModelConverter.convert(entity);
				serverDao.insert(serverModel);
				System.out.println(serverModel);
			}

			StorageDao storageDao = PlexusComponentLocator.lookup(StorageDao.class);
			DatasourceDao datasourceDao = PlexusComponentLocator.lookup(DatasourceDao.class);

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

			EndpointDao endpointDao = PlexusComponentLocator.lookup(EndpointDao.class);

			for (com.ctrip.hermes.meta.entity.Endpoint entity : metaEntity.getEndpoints().values()) {
				com.ctrip.hermes.metaservice.model.Endpoint endpointModel = EntityToModelConverter.convert(entity);
				endpointDao.insert(endpointModel);
				System.out.println(endpointModel);
			}

			AppDao appDao = PlexusComponentLocator.lookup(AppDao.class);

			for (com.ctrip.hermes.meta.entity.App entity : metaEntity.getApps().values()) {
				com.ctrip.hermes.metaservice.model.App appModel = EntityToModelConverter.convert(entity);
				appDao.insert(appModel);
				System.out.println(appModel);
			}

			CodecDao codecDao = PlexusComponentLocator.lookup(CodecDao.class);

			for (com.ctrip.hermes.meta.entity.Codec entity : metaEntity.getCodecs().values()) {
				com.ctrip.hermes.metaservice.model.Codec codecModel = EntityToModelConverter.convert(entity);
				System.out.println(codecModel);
			}

			TopicDao topicDao = PlexusComponentLocator.lookup(TopicDao.class);
			PartitionDao partitionDao = PlexusComponentLocator.lookup(PartitionDao.class);
			ProducerDao producerDao = PlexusComponentLocator.lookup(ProducerDao.class);
			ConsumerGroupDao consumerGroupDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);

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
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.warn("refactor meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}
}
