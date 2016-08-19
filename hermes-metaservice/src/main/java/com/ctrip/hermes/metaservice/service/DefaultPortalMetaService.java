package com.ctrip.hermes.metaservice.service;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.converter.EntityToModelConverter;
import com.ctrip.hermes.metaservice.model.MetaEntity;

@Named
public class DefaultPortalMetaService extends DefaultMetaService implements PortalMetaService, Initializable {
	public static final String ID = "portal-meta-service";

	protected static final Logger logger = LoggerFactory.getLogger(DefaultPortalMetaService.class);

	@Inject
	private AppService m_appService;

	@Inject
	private CodecService m_codecService;

	@Inject
	private ConsumerService m_consumerService;

	@Inject
	private StorageService m_dsService;

	@Inject
	private EndpointService m_endpointService;

	@Inject
	private IdcService m_idcService;

	@Inject
	private PartitionService m_partitionService;

	@Inject
	private ServerService m_serverService;

	@Inject
	private TopicService m_topicService;

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
		Meta metaEntity = new Meta();
		metaEntity.setVersion(m_metaDao.findLatest(MetaEntity.READSET_FULL).getVersion() + 1);
		List<com.ctrip.hermes.meta.entity.App> apps = m_appService.findApps(true);
		for (com.ctrip.hermes.meta.entity.App entity : apps) {
			metaEntity.addApp(entity);
		}
		List<com.ctrip.hermes.meta.entity.Codec> codecs = m_codecService.findCodecs(true);
		for (com.ctrip.hermes.meta.entity.Codec entity : codecs) {
			metaEntity.addCodec(entity);
		}
		List<com.ctrip.hermes.meta.entity.Endpoint> endpoints = m_endpointService.findEndpoints(true);
		for (com.ctrip.hermes.meta.entity.Endpoint entity : endpoints) {
			metaEntity.addEndpoint(entity);
		}

		List<com.ctrip.hermes.meta.entity.Idc> idcs = m_idcService.listIdcEntities();
		for (com.ctrip.hermes.meta.entity.Idc entity : idcs) {
			metaEntity.addIdc(entity);
		}

		List<com.ctrip.hermes.meta.entity.Server> servers = m_serverService.listServerEntities();
		for (com.ctrip.hermes.meta.entity.Server entity : servers) {
			metaEntity.addServer(entity);
		}

		List<com.ctrip.hermes.meta.entity.Storage> storages = m_dsService.findStorages(true);
		for (com.ctrip.hermes.meta.entity.Storage entity : storages) {
			metaEntity.addStorage(entity);
		}
		List<com.ctrip.hermes.meta.entity.Topic> topics = m_topicService.findTopicEntities(true);
		for (com.ctrip.hermes.meta.entity.Topic entity : topics) {
			metaEntity.addTopic(entity);
		}
		return metaEntity;
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

}
