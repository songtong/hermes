package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
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

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.model.MetaEntity;

@Named
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
		List<com.ctrip.hermes.meta.entity.Topic> topics = findTopics(true);
		for (com.ctrip.hermes.meta.entity.Topic entity : topics) {
			metaEntity.addTopic(entity);
		}
		metaModel.setValue(JSON.toJSONString(metaEntity));
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
