package com.ctrip.hermes.metaservice.service;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Codes;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.ObjectUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = PortalMetaService.class, value = DefaultPortalMetaService.ID)
public class DefaultPortalMetaService extends DefaultMetaService implements PortalMetaService, Initializable {
	@Inject
	private ClientEnvironment m_env;

	public static final String ID = "default-meta-service-wrapper";

	protected Meta m_meta;

	@Override
	public Meta getMeta() {
		return ObjectUtils.deepCopy(m_meta, Meta.class);
	}

	@Override
	public synchronized boolean updateMeta(Meta meta) throws DalException {
		if (!meta.getVersion().equals(m_meta.getVersion())) {
			m_logger.error(String.format("Version isn't match. Required: %s.", m_meta.getVersion()));
			syncMetaFromDB(); // maybe cached meta is outdate, sync it from db to make sure it is latest
			return false;
		}

		if (super.updateMeta(meta)) { // update db
			m_meta = meta; // update memory
			return true;
		}

		return false;
	}

	@Override
	public Map<String, Topic> getTopics() {
		return m_meta.getTopics();
	}

	@Override
	public Topic findTopicById(long id) {
		for (Entry<String, Topic> entry : m_meta.getTopics().entrySet()) {
			if (entry.getValue().getId() != null && id == entry.getValue().getId()) {
				return entry.getValue();
			}
		}
		return null;
	}

	@Override
	public Topic findTopicByName(String topic) {
		return m_meta.findTopic(topic);
	}

	@Override
	public Map<String, Codec> getCodecs() {
		return m_meta.getCodecs();
	}

	@Override
	public Codec findCodecByType(String type) {
		return m_meta.findCodec(type);
	}

	@Override
	public Codec findCodecByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		return topic != null ? m_meta.findCodec(topic.getCodecType()) : null;
	}

	@Override
	public Map<String, Endpoint> getEndpoints() {
		return m_meta.getEndpoints();
	}

	@Override
	public synchronized void addEndpoint(Endpoint endpoint) throws Exception {
		updateMeta(getMeta().addEndpoint(endpoint));
	}

	@Override
	public Map<String, Storage> getStorages() {
		return m_meta.getStorages();
	}

	@Override
	public Storage findStorageByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		return topic != null ? m_meta.findStorage(topic.getStorageType()) : null;
	}

	@Override
	public List<Partition> findPartitionsByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		return topic != null ? topic.getPartitions() : null;
	}

	@Override
	public Datasource findDatasource(String storageType, String datasourceId) {
		Storage storage = m_meta.findStorage(storageType);
		if (storage != null) {
			for (Datasource datasource : storage.getDatasources()) {
				if (datasource.getId().equals(datasourceId)) {
					Property p = datasource.getProperties().get("password");
					if (p != null && p.getValue().startsWith("~{") && p.getValue().endsWith("}")) {
						p.setValue(Codes.forDecode().decode(p.getValue().substring(2, p.getValue().length() - 1)));
						datasource.getProperties().put("password", p);
					}
					return datasource;
				}
			}
		}
		return null;
	}

	@Override
	public void deleteEndpoint(String endpointId) throws Exception {
		Meta meta = getMeta();
		if (meta.removeEndpoint(endpointId)) {
			updateMeta(meta);
		}
	}

	@Override
	public void addDatasource(Datasource datasource) throws Exception {
	}

	@Override
	public void deleteDatasource(String id) throws Exception {

	}

	private void syncMetaFromDB() {
		try {
			if (m_env.isLocalMode()) { // TODO: local mode should use db?
				m_meta = new LocalMetaLoader().load();
			} else {
				Meta latestMeta = findLatestMeta();
				if (m_meta == null || latestMeta.getVersion() > m_meta.getVersion()) {
					m_meta = latestMeta;
				}
			}
		} catch (DalException e) {
			m_logger.warn("Update meta from db failed.", e);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		if (m_env.isLocalMode()) {
			m_logger.info(">>>>> Portal started at local mode. <<<<<");
		}

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
