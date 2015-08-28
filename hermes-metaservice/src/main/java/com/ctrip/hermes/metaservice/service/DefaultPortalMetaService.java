package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.core.utils.ObjectUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = PortalMetaService.class, value = DefaultPortalMetaService.ID)
public class DefaultPortalMetaService extends DefaultMetaService implements PortalMetaService, Initializable {
	public static final String ID = "portal-meta-service";

	protected static final Logger logger = LoggerFactory.getLogger(DefaultPortalMetaService.class);

	@Inject
	private ClientEnvironment m_env;

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
	public Map<String, Datasource> getDatasources() {
		Map<String, Datasource> idMap = new HashMap<>();
		List<Datasource> dss = new ArrayList<>();

		for (Storage storage : m_meta.getStorages().values()) {
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
	public void addDatasource(Datasource datasource, String dsType) throws Exception {
		Meta meta = getMeta();
		Map<String, Storage> storages = meta.getStorages();
		if (storages.containsKey(dsType)) {
			meta.getStorages().get(dsType).addDatasource(datasource);
			updateMeta(meta);
			logger.info("Add Datasource: DS: {}, done.", datasource);
		} else {
			logger.warn("Add Datasource: unknown DSType: {}! DS: {}, won't update meta!", dsType, datasource);
		}
	}

	@Override
	public void deleteDatasource(String id, String dsType) throws Exception {
		Meta meta = getMeta();
		Storage storage = meta.getStorages().get(dsType);
		if (null != storage) {
			List<Datasource> dss = storage.getDatasources();
			Iterator<Datasource> it = dss.iterator();
			while (it.hasNext()) {
				if (it.next().getId().equals(id)) {
					it.remove();
				}
			}

			updateMeta(meta);
			logger.info("Delete Datasource: type:{}, id:{} done. updating Meta.", dsType, id);
		} else {
			logger.info("Delete Datasource: unknown type:{}, id:{}, won't delete anything.", dsType);
		}
	}

	private void syncMetaFromDB() {
		try {
			if (m_env.isLocalMode()) { // TODO: local mode should use db?
				m_meta = new LocalMetaLoader().load();
			} else {
				Meta latestMeta = findLatestMeta();
				if (m_meta == null || latestMeta.getVersion() > m_meta.getVersion()) {
					m_meta = latestMeta;
					m_zookeeperService.updateZkBaseMetaVersion(m_meta.getVersion());
				}
			}
		} catch (DalException e) {
			m_logger.warn("Update meta from db failed.", e);
		} catch (Exception e) {
			m_logger.warn("Update meta from db failed, maybe update base meta version in zk failed.", e);
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

	@Override
	public Partition findPartition(String topic, int partitionId) {
		for (Partition partition : getTopics().get(topic).getPartitions()) {
			if (partitionId == partition.getId()) {
				return partition;
			}
		}
		return null;
	}

	@Override
	public List<Datasource> findDatasources(String storageType) {
		Storage storage = getStorages().get(storageType);
		return storage == null ? new ArrayList<Datasource>() : storage.getDatasources();
	}

	@Override
	public List<ConsumerGroup> findConsumersByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null) {
			return topic.getConsumerGroups();
		}
		return new ArrayList<ConsumerGroup>();
	}

	@Override
	public String getZookeeperList() {
		Map<String, Storage> storages = m_meta.getStorages();
		for (Storage storage : storages.values()) {
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
		Map<String, Storage> storages = m_meta.getStorages();
		for (Storage storage : storages.values()) {
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
