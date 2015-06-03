package com.ctrip.hermes.portal.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Codes;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor2;
import com.ctrip.hermes.metaservice.model.MetaEntity;
import com.ctrip.hermes.metaservice.service.DefaultMetaService;

@Named(type = MetaServiceWrapper.class, value = DefaultMetaServiceWrapper.ID)
public class DefaultMetaServiceWrapper extends DefaultMetaService implements MetaServiceWrapper, Initializable {
	@Inject
	private ClientEnvironment m_env;

	public static final String ID = "default-meta-service-wrapper";

	protected Meta m_meta;

	protected Map<Long, Topic> m_topics;
	@Override
	public Codec getCodecByTopic(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null) {
			String codeType = topic.getCodecType();
			return m_meta.getCodecs().get(codeType);
		} else
			return null;
	}

	@Override
	public Map<String, Codec> getCodecs() {
		return m_meta.getCodecs();
	}

	@Override
	public Meta getMeta() {
		return getMeta(false);
	}

	public Meta getMeta(boolean isForceLatest) {
		if (!isForceLatest && m_meta != null) {
			return m_meta;
		}
		if (m_env.isLocalMode()) {
			m_meta = new LocalMetaLoader().load();
		} else {
			try {
				com.ctrip.hermes.metaservice.model.Meta dalMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
				m_meta = JSON.parseObject(dalMeta.getValue(), Meta.class);
			} catch (DalException e) {
				m_logger.warn("get meta failed", e);
				throw new RuntimeException("Get meta failed.", e);
			}
		}
		return m_meta;
	}

	@Override
	public Codec getCodecByType(String type) {
		return m_meta.findCodec(type);
	}

	@Override
	public Topic findTopicByName(String topic) {
		return m_meta.findTopic(topic);
	}

	@Override
	public Topic findTopic(long id) {
		return m_topics.get(id);
	}

	@Override
	public List<Partition> getPartitions(String topicName) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null) {
			return topic.getPartitions();
		}
		return null;
	}

	@Override
	public Storage findStorage(String topic) {
		String storageType = m_meta.findTopic(topic).getStorageType();
		return m_meta.findStorage(storageType);
	}

	@Override
	public List<Topic> findTopicsByPattern(String topicPattern) {
		List<Topic> matchedTopics = new ArrayList<>();

		Collection<Topic> topics = m_meta.getTopics().values();

		Pattern pattern = Pattern.compile(topicPattern);

		for (Topic topic : topics) {
			if (pattern.matcher(topic.getName()).matches()) {
				matchedTopics.add(topic);
			}
		}

		return matchedTopics;
	}

	@Override
	public Map<String, Storage> getStorages() {
		return m_meta.getStorages();
	}

	@Override
	public List<Server> getServers() {
		return new ArrayList<Server>(this.m_meta.getServers().values());
	}

	public synchronized void refreshMeta() {
		if (m_meta == null) {
			getMeta(true);
		}
		m_topics = new HashMap<>();

		m_meta.accept(new BaseVisitor2() {

			@Override
			protected void visitTopicChildren(Topic topic) {
				m_topics.put(topic.getId(), topic);

				super.visitTopicChildren(topic);
			}

		});
	}

	@Override
	public void initialize() throws InitializationException {
		refreshMeta();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("RefreshMeta", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      refreshMeta();
				      } catch (RuntimeException e) {
					      m_logger.warn("refresh meta failed", e);
				      }
			      }

		      }, 1, 1, TimeUnit.SECONDS);
	}

	@Override
	public Map<String, Endpoint> getEndpoints() {
		return m_meta.getEndpoints();
	}

	@Override
	public Datasource getDatasource(String storageType, String datasourceId) {
		List<Datasource> datasources = m_meta.getStorages().get(storageType).getDatasources();
		for (Datasource datasource : datasources) {
			if (datasource.getId().equals(datasourceId)) {
				Property p = datasource.getProperties().get("password");
				if (p.getValue().startsWith("~{") && p.getValue().endsWith("}")) {
					p.setValue(Codes.forDecode().decode(p.getValue().substring(2, p.getValue().length() - 1)));
					datasource.getProperties().put("password", p);
				}
				return datasource;
			}
		}
		return null;
	}
}
