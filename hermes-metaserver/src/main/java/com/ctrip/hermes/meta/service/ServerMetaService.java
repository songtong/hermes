package com.ctrip.hermes.meta.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.DefaultLease;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.meta.dal.meta.MetaDao;
import com.ctrip.hermes.meta.dal.meta.MetaEntity;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.meta.transform.BaseVisitor2;

@Named(type = MetaService.class, value = ServerMetaService.ID)
public class ServerMetaService implements MetaService, Initializable {

	public static final String ID = "server-meta-service";

	protected Meta m_meta;

	protected Map<Long, Topic> m_topics;

	@Inject
	private MetaDao m_metaDao;

	@Override
	public Meta getMeta() {
		return getMeta(false);
	}

	@Override
	public Meta getMeta(boolean isForceLatest) {
		if (!isForceLatest && m_meta != null) {
			return m_meta;
		}
		try {
			com.ctrip.hermes.meta.dal.meta.Meta dalMeta = m_metaDao.findLatest(MetaEntity.READSET_FULL);
			m_meta = JSON.parseObject(dalMeta.getValue(), Meta.class);
		} catch (DalException e) {
			throw new RuntimeException("Get meta failed.", e);
		}
		return m_meta;
	}

	@Override
	public boolean updateMeta(Meta meta) {
		if (m_meta == null) {
			getMeta();
		}

		if (!meta.getVersion().equals(m_meta.getVersion())) {
			throw new RuntimeException(String.format("Not the latest version. Latest Version: %s, Current Version: %s",
			      m_meta.getVersion(), meta.getVersion()));
		}

		com.ctrip.hermes.meta.dal.meta.Meta dalMeta = new com.ctrip.hermes.meta.dal.meta.Meta();
		try {
			meta.setVersion(meta.getVersion() + 1);
			dalMeta.setValue(JSON.toJSONString(meta));
			dalMeta.setDataChangeLastTime(new Date(System.currentTimeMillis()));
			m_metaDao.insert(dalMeta);
		} catch (DalException e) {
			throw new RuntimeException("Update meta failed.", e);
		}
		m_meta = meta;
		refreshMeta();
		return true;
	}

	@Override
	public String getEndpointType(String topicName) {
		Topic topic = m_meta.getTopics().get(topicName);
		if (topic == null) {
			throw new RuntimeException(String.format("Topic %s is not found", topicName));
		}
		List<Partition> partitions = topic.getPartitions();
		if (partitions == null || partitions.size() == 0) {
			throw new RuntimeException(String.format("Partitions for topic %s is not found", topicName));
		}
		String endpointId = partitions.get(0).getEndpoint();
		Endpoint endpoint = m_meta.getEndpoints().get(endpointId);
		if (endpoint == null) {
			throw new RuntimeException(String.format("Endpoint for topic %s is not found", topicName));
		} else {
			return endpoint.getType();
		}
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
	public Endpoint findEndpoint(String endpointId) {
		return m_meta.findEndpoint(endpointId);
	}

	@Override
	public Storage findStorage(String topic) {
		String storageType = m_meta.findTopic(topic).getStorageType();
		return m_meta.findStorage(storageType);
	}

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
	public Partition findPartition(String topicName, int partitionId) {
		Topic topic = m_meta.findTopic(topicName);
		if (topic != null)
			return topic.findPartition(partitionId);
		else
			return null;
	}

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
	public Topic findTopic(String topic) {
		return m_meta.findTopic(topic);
	}

	@Override
	public List<Partition> getPartitions(String topic, String groupId) {
		// TODO 对一个group的不同机器返回不一样的Partition
		return getPartitions(topic);
	}

	@Override
	public int getGroupIdInt(String groupName) {
		// TODO groupIdStr唯一
		for (Topic topic : m_meta.getTopics().values()) {
			for (ConsumerGroup group : topic.getConsumerGroups()) {
				if (group.getName().equals(groupName)) {
					return group.getId();
				}
			}
		}

		// TODO
		return 100;
	}

	@Override
	public List<Datasource> listMysqlDataSources() {
		// TODO
		final List<Datasource> dataSources = new ArrayList<>();

		m_meta.accept(new BaseVisitor2() {

			@Override
			protected void visitDatasourceChildren(Datasource ds) {
				Storage storage = getAncestor(2);

				if ("mysql".equalsIgnoreCase(storage.getType())) {
					dataSources.add(ds);
				}

				super.visitDatasourceChildren(ds);
			}

		});

		return dataSources;
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
	public Topic findTopic(long topicId) {
		return m_topics.get(topicId);
	}

	@Override
	public int getAckTimeoutSeconds(String topic) {
		// TODO
		return 2;
	}

	@Override
	public Codec getCodecByType(String codecType) {
		return m_meta.findCodec(codecType);
	}

	@Override
	public LeaseAcquireResponse tryAcquireConsumerLease(Tpg tpg) {
		// TODO Auto-generated method stub
		long expireTime = System.currentTimeMillis() + 10 * 1000L;
		long leaseId = 123L;
		return new LeaseAcquireResponse(true, new DefaultLease(leaseId, expireTime), expireTime);
	}

	@Override
	public LeaseAcquireResponse tryRenewLease(Tpg tpg, Lease lease) {
		// TODO
		return new LeaseAcquireResponse(false, null, System.currentTimeMillis() + 10 * 1000L);
	}

	@Override
	public void initialize() throws InitializationException {
		this.refreshMeta();
	}

	@Override
	public List<Server> getServers() {
		return new ArrayList<Server>(this.m_meta.getServers().values());
	}

}
