package com.ctrip.hermes.metaserver.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.meta.watcher.ZkReader;
import com.ctrip.hermes.metaservice.service.MetaService;

@Named
public class MetaLoader {

	@Inject
	private ZkReader m_zkReader;

	@Inject
	private MetaService m_metaService;

	@Inject
	private MetaHolder m_metaHolder;

	public Meta load() throws Exception {
		Meta base = m_metaService.findLatestMeta();

		m_metaHolder.setBaseMeta(base);

		List<String> topics = m_zkReader.listTopics();
		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoint = new HashMap<>();
		for (String topic : topics) {
			topicPartition2Endpoint.put(topic, m_zkReader.readPartition2Endpoint(topic));
		}

		List<Server> newServers = m_zkReader.listMetaServers();

		MetaMerger merger = new MetaMerger();
		Meta meta = merger.merge(base, newServers, topicPartition2Endpoint);
		BrokerAssignmentHolder brokerAssignmentHolder = PlexusComponentLocator.lookup(BrokerAssignmentHolder.class);
		brokerAssignmentHolder.reassign(new ArrayList<Topic>(meta.getTopics().values()));
		return meta;
	}
}
