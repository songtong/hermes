package com.ctrip.hermes.metaserver.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaserver.meta.watcher.ZkReader;
import com.ctrip.hermes.metaservice.service.MetaService;

@Named
public class MetaLoader {

	@Inject
	private ZkReader m_zkReader;

	@Inject
	private MetaService m_metaService;

	public Meta load() throws Exception {
		Meta base = m_metaService.findLatestMeta();

		List<String> topics = m_zkReader.listTopics();
		Map<String, Map<Integer, Endpoint>> topicPartition2Endpoint = new HashMap<>();
		for (String topic : topics) {
			topicPartition2Endpoint.put(topic, m_zkReader.readPartition2Endpoint(topic));
		}

		List<Server> newServers = m_zkReader.listMetaServers();

		MetaMerger merger = new MetaMerger();
		return merger.merge(base, newServers, topicPartition2Endpoint);
	}
}
