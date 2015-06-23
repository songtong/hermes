package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Named
public class ZkReader {

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	public Map<String, Map<Integer, Endpoint>> readPartition2Endpoints(List<Topic> topics) throws Exception {
		Map<String, Map<Integer, Endpoint>> result = new HashMap<>();

		if (topics != null) {
			for (Topic topic : topics) {
				if (Endpoint.BROKER.equals(topic.getEndpointType())) {
					String topicName = topic.getName();
					result.put(topicName, readPartition2Endpoint(topicName));
				}
			}
		}

		return result;
	}

	public Map<Integer, Endpoint> readPartition2Endpoint(String topic) throws Exception {
		Map<Integer, Endpoint> result = new HashMap<>();
		CuratorFramework client = m_zkClient.getClient();

		String topicPath = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
		List<String> partitionPaths = client.getChildren().forPath(topicPath);
		for (String partitionPath : partitionPaths) {
			// TODO
			byte[] data = client.getData().forPath(ZKPaths.makePath(topicPath, partitionPath));
			Map<String, ClientLeaseInfo> leaseMap = ZKSerializeUtils.deserialize(data,
			      new TypeReference<Map<String, ClientLeaseInfo>>() {
			      }.getType());
			int partitionId = Integer.parseInt(partitionPath);
			Endpoint endpoint = makeEndpointFromLease(leaseMap);

			if (endpoint == null) {
				endpoint = makeEndpointFromBrokerAssignment(topic, partitionId);
			}

			result.put(partitionId, endpoint);
		}

		return result;
	}

	private Endpoint makeEndpointFromBrokerAssignment(String topic, int partitionId) {
		Endpoint endpoint = null;
		Assignment<Integer> topicAssignment = m_brokerAssignmentHolder.getAssignment(topic);
		if (topicAssignment != null) {
			Map<String, ClientContext> assignment = topicAssignment.getAssignment(partitionId);
			if (CollectionUtil.isNotEmpty(assignment)) {
				Entry<String, ClientContext> entry = assignment.entrySet().iterator().next();
				ClientContext clientContext = entry.getValue();
				endpoint = new Endpoint();
				endpoint.setHost(clientContext.getIp());
				endpoint.setId(entry.getKey());
				endpoint.setPort(clientContext.getPort());
				endpoint.setType(Endpoint.BROKER);
			}
		}
		return endpoint;
	}

	private Endpoint makeEndpointFromLease(Map<String, ClientLeaseInfo> leaseMap) {
		Endpoint endpoint = null;
		if (CollectionUtil.isNotEmpty(leaseMap)) {
			Entry<String, ClientLeaseInfo> entry = leaseMap.entrySet().iterator().next();
			ClientLeaseInfo leaseInfo = entry.getValue();

			if (leaseInfo.getLease() != null && !leaseInfo.getLease().isExpired()) {
				endpoint = new Endpoint();
				endpoint.setHost(leaseInfo.getIp());
				endpoint.setId(entry.getKey());
				endpoint.setPort(leaseInfo.getPort());
				endpoint.setType(Endpoint.BROKER);
			}
		}

		return endpoint;
	}

	public List<String> listTopics() throws Exception {
		CuratorFramework client = m_zkClient.getClient();

		return client.getChildren().forPath(ZKPathUtils.getBrokerLeasesZkPath());
	}

	public List<Server> listMetaServers() throws Exception {
		CuratorFramework client = m_zkClient.getClient();

		List<Server> metaServers = new ArrayList<>();
		String metaServersPath = ZKPathUtils.getMetaServersZkPath();
		List<String> serverPaths = client.getChildren().forPath(metaServersPath);

		for (String serverPath : serverPaths) {
			HostPort hostPort = ZKSerializeUtils.deserialize(
			      client.getData().forPath(ZKPaths.makePath(metaServersPath, serverPath)), HostPort.class);

			Server s = new Server();
			s.setHost(hostPort.getHost());
			s.setId(serverPath);
			s.setPort(hostPort.getPort());

			metaServers.add(s);
		}

		return metaServers;
	}

}
