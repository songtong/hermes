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
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder.ClientLeaseInfo;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Named
public class ZkReader {

	@Inject
	private ZKClient m_zkClient;

	public Map<Integer, Endpoint> readPartition2Endpoint(String topic) throws Exception {
		Map<Integer, Endpoint> result = new HashMap<>();
		CuratorFramework client = m_zkClient.getClient();

		String topicPath = ZKPathUtils.getBrokerLeaseTopicParentZkPath(topic);
		List<String> partitionPaths = client.getChildren().forPath(topicPath);
		for (String partitionPath : partitionPaths) {
			// TODO
			Map<String, ClientLeaseInfo> leaseMap = ZKSerializeUtils.deserialize(
			      client.getData().forPath(ZKPaths.makePath(topicPath, partitionPath)),
			      new TypeReference<Map<String, ClientLeaseInfo>>() {
			      }.getType());
			int partitionId = Integer.parseInt(partitionPath);
			Endpoint endpoint = makeEndpoint(leaseMap);

			result.put(partitionId, endpoint);
		}

		return result;
	}

	private Endpoint makeEndpoint(Map<String, ClientLeaseInfo> leaseMap) {
		Endpoint endpoint = null;
		if (CollectionUtil.isNotEmpty(leaseMap)) {
			Entry<String, ClientLeaseInfo> entry = leaseMap.entrySet().iterator().next();
			ClientLeaseInfo lease = entry.getValue();

			endpoint = new Endpoint();
			endpoint.setHost(lease.getIp());
			endpoint.setId(entry.getKey());
			endpoint.setPort(lease.getPort());
			endpoint.setType(Endpoint.BROKER);
		}

		return endpoint;
	}

	public List<String> listTopics() throws Exception {
		CuratorFramework client = m_zkClient.getClient();

		return client.getChildren().forPath(ZKPathUtils.getBrokerLeasesPath());
	}

	public List<Server> listMetaServers() throws Exception {
		CuratorFramework client = m_zkClient.getClient();

		List<Server> metaServers = new ArrayList<>();
		String metaServersPath = ZKPathUtils.getMetaServersPath();
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
