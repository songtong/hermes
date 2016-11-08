package com.ctrip.hermes.portal.service.zookeeperMigration;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.model.Server;
import com.ctrip.hermes.metaservice.model.ZookeeperEnsemble;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.service.ServerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.ZookeeperEnsembleService;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKConfig;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;
import com.google.common.base.Charsets;

@Named(type = ZookeeperMigrationService.class)
public class DefaultZookeeperMigrationService implements ZookeeperMigrationService {

	private static final Logger m_logger = LoggerFactory.getLogger(DefaultZookeeperMigrationService.class);

	@Inject
	private TopicService m_topicService;

	@Inject
	private ZookeeperService m_zookeeperService;

	@Inject
	private ZookeeperEnsembleService m_zookeeperEnsembleService;

	@Inject
	private MetaService m_metaService;

	@Inject
	private ServerService m_serverService;

	@Inject
	private ClientEnvironment m_env;

	@Inject
	private ZKConfig m_zkConfig;

	private static final int ConnectTimeout = 2000;

	private static int ReadTimeout = 10000;

	private static final String MetaServerRoleNodeName = "role";

	private static final String LeaderInfoNodeName = "leaderInfo";

	private static final String ConsumerLeasesNodeName = "consumerLeases";

	private static final String BrokerLeasesNodeName = "brokerLeases";

	private static final String RunningBrokersNodeName = "runningBrokers";

	private static final String ZookeeperEnsemblesNodeName = "zookeeperEnsembles";

	private static final String ZkConnectedNodeName = "zkConnected";

	private static final String ZkClientPausedNodeName = "zkClientPaused";

	private static final int MetaServerLeaseExpiredWaitTimeSeconds = 20;

	private static final int MetaServerLeaseReassignedWaitTimeSeconds = 5;

	private static final int MetaServerDisconnectToZkWaitTimeSeconds = 5;
	
	private static final int MetaServerReconnectToZkWaitTimeSeconds = 5;

	private ExecutorService m_sendRequestExcutor = Executors.newCachedThreadPool(HermesThreadFactory.create(
	      "ZookeeperMigration", true));

	private class TempZkCli {
		private CuratorFramework m_client;

		public TempZkCli(String connectionString) {

			// TODO: adjust some params
			Builder builder = CuratorFrameworkFactory.builder();

			builder.connectionTimeoutMs(m_zkConfig.getZkConnectionTimeoutMillis());
			builder.maxCloseWaitMs(m_zkConfig.getZkCloseWaitMillis());
			builder.namespace("hermes");
			builder.retryPolicy(new RetryNTimes(m_zkConfig.getZkRetries(), m_zkConfig.getSleepMsBetweenRetries()));
			builder.sessionTimeoutMs(m_zkConfig.getZkSessionTimeoutMillis());
			builder.threadFactory(HermesThreadFactory.create("Portal-Zk-Intialize", true));
			builder.connectString(connectionString);

			m_client = builder.build();
		}

		public void ensurePath(String path) throws Exception {
			m_client.createContainers(path);
		}

		public boolean start() throws InterruptedException {
			m_client.start();
			return m_client.blockUntilConnected(10, TimeUnit.SECONDS);
		}

		public void close() {
			m_client.close();
		}

		public void updateZkBaseMetaVersion(long version) throws Exception {
			ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());
			m_client.setData().forPath(ZKPathUtils.getBaseMetaVersionZkPath(), ZKSerializeUtils.serialize(version));
		}

		public void ensureConsumerLeaseZkPath(Topic topic) throws Exception {
			List<String> paths = ZKPathUtils.getConsumerLeaseZkPaths(topic, topic.getPartitions(),
			      topic.getConsumerGroups());
			for (String path : paths) {
				ensurePath(path);
			}
		}

		public void ensureBrokerLeaseZkPath(Topic topic) throws Exception {
			List<String> paths = ZKPathUtils.getBrokerLeaseZkPaths(topic, topic.getPartitions());
			for (String path : paths) {
				ensurePath(path);
			}
		}

	}

	public void initializeZkFromBaseMeta(int zkEnsembleId) {
		m_logger.info("Start to initialize zkEnsemble({}).", zkEnsembleId);
		ReadTimeout = Math.max(m_zkConfig.getZkSessionTimeoutMillis(), m_zkConfig.getZkConnectionTimeoutMillis()) + 5000;

		ZookeeperEnsemble zkEnsemble = m_zookeeperEnsembleService.findZookeeperEnsembleModelById(zkEnsembleId);

		if (zkEnsemble == null) {
			m_logger.error("Can not find zkEnsemble({}) from bd!", zkEnsembleId);
			throw new RuntimeException(String.format("Can not find zkEnsemble(%s) from bd!", zkEnsembleId));
		}

		Meta meta;
		try {
			meta = m_metaService.refreshMeta();
		} catch (Exception e) {
			throw new RuntimeException("Failed to refresh meta from db!");
		}

		String connectionString = zkEnsemble.getConnectionString();
		TempZkCli tempZkCli = new TempZkCli(connectionString);
		try {
			if (!tempZkCli.start()) {
				throw new RuntimeException("Can not connect to zookeeper due to connection timeout!");
			}
			m_logger.info("Create temp zkCli to zkEnsemble({}) success!.", connectionString);

			for (Topic topic : meta.getTopics().values()) {
				if (Endpoint.BROKER.equals(topic.getEndpointType())) {
					tempZkCli.ensureBrokerLeaseZkPath(topic);
					tempZkCli.ensureConsumerLeaseZkPath(topic);
				}
			}

			// TODO: delay check: do I need to initialize metaInfo & metaServerAssignment?
			tempZkCli.updateZkBaseMetaVersion(meta.getVersion());
			tempZkCli.ensurePath(ZKPathUtils.getCmessageConfigPath());
			tempZkCli.ensurePath(ZKPathUtils.getCmessageExchangePath());
			tempZkCli.ensurePath(ZKPathUtils.getMetaInfoZkPath());
			tempZkCli.ensurePath(ZKPathUtils.getMetaServersZkPath());
			tempZkCli.ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
			tempZkCli.ensurePath(ZKPathUtils.getBrokerRegistryBasePath());
			tempZkCli.ensurePath(ZKPathUtils.getBrokerRegistryName(null));

		} catch (InterruptedException e) {
			m_logger.error("Can not create temp zk cli to zkEnsemble(%s)", connectionString);
			throw new RuntimeException(String.format("Can not create temp zk cli to zkEnsemble({})", connectionString), e);
		} catch (Exception e) {
			m_logger.error("Failed to initilize some paths on zkEnsemble({})", connectionString, e);
			throw new RuntimeException(String.format("Failed to initilize some path on zkEnsemble(%s):%s",
			      connectionString, e.getMessage()));
		} finally {
			tempZkCli.close();
		}

		m_logger.info("Initialize zkEnsemble({}) success!", connectionString);
	}

	@Override
	public Map<String, CheckResult> stopLeaseAssigningAndCheckStatusForAllMetaServers() throws Exception {
		List<Server> servers = null;
		try {
			servers = m_serverService.findServers();
		} catch (DalException e) {
			m_logger.error("Failed to list servers from db!", e);
			throw new RuntimeException("Failed to list servers from db!", e);
		}

		Map<Server, Pair<Integer, String>> sendResults = new HashMap<>();
		CountDownLatch latch = new CountDownLatch(servers.size());
		for (Server server : servers) {
			Pair<Integer, String> responsePair = stopLeaseAssigning(server, latch);
			sendResults.put(server, responsePair);
		}

		latch.await();

		Map<String, CheckResult> checkResults = new HashMap<>();
		List<Server> needLaterCheckServers = new ArrayList<>();
		for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
			CheckResult checkResult = handleStopLeaseAssigningResult(sendResult.getKey(), sendResult.getValue());
			checkResults.put(sendResult.getKey().getId(), checkResult);
			if (checkResult.isSuccess()) {
				needLaterCheckServers.add(sendResult.getKey());
			}
		}

		if (!needLaterCheckServers.isEmpty()) {
			try {
				TimeUnit.SECONDS.sleep(MetaServerLeaseExpiredWaitTimeSeconds);
			} catch (InterruptedException e) {
				// ignore it
			}

			int retryTimes = 2;
			for (int i = 0; i < retryTimes; i++) {
				sendResults.clear();
				latch = new CountDownLatch(needLaterCheckServers.size());

				for (Server server : needLaterCheckServers) {
					Pair<Integer, String> responsePair = checkNoLeaseAssigningStatus(server, latch);
					sendResults.put(server, responsePair);
				}

				latch.await();

				needLaterCheckServers.clear();
				for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
					CheckResult checkResult = handleCheckNoLeaseAssigningStatusResult(sendResult.getKey(),
					      sendResult.getValue());
					checkResults.put(sendResult.getKey().getId(), checkResult);
					if (!checkResult.isSuccess()) {
						needLaterCheckServers.add(sendResult.getKey());
					}
				}

				if (needLaterCheckServers.isEmpty()) {
					break;
				}

				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					// ignore it
				}
			}

		}
		return checkResults;
	}

	@Override
	public CheckResult stopLeaseAssigningAndCheckStatus(Server server) throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Pair<Integer, String> sendResult = stopLeaseAssigning(server, latch);
		latch.await();

		CheckResult checkResult = handleStopLeaseAssigningResult(server, sendResult);
		if (!checkResult.isSuccess()) {
			return checkResult;
		}

		try {
			TimeUnit.SECONDS.sleep(MetaServerLeaseExpiredWaitTimeSeconds);
		} catch (InterruptedException e) {
			// ignore it
		}

		int retryTimes = 2;
		for (int i = 0; i < retryTimes; i++) {
			latch = new CountDownLatch(1);
			sendResult = checkNoLeaseAssigningStatus(server, latch);
			latch.await();

			checkResult = handleCheckNoLeaseAssigningStatusResult(server, sendResult);
			if (checkResult.isSuccess()) {
				return checkResult;
			}

			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		m_logger.warn("Failed to stop leaseAssigning for metaServer: [{}], error message:[{}]", server,
		      checkResult.getMessage());
		return checkResult;
	}

	private Pair<Integer, String> stopLeaseAssigning(Server server, CountDownLatch latch) throws InterruptedException {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(),
		      "management/leaseAssigning/stop");

		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "POST", null, latch, responsePair);
		return responsePair;
	}

	private CheckResult handleStopLeaseAssigningResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to stop leaseAssigning for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to stop leaseAssigning for metaServer: [%s], responsePair:%s.", server, responsePair));
		}

		return new CheckResult(true, null);
	}

	private Pair<Integer, String> checkNoLeaseAssigningStatus(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "metaserver/status");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "GET", null, latch, responsePair);
		return responsePair;
	}

	private CheckResult handleCheckNoLeaseAssigningStatusResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to get metaserver status for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to get management status for metaServer: [%s], responsePair:%s.", server, responsePair));
		}

		try {
			JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
			JSONObject consumerLeases = metaServerJsonObject.getJSONObject(ConsumerLeasesNodeName);
			JSONObject brokerLeases = metaServerJsonObject.getJSONObject(BrokerLeasesNodeName);
			if (consumerLeases.isEmpty() && brokerLeases.isEmpty()) {
				return new CheckResult(true, null);
			} else {
				return new CheckResult(false, "Consumer/Broker leases exist!");
			}
		} catch (Exception e) {
			m_logger
			      .warn("Check leases status failed! Parse leases from metaserver status string failed! Metaserver status string:{}!",
			            responsePair.getValue());
			return new CheckResult(false, "Check leases status failed! Parse leases from metaserver status string failed!");
		}
	}

	@Override
	public Map<String, CheckResult> pauseAndSwitchAllMetaServersZkEnsembleAndCheckStatus() throws Exception {
		List<Server> servers = null;
		try {
			servers = m_serverService.findServers();
		} catch (DalException e) {
			m_logger.error("Failed to list servers from db!", e);
			throw new RuntimeException("Failed to list servers from db!", e);
		}

		CountDownLatch latch = new CountDownLatch(servers.size());
		Map<Server, Pair<Integer, String>> sendResults = new HashMap<>();
		for (Server server : servers) {
			Pair<Integer, String> responsePair = pauseAndSwitchMetaServer(server, latch);
			sendResults.put(server, responsePair);
		}

		latch.await();

		Map<String, CheckResult> checkResults = new HashMap<>();
		List<Server> needLaterCheckServers = new ArrayList<>();
		for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
			CheckResult checkResult = handlePauseAndSwitchMetaServerResult(sendResult.getKey(), sendResult.getValue());
			checkResults.put(sendResult.getKey().getId(), checkResult);
			if (checkResult.isSuccess()) {
				needLaterCheckServers.add(sendResult.getKey());
			}
		}

		if (!needLaterCheckServers.isEmpty()) {
			try {
				TimeUnit.SECONDS.sleep(MetaServerDisconnectToZkWaitTimeSeconds);
			} catch (InterruptedException e) {
				// ignore it
			}
			
			int retryTimes = 2;

			for (int i = 0; i < retryTimes; i++) {
				sendResults.clear();
				latch = new CountDownLatch(needLaterCheckServers.size());

				for (Server server : needLaterCheckServers) {
					Pair<Integer, String> responsePair = checkMetaServerIsDisconnectedToZkEnsemble(server, latch);
					sendResults.put(server, responsePair);
				}

				latch.await();

				needLaterCheckServers.clear();
				for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
					CheckResult checkResult = handleCheckMetaServerIsDisconnectedToZkEnsembleResult(sendResult.getKey(),
					      sendResult.getValue());
					checkResults.put(sendResult.getKey().getId(), checkResult);
					if (!checkResult.isSuccess()) {
						needLaterCheckServers.add(sendResult.getKey());
					}
				}

				if (needLaterCheckServers.isEmpty()) {
					break;
				}

				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					// ignore it
				}
			}
		}
		return checkResults;
	}

	@Override
	public CheckResult pauseAndSwitchMetaServerZkEnsembleAndCheckStatus(Server server) throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Pair<Integer, String> responsePair = pauseAndSwitchMetaServer(server, latch);
		latch.await();

		CheckResult checkResult = handlePauseAndSwitchMetaServerResult(server, responsePair);
		if (!checkResult.isSuccess()) {
			return checkResult;
		}

		int retryTimes = 2;
		for (int i = 0; i < retryTimes; i++) {
			latch = new CountDownLatch(1);
			responsePair = checkMetaServerIsDisconnectedToZkEnsemble(server, latch);
			latch.await();

			checkResult = handleCheckMetaServerIsDisconnectedToZkEnsembleResult(server, responsePair);
			if (checkResult.isSuccess()) {
				return checkResult;
			}

			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		m_logger.warn(String.format("Failed to pauseAndSwitch zk for metaserver(%s) as: %s", server.getId(),
		      checkResult.getMessage()));
		return checkResult;

	}

	private Pair<Integer, String> pauseAndSwitchMetaServer(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "management/zk/pauseAndSwitch");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "POST", null, latch, responsePair);
		return responsePair;
	}

	private CheckResult handlePauseAndSwitchMetaServerResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			return new CheckResult(false, String.format("Failed to send pauseAndSwitch command to server(id=%s)",
			      server.getId()));
		} else {
			return new CheckResult(true, null);
		}
	}

	private Pair<Integer, String> checkMetaServerIsDisconnectedToZkEnsemble(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "management/status");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "GET", null, latch, responsePair);
		return responsePair;

	}

	private CheckResult handleCheckMetaServerIsDisconnectedToZkEnsembleResult(Server server,
	      Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to get management status for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to get management status for metaServer: [%s], responsePair:%s.", server, responsePair));
		}

		try {
			JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
			boolean zkConnected = metaServerJsonObject.getBooleanValue(ZkConnectedNodeName);
			boolean zkClientPaused = metaServerJsonObject.getBooleanValue(ZkClientPausedNodeName);
			if (!zkConnected && zkClientPaused) {
				return new CheckResult(true, null);
			} else {
				return new CheckResult(false, String.format(
				      "MetaServer is still connected to zk! zkConnected: %s, zkClientPaused: %s.", zkConnected,
				      zkClientPaused));
			}
		} catch (Exception e) {
			m_logger.warn("Check MetaServer zkConnection status failed! Management status string:{}!",
			      responsePair.getValue());
			return new CheckResult(false, "Check MetaServer zkConnection status failed!");
		}
	}

	@Override
	public Map<String, CheckResult> resumeAllMetaServersAndCheckStatus() throws Exception {
		List<Server> servers = null;
		try {
			servers = m_serverService.findServers();
		} catch (DalException e) {
			m_logger.error("Failed to list servers from db!", e);
			throw new RuntimeException("Failed to list servers from db!", e);
		}

		Map<Server, Pair<Integer, String>> sendResults = new HashMap<>();
		CountDownLatch latch = new CountDownLatch(servers.size());
		for (Server server : servers) {
			Pair<Integer, String> responsePair = resumeMetaServer(server, latch);
			sendResults.put(server, responsePair);
		}

		latch.await();

		Map<String, CheckResult> checkResults = new HashMap<>();
		List<Server> needLaterCheckServers = new ArrayList<>();
		for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
			CheckResult checkResult = handleResumeMetaServerResult(sendResult.getKey(), sendResult.getValue());
			checkResults.put(sendResult.getKey().getId(), checkResult);
			if (checkResult.isSuccess()) {
				needLaterCheckServers.add(sendResult.getKey());
			}
		}

		if (!needLaterCheckServers.isEmpty()) {
			try {
				TimeUnit.SECONDS.sleep(MetaServerReconnectToZkWaitTimeSeconds);
			} catch (InterruptedException e) {
				// ignore it
			}

			ZookeeperEnsemble primaryZk = m_zookeeperEnsembleService.getPrimaryZookeeperEnsemble();
			if (primaryZk == null) {
				throw new RuntimeException("No primary zookeeper ensemble found in db!");
			}

			int retryTimes = 2;
			for (int i = 0; i < retryTimes; i++) {
				sendResults.clear();
				latch = new CountDownLatch(needLaterCheckServers.size());

				for (Server server : needLaterCheckServers) {
					Pair<Integer, String> responsePair = checkMetaServerIsConnectedToPrimaryZk(server, latch);
					sendResults.put(server, responsePair);
				}

				latch.await();

				needLaterCheckServers.clear();
				for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
					CheckResult checkResult = handleCheckMetaServerIsConnectedToPrimaryZkResult(sendResult.getKey(),
					      sendResult.getValue(), primaryZk);
					checkResults.put(sendResult.getKey().getId(), checkResult);
					if (!checkResult.isSuccess()) {
						needLaterCheckServers.add(sendResult.getKey());
					}
				}

				if (needLaterCheckServers.isEmpty()) {
					break;
				}

				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					// ignore it
				}
			}
		}
		return checkResults;
	}

	@Override
	public CheckResult resumeMetaServerAndCheckStatus(Server server) throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Pair<Integer, String> responsePair = resumeMetaServer(server, latch);
		latch.await();

		CheckResult checkResult = handleResumeMetaServerResult(server, responsePair);
		if (!checkResult.isSuccess()) {
			return checkResult;
		}

		try {
			TimeUnit.SECONDS.sleep(MetaServerReconnectToZkWaitTimeSeconds);
		} catch (InterruptedException e) {
			// ignore it
		}

		ZookeeperEnsemble primaryZk = m_zookeeperEnsembleService.getPrimaryZookeeperEnsemble();
		if (primaryZk == null) {
			throw new RuntimeException("No primary zookeeper ensemble found in db!");
		}

		int retryTimes = 2;
		for (int i = 0; i < retryTimes; i++) {
			latch = new CountDownLatch(1);
			responsePair = checkMetaServerIsConnectedToPrimaryZk(server, latch);
			latch.await();

			checkResult = handleCheckMetaServerIsConnectedToPrimaryZkResult(server, responsePair, primaryZk);
			if (checkResult.isSuccess()) {
				return checkResult;
			}

			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		m_logger.warn(String.format("Failed to reconnect to primary zk for metaserver(%s) as: %s", server.getId(),
		      checkResult.getMessage()));
		return checkResult;

	}

	private Pair<Integer, String> resumeMetaServer(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "management/zk/resume");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "POST", null, latch, responsePair);
		return responsePair;

	}

	private CheckResult handleResumeMetaServerResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			return new CheckResult(false, String.format("Failed to send resume command to server(id=%s)", server.getId()));
		} else {
			return new CheckResult(true, null);
		}
	}

	private Pair<Integer, String> checkMetaServerIsConnectedToPrimaryZk(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "management/status");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "GET", null, latch, responsePair);
		return responsePair;
	}

	private CheckResult handleCheckMetaServerIsConnectedToPrimaryZkResult(Server server,
	      Pair<Integer, String> responsePair, ZookeeperEnsemble primaryZk) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to get management status for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to get management status for metaServer: [%s], responsePair:%s.", server, responsePair));
		}

		try {
			JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
			boolean zkConnected = metaServerJsonObject.getBooleanValue(ZkConnectedNodeName);
			boolean zkClientPaused = metaServerJsonObject.getBooleanValue(ZkClientPausedNodeName);
			JSONArray zkEnsembles = metaServerJsonObject.getJSONArray(ZookeeperEnsemblesNodeName);
			if (zkConnected && !zkClientPaused) {
				for (int i = 0; i < zkEnsembles.size(); i++) {
					com.ctrip.hermes.meta.entity.ZookeeperEnsemble zkEnsemble = JSONObject.parseObject(zkEnsembles.get(i)
					      .toString(), com.ctrip.hermes.meta.entity.ZookeeperEnsemble.class);
					if (zkEnsemble.isPrimary()) {
						if (zkEnsemble.getConnectionString().equals(primaryZk.getConnectionString())) {
							return new CheckResult(true, null);
						} else {
							m_logger.warn("Server({}) still connected to zk:{}({}), which should be {}({})", server.getId(),
							      zkEnsemble.getId(), zkEnsemble.getConnectionString(), primaryZk.getName(),
							      primaryZk.getConnectionString());
							return new CheckResult(false, String.format("Current connected zk:%s(%s), which should be %s(%s)",
							      zkEnsemble.getId(), zkEnsemble.getConnectionString(), primaryZk.getName(),
							      primaryZk.getConnectionString()));
						}
					}
				}
				m_logger.warn("Can not find a primary zookeeperEnsemble on server({}).", server.getId());
				return new CheckResult(false, "Can not find a primary zookeeperEnsemble on this server!");
			} else {
				m_logger.warn("Server({} is still disconnected to zk! zkConnected: {}, zkClientPaused: {}.).",
				      server.getId(), zkConnected, zkClientPaused);
				return new CheckResult(false, String.format(
				      "MetaServer is still disconnected to zk! zkConnected: %s, zkClientPaused: %s.", zkConnected,
				      zkClientPaused));
			}
		} catch (Exception e) {
			m_logger.warn("Check MetaServer zkConnection status failed! Management status string:{}!",
			      responsePair.getValue());
			return new CheckResult(false, "Check MetaServer zkConnection status failed!");
		}
	}

	@Override
	public Map<String, CheckResult> startLeaseAssigningAndCheckStatusForAllMetaServers() throws Exception {
		List<Server> servers = null;
		try {
			servers = m_serverService.findServers();
		} catch (DalException e) {
			m_logger.error("Failed to list servers from db!", e);
			throw new RuntimeException("Failed to list servers from db!", e);
		}

		Map<Server, Pair<Integer, String>> sendResults = new HashMap<>();
		CountDownLatch latch = new CountDownLatch(servers.size());
		for (Server server : servers) {
			Pair<Integer, String> responsePair = startLeaseAssigning(server, latch);
			sendResults.put(server, responsePair);
		}

		latch.await();

		Map<String, CheckResult> checkResults = new HashMap<>();
		List<Server> needLaterCheckServers = new ArrayList<>();
		for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
			CheckResult checkResult = handleStartLeaseAssigningResult(sendResult.getKey(), sendResult.getValue());
			checkResults.put(sendResult.getKey().getId(), checkResult);
			if (checkResult.isSuccess()) {
				needLaterCheckServers.add(sendResult.getKey());
			}
		}

		if (!needLaterCheckServers.isEmpty()) {
			try {
				TimeUnit.SECONDS.sleep(MetaServerLeaseReassignedWaitTimeSeconds);
			} catch (InterruptedException e) {
				// ignore it
			}

			int retryTimes = 2;
			for (int i = 0; i < retryTimes; i++) {
				sendResults.clear();
				latch = new CountDownLatch(needLaterCheckServers.size());

				for (Server server : needLaterCheckServers) {
					Pair<Integer, String> responsePair = checkExistsLeaseAssigning(server, latch);
					sendResults.put(server, responsePair);
				}

				latch.await();

				needLaterCheckServers.clear();
				for (Entry<Server, Pair<Integer, String>> sendResult : sendResults.entrySet()) {
					CheckResult checkResult = handleCheckExistsLeaseAssigningResult(sendResult.getKey(),
					      sendResult.getValue());
					checkResults.put(sendResult.getKey().getId(), checkResult);
					if (!checkResult.isSuccess()) {
						needLaterCheckServers.add(sendResult.getKey());
					}
				}

				if (needLaterCheckServers.isEmpty()) {
					break;
				}

				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {
					// ignore it
				}
			}
		}
		return checkResults;
	}

	@Override
	public CheckResult startLeaseAssigningAndCheckStatus(Server server) throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		Pair<Integer, String> responsePair = startLeaseAssigning(server, latch);
		latch.await();

		CheckResult checkResult = handleStartLeaseAssigningResult(server, responsePair);
		if (!checkResult.isSuccess()) {
			return checkResult;
		}

		try {
			TimeUnit.SECONDS.sleep(MetaServerLeaseReassignedWaitTimeSeconds);
		} catch (InterruptedException e) {
			// ignore it
		}

		int retryTimes = 2;
		for (int i = 0; i < retryTimes; i++) {
			latch = new CountDownLatch(1);
			responsePair = checkExistsLeaseAssigning(server, latch);
			latch.await();

			checkResult = handleCheckExistsLeaseAssigningResult(server, responsePair);
			if (checkResult.isSuccess()) {
				return checkResult;
			}

			try {
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (InterruptedException e) {
				// ignore it
			}
		}

		m_logger.warn("Failed to start leaseAssigning for metaServer: [{}], error message:[{}]", server,
		      checkResult.getMessage());
		return checkResult;
	}

	private Pair<Integer, String> startLeaseAssigning(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(),
		      "management/leaseAssigning/start");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "POST", null, latch, responsePair);
		return responsePair;
	}

	private CheckResult handleStartLeaseAssigningResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to start leaseAssigning for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to start leaseAssigning for metaServer: [%s], responsePair:%s.", server, responsePair));
		}
		return new CheckResult(true, null);
	}

	private Pair<Integer, String> checkExistsLeaseAssigning(Server server, CountDownLatch latch) {
		String url = String.format("http://%s:%s/%s", server.getHost(), server.getPort(), "metaserver/status");
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "GET", null, latch, responsePair);
		return responsePair;

	}

	private CheckResult handleCheckExistsLeaseAssigningResult(Server server, Pair<Integer, String> responsePair) {
		if (responsePair == null || responsePair.getKey() == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to get metaserver status for metaServer: [{}], responsePair:{}.", server, responsePair);
			return new CheckResult(false, String.format(
			      "Failed to get metaserver status for metaServer: [%s], responsePair:%s.", server, responsePair));
		}

		try {
			JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
			String role = metaServerJsonObject.getString(MetaServerRoleNodeName);
			if (role.equals("OBSERVER")) {
				return new CheckResult(true, "Metaserver status: OBSERVER, skip checking lease assingning.");
			}
			JSONObject consumerLeases = metaServerJsonObject.getJSONObject(ConsumerLeasesNodeName);
			JSONObject brokerLeases = metaServerJsonObject.getJSONObject(BrokerLeasesNodeName);
			if (!consumerLeases.isEmpty() && !brokerLeases.isEmpty()) {
				return new CheckResult(true, null);
			} else {
				return new CheckResult(false, "Consumer or broker lease is empty!");
			}
		} catch (Exception e) {
			m_logger
			      .warn("Check leases status failed! Parse leases from metaserver status string failed! Metaserver status string:{}!",
			            responsePair.getValue());
			return new CheckResult(false, "Check leases status failed! Parse leases from metaserver status string failed!");
		}
	}

	@Override
	public Map<String, CheckResult> getRunningBrokers() throws Exception {
		String url = String.format("http://%s:%s/%s", m_env.getMetaServerDomainName(), new Integer(m_env
		      .getGlobalConfig().getProperty("meta.port", "80").trim()), "metaserver/status");
		CountDownLatch latch = new CountDownLatch(1);
		Pair<Integer, String> responsePair = new Pair<>();
		sendRequest(url, "GET", null, latch, responsePair);
		latch.await();

		if (responsePair == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
			m_logger.warn("Failed to get metaserver status! ResponsePair:{}.", responsePair);
			throw new RuntimeException(String.format("Failed to get metaserver status! ResponsePair:%s.", responsePair));
		}

		try {
			JSONObject metaServerJsonObject = JSON.parseObject(responsePair.getValue());
			String role = metaServerJsonObject.getString(MetaServerRoleNodeName);

			if (!role.equals("LEADER")) {
				JSONObject leaderInfo = metaServerJsonObject.getJSONObject(LeaderInfoNodeName);
				String host = leaderInfo.getString("host");
				int port = leaderInfo.getInteger("port");

				url = String.format("http://%s:%s/%s", host, port, "metaserver/status");
				latch = new CountDownLatch(1);
				sendRequest(url, "GET", null, latch, responsePair);
				latch.await();

				if (responsePair == null || !responsePair.getKey().equals(HttpStatus.SC_OK)) {
					m_logger.warn("Failed to get metaserver status from leader! Leader:host={}, port={},ResponsePair:{}.",
					      host, port, responsePair);
					throw new RuntimeException(String.format(
					      "Failed to get metaserver status from leader! Leader:host=%s, port=%s, ResponsePair:%s.", host,
					      port, responsePair));
				}
			}

			JSONObject runningBrokers = metaServerJsonObject.getJSONObject(RunningBrokersNodeName);
			Map<String, CheckResult> checkResults = new HashMap<>();

			for (Entry<String, Object> runningBroker : runningBrokers.entrySet()) {
				JSONObject brokerInfo = (JSONObject) runningBroker.getValue();
				String ip = brokerInfo.getString("ip");
				String port = brokerInfo.getString("port");
				checkResults.put(String.format("%s:%s", ip, port), new CheckResult(true, null));
			}

			return checkResults;

		} catch (Exception e) {
			m_logger.warn("Parse metaserver status failed! Metaserver status string:{}!", responsePair.getValue());
			throw new RuntimeException("Parse metaserver status failed!");
		}

	}

	/**
	 * 
	 * @param host
	 * @param port
	 * @param path
	 * @param method
	 *           'GET' or 'POST'
	 * @param requestParams
	 * @return
	 * @return
	 */
	private void sendRequest(final String url, final String method, final Map<String, String> requestParams,
	      final CountDownLatch latch, final Pair<Integer, String> requestResult) {

		m_sendRequestExcutor.submit(new Runnable() {

			@Override
			public void run() {
				InputStream is = null;
				try {
					String urlWithRequestParams = url;
					if (requestParams != null) {
						String encodedRequestParamStr = encodePropertiesStr(requestParams);

						if (encodedRequestParamStr != null) {
							urlWithRequestParams = urlWithRequestParams + "?" + encodedRequestParamStr;
						}

					}

					HttpURLConnection conn = (HttpURLConnection) new URL(urlWithRequestParams).openConnection();

					conn.setConnectTimeout(ConnectTimeout);
					conn.setReadTimeout(ReadTimeout);
					conn.setRequestMethod(method);
					conn.connect();

					int statusCode = conn.getResponseCode();

					if (statusCode == 200) {
						is = conn.getInputStream();
						requestResult.setKey(statusCode);
						requestResult.setValue(IO.INSTANCE.readFrom(is, Charsets.UTF_8.name()));
					} else {
						requestResult.setKey(statusCode);
						requestResult.setValue(null);
					}

				} catch (Exception e) {
					// ignore
					if (m_logger.isDebugEnabled()) {
						m_logger.debug("Request {} error.", url, e);
					}
				} finally {
					if (is != null) {
						try {
							is.close();
						} catch (Exception e) {
							// ignore it
						}
					}
					if (latch != null) {
						latch.countDown();
					}
				}
			}
		});

	}

	private String encodePropertiesStr(Map<String, String> properties) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8.name()))//
			      .append("=")//
			      .append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8.name()))//
			      .append("&");
		}

		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return null;
		}
	}

	public class CheckResult {
		private boolean success;

		private String message;

		public CheckResult() {
		}

		public CheckResult(boolean success, String message) {
			this.success = success;
			this.message = message;
		}

		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@Override
		public String toString() {
			return "checkResult [success=" + success + ", message=" + message + "]";
		}
	}

}
