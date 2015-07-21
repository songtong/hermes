package com.ctrip.hermes.metaserver.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.lease.Lease;
import com.ctrip.hermes.core.lease.LeaseAcquireResponse;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.metaserver.commons.ClientLeaseInfo;
import com.ctrip.hermes.metaserver.commons.LeaseOperationCallback;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKConfig;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BrokerLeaseHolderTest extends ComponentTestCase {

	public static class TestBrokerLeaaseHolder extends BrokerLeaseHolder {
		@Override
		protected void startHouseKeeper() {
		}
	}

	private static TestingServer m_zkServer;

	private static final int ZK_PORT = 2222;

	@AfterClass
	public static void afterClass() throws Exception {
		stopZkServer();
	}

	@BeforeClass
	public static void beforeClass() throws Exception {
		startZkServer();
	}

	private static void startZkServer() throws Exception {
		m_zkServer = new TestingServer(ZK_PORT);
	}

	private static void stopZkServer() throws Exception {
		if (m_zkServer != null) {
			m_zkServer.close();
		}
	}

	private CuratorFramework m_curator;

	@Mock
	private ClientEnvironment m_env;

	private BrokerLeaseHolder m_leaseHolder;

	private void addLeasesDataToZk(String topic, int partition, List<Pair<String, ClientLeaseInfo>> data)
	      throws Exception {
		String path = ZKPathUtils.getBrokerLeaseZkPath(topic, partition);
		String parentPath = ZKPathUtils.getBrokerAssignmentTopicParentZkPath(topic);

		byte[] bytes = null;

		if (data != null && !data.isEmpty()) {
			Map<String, ClientLeaseInfo> leases = new HashMap<>();
			for (Pair<String, ClientLeaseInfo> pair : data) {
				leases.put(pair.getKey(), pair.getValue());
			}
			bytes = ZKSerializeUtils.serialize(leases);
		} else {
			bytes = new byte[0];
		}

		ensurePath(parentPath);
		ensurePath(path);

		m_curator.setData().forPath(path, bytes);
		m_curator.setData().forPath(parentPath, ZKSerializeUtils.serialize(System.currentTimeMillis()));
	}

	private void assertLeases(Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases, String topic,
	      int partition, List<Pair<String, ClientLeaseInfo>> expectedLeaseInfos) {
		Pair<String, Integer> tp = new Pair<String, Integer>(topic, partition);
		Map<String, ClientLeaseInfo> actualLeaseInfos = allValidLeases.get(tp);
		assertEquals(expectedLeaseInfos.size(), actualLeaseInfos.size());

		for (Pair<String, ClientLeaseInfo> expectedLeaseInfo : expectedLeaseInfos) {
			ClientLeaseInfo actualClientLeaseInfo = actualLeaseInfos.get(expectedLeaseInfo.getKey());
			ClientLeaseInfo expectedClientLeaseInfo = expectedLeaseInfo.getValue();
			assertEquals(expectedClientLeaseInfo.getIp(), actualClientLeaseInfo.getIp());
			assertEquals(expectedClientLeaseInfo.getPort(), actualClientLeaseInfo.getPort());

			Lease actualLease = actualClientLeaseInfo.getLease();
			Lease expectedLease = expectedClientLeaseInfo.getLease();
			assertEquals(expectedLease.getId(), actualLease.getId());
			assertEquals(expectedLease.getExpireTime(), actualLease.getExpireTime());
		}
	}

	private void clearZk() throws Exception {
		deleteChildren("/", true);
	}

	private void configureBrokerLeaseHolder() throws Exception {
		Properties globalConf = new Properties();
		globalConf.put("meta.zk.connectionString", getZkConnectionString());
		when(m_env.getGlobalConfig()).thenReturn(globalConf);
		lookup(ZKConfig.class).setEnv(m_env);

		defineComponent(BrokerLeaseHolder.class, TestBrokerLeaaseHolder.class)//
		      .req(ZKClient.class)//
		      .req(ZookeeperService.class)//
		      .req(SystemClockService.class);

		m_leaseHolder = lookup(BrokerLeaseHolder.class);
	}

	private void configureCurator() throws Exception {

		Builder builder = CuratorFrameworkFactory.builder();

		builder.connectionTimeoutMs(50);
		builder.connectString(getZkConnectionString());
		builder.maxCloseWaitMs(50);
		builder.namespace("hermes");
		builder.retryPolicy(new ExponentialBackoffRetry(5, 3));
		builder.sessionTimeoutMs(50);

		m_curator = builder.build();
		m_curator.start();
		try {
			m_curator.blockUntilConnected();
		} catch (InterruptedException e) {
			throw new InitializationException(e.getMessage(), e);
		}

	}

	private void deleteChildren(String path, boolean deleteSelf) throws Exception {
		PathUtils.validatePath(path);

		CuratorFramework client = m_curator;
		Stat stat = client.checkExists().forPath(path);
		if (stat != null) {
			List<String> children = client.getChildren().forPath(path);
			for (String child : children) {
				String fullPath = ZKPaths.makePath(path, child);
				deleteChildren(fullPath, true);
			}

			if (deleteSelf) {
				try {
					client.delete().forPath(path);
				} catch (KeeperException.NotEmptyException e) {
					// someone has created a new child since we checked ... delete again.
					deleteChildren(path, true);
				} catch (KeeperException.NoNodeException e) {
					// ignore... someone else has deleted the node it since we checked
				}

			}
		}
	}

	private void ensurePath(String path) throws Exception {
		EnsurePath ensurePath = m_curator.newNamespaceAwareEnsurePath(path);
		ensurePath.ensure(m_curator.getZookeeperClient());
	}

	private String getZkConnectionString() {
		return "127.0.0.1:" + ZK_PORT;
	}

	private void initZkData() throws Exception {
		ensurePath(ZKPathUtils.getBrokerLeaseRootZkPath());
	}

	private void leaseHolderReload() throws Exception {
		m_leaseHolder.updateContexts(m_leaseHolder.loadExistingLeases());
	}

	@Before
	@Override
	public void setUp() throws Exception {
		configureCurator();
		initZkData();

		super.setUp();

		configureBrokerLeaseHolder();
	}

	@After
	@Override
	public void tearDown() throws Exception {
		super.tearDown();
		clearZk();
	}

	@Test
	public void testExecuteLeaseOperation() throws Exception {
		final long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;

		addLeasesDataToZk("t1", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234)),//
		      new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, 30), "0.0.0.1", 1233))//
		      ));

		leaseHolderReload();

		m_leaseHolder.executeLeaseOperation(new Pair<String, Integer>("t1", 0), new LeaseOperationCallback() {

			@Override
			public LeaseAcquireResponse execute(Map<String, ClientLeaseInfo> existingValidLeases) throws Exception {
				assertEquals(1, existingValidLeases.size());
				ClientLeaseInfo br0ClientLeaseInfo = existingValidLeases.get("br0");
				ClientLeaseInfo br1ClientLeaseInfo = existingValidLeases.get("br1");
				assertNotNull(br0ClientLeaseInfo);
				assertNull(br1ClientLeaseInfo);

				Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> leases = new HashMap<>();
				leases.put(new Pair<String, Integer>("t1", 0), existingValidLeases);

				assertLeases(
				      leases,
				      "t1",
				      0,
				      Arrays.asList(//
				      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
				            "0.0.0.0", 1234))));

				return null;
			}
		});
	}

	@Test
	public void testInit() throws Exception {
		long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;

		addLeasesDataToZk("t1", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234)),//
		      new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
		            "0.0.0.1", 1233))//
		      ));

		addLeasesDataToZk(
		      "t1",
		      1,
		      Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
		            "0.0.0.2", 2222))//
		      ));

		addLeasesDataToZk("t1", 2, null);

		addLeasesDataToZk(
		      "t2",
		      0,
		      Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234))//
		      ));

		addLeasesDataToZk("t3", 2, null);

		leaseHolderReload();

		Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
		assertEquals(3, allValidLeases.size());

		assertLeases(allValidLeases, "t1", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234)),//
		      new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
		            "0.0.0.1", 1233))//
		      ));
		assertLeases(
		      allValidLeases,
		      "t1",
		      1,
		      Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, fakeNowTimestamp + 150),
		            "0.0.0.2", 2222))//
		      ));
		assertLeases(
		      allValidLeases,
		      "t2",
		      0,
		      Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234))//
		      ));

		assertTrue(m_leaseHolder.topicWatched("t1"));
		assertTrue(m_leaseHolder.topicWatched("t2"));
		assertTrue(m_leaseHolder.topicWatched("t3"));
		assertFalse(m_leaseHolder.topicWatched("t4"));
	}

	@Test
	public void testInitWithoutData() throws Exception {
		assertTrue(m_leaseHolder.getAllValidLeases().isEmpty());
	}

	@Test
	public void testNewLease() throws Exception {
		final String topic = "t1";
		final int partition = 1;
		final String ip = "1.1.1.2";
		final int port = 1111;
		String brokerName = "br0";

		leaseHolderReload();

		Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
		m_leaseHolder.newLease(new Pair<String, Integer>(topic, partition), brokerName, existingValidLeases,
		      1000 * 1000L, ip, port);

		assertEquals(1, existingValidLeases.size());
		ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(brokerName);
		assertNotNull(clientLeaseInfo);

		assertEquals(ip, clientLeaseInfo.getIp());
		assertEquals(port, clientLeaseInfo.getPort());
		assertFalse(clientLeaseInfo.getLease().isExpired());

		Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
		      m_curator.getData().forPath(ZKPathUtils.getBrokerLeaseZkPath(topic, partition)),
		      new TypeReference<Map<String, ClientLeaseInfo>>() {
		      }.getType());

		assertEquals(1, zkLeases.size());
		clientLeaseInfo = zkLeases.get(brokerName);
		assertNotNull(clientLeaseInfo);

		assertEquals(ip, clientLeaseInfo.getIp());
		assertEquals(port, clientLeaseInfo.getPort());
		assertFalse(clientLeaseInfo.getLease().isExpired());
	}

	@Test
	public void testRemoveExpiredLeasesAndGetAllValidLeases() throws Exception {
		long fakeNowTimestamp = System.currentTimeMillis() + 500 * 1000L;

		addLeasesDataToZk("t1", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234)),//
		      new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
		            "0.0.0.1", 1233))//
		      ));

		addLeasesDataToZk("t1", 1, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br2", new ClientLeaseInfo(new Lease(3, 0), "0.0.0.2", 2222))//
		      ));

		addLeasesDataToZk("t2", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, 0), "0.0.0.0", 1234))//
		      ));

		leaseHolderReload();

		Map<Pair<String, Integer>, Map<String, ClientLeaseInfo>> allValidLeases = m_leaseHolder.getAllValidLeases();
		assertEquals(1, allValidLeases.size());

		assertLeases(allValidLeases, "t1", 0, Arrays.asList(//
		      new Pair<String, ClientLeaseInfo>("br0", new ClientLeaseInfo(new Lease(1, fakeNowTimestamp + 50),
		            "0.0.0.0", 1234)),//
		      new Pair<String, ClientLeaseInfo>("br1", new ClientLeaseInfo(new Lease(2, fakeNowTimestamp + 30),
		            "0.0.0.1", 1233))//
		      ));
	}

	@Test
	public void testRenewLease() throws Exception {
		final String topic = "t1";
		final int partition = 1;
		final String ip = "1.1.1.2";
		final int port = 1111;
		String brokerName = "br0";

		leaseHolderReload();

		long now = System.currentTimeMillis();

		Map<String, ClientLeaseInfo> existingValidLeases = new HashMap<>();
		ClientLeaseInfo existingLeaseInfo = new ClientLeaseInfo(new Lease(1, now + 1000L), ip, port);

		m_leaseHolder.renewLease(new Pair<String, Integer>(topic, partition), brokerName, existingValidLeases,
		      existingLeaseInfo, 1000 * 1000L, ip, port);

		assertEquals(1, existingValidLeases.size());
		ClientLeaseInfo clientLeaseInfo = existingValidLeases.get(brokerName);
		assertNotNull(clientLeaseInfo);

		assertEquals(ip, clientLeaseInfo.getIp());
		assertEquals(port, clientLeaseInfo.getPort());
		assertFalse(clientLeaseInfo.getLease().isExpired());
		assertEquals(1, clientLeaseInfo.getLease().getId());
		assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());

		Map<String, ClientLeaseInfo> zkLeases = ZKSerializeUtils.deserialize(
		      m_curator.getData().forPath(ZKPathUtils.getBrokerLeaseZkPath(topic, partition)),
		      new TypeReference<Map<String, ClientLeaseInfo>>() {
		      }.getType());

		assertEquals(1, zkLeases.size());
		clientLeaseInfo = zkLeases.get(brokerName);
		assertNotNull(clientLeaseInfo);

		assertEquals(ip, clientLeaseInfo.getIp());
		assertEquals(port, clientLeaseInfo.getPort());
		assertFalse(clientLeaseInfo.getLease().isExpired());
		assertEquals(1, clientLeaseInfo.getLease().getId());
		assertEquals(now + 1000L + 1000 * 1000L, clientLeaseInfo.getLease().getExpireTime());
	}
}
