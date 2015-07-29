package com.ctrip.hermes.metaserver.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.ZKSuppportTestCase;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class BrokerAssignmentHolderTest extends ZKSuppportTestCase {

	private BrokerAssignmentHolder m_holder;

	@Override
	protected void initZkData() throws Exception {
		ensurePath(ZKPathUtils.getBrokerAssignmentRootZkPath());
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		configureBrokerAssignmentHolder();
	}

	private void addAssignmentToZk(String topic, int partition, List<Pair<String, ClientContext>> data) throws Exception {
		String path = ZKPathUtils.getBrokerAssignmentZkPath(topic, partition);

		byte[] bytes = null;

		if (data != null && !data.isEmpty()) {
			Map<String, ClientContext> clients = new HashMap<>();
			for (Pair<String, ClientContext> pair : data) {
				clients.put(pair.getKey(), pair.getValue());
			}
			bytes = ZKSerializeUtils.serialize(clients);
		} else {
			bytes = new byte[0];
		}

		ensurePath(ZKPathUtils.getBrokerAssignmentTopicParentZkPath(topic));
		ensurePath(path);

		m_curator.setData().forPath(path, bytes);
	}

	private void configureBrokerAssignmentHolder() {
		BrokerAssignmentHolder holder = lookup(BrokerAssignmentHolder.class);
		m_holder = holder;
	}

	@Test
	public void testReloadWithoutData() throws Exception {
		m_holder.reload();
		assertTrue(m_holder.getAssignments().isEmpty());
	}

	private void assertAssignments(String topic, int partition, List<Pair<String, ClientContext>> expectedClientInfos)
	      throws Exception {

		Assignment<Integer> actualAssignment = m_holder.getAssignment(topic);
		assertNotNull(actualAssignment);

		Map<String, ClientContext> actualClientInfos = actualAssignment.getAssignment(partition);
		assertEquals(expectedClientInfos.size(), actualClientInfos.size());

		for (Pair<String, ClientContext> expectedClientInfo : expectedClientInfos) {
			ClientContext actualClient = actualClientInfos.get(expectedClientInfo.getKey());
			ClientContext expectedClient = expectedClientInfo.getValue();

			TestHelper.assertClientContextEquals(expectedClient.getName(), expectedClient.getIp(),
			      expectedClient.getPort(), expectedClient.getLastHeartbeatTime(), actualClient);
		}

	}

	@Test
	public void testReloadAndClear() throws Exception {
		addAssignmentToZk("t1", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br0", new ClientContext("br0", "1.1.1.1", 1111, -1)),//
		      new Pair<String, ClientContext>("br1", new ClientContext("br1", "1.1.1.2", 2222, -1))//
		      ));

		addAssignmentToZk("t1", 1, null);

		addAssignmentToZk("t2", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br2", new ClientContext("br2", "1.1.1.3", 3333, -1))//
		      ));
		addAssignmentToZk("t2", 1, null);

		// reload
		m_holder.reload();

		assertEquals(2, m_holder.getAssignments().size());

		assertAssignments("t1", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br0", new ClientContext("br0", "1.1.1.1", 1111, -1)),//
		      new Pair<String, ClientContext>("br1", new ClientContext("br1", "1.1.1.2", 2222, -1))//
		      ));

		assertNull(m_holder.getAssignment("t1").getAssignment(1));

		assertAssignments("t2", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br2", new ClientContext("br2", "1.1.1.3", 3333, -1))//
		      ));
		assertNull(m_holder.getAssignment("t2").getAssignment(1));
		assertNull(m_holder.getAssignment("t3"));

		// clear
		m_holder.clear();
		assertTrue(m_holder.getAssignments().isEmpty());
	}

	@Test
	public void testReassign() throws Exception {

		addAssignmentToZk("t1", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br0", new ClientContext("br0", "1.1.1.1", 1111, -1))//
		      ));

		m_holder.reload();

		Map<String, ClientContext> brokers = new HashMap<>();
		brokers.put("br1", new ClientContext("br1", "1.1.1.2", 2222, 0));

		m_holder.reassign(brokers, Arrays.asList(createTopic("t2", 2, Endpoint.BROKER)));

		assertNull(m_holder.getAssignment("t1"));
		Assignment<Integer> assignment = m_holder.getAssignment("t2");
		assertNotNull(assignment);
		assertAssignments("t2", 0, Arrays.asList(//
		      new Pair<String, ClientContext>("br1", new ClientContext("br1", "1.1.1.2", 2222, 0))//
		      ));
		assertAssignments("t2", 1, Arrays.asList(//
		      new Pair<String, ClientContext>("br1", new ClientContext("br1", "1.1.1.2", 2222, 0))//
		      ));

		Map<String, ClientContext> t20DataInZK = ZKSerializeUtils.deserialize(
		      m_curator.getData().forPath(ZKPathUtils.getBrokerAssignmentZkPath("t2", 0)),
		      new TypeReference<Map<String, ClientContext>>() {
		      }.getType());

		assertEquals(1, t20DataInZK.size());

		TestHelper.assertClientContextEquals("br1", "1.1.1.2", 2222, 0, t20DataInZK.values().iterator().next());

		Map<String, ClientContext> t21DataInZK = ZKSerializeUtils.deserialize(
		      m_curator.getData().forPath(ZKPathUtils.getBrokerAssignmentZkPath("t2", 1)),
		      new TypeReference<Map<String, ClientContext>>() {
		      }.getType());

		assertEquals(1, t21DataInZK.size());

		TestHelper.assertClientContextEquals("br1", "1.1.1.2", 2222, 0, t21DataInZK.values().iterator().next());
	}

	private Topic createTopic(String topicName, int partitionCount, String endpointType) {
		Topic t = new Topic(topicName);
		t.setPartitionCount(partitionCount);
		t.setEndpointType(endpointType);

		for (int i = 0; i < partitionCount; i++) {
			Partition partition = new Partition(i);
			t.addPartition(partition);
		}

		return t;
	}

}
