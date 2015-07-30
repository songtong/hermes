package com.ctrip.hermes.metaserver.event;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.TestHelper;
import com.ctrip.hermes.metaserver.ZKSuppportTestCase;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentHolder;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaserver.event.impl.FollowerInitEventHandler;
import com.ctrip.hermes.metaserver.event.impl.LeaderMetaFetcher;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.meta.MetaInfo;
import com.ctrip.hermes.metaserver.meta.MetaServerAssignmentHolder;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class FollowerEventEngineTest extends ZKSuppportTestCase {

	@Mock
	private MetaHolder m_metaHolder;

	@Mock
	private BrokerAssignmentHolder m_brokerAssignmentHolder;

	@Mock
	private MetaServerAssignmentHolder m_metaServerAssignmentHolder;

	@Mock
	private LeaderMetaFetcher m_leaderMetaFetcher;

	private FollowerEventEngine m_engine;

	@Override
	protected void initZkData() throws Exception {
		ensurePath(ZKPathUtils.getMetaInfoZkPath());
		ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());
		ensurePath(ZKPathUtils.getBrokerAssignmentRootZkPath());
		ensurePath(ZKPathUtils.getBrokerLeaseRootZkPath());
		ensurePath(ZKPathUtils.getConsumerLeaseRootZkPath());

		m_curator.setData().forPath(ZKPathUtils.getMetaInfoZkPath(),
		      ZKSerializeUtils.serialize(new MetaInfo("1.1.1.1", 1111, System.currentTimeMillis())));
	}

	@Override
	protected void doSetUp() throws Exception {
		m_engine = new FollowerEventEngine();
		FollowerInitEventHandler followerInitEventHandler = (FollowerInitEventHandler) lookup(EventHandler.class,
		      "FollowerInitEventHandler");
		followerInitEventHandler.setBrokerAssignmentHolder(m_brokerAssignmentHolder);
		followerInitEventHandler.setMetaHolder(m_metaHolder);
		followerInitEventHandler.setLeaderMetaFetcher(m_leaderMetaFetcher);
		followerInitEventHandler.setMetaServerAssignmentHolder(m_metaServerAssignmentHolder);

		when(m_leaderMetaFetcher.fetchMetaInfo(any(MetaInfo.class))).thenReturn(TestHelper.loadLocalMeta(this));
	}

	@Test
	public void testStart() throws Exception {
		m_engine.start(createClusterStateHolder());

		final CountDownLatch latch = new CountDownLatch(1);

		final AtomicReference<Meta> loadedMeta = new AtomicReference<>();
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				loadedMeta.set(invocation.getArgumentAt(0, Meta.class));
				return null;
			}
		}).when(m_metaHolder).setMeta(any(Meta.class));

		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.countDown();
				return null;
			}
		}).when(m_metaServerAssignmentHolder).reload();

		latch.await(5, TimeUnit.SECONDS);
		verify(m_brokerAssignmentHolder, times(1)).clear();
		verify(m_metaServerAssignmentHolder, times(1)).reload();

		assertEquals(TestHelper.loadLocalMeta(this).toString(), loadedMeta.get().toString());
	}

	private ClusterStateHolder createClusterStateHolder() {
		ClusterStateHolder holder = new ClusterStateHolder();
		holder.setHasLeadership(false);
		return holder;
	}
}
