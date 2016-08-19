package com.ctrip.hermes.metaserver.cluster;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatch.CloseMode;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.utils.ZKPaths;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.bo.HostPort;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.broker.BrokerLeaseHolder;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.consumer.ConsumerLeaseHolder;
import com.ctrip.hermes.metaserver.event.Event;
import com.ctrip.hermes.metaserver.event.EventBus;
import com.ctrip.hermes.metaserver.event.EventType;
import com.ctrip.hermes.metaserver.event.Guard;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ClusterStateHolder.class)
public class ClusterStateHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ClusterStateHolder.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private ZKClient m_client;

	@Inject
	private EventBus m_eventBus;

	private AtomicReference<HostPort> m_leader = new AtomicReference<>(null);

	private volatile LeaderLatch m_leaderLatch = null;

	private AtomicBoolean m_closed = new AtomicBoolean(false);

	@Inject
	private ConsumerLeaseHolder m_consumerLeaseHolder;

	@Inject
	private BrokerLeaseHolder m_brokerLeaseHolder;

	@Inject
	private Guard m_guard;

	private ReentrantReadWriteLock m_roleLock = new ReentrantReadWriteLock(true);

	private volatile Role m_role = null;

	public void becomeLeader() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Leader");
			m_leader.set(new HostPort(Networks.forIp().getLocalHostAddress(), m_config.getMetaServerPort()));
			m_role = Role.LEADER;
			m_eventBus.pubEvent(new Event(EventType.LEADER_INIT, m_guard.upgradeVersion(), this, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	public void becomeFollower() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Follower");
			m_role = Role.FOLLOWER;
			startLeaderLatch();

			m_eventBus.pubEvent(new Event(EventType.FOLLOWER_INIT, m_guard.upgradeVersion(), this, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	private void startLeaderLatch() {
		if (m_leaderLatch == null) {
			m_leaderLatch = new LeaderLatch(m_client.get(), m_config.getMetaServerLeaderElectionZkPath(),
			      m_config.getMetaServerName());

			m_leaderLatch.addListener(new LeaderLatchListener() {

				@Override
				public void notLeader() {
					becomeFollower();
				}

				@Override
				public void isLeader() {
					becomeLeader();
				}
			}, m_eventBus.getExecutor());

			delayStartLeaderLatch();
		}
	}

	public void becomeObserver() {
		m_roleLock.writeLock().lock();
		try {
			log.info("Become Observer");
			m_role = Role.OBSERVER;
			closeLeaderLatch();
			m_eventBus.pubEvent(new Event(EventType.OBSERVER_INIT, m_guard.upgradeVersion(), this, null));
		} finally {
			m_roleLock.writeLock().unlock();
		}
	}

	private void closeLeaderLatch() {
		if (m_leaderLatch != null) {
			try {
				m_leaderLatch.close(CloseMode.SILENT);
				log.info("LeaderLatch closed");
			} catch (IOException e) {
				log.error("Exception occurred while closing leaderLatch.", e);
			} finally {
				m_leaderLatch = null;
			}
		}
	}

	public Role getRole() {
		m_roleLock.readLock().lock();
		try {
			return m_role;
		} finally {
			m_roleLock.readLock().unlock();
		}
	}

	public void start() throws Exception {
		becomeObserver();
	}

	private void delayStartLeaderLatch() {
		Thread t = HermesThreadFactory.create("LeaderLatchDelayStarter", true).newThread(new Runnable() {

			@Override
			public void run() {
				while (!Thread.interrupted() && !m_closed.get()) {
					m_roleLock.readLock().lock();
					try {
						if (m_role == Role.FOLLOWER && m_leaderLatch != null && m_consumerLeaseHolder.inited()
						      && m_brokerLeaseHolder.inited()) {
							try {
								m_leaderLatch.start();
								log.info("LeaderLatch started");
								break;
							} catch (Exception e) {
								log.error("Failed to start LeaderLatch!!", e);
							}
						} else {
							try {
								TimeUnit.SECONDS.sleep(1);
							} catch (InterruptedException e) {
								log.error("Failed to start LeaderLatch!!", e);
								Thread.currentThread().interrupt();
							}
						}
					} finally {
						m_roleLock.readLock().unlock();
					}
				}
			}
		});
		t.start();
	}

	public void close() throws Exception {
		if (m_closed.compareAndSet(false, true)) {
			closeLeaderLatch();
		}
	}

	public HostPort getLeader() {
		return m_leader.get();
	}

	private HostPort fetchLeaderInfoFromZk() {
		try {
			List<String> children = m_client.get().getChildren().forPath(ZKPathUtils.getMetaServersZkPath());

			if (children != null && !children.isEmpty()) {

				List<String> sortedChildren = LockInternals.getSortedChildren("latch-", new LockInternalsSorter() {

					@Override
					public String fixForSorting(String str, String lockName) {
						return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
					}
				}, children);

				return ZKSerializeUtils.deserialize(
				      m_client.get().getData()
				            .forPath(ZKPaths.makePath(ZKPathUtils.getMetaServersZkPath(), sortedChildren.get(0))),
				      HostPort.class);
			}
		} catch (Exception e) {
			log.error("Failed to fetch leader info from zk.", e);
		}

		return null;
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("LeaderInfoFetcher", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      HostPort leaderInfo = fetchLeaderInfoFromZk();
					      if (leaderInfo != null) {
						      m_leader.set(leaderInfo);
					      }
				      } catch (Exception e) {
					      log.error("Exception occurred while fetching leader info.", e);
				      }
			      }
		      }, 5, 2, TimeUnit.SECONDS);
	}

	// for test only
	public void setRole(Role role) {
		m_role = role;
	}
}
