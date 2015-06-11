package com.ctrip.hermes.metaserver.meta;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.metaservice.zk.ZKClient;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = LeaderMetaUpdate.class, value = BuildConstants.LEADER)
public class LeaderMetaUpdate implements MetaUpdater, Initializable {

	private static final Logger log = LoggerFactory.getLogger(LeaderMetaUpdate.class);

	@Inject
	private ZKClient m_zkClient;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private MetaService m_metaService;

	private AtomicBoolean m_stopped = new AtomicBoolean(false);

	private ScheduledExecutorService m_scheduledExecutorService;

	@Override
	public void stop(ClusterStateHolder stateHolder) {
		m_stopped.set(true);
	}

	@Override
	public void start(ClusterStateHolder stateHolder) {
		if (m_stopped.compareAndSet(true, false)) {
			m_scheduledExecutorService.schedule(new MetaUpdateTask(stateHolder), 1, TimeUnit.SECONDS);
		}
	}

	@Override
	public void initialize() throws InitializationException {
		m_scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      "LeaderMetaUpdater", true));
	}

	private class MetaUpdateTask implements Runnable {

		private ClusterStateHolder m_stateHolder;

		public MetaUpdateTask(ClusterStateHolder stateHolder) {
			m_stateHolder = stateHolder;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}
}
