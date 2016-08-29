package com.ctrip.hermes.metaserver.event;

import java.util.concurrent.ExecutorService;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventBus {

	void pubEvent(Event event);

	ExecutorService getExecutor();

	void submit(long version, Task task);

	void start(ClusterStateHolder clusterStateHolder);

	public interface Task {
		public void run();

		public void onGuardNotPass();
	}

}
