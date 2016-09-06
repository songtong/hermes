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

	void start(ClusterStateHolder clusterStateHolder);

}
