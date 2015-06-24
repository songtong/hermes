package com.ctrip.hermes.metaserver.event;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EventEngine {
	public void start(ClusterStateHolder clusterStateHolder) throws Exception;

	public void stop();

}
