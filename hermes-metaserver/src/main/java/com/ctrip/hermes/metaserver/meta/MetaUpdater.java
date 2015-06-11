package com.ctrip.hermes.metaserver.meta;

import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MetaUpdater {

	void stop(ClusterStateHolder stateHolder);

	void start(ClusterStateHolder stateHolder);

}
