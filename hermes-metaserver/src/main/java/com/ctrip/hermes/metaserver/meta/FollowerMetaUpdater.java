package com.ctrip.hermes.metaserver.meta;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.build.BuildConstants;
import com.ctrip.hermes.metaserver.cluster.ClusterStateHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaUpdater.class, value = BuildConstants.FOLLOWER)
public class FollowerMetaUpdater implements MetaUpdater, Initializable {

	@Override
	public void initialize() throws InitializationException {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop(ClusterStateHolder stateHolder) {
		// TODO Auto-generated method stub

	}

	@Override
	public void start(ClusterStateHolder stateHolder) {
		// TODO Auto-generated method stub

	}

}
