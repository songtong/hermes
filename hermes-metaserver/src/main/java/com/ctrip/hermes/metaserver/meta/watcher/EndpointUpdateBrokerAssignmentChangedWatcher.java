package com.ctrip.hermes.metaserver.meta.watcher;

import java.util.ArrayList;
import java.util.Map;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaserver.broker.BrokerAssignmentChangedWatcher;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssignmentChangedWatcher.class, value = "EndpointUpdater")
public class EndpointUpdateBrokerAssignmentChangedWatcher implements BrokerAssignmentChangedWatcher {

	@Override
	public void assignmentChanged(Map<String, Assignment<Integer>> newAssignments) {
		try {
			// TODO handle this creation circularity
			ZkReader zkReader = PlexusComponentLocator.lookup(ZkReader.class);
			MetaHolder metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);
			if (zkReader != null && metaHolder != null) {
				Meta meta = metaHolder.getMeta();
				if (meta != null) {
					metaHolder.update(zkReader.readPartition2Endpoints(new ArrayList<>(meta.getTopics().values())));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
