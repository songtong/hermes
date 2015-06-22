package com.ctrip.hermes.metaserver.broker;

import java.util.Map;

import com.ctrip.hermes.metaserver.commons.Assignment;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface BrokerAssignmentChangedWatcher {

	public void assignmentChanged(Map<String, Assignment<Integer>> newAssignments);
}
