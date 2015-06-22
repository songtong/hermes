package com.ctrip.hermes.metaserver.broker;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.commons.Assignment;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = BrokerAssignmentHolder.class)
public class BrokerAssignmentHolder {

	private AtomicReference<HashMap<String, Assignment<Integer>>> m_assignments = new AtomicReference<>(
	      new HashMap<String, Assignment<Integer>>());

	public Assignment<Integer> getAssignment(String topic) {
		return m_assignments.get().get(topic);
	}

}
