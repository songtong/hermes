package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class BaseAssignmentHolder<Key1, Key2> implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(BaseAssignmentHolder.class);

	private AtomicReference<HashMap<Key1, Assignment>> m_assignments = new AtomicReference<>(
	      new HashMap<Key1, Assignment>());

	public Assignment getAssignment(Key1 key1) {
		return m_assignments.get().get(key1);
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(getAssignmentCheckerName(), true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      Map<Key1, Set<String>> changes = getActiveClientListHolder().scanChanges(getClientTimeoutMillis(),
					            TimeUnit.MILLISECONDS);

					      if (changes != null && !changes.isEmpty()) {
						      HashMap<Key1, Assignment> newAssignments = new HashMap<>(m_assignments.get());
						      for (Map.Entry<Key1, Set<String>> change : changes.entrySet()) {
							      Key1 key1 = change.getKey();
							      Set<String> clientList = change.getValue();

							      if (clientList == null || clientList.isEmpty()) {
								      newAssignments.remove(key1);
							      } else {
								      Assignment newAssignment = createNewAssignment(key1, clientList, newAssignments.get(key1));
								      if (newAssignment != null) {
									      newAssignments.put(key1, newAssignment);
								      }
							      }
						      }

						      m_assignments.set(newAssignments);

						      if (log.isDebugEnabled()) {
							      StringBuilder sb = new StringBuilder();

							      for (Map.Entry<Key1, Assignment> entry : newAssignments.entrySet()) {
								      sb.append("[");
								      sb.append("key1=").append(entry.getKey()).append(",");
								      sb.append("assignment=").append(entry.getValue());
								      sb.append("]");
							      }

							      log.debug("Assignment changed.(new assignment={})", sb.toString());
						      }
					      }
				      } catch (Exception e) {
					      log.warn(String.format("Error occured while doing assignment check in %s",
					            getAssignmentCheckerName()), e);
				      }
			      }
		      }, 0, getAssignmentCheckIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	protected abstract ActiveClientListHolder<Key1> getActiveClientListHolder();

	protected abstract Assignment createNewAssignment(Key1 key1, Set<String> clientList,
	      BaseAssignmentHolder<Key1, Key2>.Assignment originAssignment);

	protected abstract long getClientTimeoutMillis();

	protected abstract long getAssignmentCheckIntervalMillis();

	protected abstract String getAssignmentCheckerName();

	public class Assignment {
		private Map<Key2, Set<String>> m_assigment = new ConcurrentHashMap<>();

		public boolean isAssignTo(Key2 key2, String client) {
			Set<String> clients = m_assigment.get(key2);
			return clients != null && !clients.isEmpty() && clients.contains(client);
		}

		public void addAssignment(Key2 key2, Set<String> clients) {
			if (!m_assigment.containsKey(key2)) {
				m_assigment.put(key2, new HashSet<String>());
			}
			m_assigment.get(key2).addAll(clients);
		}

		public Map<Key2, Set<String>> getAssigment() {
			return m_assigment;
		}

		@Override
		public String toString() {
			return "Assignment [m_assigment=" + m_assigment + "]";
		}

	}

}
