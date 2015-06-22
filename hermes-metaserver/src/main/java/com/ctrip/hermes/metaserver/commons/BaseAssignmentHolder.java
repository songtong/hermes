package com.ctrip.hermes.metaserver.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.metaserver.commons.ActiveClientList.ClientContext;

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
					      Map<Key1, Map<String, ClientContext>> changes = getActiveClientListHolder().scanChanges(
					            getClientTimeoutMillis(), TimeUnit.MILLISECONDS);

					      if (changes != null && !changes.isEmpty()) {
						      HashMap<Key1, Assignment> newAssignments = new HashMap<>(m_assignments.get());
						      for (Map.Entry<Key1, Map<String, ClientContext>> change : changes.entrySet()) {
							      Key1 key1 = change.getKey();
							      Map<String, ClientContext> clientList = change.getValue();

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

							      sb.append("[");
							      for (Map.Entry<Key1, Assignment> entry : newAssignments.entrySet()) {
								      sb.append("key1=").append(entry.getKey()).append(",");
								      sb.append("assignment=").append(entry.getValue());
							      }
							      sb.append("]");

							      log.debug("Assignment changed.(new assignment={})", sb.toString());
						      }
					      }
				      } catch (Exception e) {
					      log.warn("Error occurred while doing assignment check in {}", getAssignmentCheckerName(), e);
				      }
			      }
		      }, 0, getAssignmentCheckIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	protected abstract ActiveClientListHolder<Key1> getActiveClientListHolder();

	protected abstract Assignment createNewAssignment(Key1 key1, Map<String, ClientContext> clientList,
	      BaseAssignmentHolder<Key1, Key2>.Assignment originAssignment);

	protected abstract long getClientTimeoutMillis();

	protected abstract long getAssignmentCheckIntervalMillis();

	protected abstract String getAssignmentCheckerName();

	public class Assignment {
		private Map<Key2, Map<String, ClientContext>> m_assigment = new ConcurrentHashMap<>();

		public boolean isAssignTo(Key2 key2, String client) {
			Map<String, ClientContext> clients = m_assigment.get(key2);
			return clients != null && !clients.isEmpty() && clients.keySet().contains(client);
		}

		public void addAssignment(Key2 key2, Map<String, ClientContext> clients) {
			if (!m_assigment.containsKey(key2)) {
				m_assigment.put(key2, new HashMap<String, ClientContext>());
			}
			m_assigment.get(key2).putAll(clients);
		}

		public Map<Key2, Map<String, ClientContext>> getAssigment() {
			return m_assigment;
		}

		@Override
		public String toString() {
			return "Assignment [m_assigment=" + m_assigment + "]";
		}

	}

}
