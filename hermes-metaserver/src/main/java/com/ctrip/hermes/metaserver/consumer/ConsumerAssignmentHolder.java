package com.ctrip.hermes.metaserver.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaserver.commons.Assignment;
import com.ctrip.hermes.metaserver.commons.ClientContext;
import com.ctrip.hermes.metaserver.config.MetaServerConfig;
import com.ctrip.hermes.metaserver.meta.MetaHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerAssignmentHolder.class)
public class ConsumerAssignmentHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(ConsumerAssignmentHolder.class);

	@Inject
	private MetaServerConfig m_config;

	@Inject
	private MetaHolder m_metaHolder;

	@Inject
	private OrderedConsumeConsumerPartitionAssigningStrategy m_partitionAssigningStrategy;

	@Inject
	private ActiveConsumerListHolder m_activeConsumerListHolder;

	private AtomicReference<Map<Pair<String, String>, Assignment<Integer>>> m_assignments = new AtomicReference<>();

	public ConsumerAssignmentHolder() {
		m_assignments.set(new HashMap<Pair<String, String>, Assignment<Integer>>());
	}

	public Assignment<Integer> getAssignment(Pair<String, String> topicGroup) {
		return m_assignments.get().get(topicGroup);
	}

	@Override
	public void initialize() throws InitializationException {
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("ConsumerRebalanceChecker", true))
		      .scheduleWithFixedDelay(new ConusmerRebalanceCheckTask(), 0,
		            m_config.getActiveConsumerCheckIntervalTimeMillis(), TimeUnit.MILLISECONDS);
	}

	private class ConusmerRebalanceCheckTask implements Runnable {

		@Override
		public void run() {
			try {
				Map<Pair<String, String>, Map<String, ClientContext>> changes = m_activeConsumerListHolder.scanChanges(
				      m_config.getConsumerHeartbeatTimeoutMillis(), TimeUnit.MILLISECONDS);

				if (changes != null && !changes.isEmpty()) {
					HashMap<Pair<String, String>, Assignment<Integer>> newAssignments = new HashMap<>(m_assignments.get());
					for (Map.Entry<Pair<String, String>, Map<String, ClientContext>> change : changes.entrySet()) {
						Pair<String, String> topicGroup = change.getKey();
						Map<String, ClientContext> consumerList = change.getValue();

						if (consumerList == null || consumerList.isEmpty()) {
							newAssignments.remove(topicGroup);
						} else {
							Assignment<Integer> newAssignment = createNewAssignment(topicGroup, consumerList,
							      newAssignments.get(topicGroup));
							if (newAssignment != null) {
								newAssignments.put(topicGroup, newAssignment);
							}
						}
					}

					m_assignments.set(newAssignments);

					if (log.isDebugEnabled()) {
						StringBuilder sb = new StringBuilder();

						sb.append("[");
						for (Map.Entry<Pair<String, String>, Assignment<Integer>> entry : newAssignments.entrySet()) {
							sb.append("TopicGroup=").append(entry.getKey()).append(",");
							sb.append("assignment=").append(entry.getValue());
						}
						sb.append("]");

						log.debug("Consumer assignment changed.(new assignment={})", sb.toString());
					}
				}
			} catch (Exception e) {
				log.warn("Error occurred while doing assignment check in ConsumerRebalanceChecker", e);
			}
		}

		private Assignment<Integer> createNewAssignment(Pair<String, String> topicGroup,
		      Map<String, ClientContext> consumers, Assignment<Integer> originAssignment) {
			Topic topic = m_metaHolder.getMeta().findTopic(topicGroup.getKey());
			if (topic != null) {
				List<Partition> partitions = topic.getPartitions();
				if (partitions == null || partitions.isEmpty()) {
					return null;
				}

				ConsumerGroup consumerGroup = topic.findConsumerGroup(topicGroup.getValue());
				if (consumerGroup == null) {
					return null;
				}

				Map<Integer, Map<String, ClientContext>> newAssignment = null;
				if (consumerGroup.isOrderedConsume()) {
					newAssignment = m_partitionAssigningStrategy.assign(partitions, consumers,
					      originAssignment == null ? null : originAssignment.getAssigment());
				} else {
					newAssignment = nonOrderedConsumeAssign(partitions, consumers);
				}

				if (newAssignment == null) {
					return null;
				}

				Assignment<Integer> assignment = new Assignment<Integer>();

				for (Map.Entry<Integer, Map<String, ClientContext>> entry : newAssignment.entrySet()) {
					assignment.addAssignment(entry.getKey(), entry.getValue());
				}

				return assignment;
			} else {
				return null;
			}
		}

		private Map<Integer, Map<String, ClientContext>> nonOrderedConsumeAssign(List<Partition> partitions,
		      Map<String, ClientContext> consumers) {
			Map<Integer, Map<String, ClientContext>> result = new HashMap<>();

			for (Partition partition : partitions) {
				result.put(partition.getId(), consumers);
			}

			return result;
		}

	}
}
