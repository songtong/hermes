package com.ctrip.hermes.metaserver.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.metaserver.commons.ClientContext;

/**
 * @author Marsqing(q_gu@ctrip.com)
 *
 */
@Named(type = OrderedConsumeConsumerPartitionAssigningStrategy.class)
public class LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategy implements
      OrderedConsumeConsumerPartitionAssigningStrategy {

	private final static Logger log = LoggerFactory
	      .getLogger(LeastAdjustmentOrderedConsumeConsumerPartitionAssigningStrategy.class);

	@Override
	public Map<Integer, Map<String, ClientContext>> assign(List<Partition> partitions,
	      Map<String, ClientContext> currentConsumers, Map<Integer, Map<String, ClientContext>> originAssigns) {
		Map<Integer, Map<String, ClientContext>> result = new HashMap<>();

		if (partitions == null || partitions.isEmpty() || currentConsumers == null || currentConsumers.isEmpty()) {
			return result;
		}

		if (originAssigns == null) {
			originAssigns = Collections.emptyMap();
		}

		Map<String, List<Integer>> originConsumerToPartition = mapConsumerToPartitions(originAssigns);
		Map<String, ClientContext> consumerToClientContext = mapConsumerToClientContext(originAssigns, currentConsumers);
		Set<String> originConsumers = originConsumerToPartition.keySet();

		Set<String> deleted = setMinus(originConsumers, currentConsumers.keySet());
		Set<String> added = setMinus(currentConsumers.keySet(), originConsumers);
		Set<String> common = setIntersect(originConsumers, currentConsumers.keySet());

		List<Integer> neverAssignedPartitions = findNeverAssignedPartitions(partitions, originAssigns.keySet());
		List<Integer> assignLostPartitions = findPartitions(deleted, originConsumerToPartition);

		LinkedList<Integer> freePartitions = new LinkedList<>();
		freePartitions.addAll(neverAssignedPartitions);
		freePartitions.addAll(assignLostPartitions);

		Allocator allocator = new Allocator(partitions.size(), Math.min(currentConsumers.size(), partitions.size()),
		      freePartitions);

		for (String commonConsumer : common) {
			List<Integer> originAssign = originConsumerToPartition.get(commonConsumer);
			List<Integer> newAssign = allocator.adjust(originAssign);
			putAssignToResult(result, consumerToClientContext, commonConsumer, newAssign);
		}

		for (String addedConsumer : added) {
			List<Integer> newAssign = allocator.adjust(Collections.<Integer> emptyList());
			putAssignToResult(result, consumerToClientContext, addedConsumer, newAssign);
		}

		return result;
	}

	private List<Integer> findNeverAssignedPartitions(List<Partition> partitions, Set<Integer> originAssign) {
		List<Integer> result = new ArrayList<>();

		for (Partition p : partitions) {
			Integer partitionId = p.getId();
			if (!originAssign.contains(partitionId)) {
				result.add(partitionId);
			}
		}

		return result;
	}

	private void putAssignToResult(Map<Integer, Map<String, ClientContext>> result,
	      Map<String, ClientContext> consumerToClientContext, String commonConsumer, List<Integer> newAssign) {
		for (Integer partition : newAssign) {
			Map<String, ClientContext> consumerMap = new HashMap<>();
			consumerMap.put(commonConsumer, consumerToClientContext.get(commonConsumer));

			result.put(partition, consumerMap);
		}
	}

	private Map<String, ClientContext> mapConsumerToClientContext(
	      Map<Integer, Map<String, ClientContext>> originAssigns, Map<String, ClientContext> currentConsumers) {
		Map<String, ClientContext> result = new HashMap<>();

		for (Map<String, ClientContext> entry : originAssigns.values()) {
			result.putAll(entry);
		}

		result.putAll(currentConsumers);

		return result;
	}

	private List<Integer> findPartitions(Set<String> consumers, Map<String, List<Integer>> consumerToPartition) {
		List<Integer> result = new ArrayList<>();

		for (String consumer : consumers) {
			List<Integer> partitions = consumerToPartition.get(consumer);
			if (partitions != null) {
				result.addAll(partitions);
			}
		}

		return result;
	}

	private Set<String> setIntersect(Set<String> left, Set<String> right) {
		HashSet<String> result = new HashSet<>(left);
		result.retainAll(right);

		return result;
	}

	private Set<String> setMinus(Set<String> left, Set<String> right) {
		HashSet<String> result = new HashSet<>(left);
		result.removeAll(right);

		return result;
	}

	private Map<String, List<Integer>> mapConsumerToPartitions(Map<Integer, Map<String, ClientContext>> originAssignment) {
		Map<String, List<Integer>> result = new HashMap<>();

		for (Entry<Integer, Map<String, ClientContext>> entry : originAssignment.entrySet()) {
			if (!entry.getValue().isEmpty()) {
				if (entry.getValue().size() != 1) {
					log.warn("Ordered partition have more than one consumer assigned");
				}

				String consumer = entry.getValue().keySet().iterator().next();
				int partition = entry.getKey();

				List<Integer> partitions = result.get(consumer);
				if (partitions == null) {
					partitions = new ArrayList<>();
					result.put(consumer, partitions);
				}
				partitions.add(partition);
			}
		}

		return result;
	}

	public static class Allocator {
		private int m_total;

		private int m_copies;

		private int m_copiesAllocated = 0;

		private LinkedList<Integer> m_freePartitions = new LinkedList<>();

		private int m_avg;

		private int m_biggerThanAvgsAllocated = 0;

		private int m_remainder;

		public Allocator(int total, int copies, List<Integer> freePartitions) {
			m_total = total;
			m_copies = copies;

			m_avg = m_total / m_copies;
			m_remainder = m_total % m_copies;

			if (freePartitions != null) {
				m_freePartitions.addAll(freePartitions);
			}
		}

		public List<Integer> adjust(List<Integer> originAssign) {
			m_copiesAllocated++;
			LinkedList<Integer> newAssign = new LinkedList<>(originAssign);

			if (lastCopyOrBeyond()) {
				newAssign.addAll(m_freePartitions);
			} else {
				int partitionsToAdjust = distanceToAvg(originAssign.size());
				if (partitionsToAdjust > 0) {
					for (int i = 0; i < partitionsToAdjust; i++) {
						newAssign.add(m_freePartitions.removeFirst());
					}
				} else if (partitionsToAdjust < 0) {
					for (int i = 0; i < Math.abs(partitionsToAdjust); i++) {
						m_freePartitions.add(newAssign.removeFirst());
					}
				}
			}

			if (m_remainder != 0 && newAssign.size() == m_avg + 1) {
				m_biggerThanAvgsAllocated++;
			}

			return newAssign;
		}

		protected int distanceToAvg(int point) {
			if (m_remainder == 0) {
				return m_avg - point;
			} else {
				if (biggerThanAvgAllowed()) {
					return point > m_avg ? m_avg + 1 - point : m_avg - point;
				} else {
					return m_avg - point;
				}
			}
		}

		private boolean biggerThanAvgAllowed() {
			return m_biggerThanAvgsAllocated < m_remainder;
		}

		private boolean lastCopyOrBeyond() {
			return m_copiesAllocated >= m_copies;
		}
	}
}
