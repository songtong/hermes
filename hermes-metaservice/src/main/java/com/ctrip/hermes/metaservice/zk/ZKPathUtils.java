package com.ctrip.hermes.metaservice.zk;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ZKPathUtils {

	private static final String PATH_SEPARATOR = "/";

	private static final String CONSUMER_LEASE_PATH_ROOT = "/consumer-lease";

	private static final String CONSUMER_LEASE_PATH_PREFIX_PATTERN = CONSUMER_LEASE_PATH_ROOT + "/%s";

	private static final String CONSUMER_LEASE_PATH_PATTERN = CONSUMER_LEASE_PATH_PREFIX_PATTERN + "/%s/%s";

	private static final String BROKER_LEASE_PATH_ROOT = "/broker-lease";

	private static final String BROKER_LEASE_PATH_PREFIX_PATTERN = BROKER_LEASE_PATH_ROOT + "/%s";

	private static final String BROKER_LEASE_PATH_PATTERN = BROKER_LEASE_PATH_PREFIX_PATTERN + "/%s";

	public static String getMetaVersionPath() {
		return "/meta-version";
	}

	public static List<String> getBrokerLeaseZkPaths(Topic topic) {
		List<String> paths = new LinkedList<>();
		if (Endpoint.BROKER.equals(topic.getEndpointType())) {
			String topicName = topic.getName();
			List<Integer> partitionIds = collectPartitionIds(topic);

			for (Integer partitionId : partitionIds) {
				paths.add(getBrokerLeaseZkPath(topicName, partitionId));
			}
		}

		return paths;
	}

	public static String getBrokerLeaseTopicParentZkPath(String topicName) {
		return String.format(BROKER_LEASE_PATH_PREFIX_PATTERN, topicName);
	}

	public static String getBrokerLeaseZkPath(String topicName, int partition) {
		return String.format(BROKER_LEASE_PATH_PATTERN, topicName, partition);
	}

	public static List<String> getConsumerLeaseZkPaths(Topic topic) {
		List<String> paths = new LinkedList<>();
		if (Endpoint.BROKER.equals(topic.getEndpointType())) {
			String topicName = topic.getName();
			List<Integer> partitionIds = collectPartitionIds(topic);
			List<String> consumerGroupNames = collectConsumerGroupNames(topic);

			for (Integer partitionId : partitionIds) {
				for (String consumerGroupName : consumerGroupNames) {
					paths.add(String.format(CONSUMER_LEASE_PATH_PATTERN, topicName, partitionId, consumerGroupName));
				}
			}
		}

		return paths;
	}

	public static String getConsumerLeaseTopicParentZkPath(String topicName) {
		return String.format(CONSUMER_LEASE_PATH_PREFIX_PATTERN, topicName);
	}

	public static String getConsumerLeaseZkPath(String topicName, int partition, String groupName) {
		return String.format(CONSUMER_LEASE_PATH_PATTERN, topicName, partition, groupName);
	}

	public static List<String> getConsumerLeaseZkPaths(Topic topic, String consumerGroupName) {
		List<String> paths = new LinkedList<>();
		if (Endpoint.BROKER.equals(topic.getEndpointType())) {
			String topicName = topic.getName();
			List<Integer> partitionIds = collectPartitionIds(topic);

			for (Integer partitionId : partitionIds) {
				paths.add(getConsumerLeaseZkPath(topicName, partitionId, consumerGroupName));
			}
		}

		return paths;
	}

	private static List<String> collectConsumerGroupNames(Topic topic) {
		List<String> groupNames = new ArrayList<>();
		CollectionUtil.collect(topic.getConsumerGroups(), new Transformer() {

			@Override
			public Object transform(Object input) {
				return ((ConsumerGroup) input).getName();
			}
		}, groupNames);

		return groupNames;
	}

	private static List<Integer> collectPartitionIds(Topic topic) {
		List<Integer> partitionIds = new ArrayList<>();
		CollectionUtil.collect(topic.getPartitions(), new Transformer() {

			@Override
			public Object transform(Object input) {
				return ((Partition) input).getId();
			}
		}, partitionIds);

		return partitionIds;
	}

	public static String getMetaServersPath() {
		return "/meta-servers";
	}

	public static String getBrokerLeasesPath() {
		return "/broker-lease";
	}

	public static String lastSegment(String path) {
		int lastSlashIdx = path.lastIndexOf(PATH_SEPARATOR);

		if (lastSlashIdx >= 0) {
			return path.substring(lastSlashIdx + 1);
		} else {
			return path;
		}
	}

	public static String getBrokerLeaseRootZkPath() {
		return BROKER_LEASE_PATH_ROOT;
	}

	public static Pair<String, Integer> parseBrokerLeaseZkPath(String path) {
		int partitionSeparatorStart = path.lastIndexOf(PATH_SEPARATOR);
		int partition = Integer.valueOf(path.substring(partitionSeparatorStart + 1));
		String newPath = path.substring(0, partitionSeparatorStart);
		int topicSeparatorStart = newPath.lastIndexOf(PATH_SEPARATOR);
		String topic = newPath.substring(topicSeparatorStart + 1);

		return new Pair<>(topic, partition);
	}

	public static Tpg parseConsumerLeaseZkPath(String path) {
		int groupSeparatorStart = path.lastIndexOf(PATH_SEPARATOR);
		String group = path.substring(groupSeparatorStart + 1);

		String newPath = path.substring(0, groupSeparatorStart);
		int partitionSeparatorStart = newPath.lastIndexOf(PATH_SEPARATOR);
		int partition = Integer.valueOf(newPath.substring(partitionSeparatorStart + 1));

		newPath = newPath.substring(0, partitionSeparatorStart);
		int topicSeparatorStart = newPath.lastIndexOf(PATH_SEPARATOR);
		String topic = newPath.substring(topicSeparatorStart + 1);

		return new Tpg(topic, partition, group);
	}

	public static String getConsumerLeaseRootZkPath() {
		return CONSUMER_LEASE_PATH_ROOT;
	}
}
