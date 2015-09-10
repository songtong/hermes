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

	private static final String META_SERVER_ASSIGNMENT_PATH_ROOT = "/metaserver-assignment";

	private static final String META_SERVER_ASSIGNMENT_PATH_PATTERN = META_SERVER_ASSIGNMENT_PATH_ROOT + "/%s";

	private static final String BROKER_ASSIGNMENT_PATH_ROOT = "/broker-assignment";

	private static final String BROKER_ASSIGNMENT_PATH_PREFIX_PATTERN = BROKER_ASSIGNMENT_PATH_ROOT + "/%s";

	private static final String BROKER_ASSIGNMENT_PATH_PATTERN = BROKER_ASSIGNMENT_PATH_PREFIX_PATTERN + "/%s";

	private static final String CONSUMER_LEASE_PATH_ROOT = "/consumer-lease";

	private static final String CONSUMER_LEASE_PATH_PREFIX_PATTERN = CONSUMER_LEASE_PATH_ROOT + "/%s";

	private static final String CONSUMER_LEASE_PATH_PATTERN = CONSUMER_LEASE_PATH_PREFIX_PATTERN + "/%s/%s";

	private static final String BROKER_LEASE_PATH_ROOT = "/broker-lease";

	private static final String BROKER_LEASE_PATH_PREFIX_PATTERN = BROKER_LEASE_PATH_ROOT + "/%s";

	private static final String BROKER_LEASE_PATH_PATTERN = BROKER_LEASE_PATH_PREFIX_PATTERN + "/%s";

	public static String getCmessageExchangePath() {
		return "/cmessage-exchange";
	}

	public static String getBaseMetaVersionZkPath() {
		return "/base-meta-version";
	}

	public static String getMetaInfoZkPath() {
		return "/meta-info";
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

	public static String getMetaServersZkPath() {
		return "/meta-servers";
	}

	public static String getBrokerLeasesZkPath() {
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
		String[] pathSegments = path.split(PATH_SEPARATOR);

		int len = pathSegments == null ? 0 : pathSegments.length;

		if (len > 2) {
			return new Pair<>(pathSegments[len - 2], Integer.valueOf(pathSegments[len - 1]));
		} else {
			return null;
		}
	}

	public static Tpg parseConsumerLeaseZkPath(String path) {
		String[] pathSegments = path.split(PATH_SEPARATOR);

		int len = pathSegments == null ? 0 : pathSegments.length;

		if (len > 3) {
			return new Tpg(pathSegments[len - 3], Integer.valueOf(pathSegments[len - 2]), pathSegments[len - 1]);
		} else {
			return null;
		}

	}

	public static String getConsumerLeaseRootZkPath() {
		return CONSUMER_LEASE_PATH_ROOT;
	}

	public static String getBrokerAssignmentZkPath(String topic, int partition) {
		return String.format(BROKER_ASSIGNMENT_PATH_PATTERN, topic, partition);
	}

	public static String getBrokerAssignmentRootZkPath() {
		return BROKER_ASSIGNMENT_PATH_ROOT;
	}

	public static String getBrokerAssignmentTopicParentZkPath(String topic) {
		return String.format(BROKER_ASSIGNMENT_PATH_PREFIX_PATTERN, topic);
	}

	public static String getMetaServerAssignmentRootZkPath() {
		return META_SERVER_ASSIGNMENT_PATH_ROOT;
	}

	public static String getMetaServerAssignmentZkPath(String topic) {
		return String.format(META_SERVER_ASSIGNMENT_PATH_PATTERN, topic);
	}

}
