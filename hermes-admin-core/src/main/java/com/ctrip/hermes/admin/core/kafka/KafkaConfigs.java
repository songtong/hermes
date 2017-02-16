package com.ctrip.hermes.admin.core.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class KafkaConfigs {
	public static final Pattern IPV4_PATTERN = Pattern
	      .compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

	public static List<String> VALID_KAFKA_CONFIG_KEYS = Arrays.asList("segment.index.bytes", "segment.jitter.ms",
	      "min.cleanable.dirty.ratio", "retention.bytes", "file.delete.delay.ms", "flush.ms", "cleanup.policy",
	      "unclean.leader.election.enable", "flush.messages", "retention.ms", "min.insync.replicas",
	      "delete.retention.ms", "index.interval.bytes", "segment.bytes", "segment.ms");

	public static final int DEFAULT_KAFKA_PARTITIONS = 3;

	public static final int DEFAULT_KAFKA_REPLICATION_FACTOR = 2;
	
	public static final String DEFAULT_KAFKA_SEGMENT_MS = "3600000";

	public static final String APOLLO_KAFKA_DEPLOY_PROPERTY_PREFIX = "portal.kafka.deploy";
}
