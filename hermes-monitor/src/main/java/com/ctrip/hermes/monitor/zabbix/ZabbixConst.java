package com.ctrip.hermes.monitor.zabbix;

public class ZabbixConst {

	/**
	 * Zabbix Group Name
	 */
	public static final String GROUP_KAFKA_BROKER = "KafkaBroker";

	public static final String GROUP_MYSQL_BROKER = "MysqlBroker";

	public static final String GROUP_METASERVER = "MetaServer";

	public static final String GROUP_PORTAL = "Portal";

	public static final String GROUP_ZOOKEEPER = "Zookeeper";

	/**
	 * Disk
	 */
	public static final String DISK_FREE_PERCENTAGE = "Free disk space on $1 (percentage)";

	/**
	 * CPU
	 */
	public static final String CPU_PROCESSOR_LOAD = "Processor load (avg1)";

	public static final String CPU_USER_TIME = "system.cpu.util[,user]";

	public static final String CPU_SYSTEM_TIME = "system.cpu.util[,system]";

	public static final String CPU_IOWAIT_TIME = "system.cpu.util[,iowait]";

	public static final String CPU_RATIO_OF_CPU_LOAD_AND_CPU_NUMBER = "The ratio of cpu load and cpu number";

	/**
	 * Kafka
	 */
	public static final String KAFKA_MESSAGE_IN_RATE = "Message in rate";

	public static final String KAFKA_BYTE_IN_RATE = "Byte in rate";

	public static final String KAFKA_BYTE_OUT_RATE = "Byte out rate";

	public static final String KAFKA_FAILED_PRODUCE_REQUESTS = "FailedProduceRequestsPerSec";

	public static final String KAFKA_FAILED_FETCH_REQUESTS = "FailedFetchRequestsPerSec";

	public static final String KAFKA_REQUEST_QUEUE_SIZE = "RequestQueueSize";

	public static final String KAFKA_REQUEST_RATE_PRODUCE = "Request rate (Produce)";

	public static final String KAFKA_REQUEST_RATE_FETCHCONSUMER = "Request rate (FetchConsumer)";

	public static final String KAFKA_REQUEST_RATE_FETCHFOLLOWER = "Request rate (FetchFollower)";

	/**
	 * Category
	 */
	public static final String CATEGORY_KAFKA = "Kafka";

	public static final String CATEGORY_DISK = "Disk";

	public static final String CATEGORY_CPU = "Cpu";

	/**
	 * Source
	 */
	public static final String SOURCE_ZABBIX = "Zabbix";
}
