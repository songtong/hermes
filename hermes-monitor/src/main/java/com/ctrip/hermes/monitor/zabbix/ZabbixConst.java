package com.ctrip.hermes.monitor.zabbix;


public class ZabbixConst {

	/**
	 * Zabbix Group Name
	 */
	public static final String GROUP_NAME_KAFKA = "REALTIME-KAFKA";
	
	public static final String GROUP_NAME_HERMES_BROKER="hermesbroker";
	
	public static final String GROUP_NAME_HERMES_METASERVER="hermesmeteserver";
	
	public static final String GROUP_NAME_HERMES_PORTAL="hermesportal";
	
	public static final String GROUP_NAME_ZOOKEEPER="realtime-kafka-zk";
	
	/**
	 * Disk 
	 */
	public static final String DISK_FREE_PERCENTAGE = "Free disk space on $1 (percentage)";

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

}
