package com.ctrip.hermes.monitor.zabbix;

public class ZabbixConst {

	/**
	 * Zabbix Group Name
	 */
	public static final String GROUP_KAFKA_BROKER = "REALTIME-KAFKA";

	public static final String GROUP_MYSQL_BROKER = "hermesbroker";

	public static final String GROUP_METASERVER = "hermesmeteserver";

	public static final String GROUP_PORTAL = "hermesportal";

	public static final String GROUP_ZOOKEEPER = "realtime-kafka-zk";

	/**
	 * Host
	 */
	public static final String[] HOST_KAFKA_BROKER = { "SVR6852HW1288", "SVR6853HW1288", "SVR6854HW1288",
	      "SVR6855HW1288", "SVR6856HW1288", "SVR3360HW1288", "SVR3361HW1288", "SVR3362HW1288", "SVR3363HW1288",
	      "SVR3364HW1288", "SVR2310HP360", "SVR4260HP360", "SVR4261HP360", "SVR4262HP360", "SVR4299HP360",
	      "SVR4300HP360" };

	public static final String[] HOST_MYSQL_BROKER = { "SVR3764HW1288", "SVR3765HW1288", "SVR3767HW1288", "VMS12602",
	      "VMS12603", "VMS12604", "VMS12605" };

	public static final String[] HOST_METASERVER = { "SVR3684HW1288", "SVR3685HW1288" };

	public static final String[] HOST_PORTAL = { "VMS12600", "VMS12601" };

	public static final String[] HOST_ZOOKEEPER = { "VMS09583", "VMS09584", "VMS09585", "VMS09586", "VMS09587" };

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
