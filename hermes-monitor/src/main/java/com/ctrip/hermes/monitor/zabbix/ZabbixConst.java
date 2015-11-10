package com.ctrip.hermes.monitor.zabbix;

public class ZabbixConst {

	/**
	 * Group
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

	public static final String DISK_USED = "Used disk space on $1";

	public static final String DISK_READ_OPS = "Disk read ops";

	public static final String DISK_WRITE_OPS = "Disk write ops";

	/**
	 * CPU
	 */
	public static final String CPU_PROCESSOR_LOAD = "Processor load (avg1)";

	public static final String CPU_USER_TIME = "system.cpu.util[,user]";

	public static final String CPU_SYSTEM_TIME = "system.cpu.util[,system]";

	public static final String CPU_IOWAIT_TIME = "system.cpu.util[,iowait]";

	public static final String CPU_RATIO_OF_CPU_LOAD_AND_CPU_NUMBER = "The ratio of cpu load and cpu number";

	/**
	 * Memory
	 */
	public static final String MEMORY_AVAILABLE_PERCENTAGE = "Available memory in %";

	public static final String MEMORY_FREE_SWAP_PERCENTAGE = "Free swap space in %";
	
	public static final String MEMORY_AVAILABLE = "Available memory";
	
	public static final String MEMORY_FREE_SWAP = "Free swap space";
	
	public static final String MEMORY_SWAP_IN = "System swap in";
	
	public static final String MEMORY_SWAP_OUT = "System swap out";

	/**
	 * Network
	 */
	public static final String NETWORK_ESTABLISHED = "Network ESTABLISHED";

	public static final String NETWORK_SYN_RECV = "Network SYN_RECV";

	public static final String NETWORK_TIME_WAIT = "Network TIME_WAIT";

	public static final String NETWORK_INCOMING_TRAFFIC = "Incoming network traffic on";

	public static final String NETWORK_OUTGOING_TRAFFIC = "Outgoing network traffic on";

	/**
	 * Process
	 */
	public static final String PROCESS_NUMBER = "Number of processes";
	
	public static final String PROCESS_RUNNING_NUMBER = "Number of running processes";
	
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
	 * ZK
	 */
	public static final String ZK_ZNODE_COUNT = "zk_znode_count";

	public static final String ZK_WATCH_COUNT = "zk_watch_count";

	public static final String ZK_OUTSTANDING_REQUESTS = "zk_outstanding_requests";

	public static final String ZK_OPEN_FD_COUNT = "zk_open_file_descriptor_count";

	public static final String ZK_NUM_ALIVE_CONNECTIONS = "zk_num_alive_connections";

	public static final String ZK_MAX_LATENCY = "zk_max_latency";

	public static final String ZK_EPHEMERALS_COUNT = "zk_ephemerals_count";

	public static final String ZK_AVG_LATENCY = "zk_avg_latency";

	/**
	 * Category
	 */
	public static final String CATEGORY_KAFKA = "Kafka";

	public static final String CATEGORY_DISK = "Disk";

	public static final String CATEGORY_CPU = "Cpu";

	public static final String CATEGORY_ZK = "Zookeeper";

	public static final String CATEGORY_MEMORY = "Memory";

	public static final String CATEGORY_NETWORK = "Network";

	public static final String CATEGORY_PROCESS = "Process";
	
	/**
	 * Source
	 */
	public static final String SOURCE_ZABBIX = "Zabbix";

	public static final String SOURCE_CAT = "Cat";
}
