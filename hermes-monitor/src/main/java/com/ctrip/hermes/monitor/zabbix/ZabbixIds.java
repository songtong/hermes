package com.ctrip.hermes.monitor.zabbix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZabbixIds {
	public static final List<Integer> Kafka_Broker_Hostids = Arrays.asList(24774, 24775, 24776, 24777, 24778, 26378,
	      26379, 26380, 26381, 26382, 28611, 28612, 28613, 28614, 28615, 28616);

	public static final List<Integer> Disk_Free_Percentage_Itemids = new ArrayList<Integer>();
	static {
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(5381453, 5843630, 5843631, 5843632, 5843633, 5843634, 5843635));// Host
		                                                                                                                  // 24774
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(5381461, 5843654, 5843655, 5843656, 5843657, 5843658, 5843659));// Host
		                                                                                                                  // 24775
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(5381469, 5843678, 5843679, 5843680, 5843681, 5843682, 5843683));// Host
		                                                                                                                  // 24776
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(5381477, 5843706, 5843707, 5843708, 5843709, 5843710, 5843711));// Host
		                                                                                                                  // 24777
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(5381485, 5843734, 5843735, 5843736, 5843737, 5843738, 5843739));// Host
		                                                                                                                  // 24778
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6301350, 6326800, 6326801, 6326802, 6326803, 6326804, 6326805));// Host
		                                                                                                                  // 26379
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6301370, 6322144, 6322145, 6322146, 6322147, 6322148, 6322149));// Host
		                                                                                                                  // 26380
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6301398, 6322616, 6322617, 6322618, 6322619, 6322620, 6322621));// Host
		                                                                                                                  // 26381
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6301422, 6322804, 6322805, 6322806, 6322807, 6322808, 6322809));// Host
		                                                                                                                  // 26382
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6301442, 6322828, 6322829, 6322830, 6322831, 6322832, 6322833));// Host
		                                                                                                                  // 26378
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6911526, 6926293, 6926294, 6926295, 6926296, 6926297, 6926298));// Host
		                                                                                                                  // 28611
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6911546, 6926953, 6926954, 6926955, 6926956, 6926957, 6926958));// Host
		                                                                                                                  // 28612
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6927599, 6927601, 6927602, 6927603, 6927604, 6927605, 6927606));// Host
		                                                                                                                  // 28613
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6911131, 6927005, 6927006, 6927007, 6927008, 6927009, 6927010));// Host
		                                                                                                                  // 28614
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6911155, 6927065, 6927066, 6927067, 6927068, 6927069, 6927070));// Host
		                                                                                                                  // 28615
		Disk_Free_Percentage_Itemids.addAll(Arrays.asList(6911211, 6927089, 6927090, 6927091, 6927092, 6927093, 6927094));// Host
		                                                                                                                  // 28616
	}

	public static final List<Integer> Kafka_Message_In_Rate_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Message_In_Rate_Itemids.addAll(Arrays.asList(6092463, 6092473, 6092487, 6092497, 6092507, 6343192, 6343220,
		      6343248, 6343284, 6343312, 6928198, 6928230, 6928258, 6928294, 6928326, 6928354));
	}

	public static final List<Integer> Kafka_Byte_In_Rate_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Byte_In_Rate_Itemids.addAll(Arrays.asList(6092459, 6092469, 6092483, 6092493, 6092503, 6343188, 6343216,
		      6343244, 6343280, 6343308, 6928194, 6928226, 6928254, 6928290, 6928322, 6928350));
	}

	public static final List<Integer> Kafka_Byte_Out_Rate_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Byte_Out_Rate_Itemids.addAll(Arrays.asList(6092460, 6092470, 6092484, 6092494, 6092504, 6343189, 6343217,
		      6343245, 6343281, 6343309, 6928195, 6928227, 6928255, 6928291, 6928323, 6928351));
	}

	public static final List<Integer> Kafka_Failed_Produce_Requests_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Failed_Produce_Requests_Itemids.addAll(Arrays.asList(6092462, 6092472, 6092486, 6092496, 6092506, 6343191,
		      6343219, 6343247, 6343283, 6343311, 6928197, 6928229, 6928257, 6928293, 6928325, 6928353));
	}

	public static final List<Integer> Kafka_Failed_Fetch_Requests_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Failed_Fetch_Requests_Itemids.addAll(Arrays.asList(6092461, 6092471, 6092485, 6092495, 6092505, 6343190,
		      6343218, 6343246, 6343282, 6343310, 6928196, 6928228, 6928256, 6928292, 6928324, 6928352));
	}

	public static final List<Integer> Kafka_Request_Queue_Size_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Request_Queue_Size_Itemids.addAll(Arrays.asList(6191533, 6191534, 6191535, 6191536, 6191537, 6343179,
		      6343207, 6343235, 6343271, 6343299, 6928185, 6928217, 6928245, 6928281, 6928313, 6928341));
	}

	public static final List<Integer> Kafka_Request_Rate_Produce_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Request_Rate_Produce_Itemids.addAll(Arrays.asList(6188588, 6188589, 6188590, 6188591, 6188592, 6343183,
		      6343211, 6343239, 6343275, 6343303, 6928189, 6928221, 6928249, 6928285, 6928317, 6928345));
	}

	public static final List<Integer> Kafka_Request_Rate_FetchFollower_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Request_Rate_FetchFollower_Itemids.addAll(Arrays.asList(6188600, 6188601, 6188602, 6188603, 6188604, 6343182,
		      6343210, 6343238, 6343274, 6343302, 6928188, 6928220, 6928248, 6928284, 6928316, 6928344));
	}

	public static final List<Integer> Kafka_Request_Rate_FetchConsumer_Itemids = new ArrayList<Integer>();
	static {
		Kafka_Request_Rate_FetchConsumer_Itemids.addAll(Arrays.asList(6188594, 6188595, 6188596, 6188597, 6188598, 6343181,
		      6343209, 6343237, 6343273, 6343301, 6928187, 6928219, 6928247, 6928283, 6928315, 6928343));
	}
}
