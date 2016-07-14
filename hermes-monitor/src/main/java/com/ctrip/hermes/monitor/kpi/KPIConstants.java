package com.ctrip.hermes.monitor.kpi;

import java.io.File;

public class KPIConstants {
	public static final String TOPIC = "fx.hermes.kpi";

	public static final String CONSUMER = "fx.hermes.kpi.consumer";

	public static final File PRODUCE_OFFSET_FILE = new File("/opt/data/hermes/KPI.OFFSET.PRODUCE");

	public static final File CONSUME_OFFSET_FILE = new File("/opt/data/hermes/KPI.OFFSET.CONSUME");

}
