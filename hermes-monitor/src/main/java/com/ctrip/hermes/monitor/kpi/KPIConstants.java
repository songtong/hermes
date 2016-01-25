package com.ctrip.hermes.monitor.kpi;

import java.io.File;

public class KPIConstants {
	public static final String TOPIC = "fx.hermes.kpi";

	public static final String CONSUMER = "fx.hermes.kpi.consumer";

	public static final File OFFSET_FILE = new File("/opt/ctrip/data/hermes/KPI.OFFSET");

}
