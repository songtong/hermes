package com.ctrip.hermes.collector.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IndexUtils {
	public static final String BIZ_INDEX_NAME = "hermes-biz-%s";
	public static final String SERVICE_LOG_INDEX_NAME = "hermes-log-%s";
	public static final String QUERY_INDEX_NAME = "%s-%s";
	public static final String AGGREGATED_QUERY_INDEX_NAME = "%s-aggregation-%s";
	public static final String AGGREGATED_TARGET_INDEX_NAME = "%s-aggregation";
	public static final SimpleDateFormat DAILY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM.dd");
	public static final SimpleDateFormat MONTHLY_DATE_FORMAT = new SimpleDateFormat("yyyy.MM");
	
	public static String getBizIndex(long milliseconds) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(milliseconds - TimeUnit.HOURS.toMillis(8));
		return String.format(BIZ_INDEX_NAME, DAILY_DATE_FORMAT.format(calendar.getTime()));
	}
	
	public static String getLogIndex(long milliseconds) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(milliseconds - TimeUnit.HOURS.toMillis(8));
		return String.format(SERVICE_LOG_INDEX_NAME, DAILY_DATE_FORMAT.format(calendar.getTime()));
	}
	
	public static String getDataQueryIndex(String name, long milliseconds, boolean aggregated) {
		long adjustedMilliseconds = milliseconds - TimeUnit.HOURS.toMillis(8);
		if (aggregated) {
			return String.format(AGGREGATED_QUERY_INDEX_NAME, name, MONTHLY_DATE_FORMAT.format(adjustedMilliseconds));
		} else {
			return String.format(QUERY_INDEX_NAME, name, DAILY_DATE_FORMAT.format(adjustedMilliseconds));
		}
	}
	
	public static String[] getDataQueryIndices(String name, long from, long to, boolean aggregated) {
		List<String> indices = new ArrayList<String>();
		while (from <= to) {
			indices.add(getDataQueryIndex(name, from, aggregated));
			from += TimeUnit.DAYS.toMillis(1); 
		}
		
		String index = getDataQueryIndex(name, to, aggregated);
		if (!indices.get(indices.size() - 1).equals(index)) {
			indices.add(index);
		}
		
		String[] array = new String[indices.size()];
		return indices.toArray(array);
	}
	
	public static String getDataStoredIndex(String name, boolean aggregated) {
		if (aggregated) {
			return String.format(AGGREGATED_TARGET_INDEX_NAME, name);
		} else {
			return name;
		}
	}
}
