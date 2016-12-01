package com.ctrip.hermes.collector.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.codehaus.jackson.JsonNode;

public class TypeConverter {
	private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static Date toDate(long milliseconds) {
		return new Date(milliseconds);
	}
	
	public static Date toDate(String dateString) {
		try {
			return format.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Date toDate(JsonNode json) {
		if (json.isLong()) {
			return toDate(json.asLong());
		} else {
			return toDate(json.asText());
		}
	}
	
	public static String toString(Date date) {
		return format.format(date);
	}
	
	public static long toLong(Date date) {
		return date.getTime();
	}

}
