package com.ctrip.hermes.collector.utils;

import java.util.Date;

public class EsperUtils {
	public static Date maxDate(Date d1, Date d2) {
		if (d1 == null) {
			return d2;
		}
		
		if (d2 == null) {
			return d1;
		}
		
		return d1.compareTo(d2) > 0? d1: d2;
	}
}
