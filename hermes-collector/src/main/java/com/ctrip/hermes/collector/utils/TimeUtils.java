package com.ctrip.hermes.collector.utils;

import java.util.concurrent.TimeUnit;

public class TimeUtils {
	public static long correctTime(long interval, TimeUnit timeUnit) {
		long milliseconds = System.currentTimeMillis();
		milliseconds -= milliseconds % timeUnit.toMillis(interval);
		return milliseconds;
	}

	public static long chineseCorrectTime(long interval, TimeUnit timeUnit) {
		long milliseconds = correctTime(interval, timeUnit);
		milliseconds = milliseconds - TimeUnit.HOURS.toMillis(8);
		return milliseconds;
	}

	public static long prevTime(long base, long interval, TimeUnit unit) {
		return base - unit.toMillis(interval);
	}

	public static long nextTime(long base, long interval, TimeUnit unit) {
		return base + unit.toMillis(interval);
	}
}
