package com.ctrip.hermes.collector.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimeUtils {
    public static final String CAT_DATE_PATTERN = "yyyyMMddkk";
    public static final String ES_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String COMMON_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
	public static final ThreadLocal<SimpleDateFormat> CAT_DATE_FORMAT = new ThreadLocal<>();
	public static final ThreadLocal<SimpleDateFormat> ES_DATE_FORMAT = new ThreadLocal<>();
	public static final ThreadLocal<SimpleDateFormat> COMMON_DATE_FORMAT = new ThreadLocal<>();
	
	public static long correctTime(long milliseconds, TimeUnit timeUnit) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(milliseconds);
		switch(timeUnit) {
		case DAYS: 
			if (timeUnit != TimeUnit.DAYS) {
				c.set(Calendar.DAY_OF_MONTH, 1);
			}
		case HOURS: 
			if (timeUnit != TimeUnit.HOURS) {
				c.set(Calendar.HOUR_OF_DAY, 0);
			}
		case MINUTES: 
			if (timeUnit != TimeUnit.MINUTES) {
				c.set(Calendar.MINUTE, 0);
			}
		case SECONDS: 			
			if (timeUnit != TimeUnit.SECONDS) {
				c.set(Calendar.SECOND, 0);
			}
		case MILLISECONDS: 
			if (timeUnit != TimeUnit.MILLISECONDS) {
				c.set(Calendar.MILLISECOND, 0);
			}
		default: 
		}
		return c.getTimeInMillis();
	}
	
	public static long correctTime(Date date, TimeUnit timeUnit) {
		return correctTime(date.getTime(), timeUnit);
	}

	public static long before(long base, long interval, TimeUnit unit) {
		return base - unit.toMillis(interval);
	}

	public static long after(long base, long interval, TimeUnit unit) {
		return base + unit.toMillis(interval);
	}
	
	public static String formatDate(Date date) {
		if (date == null) {
			return null;
		}
	    return formatDate(date.getTime());
	}
	
	public static String formatDate(long timestamp) {
		SimpleDateFormat format = COMMON_DATE_FORMAT.get();
	    if (format == null) {
	        COMMON_DATE_FORMAT.set(format = new SimpleDateFormat(COMMON_DATE_PATTERN));
	    }
		return format.format(timestamp);
	}
	
	public static String formatCatTimestamp(long milliseconds) {
	    SimpleDateFormat format = CAT_DATE_FORMAT.get();
        if (format == null) {
            CAT_DATE_FORMAT.set(format = new SimpleDateFormat(CAT_DATE_PATTERN));
        }
        return format.format(milliseconds);
	}
	
	public static String formatCatTimestamp(Date date) {
	    if (date == null) {
	        return null;
	    }
	    return formatCatTimestamp(date.getTime());
	}
	
	public static long parseTimeFromString(String date) throws ParseException {
	    SimpleDateFormat format = ES_DATE_FORMAT.get();
        if (format == null) {
            ES_DATE_FORMAT.set(format = new SimpleDateFormat(ES_DATE_PATTERN));
        }
        return format.parse(date).getTime();
	}
	
	public static Date longToDate(long milliseconds) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(milliseconds);
		return calendar.getTime();
	}
	
	public static String toHumanReadableTimeInterval(Date date1, Date date2) {
		int seconds = (int)((date1.getTime() - date2.getTime()) / 1000);
		int minutes = seconds / 60;
		seconds = seconds % 60;
		int hours = minutes / 60;
		minutes = minutes % 60;
		String timeInterval = "";
		if (hours > 0) {
			timeInterval += hours + "小时";
		}
		
		if (minutes > 0 || timeInterval.length() > 0) {
			timeInterval += minutes + "分";
		}
		
		if (seconds > 0 || timeInterval.length() > 0) {
			timeInterval += seconds + "秒";
		}
		return timeInterval;
	}
}
