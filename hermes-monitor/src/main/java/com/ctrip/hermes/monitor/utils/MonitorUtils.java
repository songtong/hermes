package com.ctrip.hermes.monitor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.fluent.Request;

public class MonitorUtils {
	public static <K, V> List<Map<K, V>> splitMap(Map<K, V> map, int batchCount) {
		if (batchCount <= 0) {
			throw new IllegalArgumentException("BatchCount must be a positive number.");
		}

		List<Map<K, V>> list = new ArrayList<Map<K, V>>();
		for (int i = 0; i < batchCount; i++) {
			list.add(new HashMap<K, V>());
		}

		int currentIdx = 0;
		for (Entry<K, V> entry : map.entrySet()) {
			list.get(currentIdx).put(entry.getKey(), entry.getValue());
			currentIdx = ++currentIdx % batchCount;
		}

		Iterator<Map<K, V>> iter = list.iterator();
		while (iter.hasNext()) {
			if (iter.next().size() == 0) {
				iter.remove();
			}
		}
		return list;
	}

	public static interface Matcher<T> {
		public boolean match(T obj);
	}

	public static <T> List<T> findMatched(Collection<T> collection, Matcher<T> matcher) {
		List<T> list = new ArrayList<T>();
		for (T t : collection) {
			if (matcher.match(t)) {
				list.add(t);
			}
		}
		return list;
	}

	public static String curl(String url, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
		try {
			return Request.Get(url)//
			      .connectTimeout(connectTimeoutMillis)//
			      .socketTimeout(readTimeoutMillis)//
			      .execute()//
			      .returnContent()//
			      .asString();
		} catch (IOException e) {
			throw new IOException(String.format("Failed to fetch data from url %s", url), e);
		}
	}

}
