package com.ctrip.hermes.core.utils;

import java.util.List;

import org.unidal.tuple.Pair;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;

public class CatUtil {

	public static void logElapse(String type, String name, long startTimestamp, int count,
	      List<Pair<String, String>> datas, String status) {
		if (count > 0) {
			Transaction latencyT = Cat.newTransaction(type, name);
			long delta = System.currentTimeMillis() - startTimestamp;

			if (latencyT instanceof DefaultTransaction) {
				((DefaultTransaction) latencyT).setDurationStart(System.nanoTime() - delta * 1000000L);
			}
			latencyT.addData("*count", count);
			if (datas != null && !datas.isEmpty()) {
				for (Pair<String, String> data : datas) {
					latencyT.addData(data.getKey(), data.getValue());
				}
			}
			latencyT.setStatus(status);
			latencyT.complete();
		}
	}

}
