package com.ctrip.hermes.core.utils;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultTransaction;

public class CatUtil {

	public static void logElapse(String type, String name, long startTimestamp) {
		Transaction latencyT = Cat.newTransaction(type, name);
		long delta = System.currentTimeMillis() - startTimestamp;

		if (latencyT instanceof DefaultTransaction) {
			((DefaultTransaction) latencyT).setDurationStart(System.nanoTime() - delta * 1000000L);
		}
		latencyT.setStatus(Transaction.SUCCESS);
		latencyT.complete();
	}

}
