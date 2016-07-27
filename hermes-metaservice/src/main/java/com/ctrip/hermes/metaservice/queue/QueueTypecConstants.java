package com.ctrip.hermes.metaservice.queue;

import java.util.ArrayList;
import java.util.List;

public class QueueTypecConstants {
	public static final String PRIORITY_TRUE = "priority";

	public static final String PRIORITY_FALSE = "nonPriority";

	public static final String RESEND = "resend";

	public static List<String> getQueueTypes() {
		List<String> queueTypes = new ArrayList<>();
		queueTypes.add(PRIORITY_TRUE);
		queueTypes.add(PRIORITY_FALSE);
		queueTypes.add(RESEND);
		return queueTypes;
	}

	public static boolean isValidQueueType(String queueType) {
		for (String qt : getQueueTypes()) {
			if (queueType.equals(qt)) {
				return true;
			}
		}
		return false;
	}
}
