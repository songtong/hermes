package com.ctrip.hermes.admin.core.queue;

public enum QueueType {
	PRIORITY("priority"), NON_PRIORITY("nonPriority"), RESEND("resend");

	private String name;

	private QueueType(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public static boolean isValidQueueType(String queueType) {
		for (QueueType qt : QueueType.values()) {
			if (queueType.equals(qt.getName())) {
				return true;
			}
		}
		return false;
	}

	public static QueueType getQueueTypeByName(String name) {
		for (QueueType qt : QueueType.values()) {
			if (qt.getName().equals(name)) {
				return qt;
			}
		}

		return null;
	}
}
