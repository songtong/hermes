package com.ctrip.hermes.consumer.api;

public interface OffsetStorage {
	public MessageStreamOffset queryLatestOffset(String topic, int partitionId);
}
