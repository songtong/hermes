package com.ctrip.hermes.consumer.api;

public interface PartitionMetaListener {
	public void onPartitionCountChanged(String topic);
}
