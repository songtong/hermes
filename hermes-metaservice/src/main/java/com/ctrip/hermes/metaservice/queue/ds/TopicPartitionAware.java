package com.ctrip.hermes.metaservice.queue.ds;

public interface TopicPartitionAware {

	public String getTopic();

	public int getPartition();

}
