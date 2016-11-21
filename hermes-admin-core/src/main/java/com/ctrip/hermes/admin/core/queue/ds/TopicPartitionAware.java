package com.ctrip.hermes.admin.core.queue.ds;

public interface TopicPartitionAware {

	public String getTopic();

	public int getPartition();

}
