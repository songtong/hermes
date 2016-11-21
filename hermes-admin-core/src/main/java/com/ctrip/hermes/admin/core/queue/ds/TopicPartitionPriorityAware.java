package com.ctrip.hermes.admin.core.queue.ds;

public interface TopicPartitionPriorityAware extends TopicPartitionAware {

	public int getPriority();

}
