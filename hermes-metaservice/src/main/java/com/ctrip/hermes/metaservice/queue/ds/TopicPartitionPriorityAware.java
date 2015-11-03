package com.ctrip.hermes.metaservice.queue.ds;

public interface TopicPartitionPriorityAware extends TopicPartitionAware {

	public int getPriority();

}
