package com.ctrip.hermes.metaservice.queue.ds;

public interface TopicPartitionPriorityGroupAware extends TopicPartitionPriorityAware {

	public int getGroupId();

}
