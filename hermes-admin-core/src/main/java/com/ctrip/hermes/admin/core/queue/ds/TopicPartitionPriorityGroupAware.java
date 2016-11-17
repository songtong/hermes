package com.ctrip.hermes.admin.core.queue.ds;

public interface TopicPartitionPriorityGroupAware extends TopicPartitionPriorityAware {

	public int getGroupId();

}
