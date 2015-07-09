package com.ctrip.hermes.portal.dal.ds;

public interface TopicPartitionPriorityAware extends TopicPartitionAware {

	public int getPriority();

}
