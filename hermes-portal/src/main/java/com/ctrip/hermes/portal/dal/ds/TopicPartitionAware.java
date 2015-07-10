package com.ctrip.hermes.portal.dal.ds;

public interface TopicPartitionAware {

	public String getTopic();

	public int getPartition();

}
