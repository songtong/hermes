package com.ctrip.hermes.metaservice.queue;

import java.util.List;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

public interface TableContext {
	public Datasource getDatasource();

	public TableType getType();

	public Topic getTopic();

	public Partition getPartition();

	public String getTableName();

	public List<PartitionInfo> getPartitionInfos();

	public int getRetainInDay();

	public int getWatermarkInDay();

	public int getIncrementInDay();

	public static enum TableType {
		MESSAGE, RESEND;
	}
}