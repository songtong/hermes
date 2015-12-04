package com.ctrip.hermes.monitor.job.partition.context;

import java.util.List;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.job.partition.PartitionManagementJob.TableType;

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

}
