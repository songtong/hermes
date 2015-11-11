package com.ctrip.hermes.monitor.checker.mysql.task.partition.context;

import java.util.List;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.mysql.PartitionChecker.TableType;
import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;

public interface TableContext {
	public Datasource getDatasource();

	public TableType getType();

	public Topic getTopic();

	public Partition getPartition();

	public String getTableName();

	public List<PartitionInfo> getPartitionInfos();

	public int getRetainInDay();

	public int getCordonInDay();

	public int getIncrementInDay();

}
