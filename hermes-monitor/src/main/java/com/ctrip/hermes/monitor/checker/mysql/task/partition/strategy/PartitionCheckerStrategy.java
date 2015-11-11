package com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy;

import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;

public interface PartitionCheckerStrategy {
	public Pair<List<PartitionInfo>, List<PartitionInfo>> analysisTable(TableContext ctx);
}
