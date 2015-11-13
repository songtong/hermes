package com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy;

import java.util.List;

import com.ctrip.hermes.monitor.checker.mysql.dal.entity.PartitionInfo;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;

public interface PartitionCheckerStrategy {
	public AnalysisResult analysisTable(TableContext ctx);

	public static class AnalysisResult {
		private List<PartitionInfo> m_addList;

		private List<PartitionInfo> m_dropList;

		public AnalysisResult(List<PartitionInfo> adds, List<PartitionInfo> drops) {
			m_addList = adds;
			m_dropList = drops;
		}

		public List<PartitionInfo> getAddList() {
			return m_addList;
		}

		public List<PartitionInfo> getDropList() {
			return m_dropList;
		}
	}
}
