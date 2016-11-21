package com.ctrip.hermes.monitor.job.partition.strategy;

import java.util.List;

import com.ctrip.hermes.admin.core.queue.PartitionInfo;
import com.ctrip.hermes.admin.core.queue.TableContext;

public interface PartitionCheckerStrategy {
	public AnalysisResult analysisTable(TableContext ctx);

	public static class AnalysisResult {
		private List<PartitionInfo> m_addList;

		private List<PartitionInfo> m_dropList;

		private List<PartitionInfo> m_wasteList;

		public AnalysisResult(List<PartitionInfo> adds, List<PartitionInfo> drops, List<PartitionInfo> wasteList) {
			m_addList = adds;
			m_dropList = drops;
			m_wasteList = wasteList;
		}

		public List<PartitionInfo> getAddList() {
			return m_addList;
		}

		public List<PartitionInfo> getDropList() {
			return m_dropList;
		}

		public List<PartitionInfo> getWasteList() {
			return m_wasteList;
		}

		@Override
		public String toString() {
			return "AnalysisResult [m_addList=" + m_addList + ", m_dropList=" + m_dropList + "]";
		}
	}
}
