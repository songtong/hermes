package com.ctrip.hermes.monitor.job.partition.strategy;

import java.util.List;

import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;

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

		@Override
		public String toString() {
			return "AnalysisResult [m_addList=" + m_addList + ", m_dropList=" + m_dropList + "]";
		}
	}
}
