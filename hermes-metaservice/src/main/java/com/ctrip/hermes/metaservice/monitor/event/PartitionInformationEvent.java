package com.ctrip.hermes.metaservice.monitor.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.model.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.MonitorEventType;
import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;

public class PartitionInformationEvent extends BaseMonitorEvent {

	public static final int IMAGINED_RESERVE_COUNT_WHEN_NOT_SURE = 100;

	private int m_totalDatasourceCount = 0;

	private int m_totalTableCount = 0;

	private int m_totalPartitionCount = 0;

	private int m_totalWastePartitionCount = 0;

	private Map<String, DatasourceInformation> m_dsInfos;

	public PartitionInformationEvent() {
		this(null, null);
	}

	public PartitionInformationEvent( //
	      List<TableContext> tableContexts, ConcurrentHashMap<String, List<PartitionInfo>> wastes) {
		super(MonitorEventType.PARTITION_INFO);
		m_dsInfos = generateDatasourceInformations(tableContexts, wastes);
		for (Entry<String, DatasourceInformation> entry : m_dsInfos.entrySet()) {
			m_totalDatasourceCount++;
			m_totalTableCount += entry.getValue().getTotalTableCount();
			m_totalPartitionCount += entry.getValue().getTotalPartitionCount();
			m_totalWastePartitionCount += entry.getValue().getTotalWastePartitionCount();
		}
	}

	private Map<String, DatasourceInformation> generateDatasourceInformations( //
	      List<TableContext> tableContexts, Map<String, List<PartitionInfo>> wastes) {
		Map<String, DatasourceInformation> result = new HashMap<>();
		for (TableContext ctx : tableContexts) {
			String dsUrl = ctx.getDatasource().getProperties().get("url").getValue();
			DatasourceInformation datasourceInformation = result.get(dsUrl);
			if (datasourceInformation == null) {
				datasourceInformation = new DatasourceInformation(dsUrl);
				result.put(dsUrl, datasourceInformation);
			}
			datasourceInformation.recordTableContext(ctx, wastes.get(ctx.getTableName()));
		}
		return result;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_totalDatasourceCount = Integer.valueOf(dbEntity.getKey1());
		m_totalTableCount = Integer.valueOf(dbEntity.getKey2());
		m_totalPartitionCount = Integer.valueOf(dbEntity.getKey3());
		m_totalWastePartitionCount = Integer.valueOf(dbEntity.getKey4());
		m_dsInfos = JSON.parseObject(dbEntity.getMessage(), new TypeReference<Map<String, DatasourceInformation>>() {
		});
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(String.valueOf(m_totalDatasourceCount));
		e.setKey2(String.valueOf(m_totalTableCount));
		e.setKey3(String.valueOf(m_totalPartitionCount));
		e.setKey4(String.valueOf(m_totalWastePartitionCount));
		e.setMessage(JSON.toJSONString(m_dsInfos));
	}

	public static class DatasourceInformation {
		private String m_datasource;

		private Map<String, Pair<Integer, Integer>> m_table2PartitionCount;

		private Map<String, Long> m_table2RowsCount;

		public DatasourceInformation(String datasource) {
			if (StringUtils.isBlank(datasource)) {
				throw new IllegalArgumentException("Datasource name can not be null!");
			}
			m_datasource = datasource;
			m_table2PartitionCount = new HashMap<>();
			m_table2RowsCount = new HashMap<>();
		}

		public boolean recordTableContext(TableContext ctx, List<PartitionInfo> wastes) {
			if (m_datasource.equals(ctx.getDatasource().getProperties().get("url").getValue())) {
				m_table2PartitionCount.put(ctx.getTableName(), //
				      new Pair<Integer, Integer>(ctx.getPartitionInfos().size(), wastes != null ? wastes.size() : 0));
				long totalRowsCount = 0;
				for (PartitionInfo partition : ctx.getPartitionInfos()) {
					if (partition.getRows() == 0) {
						break;
					}
					totalRowsCount += partition.getRows();
				}
				m_table2RowsCount.put(ctx.getTableName(), totalRowsCount);
				return true;
			}
			return false;
		}

		public String getDatasource() {
			return m_datasource;
		}

		public int getTotalTableCount() {
			return m_table2PartitionCount.size();
		}

		public int getTotalPartitionCount() {
			int total = 0;
			for (Pair<Integer, Integer> pair : m_table2PartitionCount.values()) {
				total += pair.getKey();
			}
			return total;
		}

		public int getTotalWastePartitionCount() {
			int total = 0;
			for (Pair<Integer, Integer> pair : m_table2PartitionCount.values()) {
				total += pair.getValue();
			}
			return total;
		}

		public Map<String, Pair<Integer, Integer>> getTable2PartitionCount() {
			return m_table2PartitionCount;
		}

		public Map<String, Long> getTable2RowsCount() {
			return m_table2RowsCount;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_datasource == null) ? 0 : m_datasource.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DatasourceInformation other = (DatasourceInformation) obj;
			if (m_datasource == null) {
				if (other.m_datasource != null)
					return false;
			} else if (!m_datasource.equals(other.m_datasource))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "DatasourceInformation [m_datasource=" + m_datasource + ", m_table2PartitionCount="
			      + m_table2PartitionCount + "]";
		}
	}

}
