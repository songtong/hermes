package com.ctrip.hermes.monitor.job.partition.context;

import java.util.List;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;

public abstract class BaseTableContext implements TableContext {

	protected Topic m_topic;

	protected Partition m_partition;

	private List<PartitionInfo> m_partitionInfos;

	private int m_retainInDay;

	private int m_watermarkInDay;

	private int m_incrementInDay;

	private String m_tableName;

	private Datasource m_datasource;

	public BaseTableContext(Topic topic, Partition partition, int retain, int watermark, int increment) {
		m_topic = topic;
		m_partition = partition;
		m_retainInDay = retain;
		m_watermarkInDay = watermark;
		m_incrementInDay = increment;
	}

	protected void setTableName(String tableName) {
		m_tableName = tableName;
	}

	@Override
	public Datasource getDatasource() {
		return m_datasource;
	}

	@Override
	public String getTableName() {
		return m_tableName;
	}

	@Override
	public Topic getTopic() {
		return m_topic;
	}

	@Override
	public Partition getPartition() {
		return m_partition;
	}

	@Override
	public List<PartitionInfo> getPartitionInfos() {
		return m_partitionInfos;
	}

	@Override
	public int getRetainInDay() {
		return m_retainInDay;
	}

	@Override
	public int getWatermarkInDay() {
		return m_watermarkInDay;
	}

	@Override
	public int getIncrementInDay() {
		return m_incrementInDay;
	}

	public BaseTableContext setPartitionInfos(List<PartitionInfo> partitionInfos) {
		m_partitionInfos = partitionInfos;
		return this;
	}

	public BaseTableContext setDatasource(Datasource ds) {
		m_datasource = ds;
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_partition == null) ? 0 : m_partition.hashCode());
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
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
		BaseTableContext other = (BaseTableContext) obj;
		if (m_partition == null) {
			if (other.m_partition != null)
				return false;
		} else if (!m_partition.equals(other.m_partition))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}
}
