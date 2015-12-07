package com.ctrip.hermes.monitor.job.partition.context;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.job.partition.PartitionManagementJob.TableType;

public class ResendTableContext extends BaseTableContext {
	private ConsumerGroup m_consumer;

	public ResendTableContext( //
	      Topic topic, Partition partition, ConsumerGroup consumer, int retain, int cordon, int increment) {
		super(topic, partition, retain, cordon, increment);
		m_consumer = consumer;

		setTableName(String.format("%s_%s_resend_%s", topic.getId(), partition.getId(), consumer.getId()));
	}

	public ConsumerGroup getConsumer() {
		return m_consumer;
	}

	@Override
	public TableType getType() {
		return TableType.RESEND;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_consumer == null) ? 0 : m_consumer.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResendTableContext other = (ResendTableContext) obj;
		if (m_consumer == null) {
			if (other.m_consumer != null)
				return false;
		} else if (!m_consumer.equals(other.m_consumer))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ResendTableContext [m_consumer=" + m_consumer.getName() + ", getTableName()=" + getTableName()
		      + ", getTopic()=" + getTopic().getName() + ", getPartition()=" + getPartition().getId()
		      + ", getPartitionInfos()=" + getPartitionInfos() + "]";
	}
}
