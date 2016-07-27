package com.ctrip.hermes.monitor.job.partition.context;

import java.util.regex.Pattern;

import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author song
 *
 */
public class MessageTableContext extends BaseTableContext {
	public static final Pattern MESSAGE_TABLE_PATTERN = Pattern.compile("\\d+_\\d+_message_\\d?");

	private int m_priority;

	public MessageTableContext( //
	      Topic topic, Partition partition, int priority, int retain, int cordon, int increment) {
		super(topic, partition, retain, cordon, increment);
		m_priority = priority;

		setTableName(String.format("%s_%s_message_%s", topic.getId(), partition.getId(), priority));
	}

	public static boolean isMessageTable(String tableName) {
		return MESSAGE_TABLE_PATTERN.matcher(tableName).matches();
	}

	@Override
	public TableType getType() {
		return TableType.MESSAGE;
	}

	public int getPriority() {
		return m_priority;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + m_priority;
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
		MessageTableContext other = (MessageTableContext) obj;
		if (m_priority != other.m_priority)
			return false;
		return true;
	}

	public Tpp getTpp() {
		return new Tpp(m_topic.getName(), m_partition.getId(), m_priority == 0);
	}

	@Override
	public String toString() {
		return "MessageTableContext [m_priority=" + m_priority + ", getTableName()=" + getTableName() + ", getTopic()="
		      + getTopic().getName() + ", getPartition()=" + getPartition().getId() + ", getPartitionInfos()="
		      + getPartitionInfos() + "]";
	}
}
