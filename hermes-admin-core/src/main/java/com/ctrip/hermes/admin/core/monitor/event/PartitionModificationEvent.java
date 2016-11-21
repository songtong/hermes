package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.model.MonitorEvent;

public class PartitionModificationEvent extends BaseMonitorEvent {
	public static enum PartitionOperation {
		ADD(0), DROP(1);

		private int m_code;

		private PartitionOperation(int code) {
			m_code = code;
		}

		public int code() {
			return m_code;
		}

		public static PartitionOperation getByCode(int code) {
			for (PartitionOperation op : PartitionOperation.values()) {
				if (op.code() == code) {
					return op;
				}
			}
			throw new IllegalArgumentException("Operaiont of code does not exist: " + code);
		}
	}

	private String m_topic;

	private int m_partition;

	private String m_tableName;

	private PartitionOperation m_op;

	private String m_sql;

	public PartitionModificationEvent() {
		this(null, -1, null, null, null);
	}

	public PartitionModificationEvent(String topic, int partition, String table, PartitionOperation op, String sql) {
		super(MonitorEventType.PARTITION_MODIFICATION);
		m_topic = topic;
		m_partition = partition;
		m_tableName = table;
		m_op = op;
		m_sql = sql;
	}

	@Override
	protected void parse0(MonitorEvent dbEntity) {
		m_topic = dbEntity.getKey1();
		m_partition = Integer.valueOf(dbEntity.getKey2());
		m_tableName = dbEntity.getKey3();
		m_op = PartitionOperation.getByCode(Integer.valueOf(dbEntity.getKey4()));
	}

	@Override
	protected void toDBEntity0(MonitorEvent e) {
		e.setKey1(m_topic);
		e.setKey2(String.valueOf(m_partition));
		e.setKey3(m_tableName);
		e.setKey4(String.valueOf(m_op.code()));
		e.setMessage(m_sql);
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	public String getTableName() {
		return m_tableName;
	}

	public void setTableName(String tableName) {
		m_tableName = tableName;
	}

	public PartitionOperation getOp() {
		return m_op;
	}

	public void setOp(PartitionOperation op) {
		m_op = op;
	}

	public String getSql() {
		return m_sql;
	}

	public void setSql(String sql) {
		m_sql = sql;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((m_op == null) ? 0 : m_op.hashCode());
		result = prime * result + m_partition;
		result = prime * result + ((m_sql == null) ? 0 : m_sql.hashCode());
		result = prime * result + ((m_tableName == null) ? 0 : m_tableName.hashCode());
		result = prime * result + ((m_topic == null) ? 0 : m_topic.hashCode());
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
		PartitionModificationEvent other = (PartitionModificationEvent) obj;
		if (m_op != other.m_op)
			return false;
		if (m_partition != other.m_partition)
			return false;
		if (m_sql == null) {
			if (other.m_sql != null)
				return false;
		} else if (!m_sql.equals(other.m_sql))
			return false;
		if (m_tableName == null) {
			if (other.m_tableName != null)
				return false;
		} else if (!m_tableName.equals(other.m_tableName))
			return false;
		if (m_topic == null) {
			if (other.m_topic != null)
				return false;
		} else if (!m_topic.equals(other.m_topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PartitionModificationEvent [m_topic=" + m_topic + ", m_partition=" + m_partition + ", m_tableName="
		      + m_tableName + ", m_op=" + m_op + ", m_sql=" + m_sql + ", getMessage()=" + getMessage() + "]";
	}
}
