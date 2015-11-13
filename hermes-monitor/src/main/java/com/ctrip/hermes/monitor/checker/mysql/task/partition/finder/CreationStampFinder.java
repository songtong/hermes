package com.ctrip.hermes.monitor.checker.mysql.task.partition.finder;

import java.util.Date;

import com.ctrip.hermes.monitor.checker.mysql.task.partition.context.TableContext;

public interface CreationStampFinder {
	public CreationStamp findLatest(TableContext ctx);

	public CreationStamp findOldest(TableContext ctx);

	public CreationStamp findSpecific(TableContext ctx, long id);

	public static class CreationStamp {
		private long m_id;

		private Date m_date;

		public CreationStamp(long id, Date date) {
			m_id = id;
			m_date = date;

		}

		public long getId() {
			return m_id;
		}

		public Date getDate() {
			return m_date;
		}
	}
}
