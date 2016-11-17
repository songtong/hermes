package com.ctrip.hermes.admin.core.queue;

import java.util.Date;

public class CreationStamp {
	private long m_id;

	private Date m_date;

	public CreationStamp() {
		this(-1L, null);
	}

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

	public void setId(long id) {
		m_id = id;
	}

	public void setDate(Date date) {
		m_date = date;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_date == null) ? 0 : m_date.hashCode());
		result = prime * result + (int) (m_id ^ (m_id >>> 32));
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
		CreationStamp other = (CreationStamp) obj;
		if (m_date == null) {
			if (other.m_date != null)
				return false;
		} else if (!m_date.equals(other.m_date))
			return false;
		if (m_id != other.m_id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CreationStamp [m_id=" + m_id + ", m_date=" + m_date + "]";
	}
}
