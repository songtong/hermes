package com.ctrip.hermes.monitor.checker.mysql.dal.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.utils.StringUtils;

public class PartitionInfo {
	public static final String SQL_PARTITION = //
	"SELECT TABLE_NAME, PARTITION_NAME, PARTITION_ORDINAL_POSITION, PARTITION_DESCRIPTION, TABLE_ROWS " //
	      + "FROM PARTITIONS " //
	      + "ORDER BY TABLE_NAME ASC, PARTITION_ORDINAL_POSITION ASC";

	private String m_table;

	private String m_name;

	private int m_ordinal;

	private long m_upperbound;

	private long m_rows;

	public String getTable() {
		return m_table;
	}

	public void setTable(String table) {
		m_table = table;
	}

	public String getName() {
		return m_name;
	}

	public void setName(String name) {
		m_name = name;
	}

	public int getOrdinal() {
		return m_ordinal;
	}

	public void setOrdinal(int ordinal) {
		m_ordinal = ordinal;
	}

	public long getUpperbound() {
		return m_upperbound;
	}

	public void setUpperbound(long upperbound) {
		m_upperbound = upperbound;
	}

	public long getRows() {
		return m_rows;
	}

	public void setRows(long rows) {
		m_rows = rows;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
		result = prime * result + m_ordinal;
		result = prime * result + ((m_table == null) ? 0 : m_table.hashCode());
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
		PartitionInfo other = (PartitionInfo) obj;
		if (m_name == null) {
			if (other.m_name != null)
				return false;
		} else if (!m_name.equals(other.m_name))
			return false;
		if (m_ordinal != other.m_ordinal)
			return false;
		if (m_table == null) {
			if (other.m_table != null)
				return false;
		} else if (!m_table.equals(other.m_table))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PartitionInfo [m_table=" + m_table + ", m_name=" + m_name + ", m_ordinal=" + m_ordinal
		      + ", m_upperbound=" + m_upperbound + ", m_rows=" + m_rows + "]";
	}

	public static Map<String, List<PartitionInfo>> parseResultSet(ResultSet rs) throws SQLException {
		Map<String, List<PartitionInfo>> map = new HashMap<String, List<PartitionInfo>>();
		while (rs.next()) {
			if (!StringUtils.isBlank(rs.getString(2))) {
				String table = rs.getString(1);

				List<PartitionInfo> partitions = map.get(table);
				if (partitions == null) {
					partitions = new ArrayList<PartitionInfo>();
					map.put(table, partitions);
				}

				PartitionInfo p = new PartitionInfo();
				p.setTable(rs.getString(1));
				p.setName(rs.getString(2));
				p.setOrdinal(rs.getInt(3));
				p.setUpperbound(getBorder(rs.getString(4)));
				p.setRows(rs.getLong(5));
				partitions.add(p);
			}
		}
		return map;
	}

	private static long getBorder(String description) {
		if ("MAXVALUE".equals(description.trim().toUpperCase())) {
			return Long.MAX_VALUE;
		}
		return Long.valueOf(description);
	}
}
