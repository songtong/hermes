package com.ctrip.hermes.monitor.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.monitor.checker.mysql.dal.ds.DataSourceManager;
import com.ctrip.hermes.monitor.job.partition.context.TableContext;
import com.ctrip.hermes.monitor.job.partition.entity.PartitionInfo;

@Service
public class PartitionService {
	private static final Logger log = LoggerFactory.getLogger(PartitionService.class);

	private static final String INFO_SCHEMA_TABLE = "information_schema";

	@Autowired
	private DataSourceManager m_dsManager;

	public String addPartitions(TableContext ctx, List<PartitionInfo> list) throws Exception {
		String sql = getAddPartitionSQL(ctx, list);
		log.info("Add partitions[{} {} {}]: {}", //
		      ctx.getTopic().getName(), ctx.getPartition().getId(), ctx.getTableName(), sql);
		executeSQL(ctx.getDatasource(), sql);
		return sql;
	}

	public String dropPartitions(TableContext ctx, List<PartitionInfo> list) throws Exception {
		String sql = getDropPartitionSQL(ctx, list);
		log.info("Drop partitions[{} {} {}]: {}", //
		      ctx.getTopic().getName(), ctx.getPartition().getId(), ctx.getTableName(), sql);
		executeSQL(ctx.getDatasource(), sql);
		return sql;
	}

	public static String getAddPartitionSQL(TableContext ctx, List<PartitionInfo> list) {
		StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s ADD PARTITION (", ctx.getTableName()));
		for (PartitionInfo pInfo : list) {
			sb.append( //
			String.format("PARTITION %s VALUES LESS THAN (%s) ENGINE = InnoDB, ", pInfo.getName(), pInfo.getUpperbound()));
		}
		return sb.toString().substring(0, sb.length() - 2) + ");";
	}

	public static String getDropPartitionSQL(TableContext ctx, List<PartitionInfo> list) {
		StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s DROP PARTITION ", ctx.getTableName()));
		for (PartitionInfo pInfo : list) {
			sb.append(pInfo.getName() + ", ");
		}
		return sb.toString().substring(0, sb.length() - 2) + ";";
	}

	public boolean executeSQL(Datasource ds, String sql) throws Exception {
		Connection conn = null;
		Statement stat = null;
		try {
			conn = getConnection(ds, false);
			stat = conn.createStatement();
			return stat.execute(sql);
		} finally {
			if (stat != null) {
				stat.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	public Map<String, Pair<Datasource, List<PartitionInfo>>> getDatasourcePartitions(Datasource ds) throws Exception {
		Connection conn = getConnection(ds, true);
		Statement stat = null;
		ResultSet rs = null;
		try {
			stat = conn.createStatement();
			rs = stat.executeQuery(PartitionInfo.SQL_PARTITION);
			return formatPartitionMap(PartitionInfo.parseResultSet(rs), ds);
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (stat != null) {
				stat.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	public Map<String, Pair<Datasource, List<PartitionInfo>>> formatPartitionMap( //
	      Map<String, List<PartitionInfo>> origin, Datasource ds) {
		Map<String, Pair<Datasource, List<PartitionInfo>>> map = new HashMap<String, Pair<Datasource, List<PartitionInfo>>>();
		for (Entry<String, List<PartitionInfo>> entry : origin.entrySet()) {
			map.put(entry.getKey(), new Pair<Datasource, List<PartitionInfo>>(ds, entry.getValue()));
		}
		return map;
	}

	private Connection getConnection(Datasource ds, boolean forSchemaInfo) throws Exception {
		PropertiesDef def = new PropertiesDef();
		String url = ds.getProperties().get("url").getValue();

		def.setDriver("com.mysql.jdbc.Driver");
		def.setUrl(forSchemaInfo ? wrapperJdbcUrl(url) : url);
		def.setUser(ds.getProperties().get("user").getValue());
		def.setPassword(ds.getProperties().get("password").getValue());

		return m_dsManager.getConnection(def);
	}

	private String wrapperJdbcUrl(String source) {
		String[] splits = source.split("\\/\\/");
		return String.format("%s//%s/%s", splits[0], splits[1].split("\\/")[0], INFO_SCHEMA_TABLE);
	}
}
