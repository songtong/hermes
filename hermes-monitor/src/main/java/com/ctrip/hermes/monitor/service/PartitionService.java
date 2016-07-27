package com.ctrip.hermes.monitor.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.metaservice.queue.PartitionInfo;
import com.ctrip.hermes.metaservice.queue.TableContext;
import com.ctrip.hermes.monitor.checker.mysql.dal.ds.DataSourceManager;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.job.partition.context.AbandonedTableContext;
import com.ctrip.hermes.monitor.job.partition.context.DeadLetterTableContext;
import com.ctrip.hermes.monitor.job.partition.context.MessageTableContext;
import com.ctrip.hermes.monitor.job.partition.context.ResendTableContext;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

@Service
public class PartitionService {
	private static final Logger log = LoggerFactory.getLogger(PartitionService.class);

	private static final String INFO_SCHEMA_TABLE = "information_schema";

	private static final int SQL_COOL_TIME_SECOND = 1;

	@Autowired
	private DataSourceManager m_dsManager;

	@Autowired
	private MonitorConfig m_config;

	public boolean isHermesTable(String tableName) {
		return MessageTableContext.isMessageTable(tableName) //
		      || ResendTableContext.isResendTable(tableName) //
		      || DeadLetterTableContext.isDeadTable(tableName);
	}

	public void dropWholeTable(TableContext ctx) throws Exception {
		if (ctx instanceof AbandonedTableContext && isHermesTable(ctx.getTableName())) {
			log.warn("Hermes table [{}] of datasource [{}] is abandoned, ready to drop it.",//
			      ctx.getTableName(), ctx.getDatasource().getProperties().get("url").getValue());
			if (m_config.isPartitionServiceDropTableEnable()) {
				String sql = String.format("drop table %s;", ctx.getTableName());
				log.warn("Drop table sql: {}", sql);
				executeSQL(ctx.getDatasource(), sql);
			}
		}
	}

	public String addPartitions(TableContext ctx, List<PartitionInfo> list) throws Exception {
		String sql = getAddPartitionSQL(ctx, list);
		log.info("Add partitions[{} {} {}]: {}", //
		      ctx.getTopic().getName(), ctx.getPartition().getId(), ctx.getTableName(), sql);
		try {
			executeSQL(ctx.getDatasource(), sql);
		} catch (Exception e) {
			log.error("Add partition failed: {}, {}", ctx.getTableName(), list, e);
			throw e;
		}
		TimeUnit.SECONDS.sleep(SQL_COOL_TIME_SECOND);
		return sql;
	}

	public String dropPartitions(TableContext ctx, List<PartitionInfo> list) throws Exception {
		String sql = getDropPartitionSQL(ctx, list);
		log.info("Drop partitions[{} {} {}]: {}", //
		      ctx.getTopic().getName(), ctx.getPartition().getId(), ctx.getTableName(), sql);
		try {
			executeSQL(ctx.getDatasource(), sql);
		} catch (Exception e) {
			log.error("Drop partition failed: {}, {}", ctx.getTableName(), list, e);
			throw e;
		}
		TimeUnit.SECONDS.sleep(SQL_COOL_TIME_SECOND);
		return sql;
	}

	private String getAddPartitionSQL(TableContext ctx, List<PartitionInfo> list) {
		StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s ADD PARTITION (", ctx.getTableName()));
		for (PartitionInfo pInfo : list) {
			sb.append( //
			String.format("PARTITION %s VALUES LESS THAN (%s) ENGINE = InnoDB, ", pInfo.getName(), pInfo.getUpperbound()));
		}
		return sb.toString().substring(0, sb.length() - 2) + ");";
	}

	private String getDropPartitionSQL(TableContext ctx, List<PartitionInfo> list) {
		StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s DROP PARTITION ", ctx.getTableName()));
		for (PartitionInfo pInfo : list) {
			sb.append(pInfo.getName() + ", ");
		}
		return sb.toString().substring(0, sb.length() - 2) + ";";
	}

	private void closeStatement(String sql, Statement stat) {
		try {
			if (stat != null && !stat.isClosed()) {
				stat.close();
			}
		} catch (SQLException e) {
			log.error("Close statement failed: {}", sql, e);
		}
	}

	private void closeConnection(Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			log.error("Close connection failed: {}", conn);
		}
	}

	private void closeResultSet(ResultSet rs) {
		try {
			if (rs != null && !rs.isClosed()) {
				rs.close();
			}
		} catch (SQLException e) {
			log.error("Close result-set failed: {}", rs);
		}
	}

	public boolean executeSQL(Datasource ds, String sql) throws Exception {
		Statement stat = null;
		Connection conn = null;
		try {
			conn = getConnection(ds, false);
			stat = conn.createStatement();
			return stat.execute(sql);
		} finally {
			closeStatement(sql, stat);
			closeConnection(conn);
		}
	}

	public Map<String, Pair<Datasource, List<PartitionInfo>>> queryDatasourcePartitions(Datasource ds) throws Exception {
		Transaction t = Cat.newTransaction("Partition.Query", getDbName(ds));
		t.addData("SQL", PartitionInfo.SQL_PARTITION);
		Connection conn = null;
		Statement stat = null;
		ResultSet rs = null;
		try {
			conn = getConnection(ds, true);
			stat = conn.createStatement();
			rs = stat.executeQuery(PartitionInfo.SQL_PARTITION);
			t.setStatus(Message.SUCCESS);
			return formatPartitionMap(PartitionInfo.parseResultSet(rs), ds);
		} catch (Exception e) {
			t.setStatus("Failed");
			throw e;
		} finally {
			t.complete();
			closeResultSet(rs);
			closeStatement(PartitionInfo.SQL_PARTITION, stat);
			closeConnection(conn);
		}
	}

	public static String getDbName(Datasource ds) {
		try {
			return ds.getProperties().get("url").getValue().split("[\\//.]")[2];
		} catch (Exception e) {
			return "Unknow";
		}
	}

	private Map<String, Pair<Datasource, List<PartitionInfo>>> formatPartitionMap( //
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
