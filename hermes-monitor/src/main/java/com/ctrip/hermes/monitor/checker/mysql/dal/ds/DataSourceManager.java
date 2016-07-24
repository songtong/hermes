package com.ctrip.hermes.monitor.checker.mysql.dal.ds;

import java.sql.Connection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptorManager;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component(value = "DataSourceManager")
public class DataSourceManager {
	private static final Logger log = LoggerFactory.getLogger(DataSourceManager.class);

	private ConcurrentHashMap<String, Pair<DataSource, Connection>> m_datasources = new ConcurrentHashMap<>();

	private static final HermesDSDManager DSD_BUILDER = new HermesDSDManager();

	private static class HermesDSDManager extends JdbcDataSourceDescriptorManager {
		public JdbcDataSourceDescriptor buildDescriptor(DataSourceDef ds) {
			return super.buildDescriptor(ds);
		}
	}

	public Connection getConnection(PropertiesDef df, boolean forceNew) throws Exception {
		if (!m_datasources.containsKey(df.getUrl())) {
			DataSource ds = PlexusComponentLocator.lookup(DataSource.class, "jdbc");
			ds.initialize(DSD_BUILDER.buildDescriptor(new DataSourceDef("PARTITION_CHECKER").setProperties(df)));
			log.info("Init datasource for: {}", df.getUrl());
			Connection conn = ds.getConnection();
			if (m_datasources.putIfAbsent(df.getUrl(), new Pair<>(ds, conn)) != null) {
				log.warn("Already cached connection for ds: {}", df.getUrl());
				conn.close();
			}
		}
		Pair<DataSource, Connection> pair = m_datasources.get(df.getUrl());
		Connection conn = pair.getValue();
		if (!conn.isValid(0) || forceNew) {
			if (!conn.isClosed()) {
				conn.close();
			}
			log.info("Connection for {} is closed (force new= {}), open connection again.", df.getUrl(), forceNew);
			pair.setValue(pair.getKey().getConnection());
		}
		return m_datasources.get(df.getUrl()).getValue();
	}

	@PreDestroy
	public void destroy() {
		log.info("DataSourceManager is destoried.");
		for (Entry<String, Pair<DataSource, Connection>> entry : m_datasources.entrySet()) {
			try {
				Connection c = entry.getValue().getValue();
				if (!c.isClosed()) {
					c.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
