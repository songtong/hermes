package com.ctrip.hermes.monitor.checker.mysql.dal.ds;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptorManager;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component(value = "DataSourceManager")
public class DataSourceManager {
	ConcurrentHashMap<String, DataSource> m_datasources = new ConcurrentHashMap<String, DataSource>();

	private static final HermesDSDManager DSD_BUILDER = new HermesDSDManager();

	private static class HermesDSDManager extends JdbcDataSourceDescriptorManager {
		public JdbcDataSourceDescriptor buildDescriptor(DataSourceDef ds) {
			return super.buildDescriptor(ds);
		}
	}

	public Connection getConnection(PropertiesDef df) throws Exception {
		if (!m_datasources.contains(df.getUrl())) {
			DataSource ds = PlexusComponentLocator.lookup(DataSource.class, "jdbc");
			ds.initialize(DSD_BUILDER.buildDescriptor(new DataSourceDef("PARTITION_CHECKER").setProperties(df)));
			m_datasources.putIfAbsent(df.getUrl(), ds);
		}
		return m_datasources.get(df.getUrl()).getConnection();
	}
}
