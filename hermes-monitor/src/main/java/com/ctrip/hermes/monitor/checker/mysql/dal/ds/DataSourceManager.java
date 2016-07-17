package com.ctrip.hermes.monitor.checker.mysql.dal.ds;

import java.sql.Connection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;

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
	ConcurrentHashMap<String, Pair<DataSource, Connection>> m_datasources = new ConcurrentHashMap<>();

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
			m_datasources.putIfAbsent(df.getUrl(), new Pair<DataSource, Connection>(ds, ds.getConnection()));
		}
		return m_datasources.get(df.getUrl()).getValue();
	}

	@PreDestroy
	public void destroy() {
		for (Entry<String, Pair<DataSource, Connection>> entry : m_datasources.entrySet()) {
			try {
				Connection c = entry.getValue().getValue();
				if (!c.isClosed()) {
					entry.getValue().getValue().close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
