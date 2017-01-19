package com.ctrip.hermes.monitor.checker.mysql.dal.ds;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.datasource.DataSource;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptor;
import org.unidal.dal.jdbc.datasource.JdbcDataSourceDescriptorManager;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;

@Component(value = "DataSourceManager")
public class DataSourceManager {
	private static final Logger log = LoggerFactory.getLogger(DataSourceManager.class);

	private ConcurrentHashMap<String, DataSource> m_datasources = new ConcurrentHashMap<>();

	private static final HermesDSDManager DSD_BUILDER = new HermesDSDManager();

	private static class HermesDSDManager extends JdbcDataSourceDescriptorManager {
		public JdbcDataSourceDescriptor buildDescriptor(DataSourceDef ds) {
			ds.setConnectionTimeout("30m");
			ds.setIdleTimeout("30m");
			ds.setCheckoutTimeoutInMillis(0);
			return super.buildDescriptor(ds);
		}
	}

	public Connection getConnection(PropertiesDef df) throws Exception {
		if (!m_datasources.containsKey(df.getUrl())) {
			synchronized (m_datasources) {
				if (!m_datasources.containsKey(df.getUrl())) {
					log.info("Init datasource for: {}", df.getUrl());
					DataSource ds = PlexusComponentLocator.lookupWithoutCache(DataSource.class, "jdbc");
					ds.initialize(DSD_BUILDER.buildDescriptor(new DataSourceDef("PARTITION_CHECKER").setProperties(df)));
					m_datasources.put(df.getUrl(), ds);
				}
			}
		}
		return m_datasources.get(df.getUrl()).getConnection();
	}
}
