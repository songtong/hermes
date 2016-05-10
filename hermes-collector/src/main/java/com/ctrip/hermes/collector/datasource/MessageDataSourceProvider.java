package com.ctrip.hermes.collector.datasource;

import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;

import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;

public class MessageDataSourceProvider implements DataSourceProvider {
	public List<Datasource> datasources = null;
	
	public MessageDataSourceProvider(List<Datasource> datasources) {
		this.datasources = datasources;
	}
	
	@Override
	public DataSourcesDef defineDatasources() {
		DataSourcesDef def = new DataSourcesDef();

		if (this.datasources != null) {
			for (Datasource ds : datasources) {
				Map<String, Property> dsProps = ds.getProperties();
				DataSourceDef dsDef = new DataSourceDef(ds.getId());
				PropertiesDef props = new PropertiesDef();

				props.setDriver("com.mysql.jdbc.Driver");
				if (dsProps.get("url") == null || dsProps.get("user") == null) {
					throw new IllegalArgumentException("url and user property can not be null in datasource definition " + ds);
				}
				props.setUrl(dsProps.get("url").getValue());
				props.setUser(dsProps.get("user").getValue());
				if (dsProps.get("password") != null) {
					props.setPassword(dsProps.get("password").getValue());
				}

				props.setConnectionProperties("useUnicode=true&autoReconnect=true&rewriteBatchedStatements=true");
				dsDef.setProperties(props);

				int maxPoolSize = 20;
				if (dsProps.get("maximumPoolSize") != null) {
					maxPoolSize = Integer.parseInt(dsProps.get("maximumPoolSize").getValue());
				}
				dsDef.setMaximumPoolSize(maxPoolSize);

				int minPoolSize = 10;
				if (dsProps.get("minimumPoolSize") != null) {
					minPoolSize = Integer.parseInt(dsProps.get("minimumPoolSize").getValue());
				}
				dsDef.setMinimumPoolSize(minPoolSize);

				def.addDataSource(dsDef);
			}
		}
		
		return def;
	}

}
