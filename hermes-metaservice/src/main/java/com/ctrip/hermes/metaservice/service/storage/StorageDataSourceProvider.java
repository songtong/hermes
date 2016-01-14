package com.ctrip.hermes.metaservice.service.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.service.StorageService;

@Named(type = DataSourceProvider.class, value = StorageDataSourceProvider.ID)
public class StorageDataSourceProvider implements DataSourceProvider {
	public static final String ID = "storage";

	/**
	 * Note: Can't use @Inject, "Caused by: org.codehaus.plexus.component.factory.ComponentInstantiationException: Creation
	 * circularity ..."
	 */
	private StorageService m_dsService;

	int avoidInitialize = 0;

	@Override
	public DataSourcesDef defineDatasources() {
		DataSourcesDef def = new DataSourcesDef();

		if (avoidInitialize > 0) {
			List<Datasource> dataSources = new ArrayList<>();
			try {

				if (null == m_dsService) {
					m_dsService = PlexusComponentLocator.lookup(StorageService.class);
				}
				
				for (Storage storage : m_dsService.findStorages()) {
					if (storage.getType().equals("mysql")) {
						dataSources.addAll(storage.getDatasources());
					}
				}
			} catch (DalException e) {
				e.printStackTrace();
			}

			for (Datasource ds : dataSources) {

				Map<String, Property> dsProps = ds.getProperties();
				DataSourceDef dsDef = new DataSourceDef(ds.getId());
				PropertiesDef props = new PropertiesDef();

				props.setDriver("com.mysql.jdbc.Driver");
				if (dsProps.get("url") == null || dsProps.get("user") == null) {
					throw new IllegalArgumentException("url and user property can not be null in datasource definition "
					      + ds);
				}
				props.setUrl(dsProps.get("url").getValue());
				props.setUser(dsProps.get("user").getValue());
				if (dsProps.get("password") != null) {
					props.setPassword(dsProps.get("password").getValue());
				}

				props.setConnectionProperties("useUnicode=true&autoReconnect=true&rewriteBatchedStatements=true");
				dsDef.setProperties(props);

				def.addDataSource(dsDef);
			}
		}
		avoidInitialize++;

		return def;
	}

}
