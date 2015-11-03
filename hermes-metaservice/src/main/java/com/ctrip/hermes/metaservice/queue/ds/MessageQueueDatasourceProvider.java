package com.ctrip.hermes.metaservice.queue.ds;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.service.MetaService;

@Named(type = DataSourceProvider.class, value = "meta-service")
public class MessageQueueDatasourceProvider implements DataSourceProvider {
	private static final Logger log = LoggerFactory.getLogger(MessageQueueDatasourceProvider.class);

	private MetaService m_metaService;

	private boolean m_inited;

	@Override
	public synchronized DataSourcesDef defineDatasources() {
		if (!m_inited) {
			m_inited = true;
			return new DataSourcesDef();
		}

		DataSourcesDef def = new DataSourcesDef();

		if (m_metaService == null) {
			m_metaService = PlexusComponentLocator.lookup(MetaService.class);
		}

		try {
			Storage storage = m_metaService.findLatestMeta().getStorages().get(Storage.MYSQL);
			if (storage != null) {
				for (Datasource ds : storage.getDatasources()) {
					Map<String, Property> dsProps = ds.getProperties();
					DataSourceDef dsDef = new DataSourceDef(ds.getId());
					PropertiesDef props = new PropertiesDef();

					props.setDriver("com.mysql.jdbc.Driver");
					if (dsProps.get("url") == null || dsProps.get("user") == null) {
						throw new IllegalArgumentException("url and user property can't be null in datasource definition "
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
		} catch (DalException e) {
			log.error("Initial message queue datasource failed due to aquire meta-info failed.", e);
		}
		return def;
	}
}
