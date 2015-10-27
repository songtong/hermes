package com.ctrip.hermes.metaservice.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaservice.monitor.dao.DefaultMonitorEventStorage;
import com.ctrip.hermes.metaservice.service.DefaultMetaService;
import com.ctrip.hermes.metaservice.service.DefaultZookeeperService;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.StorageDataSourceProvider;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKConfig;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultMetaService.class));
		all.add(A(DefaultZookeeperService.class));
		all.add(A(ZKConfig.class));
		all.add(A(ZKClient.class));

		all.add(A(StorageDataSourceProvider.class));
		all.add(A(DefaultTopicStorageService.class));

		all.add(A(MysqlStorageHandler.class));

		all.add(A(DefaultMonitorEventStorage.class));

		all.addAll(new FxhermesmetadbDatabaseConfigurator().defineComponents());

		all.add(defineJdbcDataSourceConfigurationManagerComponent("/opt/ctrip/data/hermes/datasources.xml"));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
