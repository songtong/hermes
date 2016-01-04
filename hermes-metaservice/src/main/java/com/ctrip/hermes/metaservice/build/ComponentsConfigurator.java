package com.ctrip.hermes.metaservice.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.mail.DefaultMailService;
import com.ctrip.hermes.mail.FileMailAccountProvider;
import com.ctrip.hermes.metaservice.monitor.dao.DefaultMonitorEventStorage;
import com.ctrip.hermes.metaservice.queue.ds.MessageQueueDatasourceProvider;
import com.ctrip.hermes.metaservice.queue.ds.MessageQueueTableProvider;
import com.ctrip.hermes.metaservice.service.DefaultMetaService;
import com.ctrip.hermes.metaservice.service.DefaultZookeeperService;
import com.ctrip.hermes.metaservice.service.MetaRefactor;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.StorageDataSourceProvider;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKConfig;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.addAll(new FxhermesmetadbDatabaseConfigurator().defineComponents());
		all.addAll(new FxHermesShardDbDatabaseConfigurator().defineComponents());
		
		all.add(A(DefaultMetaService.class));
		all.add(A(DefaultZookeeperService.class));
		all.add(A(ZKConfig.class));
		all.add(A(ZKClient.class));

		all.add(A(StorageDataSourceProvider.class));
		all.add(A(DefaultTopicStorageService.class));

		all.add(A(MysqlStorageHandler.class));

		all.add(A(DefaultMonitorEventStorage.class));

		all.add(C(TableProvider.class, "message-priority", MessageQueueTableProvider.class));
		all.add(C(TableProvider.class, "resend-group-id", MessageQueueTableProvider.class));
		all.add(C(TableProvider.class, "offset-message", MessageQueueTableProvider.class));
		all.add(C(TableProvider.class, "offset-resend", MessageQueueTableProvider.class));
		all.add(C(TableProvider.class, "dead-letter", MessageQueueTableProvider.class));
		all.add(A(MessageQueueDatasourceProvider.class));

		all.add(A(FileMailAccountProvider.class));
		all.add(A(DefaultMailService.class));

		all.addAll(new FxhermesmetadbDatabaseConfigurator().defineComponents());
		all.addAll(new FxHermesShardDbDatabaseConfigurator().defineComponents());

		all.add(A(MetaRefactor.class));
		
		all.add(defineJdbcDataSourceConfigurationManagerComponent("/opt/ctrip/data/hermes/datasources.xml"));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
