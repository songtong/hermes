package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaservice.service.CodecService;
import com.ctrip.hermes.metaservice.service.CompileService;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.DefaultPortalMetaService;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.metaservice.service.SchemaRegistryService;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.SubscriptionService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.dal.DefaultHermesPortalDao;
import com.ctrip.hermes.portal.dal.ds.PortalDataSourceProvider;
import com.ctrip.hermes.portal.dal.ds.PortalTableProvider;
import com.ctrip.hermes.portal.service.elastic.DefaultElasticClient;
import com.ctrip.hermes.portal.service.monitor.DefaultMonitorService;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// storage handler
		all.add(A(DefaultTopicStorageService.class));
		all.add(A(MysqlStorageHandler.class));

		all.add(A(TopicService.class));
		all.add(A(SchemaService.class));
		all.add(A(SchemaRegistryService.class));
		all.add(A(CodecService.class));
		all.add(A(CompileService.class));
		all.add(A(ConsumerService.class));
		all.add(A(SubscriptionService.class));
		all.add(A(PortalConfig.class));

		all.add(A(DefaultPortalMetaService.class));

		all.add(C(TableProvider.class, "message-priority", PortalTableProvider.class).req(PortalMetaService.class));
		all.add(C(TableProvider.class, "resend-group-id", PortalTableProvider.class).req(PortalMetaService.class));
		all.add(C(TableProvider.class, "offset-message", PortalTableProvider.class).req(PortalMetaService.class));
		all.add(C(TableProvider.class, "offset-resend", PortalTableProvider.class).req(PortalMetaService.class));
		all.add(C(TableProvider.class, "dead-letter", PortalTableProvider.class).req(PortalMetaService.class));
		all.add(A(PortalDataSourceProvider.class));

		all.add(A(DefaultHermesPortalDao.class));

		all.add(A(DefaultMonitorService.class));

		all.add(A(DefaultElasticClient.class));

		// Please keep it as last
		all.addAll(new PortalDatabaseConfigurator().defineComponents());
		all.addAll(new WebComponentConfigurator().defineComponents());

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
