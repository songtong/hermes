package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaservice.queue.DefaultMessageQueueDao;
import com.ctrip.hermes.metaservice.service.CodecService;
import com.ctrip.hermes.metaservice.service.CompileService;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.DefaultPortalMetaService;
import com.ctrip.hermes.metaservice.service.SchemaRegistryService;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.SubscriptionService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.dal.application.DefaultHermesApplicationDao;
import com.ctrip.hermes.portal.service.application.DefaultApplicationService;
import com.ctrip.hermes.portal.service.dashboard.DefaultDashboardService;
import com.ctrip.hermes.portal.service.elastic.DefaultPortalElasticClient;

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

		all.add(A(DefaultMessageQueueDao.class));
		all.add(A(DefaultHermesApplicationDao.class));

		all.add(A(DefaultDashboardService.class));

		all.add(A(DefaultPortalElasticClient.class));
		
		all.add(A(DefaultApplicationService.class));


		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());
		all.addAll(new FxHermesMetaDatabaseConfigurator().defineComponents());
		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
