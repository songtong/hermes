package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaservice.cmessage.DefaultCmessageConfigService;
import com.ctrip.hermes.metaservice.queue.DefaultMessageQueueDao;
import com.ctrip.hermes.metaservice.service.mail.DefaultMailService;
import com.ctrip.hermes.metaservice.service.mail.FileMailAccountProvider;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.dal.application.DefaultHermesApplicationDao;
import com.ctrip.hermes.portal.dal.tag.CachedTagDao;
import com.ctrip.hermes.portal.service.SyncService;
import com.ctrip.hermes.portal.service.TracerEsQueryService;
import com.ctrip.hermes.portal.service.TracerService;
import com.ctrip.hermes.portal.service.application.DefaultApplicationService;
import com.ctrip.hermes.portal.service.application.KafkaPartitionStrategy;
import com.ctrip.hermes.portal.service.application.MysqlPartitionStrategy;
import com.ctrip.hermes.portal.service.dashboard.DefaultDashboardService;
import com.ctrip.hermes.portal.service.elastic.DefaultPortalElasticClient;
import com.ctrip.hermes.portal.service.mail.DefaultPortalMailService;
import com.ctrip.hermes.portal.service.meta.DefaultPortalMetaService;
import com.ctrip.hermes.portal.service.tag.DefaultTagService;
import com.ctrip.hermes.portal.service.zookeeperMigration.DefaultZookeeperMigrationService;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		// storage handler
		all.add(A(DefaultTopicStorageService.class));
		all.add(A(MysqlStorageHandler.class));

		all.add(A(PortalConfig.class));

		all.add(A(DefaultMessageQueueDao.class));
		all.add(A(DefaultHermesApplicationDao.class));

		all.add(A(DefaultDashboardService.class));

		all.add(A(DefaultPortalElasticClient.class));

		all.add(A(DefaultApplicationService.class));

		all.add(A(DefaultMailService.class));
		all.add(A(DefaultPortalMailService.class));

		all.add(A(FileMailAccountProvider.class));

		all.add(A(SyncService.class));

		all.add(A(TracerEsQueryService.class));

		all.add(A(TracerService.class));

		all.add(A(CachedTagDao.class));

		all.add(A(DefaultTagService.class));

		all.add(A(MysqlPartitionStrategy.class));

		all.add(A(KafkaPartitionStrategy.class));

		all.add(A(DefaultPortalMetaService.class));
		
		all.add(A(DefaultCmessageConfigService.class));

		all.add(A(DefaultZookeeperMigrationService.class));

		// Please keep it as last
		all.addAll(new WebComponentConfigurator().defineComponents());
		all.addAll(new FxHermesMetaDatabaseConfigurator().defineComponents());
		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
