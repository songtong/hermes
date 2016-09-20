package com.ctrip.hermes.metaservice.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.metaservice.dal.CachedAppDao;
import com.ctrip.hermes.metaservice.dal.CachedCodecDao;
import com.ctrip.hermes.metaservice.dal.CachedConsumerGroupDao;
import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.dal.CachedProducerDao;
import com.ctrip.hermes.metaservice.dal.CachedSchemaDao;
import com.ctrip.hermes.metaservice.dal.CachedStorageDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;
import com.ctrip.hermes.metaservice.monitor.dao.DefaultMonitorEventStorage;
import com.ctrip.hermes.metaservice.monitor.service.DefaultMonitorConfigService;
import com.ctrip.hermes.metaservice.queue.ds.MessageQueueDatasourceProvider;
import com.ctrip.hermes.metaservice.queue.ds.MessageQueueTableProvider;
import com.ctrip.hermes.metaservice.service.AppService;
import com.ctrip.hermes.metaservice.service.CacheDalService;
import com.ctrip.hermes.metaservice.service.CodecService;
import com.ctrip.hermes.metaservice.service.CompileService;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.DefaultKVService;
import com.ctrip.hermes.metaservice.service.DefaultMetaService;
import com.ctrip.hermes.metaservice.service.DefaultZookeeperEnsembleService;
import com.ctrip.hermes.metaservice.service.DefaultZookeeperService;
import com.ctrip.hermes.metaservice.service.EndpointService;
import com.ctrip.hermes.metaservice.service.IdcService;
import com.ctrip.hermes.metaservice.service.KafkaService;
import com.ctrip.hermes.metaservice.service.MetaRefactor;
import com.ctrip.hermes.metaservice.service.PartitionService;
import com.ctrip.hermes.metaservice.service.SchemaRegistryService;
import com.ctrip.hermes.metaservice.service.SchemaService;
import com.ctrip.hermes.metaservice.service.ServerService;
import com.ctrip.hermes.metaservice.service.StorageService;
import com.ctrip.hermes.metaservice.service.SubscriptionService;
import com.ctrip.hermes.metaservice.service.TopicDeployService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.mail.DefaultMailService;
import com.ctrip.hermes.metaservice.service.mail.FileMailAccountProvider;
import com.ctrip.hermes.metaservice.service.notify.DefaultNotifyService;
import com.ctrip.hermes.metaservice.service.notify.handler.EmailNotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.handler.SMSNotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.handler.TTSNotifyHandler;
import com.ctrip.hermes.metaservice.service.notify.storage.DefaultNoticeStorage;
import com.ctrip.hermes.metaservice.service.storage.DefaultTopicStorageService;
import com.ctrip.hermes.metaservice.service.storage.StorageDataSourceProvider;
import com.ctrip.hermes.metaservice.service.storage.handler.MysqlStorageHandler;
import com.ctrip.hermes.metaservice.service.template.DefaultTemplateService;
import com.ctrip.hermes.metaservice.zk.ZKClient;
import com.ctrip.hermes.metaservice.zk.ZKConfig;

public class ComponentsConfigurator extends AbstractJdbcResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(DefaultMetaService.class));
		all.add(A(DefaultZookeeperService.class));
		all.add(A(DefaultZookeeperEnsembleService.class));
		all.add(A(ZKConfig.class));
		all.add(A(ZKClient.class));

		all.add(A(StorageDataSourceProvider.class));
		all.add(A(DefaultTopicStorageService.class));

		all.add(A(MysqlStorageHandler.class));

		all.add(A(DefaultMonitorEventStorage.class));

		all.add(C(TableProvider.class, "message-priority", MessageQueueTableProvider.class) //
		      .req(CachedTopicDao.class).req(CachedPartitionDao.class));
		all.add(C(TableProvider.class, "resend-group-id", MessageQueueTableProvider.class) //
		      .req(CachedTopicDao.class).req(CachedPartitionDao.class));
		all.add(C(TableProvider.class, "offset-message", MessageQueueTableProvider.class) //
		      .req(CachedTopicDao.class).req(CachedPartitionDao.class));
		all.add(C(TableProvider.class, "offset-resend", MessageQueueTableProvider.class) //
		      .req(CachedTopicDao.class).req(CachedPartitionDao.class));
		all.add(C(TableProvider.class, "dead-letter", MessageQueueTableProvider.class) //
		      .req(CachedTopicDao.class).req(CachedPartitionDao.class));
		all.add(A(MessageQueueDatasourceProvider.class));

		all.add(A(FileMailAccountProvider.class));
		all.add(A(DefaultMailService.class));

		all.addAll(new FxhermesmetadbDatabaseConfigurator().defineComponents());
		all.addAll(new FxHermesShardDbDatabaseConfigurator().defineComponents());

		all.add(A(AppService.class));
		all.add(A(CacheDalService.class));
		all.add(A(CodecService.class));
		all.add(A(CompileService.class));
		all.add(A(ConsumerService.class));
		all.add(A(StorageService.class));
		all.add(A(DefaultZookeeperService.class));
		all.add(A(EndpointService.class));
		all.add(A(PartitionService.class));
		all.add(A(ServerService.class));
		all.add(A(SchemaService.class));
		all.add(A(SchemaRegistryService.class));
		all.add(A(SubscriptionService.class));
		all.add(A(TopicService.class));
		all.add(A(TopicDeployService.class));
		all.add(A(KafkaService.class));
		all.add(A(DefaultKVService.class));
		all.add(A(IdcService.class));

		all.add(A(DefaultNoticeStorage.class));
		all.add(A(DefaultNotifyService.class));
		all.add(A(EmailNotifyHandler.class));
		all.add(A(SMSNotifyHandler.class));
		all.add(A(TTSNotifyHandler.class));

		all.add(A(MetaRefactor.class));
		all.add(A(CachedAppDao.class));
		all.add(A(CachedCodecDao.class));
		all.add(A(CachedConsumerGroupDao.class));
		all.add(A(CachedDatasourceDao.class));
		all.add(A(CachedEndpointDao.class));
		all.add(A(CachedStorageDao.class));
		all.add(A(CachedTopicDao.class));
		all.add(A(CachedSchemaDao.class));
		all.add(A(CachedPartitionDao.class));
		all.add(A(CachedProducerDao.class));

		all.add(A(DefaultTemplateService.class));
		all.add(A(DefaultMonitorConfigService.class));

		all.add(defineJdbcDataSourceConfigurationManagerComponent("/opt/data/hermes/datasources.xml"));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
