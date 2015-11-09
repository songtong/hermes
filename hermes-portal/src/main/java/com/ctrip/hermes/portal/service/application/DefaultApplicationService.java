package com.ctrip.hermes.portal.service.application;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.dal.application.Application;
import com.ctrip.hermes.portal.dal.application.HermesApplicationDao;

@Named(type = ApplicationService.class)
public class DefaultApplicationService implements ApplicationService {
	private static final Logger log = LoggerFactory.getLogger(DefaultApplicationService.class);

	@Inject
	private HermesApplicationDao m_dao;

	@Override
	public TopicApplication createTopicApplication(TopicApplication topicApplication) {
		try {
			long id = m_dao.saveApplication(topicApplication.toDBEntity());
			return (TopicApplication) this.getApplicationById(id);
		} catch (DalException e) {
			log.error("Create new topic application : {}.{}.{} failed", topicApplication.getProductLine(),
					topicApplication.getEntity(), topicApplication.getEvent(), e);
		}
		return null;
	}

	@Override
	public HermesApplication getApplicationById(long id) {
		HermesApplication app = null;
		Application dbApp = null;
		try {
			dbApp = m_dao.getAppById(id);
		} catch (DalException e) {
			log.error("Read application:id={} from db failed.", id, e);
		}
		app = HermesApplication.parse(dbApp);
		return app;
	}

	@Override
	public List<HermesApplication> getApplicationsByStatus(int status) {
		List<HermesApplication> applications = new ArrayList<HermesApplication>();
		List<Application> dbApps = null;
		try {
			dbApps = m_dao.getApplicationsByStatus(status);
		} catch (DalException e) {
			log.error("Read applications: status={} from db failed.", status, e);
		}
		for (Application dbApp : dbApps) {
			applications.add(HermesApplication.parse(dbApp));
		}
		return applications;
	}
	
	
	@Override
	public TopicView generageTopicView(TopicApplication app) {
		TopicView topicView = new TopicView();

		String defaultReadDS = "ds0";
		String defaultWriteDS = "ds0";
		if ("mysql".equals(app.getStorageType())) {
			topicView.setEndpointType("broker");
			switch (app.getProductLine()) {
			case "flight":
				defaultReadDS = "ds2";
				defaultWriteDS = "ds2";
				break;
			case "hotel":
				defaultReadDS = "ds1";
				defaultWriteDS = "ds1";
				break;
			}
		} else if ("kafka".equals(app.getStorageType())) {
			List<Property> kafkaProperties = new ArrayList<>();
			kafkaProperties.add(new Property("partitions").setValue("3"));
			kafkaProperties.add(new Property("replication-factor").setValue("2"));
			kafkaProperties.add(new Property("retention.ms").setValue(""+(app.getRetentionDays()*24*3600*1000)));
			defaultReadDS = "kafka-consumer";
			defaultWriteDS = "kafka-producer";
			if ("java".equals(app.getLanguageType())) {
				topicView.setEndpointType("kafka");
			} else if (".net".equals(app.getLanguageType())) {
				topicView.setEndpointType("broker");
			}
		}
		int partitionCount = 1;
		int storagePartitionCount = 10;
		if (app.getMaxMsgNumPerDay() > 10000000) {
			partitionCount = 10;
			storagePartitionCount = partitionCount / 1000000 * app.getRetentionDays() / 10;
		} else if (app.getMaxMsgNumPerDay() > 1000000) {
			partitionCount = 5;
			storagePartitionCount = partitionCount / 1000000 * app.getRetentionDays() / 5;
		} else {
			partitionCount = 3;
			storagePartitionCount = partitionCount * app.getRetentionDays() / 1000000 / 5;
		}
		if (storagePartitionCount < 5) {
			storagePartitionCount = 5;
		}
		List<Partition> topicPartition = new ArrayList<Partition>();
		for (int i = 0; i < partitionCount; i++) {
			Partition p = new Partition();
			p.setReadDatasource(defaultReadDS);
			p.setWriteDatasource(defaultWriteDS);
			topicPartition.add(p);
		}

		topicView.setPartitions(topicPartition);
		topicView.setName(app.getProductLine() + "." + app.getEntity() + "." + app.getEvent());
		topicView.setStorageType(app.getStorageType());
		topicView.setCodecType(app.getCodecType());
		topicView.setConsumerRetryPolicy("1:[3,3,3]");
		topicView.setAckTimeoutSeconds(5);
		topicView.setStoragePartitionSize(1000000);
		topicView.setResendPartitionSize(5000);
		topicView.setStoragePartitionCount(storagePartitionCount);
		topicView.setDescription(app.getDescription());
		
		return topicView;
	}

}
