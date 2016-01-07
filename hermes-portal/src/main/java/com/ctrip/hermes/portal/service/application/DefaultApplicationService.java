package com.ctrip.hermes.portal.service.application;

import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.ConsumerView;
import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.mail.HermesMail;
import com.ctrip.hermes.mail.MailService;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.dal.application.Application;
import com.ctrip.hermes.portal.dal.application.HermesApplicationDao;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

@Named(type = ApplicationService.class)
public class DefaultApplicationService implements ApplicationService {
	private static final Logger log = LoggerFactory.getLogger(DefaultApplicationService.class);

	@Inject
	private HermesApplicationDao m_dao;

	@Inject
	private MailService m_mailService;

	@Inject
	private PortalConfig m_config;

	@Override
	public TopicApplication saveTopicApplication(TopicApplication topicApplication) {
		Application dbApp = HermesApplication.toDBEntity(topicApplication);
		try {
			m_dao.saveApplication(dbApp);
		} catch (DalException e) {
			log.error("Create new topic application : {}.{}.{} failed", topicApplication.getProductLine(),
					topicApplication.getEntity(), topicApplication.getEvent(), e);
			return null;
		}

		HermesMail mail;
		try {
			mail = generateEmail(dbApp);
			m_mailService.sendEmail(mail);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return (TopicApplication) HermesApplication.parse(dbApp);
	}

	@Override
	public HermesApplication getApplicationById(long id) {
		Application dbApp = null;
		try {
			dbApp = m_dao.getAppById(id);
			return HermesApplication.parse(dbApp);
		} catch (DalException e) {
			log.error("Read application:id={} from db failed.", id, e);
		}
		return null;
	}

	@Override
	public List<HermesApplication> getApplicationsByStatus(int status) {
		List<HermesApplication> applications = new ArrayList<HermesApplication>();
		List<Application> dbApps = null;
		try {
			dbApps = m_dao.getApplicationsByStatus(status);
			for (Application dbApp : dbApps) {
				applications.add(HermesApplication.parse(dbApp));
			}
			return applications;
		} catch (DalException e) {
			log.error("Read applications: status={} from db failed.", status, e);
		}
		return new ArrayList<>();
	}

	@Override
	public TopicView generateTopicView(TopicApplication app) {
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
			kafkaProperties.add(new Property("retention.ms")
					.setValue(String.valueOf(TimeUnit.DAYS.toMillis(app.getRetentionDays()))));
			topicView.setProperties(kafkaProperties);
			defaultReadDS = "kafka-consumer";
			defaultWriteDS = "kafka-producer";
			if ("java".equals(app.getLanguageType())) {
				topicView.setEndpointType("kafka");
			} else if (".net".equals(app.getLanguageType())) {
				topicView.setEndpointType("broker");
			}

		}
		int partitionCount = 1;
		if (app.getMaxMsgNumPerDay() >= 20000000) {
			partitionCount = 20;
		} else if (app.getMaxMsgNumPerDay() >= 10000000) {
			partitionCount = 10;
		} else {
			partitionCount = 5;
		}
		List<Partition> topicPartition = new ArrayList<Partition>();
		for (int i = 0; i < partitionCount; i++) {
			Partition p = new Partition();
			p.setReadDatasource(defaultReadDS);
			p.setWriteDatasource(defaultWriteDS);
			topicPartition.add(p);
		}

		topicView.setStoragePartitionSize(5000000);
		topicView.setCreateBy(app.getOwnerName() + "/" + app.getOwnerEmail());
		topicView.setPartitions(topicPartition);
		topicView.setName(app.getProductLine() + "." + app.getEntity() + "." + app.getEvent());
		topicView.setStorageType(app.getStorageType());
		topicView.setCodecType(app.getCodecType());
		topicView.setConsumerRetryPolicy("3:[3,3000]");
		topicView.setAckTimeoutSeconds(5);
		topicView.setStoragePartitionCount(3);
		topicView.setResendPartitionSize(topicView.getStoragePartitionSize() / 10);
		topicView.setDescription(app.getDescription());

		return topicView;
	}

	@Override
	public HermesApplication updateApplication(HermesApplication app) {
		Application dbApp = null;
		try {
			app.setStatus(PortalConstants.APP_STATUS_PROCESSING);
			dbApp = HermesApplication.toDBEntity(app);
			dbApp = m_dao.updateApplication(dbApp);
			app = HermesApplication.parse(dbApp);
		} catch (Exception e) {
			log.error("Update application:id={} failed!", app.getId(), e);
			return null;
		}
		HermesMail mail;
		try {
			mail = generateEmail(dbApp);
			m_mailService.sendEmail(mail);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return app;

	}

	@Override
	public HermesApplication updateStatus(long id, int status, String comment, String approver) {
		Application dbApp = null;
		try {
			dbApp = m_dao.getAppById(id);
			dbApp.setStatus(status);
			dbApp.setComment(comment);
			dbApp.setApprover(approver);
			dbApp = m_dao.updateApplication(dbApp);
		} catch (DalException e) {
			log.error("Update status of apllication: id={} failed.", id, e);
			return null;
		}
		HermesMail mail;
		try {
			mail = generateEmail(dbApp);
			m_mailService.sendEmail(mail);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return HermesApplication.parse(dbApp);
	}

	private HermesMail generateEmail(Application app) throws Exception {
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		String title;
		String content = null;
		Map<String, Object> root = new HashMap<>();

		cfg.setDirectoryForTemplateLoading(new File(getClass().getResource("/templates").toURI()));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		String url = String.format("http://%s:%d/%s/%d", m_config.getApplicationUrl(), 80,
				"console/application#/review", app.getId());
		root.put("url", url);
		root.put("id", app.getId());
		DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		root.put("createTime", sdf.format(app.getCreateTime()));
		String statusString;
		switch (app.getStatus()) {
		case PortalConstants.APP_STATUS_PROCESSING:
			title = "Hermes申请单处理中";
			statusString = "进入处理流程";
			break;
		case PortalConstants.APP_STATUS_SUCCESS:
			title = "Hermes申请单已生效";
			statusString = "生效";
			break;
		case PortalConstants.APP_STATUS_REJECTED:
			title = "Hermes申请单已被拒绝";
			statusString = "被拒绝";
			break;
		default:
			title = "Hermes申请单状态改变";
			statusString = "改变";
			break;
		}

		root.put("status", statusString);
		Template temp = cfg.getTemplate("applicationMailTemplate.html");
		Writer out = new StringWriter();
		temp.process(root, out);
		content = out.toString();

		HermesMail mail = new HermesMail(title, content, app.getOwner() + "," + m_config.getHermesEmailGroupAddress());
		return mail;
	}

	@Override
	public ConsumerApplication saveConsumerApplication(ConsumerApplication consumerApplication) {
		Application dbApp = null;
		try {
			dbApp = HermesApplication.toDBEntity(consumerApplication);
			m_dao.saveApplication(dbApp);
		} catch (DalException e) {
			log.error("Create new consumer application : {}.{}.{} failed", consumerApplication.getProductLine(),
					consumerApplication.getProduct(), consumerApplication.getProject(), e);
			return null;
		}
		HermesMail mail;
		try {
			mail = generateEmail(dbApp);
			m_mailService.sendEmail(mail);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return (ConsumerApplication) HermesApplication.parse(dbApp);
	}

	@Override
	public ConsumerView generateConsumerView(ConsumerApplication app) {
		ConsumerView consumerView = new ConsumerView();
		consumerView.setOrderedConsume(true);
		consumerView.setTopicName(app.getTopicName());
		consumerView.setGroupName(app.getProductLine() + "." + app.getProduct() + "." + app.getProject());
		consumerView.setAckTimeoutSeconds(app.getAckTimeoutSeconds());
		consumerView.setAppId(app.getAppName());
		consumerView.setOwner(app.getOwnerName() + "/" + app.getOwnerEmail());
		if (app.isNeedRetry()) {
			consumerView
					.setRetryPolicy(String.format("3:[%s,%s]", app.getRetryCount(), app.getRetryInterval() * 1000L));
		} else {
			consumerView.setRetryPolicy("2:[]");
		}

		return consumerView;
	}
}
