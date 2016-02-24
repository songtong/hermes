package com.ctrip.hermes.portal.service.application;

import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
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

import com.ctrip.hermes.mail.HermesMail;
import com.ctrip.hermes.mail.MailService;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.metaservice.view.TopicView;
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
	private TopicService m_topicService;

	@Inject
	private ConsumerService m_consumerService;

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

		try {
			HermesMail mailToProposer = generateApplicationEmailForProposer(dbApp);
			HermesMail mailToHermes = generateApplicationEmailForHermes(dbApp);
			m_mailService.sendEmail(mailToProposer);
			m_mailService.sendEmail(mailToHermes);
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
		topicView.setOwner1(app.getOwnerName1() + "/" + app.getOwnerEmail1());
		topicView.setOwner2(app.getOwnerName2() + "/" + app.getOwnerEmail2());
		topicView.setPhone1(app.getOwnerPhone1());
		topicView.setPhone2(app.getOwnerPhone2());
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
		try {
			HermesMail mailToProposer = generateApplicationEmailForProposer(dbApp);
			HermesMail mailToHermes = generateApplicationEmailForHermes(dbApp);
			m_mailService.sendEmail(mailToProposer);
			m_mailService.sendEmail(mailToHermes);
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
		try {
			HermesMail mailToProposer = generateApplicationEmailForProposer(dbApp);
			HermesMail mailToHermes = generateApplicationEmailForHermes(dbApp);
			m_mailService.sendEmail(mailToProposer);
			m_mailService.sendEmail(mailToHermes);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return HermesApplication.parse(dbApp);
	}

	private String getApplicationEmailTitle(int status) {
		String title;
		switch (status) {
		case PortalConstants.APP_STATUS_PROCESSING:
			title = "Hermes申请单处理中";
			break;
		case PortalConstants.APP_STATUS_SUCCESS:
			title = "Hermes申请单已生效";
			break;
		case PortalConstants.APP_STATUS_REJECTED:
			title = "Hermes申请单已被拒绝";
			break;
		default:
			title = "Hermes申请单状态改变";
			break;
		}
		return title;
	}

	private HermesMail generateApplicationEmailForProposer(Application app) throws Exception {
		String title = getApplicationEmailTitle(app.getStatus());
		String approver = app.getOwner1() + "," + app.getOwner2();
		String content = generateMailContentForProposer(app);
		HermesMail mail = new HermesMail(title, content, approver);
		return mail;
	}

	private HermesMail generateApplicationEmailForHermes(Application app) throws Exception {
		String title = getApplicationEmailTitle(app.getStatus());
		String approver = m_config.getHermesEmailGroupAddress();
		String content = generateMailContentForHermes(app);
		HermesMail mail = new HermesMail(title, content, approver);
		return mail;
	}

	private String generateMailContentForProposer(Application app) throws Exception {
		String content = null;
		switch (app.getType()) {
		case PortalConstants.APP_TYPE_CREATE_TOPIC:
			content = generateCreateTopicMailTemplateForProposer(app);
			break;
		case PortalConstants.APP_TYPE_CREATE_CONSUMER:
			content = generateCreateConsumerMailTemplateForProposer(app);
			break;
		default:
			break;
		}
		return content;
	}

	private String generateMailContentForHermes(Application app) throws Exception {
		String content = null;
		switch (app.getType()) {
		case PortalConstants.APP_TYPE_CREATE_TOPIC:
			content = generateCreateTopicMailTemplateForHermes(app);
			break;
		case PortalConstants.APP_TYPE_CREATE_CONSUMER:
			content = generateCreateConsumerMailTemplateForHermes(app);
			break;
		default:
			break;
		}
		return content;
	}

	private String generateCreateTopicMailTemplateForProposer(Application app) throws Exception {
		TopicApplication topicApplication = (TopicApplication) HermesApplication.parse(app);
		String content = null;
		Map<String, Object> mailContent = new HashMap<>();
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		cfg.setDirectoryForTemplateLoading(new File(getClass().getResource(m_config.getEmailTemplateDir()).toURI()));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		Template temp = cfg.getTemplate(PortalConstants.APP_EMAIL_TEMPLATE_CREATE_TOPIC_FOR_PROPOSER);

		mailContent.put("createTime",
				new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(topicApplication.getCreateTime()));
		mailContent.put("url", String.format("http://%s/%s/%d", m_config.getPortalFwsUrl(),
				"console/application#/review", topicApplication.getId()));
		mailContent.put("app", topicApplication);
		mailContent.put("status", getApplicationStatusString(topicApplication.getStatus()));
		if (PortalConstants.APP_STATUS_REJECTED == topicApplication.getStatus())
			mailContent.put("rejectReason", topicApplication.getComment());
		if (PortalConstants.APP_STATUS_SUCCESS == topicApplication.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicApplication.getProductLine() + "."
					+ topicApplication.getEntity() + "." + topicApplication.getEvent());
			mailContent.put("topic", topic);
			mailContent.put("fwsTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalFwsUrl(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
			mailContent.put("uatTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalUatHost(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
			mailContent.put("prodTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalProdHost(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
		}

		Writer out = new StringWriter();
		temp.process(mailContent, out);
		content = out.toString();

		return content;
	}

	private String generateCreateConsumerMailTemplateForProposer(Application app) throws Exception {
		String content = null;
		ConsumerApplication consumerApplication = (ConsumerApplication) HermesApplication.parse(app);
		Map<String, Object> mailContent = new HashMap<>();
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		cfg.setDirectoryForTemplateLoading(new File(getClass().getResource(m_config.getEmailTemplateDir()).toURI()));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		Template temp = cfg.getTemplate(PortalConstants.APP_EMAIL_TEMPLATE_CREATE_CONSUMER_FOR_PROPOSER);

		mailContent.put("createTime",
				new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(consumerApplication.getCreateTime()));
		mailContent.put("url", String.format("http://%s/%s/%d", m_config.getPortalFwsUrl(),
				"console/application#/review", consumerApplication.getId()));
		mailContent.put("app", consumerApplication);
		mailContent.put("status", getApplicationStatusString(consumerApplication.getStatus()));
		if (PortalConstants.APP_STATUS_REJECTED == consumerApplication.getStatus())
			mailContent.put("rejectReason", consumerApplication.getComment());
		if (PortalConstants.APP_STATUS_SUCCESS == consumerApplication.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(consumerApplication.getTopicName());
			ConsumerGroupView consumer = m_consumerService.findConsumerView(topic.getId(),
					consumerApplication.getProductLine() + "." + consumerApplication.getProduct() + "."
							+ consumerApplication.getProject());
			mailContent.put("consumer", consumer);
		}

		Writer out = new StringWriter();
		temp.process(mailContent, out);
		content = out.toString();

		return content;
	}

	private String generateCreateTopicMailTemplateForHermes(Application app) throws Exception {
		TopicApplication topicApplication = (TopicApplication) HermesApplication.parse(app);
		String content = null;
		Map<String, Object> mailContent = new HashMap<>();
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		cfg.setDirectoryForTemplateLoading(new File(getClass().getResource(m_config.getEmailTemplateDir()).toURI()));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		Template temp = cfg.getTemplate(PortalConstants.APP_EMAIL_TEMPLATE_CREATE_TOPIC_FOR_HERMES);

		mailContent.put("createTime",
				new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(topicApplication.getCreateTime()));
		mailContent.put("url", String.format("http://%s/%s/%d", m_config.getPortalFwsUrl(),
				"console/application#/approval", topicApplication.getId()));
		mailContent.put("app", topicApplication);
		mailContent.put("status", getApplicationStatusString(topicApplication.getStatus()));
		if (PortalConstants.APP_STATUS_REJECTED == topicApplication.getStatus())
			mailContent.put("rejectReason", topicApplication.getComment());
		if (PortalConstants.APP_STATUS_SUCCESS == topicApplication.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicApplication.getProductLine() + "."
					+ topicApplication.getEntity() + "." + topicApplication.getEvent());
			mailContent.put("topic", topic);
			mailContent.put("fwsTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalFwsUrl(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
			mailContent.put("uatTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalUatHost(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
			mailContent.put("prodTopicUrl", String.format("http://%s/%s/%s/%s/%s", m_config.getPortalProdHost(),
					"console/topic#/detail", topic.getStorageType(), topic.getStorageType(), topic.getName()));
		}

		Writer out = new StringWriter();
		temp.process(mailContent, out);
		content = out.toString();

		return content;
	}

	private String generateCreateConsumerMailTemplateForHermes(Application app) throws Exception {
		String content = null;
		ConsumerApplication consumerApplication = (ConsumerApplication) HermesApplication.parse(app);
		Map<String, Object> mailContent = new HashMap<>();
		Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
		cfg.setDirectoryForTemplateLoading(new File(getClass().getResource(m_config.getEmailTemplateDir()).toURI()));
		cfg.setDefaultEncoding("UTF-8");
		cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		Template temp = cfg.getTemplate(PortalConstants.APP_EMAIL_TEMPLATE_CREATE_CONSUMER_FOR_HERMES);

		mailContent.put("createTime",
				new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(consumerApplication.getCreateTime()));
		mailContent.put("url", String.format("http://%s/%s/%d", m_config.getPortalFwsUrl(),
				"console/application#/approval", consumerApplication.getId()));
		mailContent.put("app", consumerApplication);
		mailContent.put("status", getApplicationStatusString(consumerApplication.getStatus()));
		if (PortalConstants.APP_STATUS_REJECTED == consumerApplication.getStatus())
			mailContent.put("rejectReason", consumerApplication.getComment());
		if (PortalConstants.APP_STATUS_SUCCESS == consumerApplication.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(consumerApplication.getTopicName());
			ConsumerGroupView consumer = m_consumerService.findConsumerView(topic.getId(),
					consumerApplication.getProductLine() + "." + consumerApplication.getProduct() + "."
							+ consumerApplication.getProject());
			mailContent.put("consumer", consumer);
		}

		Writer out = new StringWriter();
		temp.process(mailContent, out);
		content = out.toString();

		return content;
	}

	private String getApplicationStatusString(int status) {
		String statusString;
		switch (status) {
		case PortalConstants.APP_STATUS_PROCESSING:
			statusString = "进入处理流程";
			break;
		case PortalConstants.APP_STATUS_SUCCESS:
			statusString = "生效";
			break;
		case PortalConstants.APP_STATUS_REJECTED:
			statusString = "被拒绝";
			break;
		default:
			statusString = "改变";
			break;
		}
		return statusString;
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
		try {
			HermesMail mailToProposer = generateApplicationEmailForProposer(dbApp);
			HermesMail mailToHermes = generateApplicationEmailForHermes(dbApp);
			m_mailService.sendEmail(mailToProposer);
			m_mailService.sendEmail(mailToHermes);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", dbApp.getId(), e);
		}
		return (ConsumerApplication) HermesApplication.parse(dbApp);
	}

	@Override
	public ConsumerGroupView generateConsumerView(ConsumerApplication app) {
		ConsumerGroupView consumerView = new ConsumerGroupView();
		consumerView.setOrderedConsume(true);
		consumerView.setTopicName(app.getTopicName());
		consumerView.setName(app.getProductLine() + "." + app.getProduct() + "." + app.getProject());
		consumerView.setAckTimeoutSeconds(app.getAckTimeoutSeconds());
		consumerView.setAppIds(app.getAppName());
		consumerView.setOwner1(app.getOwnerName1() + "/" + app.getOwnerEmail1());
		consumerView.setOwner2(app.getOwnerName2() + "/" + app.getOwnerEmail2());
		consumerView.setPhone1(app.getOwnerPhone1());
		consumerView.setPhone2(app.getOwnerPhone2());
		if (app.isNeedRetry()) {
			consumerView
					.setRetryPolicy(String.format("3:[%s,%s]", app.getRetryCount(), app.getRetryInterval() * 1000L));
		} else {
			consumerView.setRetryPolicy("2:[]");
		}

		return consumerView;
	}
}
