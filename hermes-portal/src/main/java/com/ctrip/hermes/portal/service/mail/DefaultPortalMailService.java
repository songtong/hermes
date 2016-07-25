package com.ctrip.hermes.portal.service.mail;

import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.mail.HermesMail;
import com.ctrip.hermes.metaservice.service.mail.MailService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.metaservice.view.SchemaView;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.config.PortalConstants;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

@Named(type = PortalMailService.class)
public class DefaultPortalMailService implements PortalMailService, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultPortalMailService.class);

	@Inject
	private MailService m_mailService;

	@Inject
	private TopicService m_topicService;

	@Inject
	private ConsumerService m_consumerService;

	@Inject
	private PortalConfig m_config;

	@Inject
	private ClientEnvironment m_env;

	private Configuration m_templateConfig;

	private final SimpleDateFormat m_dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	private static final Pattern EMAIL_PATTERN = //
	Pattern.compile("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}", Pattern.CASE_INSENSITIVE);

	@Override
	public void initialize() throws InitializationException {
		try {
			m_templateConfig = new Configuration(Configuration.VERSION_2_3_22);
			m_templateConfig.setDirectoryForTemplateLoading(new File(getClass()
			      .getResource(m_config.getEmailTemplateDir()).toURI()));
			m_templateConfig.setDefaultEncoding("UTF-8");
			m_templateConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		} catch (Exception e) {
			log.error("Initialize mail template configration failed!", e);
		}
	}

	private void sendEmail(String title, String address, String template, Map<String, Object> contentMap) {
		Template temp;
		try {
			temp = m_templateConfig.getTemplate(template);
			Writer out = new StringWriter();
			temp.process(contentMap, out);
			String content = out.toString();

			HermesMail mail = new HermesMail(title, content, address);
			m_mailService.sendEmail(mail);
		} catch (Exception e) {
			log.warn("Send Application Mail failed: title={}, address={}, template={}, contentMap={}.", title, address,
			      template, contentMap, e);
		}

	}

	@Override
	public void sendApplicationMail(HermesApplication app) {
		switch (app.getType()) {
		case PortalConstants.APP_TYPE_CREATE_TOPIC:
			TopicApplication topicApp = (TopicApplication) app;
			sendCreateTopicMailToProposer(topicApp);
			sendCreateTopicMailToAdmin(topicApp);
			break;
		case PortalConstants.APP_TYPE_CREATE_CONSUMER:
			ConsumerApplication consumerApp = (ConsumerApplication) app;
			sendCreateConsumerMailToProposer(consumerApp);
			sendCreateConsumerMailToAdmin(consumerApp);
			break;
		default:
			log.warn("Send Application Mail failed with wrong application type: type={}, app={}", app.getType(), app);
		}
	}

	private void sendCreateTopicMailToProposer(TopicApplication app) {
		String topicName = app.getProductLine() + "." + app.getEntity() + "." + app.getEvent();
		String status = getApplicationStatusString(app.getStatus());

		String title = String.format("[Hermes申请单(TC%06d)%s] Topic：%s", app.getId(), status, topicName);
		String address = app.getOwnerEmail1() + "," + app.getOwnerEmail2();

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("createTime", m_dateFormatter.format(app.getCreateTime()));
		contentMap.put("url",
		      String.format("http://%s/%s/%d", m_config.getPortalFwsHost(), "console/application#/review", app.getId()));
		contentMap.put("app", app);
		contentMap.put("status", status);
		if (PortalConstants.APP_STATUS_REJECTED == app.getStatus()
		      || PortalConstants.APP_STATUS_ROLLOUT_REJECTED == app.getStatus())
			contentMap.put("rejectReason", app.getComment());
		if (PortalConstants.APP_STATUS_SYNCED == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicName);
			contentMap.put("topic", topic);
			contentMap.put(
			      "fwsTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalFwsHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));
			contentMap.put(
			      "uatTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalUatHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));
		} else if (PortalConstants.APP_STATUS_ONLINE == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicName);
			contentMap.put("topic", topic);
			contentMap.put(
			      "prodTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalProdHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));
		}

		sendEmail(title, address, PortalConstants.APP_EMAIL_TEMPLATE_CREATE_TOPIC_FOR_PROPOSER, contentMap);
	}

	private void sendCreateTopicMailToAdmin(TopicApplication app) {
		String topicName = app.getProductLine() + "." + app.getEntity() + "." + app.getEvent();
		String status = getApplicationStatusString(app.getStatus());

		String title = String.format("[Hermes申请单(TC%06d)%s] Topic：%s", app.getId(), status, topicName);
		String address = m_config.getHermesEmailGroupAddress();

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("createTime", m_dateFormatter.format(app.getCreateTime()));
		contentMap
		      .put("url",
		            String.format("http://%s/%s/%d", m_config.getPortalFwsHost(), "console/application#/approval",
		                  app.getId()));
		contentMap.put("app", app);
		contentMap.put("status", status);
		if (PortalConstants.APP_STATUS_REJECTED == app.getStatus()
		      || PortalConstants.APP_STATUS_ROLLOUT_REJECTED == app.getStatus())
			contentMap.put("rejectReason", app.getComment());
		if (PortalConstants.APP_STATUS_SYNCED == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicName);
			contentMap.put("topic", topic);
			contentMap.put(
			      "fwsTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalFwsHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));
			contentMap.put(
			      "uatTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalUatHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));

		} else if (PortalConstants.APP_STATUS_ONLINE == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicName);
			contentMap.put("topic", topic);
			contentMap.put(
			      "prodTopicUrl",
			      String.format("http://%s/%s/%s/%s/%s", m_config.getPortalProdHost(), "console/topic#/detail",
			            topic.getStorageType(), topic.getStorageType(), topic.getName()));
		}

		sendEmail(title, address, PortalConstants.APP_EMAIL_TEMPLATE_CREATE_TOPIC_FOR_ADMIN, contentMap);

	}

	private void sendCreateConsumerMailToProposer(ConsumerApplication app) {
		String consumerName = String.format("%s.%s.%s", app.getProductLine(), app.getProduct(), app.getProject());
		String status = getApplicationStatusString(app.getStatus());
		String[] topicNames = app.getTopicName().split(",");

		String title = String.format("[Hermes申请单(CC%06d)%s] Consumer：%s", app.getId(), status, consumerName);
		String address = app.getOwnerEmail1() + "," + app.getOwnerEmail2();

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("createTime", m_dateFormatter.format(app.getCreateTime()));
		contentMap.put("url",
		      String.format("http://%s/%s/%d", m_config.getPortalFwsHost(), "console/application#/review", app.getId()));
		contentMap.put("app", app);
		contentMap.put("status", status);
		contentMap.put("topicNames", topicNames);
		contentMap.put("topicCount", topicNames.length);
		if (PortalConstants.APP_STATUS_REJECTED == app.getStatus()
		      || PortalConstants.APP_STATUS_ROLLOUT_REJECTED == app.getStatus()) {
			contentMap.put("rejectReason", app.getComment());
		} else if (PortalConstants.APP_STATUS_SYNCED == app.getStatus()
		      || PortalConstants.APP_STATUS_ONLINE == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicNames[0]);
			ConsumerGroupView consumer = m_consumerService.findConsumerView(topic.getId(), consumerName);

			if (consumer == null) {
				log.error("Find consumer failed: consumerName=%s, topic=%s.", consumerName, app.getTopicName());
			}

			contentMap.put("consumer", consumer);
		}

		sendEmail(title, address, PortalConstants.APP_EMAIL_TEMPLATE_CREATE_CONSUMER_FOR_PROPOSER, contentMap);

	}

	private void sendCreateConsumerMailToAdmin(ConsumerApplication app) {
		String consumerName = String.format("%s.%s.%s", app.getProductLine(), app.getProduct(), app.getProject());
		String status = getApplicationStatusString(app.getStatus());
		String[] topicNames = app.getTopicName().split(",");

		String title = String.format("[Hermes申请单(CC%06d)%s] Consumer：%s", app.getId(), status, consumerName);
		String address = m_config.getHermesEmailGroupAddress();

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("createTime", m_dateFormatter.format(app.getCreateTime()));
		contentMap
		      .put("url",
		            String.format("http://%s/%s/%d", m_config.getPortalFwsHost(), "console/application#/approval",
		                  app.getId()));
		contentMap.put("app", app);
		contentMap.put("status", status);
		contentMap.put("topicNames", topicNames);
		contentMap.put("topicCount", topicNames.length);
		if (PortalConstants.APP_STATUS_REJECTED == app.getStatus()
		      || PortalConstants.APP_STATUS_ROLLOUT_REJECTED == app.getStatus()) {
			contentMap.put("rejectReason", app.getComment());
		} else if (PortalConstants.APP_STATUS_SYNCED == app.getStatus()
		      || PortalConstants.APP_STATUS_ONLINE == app.getStatus()) {
			TopicView topic = m_topicService.findTopicViewByName(topicNames[0]);
			ConsumerGroupView consumer = m_consumerService.findConsumerView(topic.getId(), consumerName);
			if (consumer == null) {
				log.error("Find consumer failed: consumerName=%s, topic=%s.", consumerName, app.getTopicName());
			}
			contentMap.put("consumer", consumer);
		}

		sendEmail(title, address, PortalConstants.APP_EMAIL_TEMPLATE_CREATE_CONSUMER_FOR_ADMIN, contentMap);
	}

	private String getApplicationStatusString(int status) {
		String statusString;
		switch (status) {
		case PortalConstants.APP_STATUS_PROCESSING:
			statusString = "处理中";
			break;
		case PortalConstants.APP_STATUS_SYNCED:
			statusString = "生效";
			break;
		case PortalConstants.APP_STATUS_ONLINE:
			statusString = "生效";
			break;
		case PortalConstants.APP_STATUS_REJECTED:
			statusString = "被拒绝";
			break;
		case PortalConstants.APP_STATUS_ROLLOUT_REJECTED:
			statusString = "被拒绝";
			break;
		default:
			statusString = "改变";
			break;
		}
		return statusString;
	}

	@Override
	public void sendUploadSchemaMail(SchemaView schema, String mailAddress, String userName) {
		Topic topic = m_topicService.findTopicEntityById(schema.getTopicId());
		List<ConsumerGroupView> consumers = new ArrayList<>();
		try {
			consumers = m_consumerService.findConsumerViews(schema.getTopicId());
		} catch (DalException e) {
			log.error("Failed to get consumers for topic:{}!", topic.getName(), e);
		}
		String environment = m_env.getEnv().name();
		String host = m_config.getCurrentPortalHost();
		String jarUrl = String.format("%s/api/schemas/%s/jar", host, schema.getId());
		String csUrl = String.format("%s/api/schemas/%s/cs", host, schema.getId());
		String schemaUrl = String.format("%s/api/schemas/%s/schema", host, schema.getId());

		String title = String.format("[Hermes schema变更]环境：%s, Topic： %s", environment, topic.getName());
		String address = m_config.getHermesEmailGroupAddress();
		for (ConsumerGroupView c : consumers) {
			String mail1 = getValidMailAddressFromOwner(c.getOwner1());
			String mail2 = getValidMailAddressFromOwner(c.getOwner2());
			address = addAddress(address, mail1);
			address = addAddress(address, mail2);
		}

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("schema", schema);
		contentMap.put("topic", topic);
		contentMap.put("environment", environment);
		contentMap.put("userName", userName);
		contentMap.put("userMail", mailAddress);
		contentMap.put("jarUrl", jarUrl);
		contentMap.put("csUrl", csUrl);
		contentMap.put("schemaUrl", schemaUrl);

		sendEmail(title, address, PortalConstants.UPLOAD_SCHEMA_EMAIL_TEMPLATE, contentMap);

	}

	private String addAddress(String address, String mail) {
		if (address == null || address.isEmpty()) {
			return mail;
		}
		return address + "," + mail;
	}

	public static String getValidMailAddressFromOwner(String email) {
		if (!StringUtils.isBlank(email)) {
			java.util.regex.Matcher matcher = EMAIL_PATTERN.matcher(email);
			if (matcher.find()) {
				return matcher.group();
			}
		}
		return null;
	}

	@Override
	public void sendCreateTopicFromCatMail(com.ctrip.hermes.metaservice.model.Topic topic) {
		String environment = m_env.getEnv().name();
		String title = String.format("[Hermes new topic]名称：%s, 环境：%s", topic.getName(), environment);
		String address = m_config.getHermesEmailGroupAddress();

		Map<String, Object> contentMap = new HashMap<>();
		contentMap.put("topic", topic);
		contentMap.put("environment", environment);

		sendEmail(title, address, PortalConstants.CREATE_TOPIC_FROM_CAT_EMAIL_TEMPLATE, contentMap);

	}

}
