package com.ctrip.hermes.portal.resource;

import java.util.Date;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.HermesApplicationType;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.application.ApplicationService;
import com.ctrip.hermes.portal.service.mail.PortalMailService;
import com.ctrip.hermes.portal.util.ResponseUtils;

@Path("/applications/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ApplicationResource {
	private static final Logger log = LoggerFactory.getLogger(ApplicationResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private ApplicationService appService = PlexusComponentLocator.lookup(ApplicationService.class);

	private PortalMailService m_mailService = PlexusComponentLocator.lookup(PortalMailService.class);

	@POST
	@Path("topic/create")
	public Response createTopicApplication(String content) {
		log.info("Submitting topic application with payload", content);
		if (StringUtils.isEmpty(content)) {
			log.error("Payload content is empty, submit application failed.");
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		TopicApplication topicApplication = null;
		// parse
		try {
			topicApplication = JSON.parseObject(content, TopicApplication.class);
		} catch (Exception e) {
			log.error("Can not parse payload : {}, submit topic application failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		// 参数校验
		Pair<Boolean, String> result = validateTopicApplication(topicApplication);
		if (!result.getKey()) {
			throw new RestException((String) result.getValue());
		}

		// 检验重复
		String topicName = String.format("%s.%s.%s", topicApplication.getProductLine(), topicApplication.getEntity(),
				topicApplication.getEvent());
		if (topicService.findTopicEntityByName(topicName) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}

		topicApplication.setContent(content);
		topicApplication.setType(PortalConstants.APP_TYPE_CREATE_TOPIC);
		topicApplication.setStatus(PortalConstants.APP_STATUS_PROCESSING);
		topicApplication.setCreateTime(new Date(System.currentTimeMillis()));

		// 存入数据库
		topicApplication = appService.saveTopicApplication(topicApplication);
		if (topicApplication == null) {
			throw new RestException("Save topic application failed!", Status.INTERNAL_SERVER_ERROR);
		}

		try {
			m_mailService.sendApplicationMail(topicApplication);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", topicApplication.getId(), e);
		}

		// 返回
		return Response.status(Status.CREATED).entity(topicApplication).build();
	}

	@POST
	@Path("consumer/create")
	public Response createConsumerApplication(String content) {
		log.info("Submitting  consumer application with payload", content);
		if (StringUtils.isBlank(content)) {
			log.error("Payload content is empty, submit consuemr application failed");
			throw new RestException("HTTP Post body is empty!", Status.BAD_REQUEST);
		}
		ConsumerApplication app = null;
		try {
			app = JSON.parseObject(content, ConsumerApplication.class);
		} catch (Exception e) {
			log.error("Can not parse payload: {}, submit consumer application failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		Pair<Boolean, String> result = validateConsumerApplication(app);
		if (result.getKey() == false) {
			throw new RestException(result.getValue());
		}

		app.setContent(content);
		app.setType(PortalConstants.APP_TYPE_CREATE_CONSUMER);
		app.setStatus(PortalConstants.APP_STATUS_PROCESSING);
		app.setCreateTime(new Date(System.currentTimeMillis()));

		app = appService.saveConsumerApplication(app);
		if (app == null) {
			throw new RestException("Save consumer application failed!", Status.INTERNAL_SERVER_ERROR);
		}

		try {
			m_mailService.sendApplicationMail(app);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", app.getId(), e);
		}

		return Response.status(Status.OK).entity(app).build();

	}

	private Pair<Boolean, String> validateTopicApplication(TopicApplication app) {
		boolean pass = true;
		String reason = "";
		if (StringUtils.isBlank(app.getProductLine())) {
			pass = false;
			reason = "Topic producLine should not be null!";
		} else if (StringUtils.isBlank(app.getEntity())) {
			pass = false;
			reason = "Topic entity should not be null!";
		} else if (StringUtils.isBlank(app.getEvent())) {
			pass = false;
			reason = "Topic event should not be null!";
		} else if (app.getMaxMsgNumPerDay() <= 0) {
			pass = false;
			reason = "消息上限不能小于0!";
		} else if (app.getRetentionDays() <= 0) {
			pass = false;
			reason = "消息保留天数不能小于0!";
		} else if (app.getSize() <= 0) {
			pass = false;
			reason = "消息大小不能小于0!";
		} else if (StringUtils.isBlank(app.getOwnerName1())) {
			pass = false;
			reason = "负责人1不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerPhone1()) || app.getOwnerPhone1().length() != 11) {
			pass = false;
			reason = "负责人1手机填写不正确！";
		} else if (StringUtils.isBlank(app.getOwnerEmail1())) {
			pass = false;
			reason = "负责人1邮箱不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerName2())) {
			pass = false;
			reason = "负责人2不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerPhone2()) || app.getOwnerPhone2().length() != 11) {
			pass = false;
			reason = "负责人2手机填写不正确！";
		} else if (StringUtils.isBlank(app.getOwnerEmail2())) {
			pass = false;
			reason = "负责人2邮箱不能为空！";
		}

		return new Pair<Boolean, String>(pass, reason);

	}

	private Pair<Boolean, String> validateConsumerApplication(ConsumerApplication app) {
		boolean pass = true;
		String reason = "";
		if (StringUtils.isBlank(app.getTopicName())) {
			pass = false;
			reason = "Topic name should not be null.";
		} else if (StringUtils.isBlank(app.getProductLine())) {
			pass = false;
			reason = "Product line should not be null.";
		} else if (StringUtils.isBlank(app.getProduct())) {
			pass = false;
			reason = "Product should not be null.";
		} else if (StringUtils.isBlank(app.getProject())) {
			pass = false;
			reason = "Project should not be null.";
		} else if (StringUtils.isBlank(app.getAppName())) {
			pass = false;
			reason = "App name should not be null.";
		} else if (StringUtils.isBlank(app.getOwnerName1())) {
			pass = false;
			reason = "负责人1不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerPhone1())) {
			pass = false;
			reason = "负责人1联系电话不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerEmail1())) {
			pass = false;
			reason = "负责人1邮箱不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerName2())) {
			pass = false;
			reason = "负责人2不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerPhone2())) {
			pass = false;
			reason = "负责人2联系电话不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerEmail2())) {
			pass = false;
			reason = "负责人2邮箱不能为空！";
		} else {
			String[] topicNames = app.getTopicName().split(",");
			for (String topicName : topicNames) {
				Topic t = topicService.findTopicEntityByName(topicName);
				for (ConsumerGroup c : t.getConsumerGroups()) {
					String currentConsuemrName = app.getProductLine() + "." + app.getProduct() + "." + app.getProject();
					if (currentConsuemrName.equals(c.getName())) {
						pass = false;
						reason = "当前Topic下已有ConsumerGroup名字为" + currentConsuemrName;
						return new Pair<Boolean, String>(pass, reason);
					}
				}
				if ("mysql".equals(t.getStorageType())) {
					if (app.getAckTimeoutSeconds() <= 0) {
						pass = false;
						reason = "超时不能小于0！";
					} else if (app.isNeedRetry()) {
						if (app.getRetryCount() <= 0) {
							pass = false;
							reason = "重试次数不能小于0！";
						} else if (app.getRetryInterval() <= 0) {
							pass = false;
							reason = "重试时间间隔不能小于0！";
						}
					}
				}
			}
		}
		return new Pair<Boolean, String>(pass, reason);
	}

	@GET
	@Path("review/{id}")
	public Response getApplicationById(@PathParam("id") long id) {
		if (id <= 0) {
			throw new RestException("Application id not available", Status.BAD_REQUEST);
		}

		HermesApplication defaultApp = appService.getApplicationById(id);
		return Response.status(Status.OK).entity(defaultApp).build();

	}

	@GET
	@Path("{status}")
	public Response getApplicationsByStatus(@PathParam("status") int status, @QueryParam("owner") String owner,
			@QueryParam("offset") int offset, @QueryParam("size") int size) {
		List<HermesApplication> apps = appService.getApplicationsByOwnerStatus(owner, status, offset, size);
		int count = appService.countApplicationsByOwnerStatus(owner, status);
		return Response.status(Status.OK)
				.entity(ResponseUtils.wrapPaginationResponse(Status.OK, apps.toArray(), count)).build();
	}

	@GET
	@Path("generated/{id}")
	public Response getGeneratedApplication(@PathParam("id") long id) {
		if (id <= 0) {
			throw new RestException("Application id not available", Status.BAD_REQUEST);
		}
		HermesApplication app = appService.getApplicationById(id);
		switch (HermesApplicationType.findByTypeCode(app.getType())) {
		case CREATE_TOPIC:
			TopicView topicView = appService.generateTopicView((TopicApplication) app);
			return Response.status(Status.OK).entity(new Pair<HermesApplication, TopicView>(app, topicView)).build();
		case CREATE_CONSUMER:
			ConsumerGroupView consumerView = appService.generateConsumerView((ConsumerApplication) app);
			return Response.status(Status.OK).entity(new Pair<HermesApplication, ConsumerGroupView>(app, consumerView))
					.build();
		default:
			throw new RestException("Generate view failed.");
		}
	}

	@POST
	@Path("generatedByType/{type}")
	public Response getGeneratedApplication(@PathParam("type") int type, String content) {
		HermesApplication app = null;
		try {
			app = JSON.parseObject(content, HermesApplicationType.findByTypeCode(type).getClazz());
		} catch (Exception e) {
			log.error("Can not parse payload : {}, submit topic application failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		switch (HermesApplicationType.findByTypeCode(app.getType())) {
		case CREATE_TOPIC:
			TopicView topicView = appService.generateTopicView((TopicApplication) app);
			return Response.status(Status.OK).entity(new Pair<HermesApplication, TopicView>(app, topicView)).build();
		case CREATE_CONSUMER:
			ConsumerGroupView consumerView = appService.generateConsumerView((ConsumerApplication) app);
			return Response.status(Status.OK).entity(new Pair<HermesApplication, ConsumerGroupView>(app, consumerView))
					.build();
		default:
			throw new RestException("Generate view failed.");
		}
	}

	@PUT
	@Path("update/{type}")
	public Response updateApplication(@PathParam("type") int type, String content) {
		HermesApplication app = null;
		try {
			app = JSON.parseObject(content, HermesApplicationType.findByTypeCode(type).getClazz());
		} catch (Exception e) {
			log.error("Can not parse payload : {}, submit topic application failed.", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}
		app.setContent(content);
		app = appService.updateApplication(app);
		if (app == null) {
			throw new RestException("Update application failed!", Status.INTERNAL_SERVER_ERROR);
		}

		try {
			m_mailService.sendApplicationMail(app);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", app.getId(), e);
		}
		return Response.status(Status.OK).entity(app).build();
	}

	@PUT
	@Path("reject/{id}")
	public Response rejectApplication(@PathParam("id") long id, @QueryParam("comment") String comment,
			@QueryParam("approver") String approver) {
		if (id < 0) {
			throw new RestException("Application id unavailable");
		}

		HermesApplication app = appService.getApplicationById(id);
		if (app == null) {
			throw new RestException("Failed to find application!", Status.BAD_REQUEST);
		}

		int status = PortalConstants.APP_STATUS_REJECTED;
		if (app.getStatus() == PortalConstants.APP_STATUS_ROLLOUT) {
			status = PortalConstants.APP_STATUS_ROLLOUT_REJECTED;
		}

		app = appService.updateStatus(id, status, comment, approver);
		if (app == null) {
			throw new RestException("Reject application failed!", Status.INTERNAL_SERVER_ERROR);
		}

		try {
			m_mailService.sendApplicationMail(app);
		} catch (Exception e) {
			log.error("Send email of hermes application id={} failed.", app.getId(), e);
		}

		return Response.status(Status.OK).entity(app).build();
	}

	@PUT
	@Path("pass/{id}")
	public Response passApplication(@PathParam("id") long id, @QueryParam("comment") String comment,
			@QueryParam("approver") String approver) {
		if (id < 0) {
			throw new RestException("Application id unavailable");
		}
		HermesApplication app = appService.getApplicationById(id);
		if (app == null) {
			throw new RestException("Application not available");
		}
		int status = PortalConstants.APP_STATUS_SUCCESS;
		if (app.getStatus() == PortalConstants.APP_STATUS_ROLLOUT) {
			status = PortalConstants.APP_STATUS_ROLLOUT_SUCCESS;
		}
		app = appService.updateStatus(id, status, comment, approver);
		if (app == null) {
			throw new RestException("Pass application failed!", Status.INTERNAL_SERVER_ERROR);
		}
		
		return Response.status(Status.OK).entity(app).build();
	}

	@PUT
	@Path("status/{id}")
	public Response updateApplicationStatus(@PathParam("id") long id, @QueryParam("status") int status,
			@QueryParam("comment") String comment, @QueryParam("approver") String approver, String polishedContent) {
		if (id < 0) {
			throw new RestException("Application id unavailable");
		}
		HermesApplication app = appService.updateStatus(id, status, comment, approver);
		if (app == null) {
			throw new RestException("Update application status failed!", Status.INTERNAL_SERVER_ERROR);
		}

		if (app.getStatus() == PortalConstants.APP_STATUS_SYNCED || app.getStatus() == PortalConstants.APP_STATUS_ONLINE) {
			try {
				m_mailService.sendApplicationMail(app);
			} catch (Exception e) {
				log.error("Send email of hermes application id={} failed.", app.getId(), e);
			}

		}

		return Response.status(Status.OK).entity(app).build();
	}
}
