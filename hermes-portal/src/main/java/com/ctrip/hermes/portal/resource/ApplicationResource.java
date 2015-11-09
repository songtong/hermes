package com.ctrip.hermes.portal.resource;

import java.util.Date;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.TopicView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.application.HermesApplication;
import com.ctrip.hermes.portal.application.HermesApplicationType;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.config.PortalConstants;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.application.ApplicationService;

@Path("/applications/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class ApplicationResource {
	private static final Logger log = LoggerFactory.getLogger(ApplicationResource.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	private ApplicationService appService = PlexusComponentLocator.lookup(ApplicationService.class);

	@POST
	@Path("topic/create")
	public Response createTopicApplication(String content) {
		log.info("Submitting application with payload", content);
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
		String topicName = topicApplication.getProductLine() + topicApplication.getEntity()
				+ topicApplication.getEvent();
		if (topicService.findTopicByName(topicName) != null) {
			throw new RestException("Topic already exists.", Status.CONFLICT);
		}

		topicApplication.setContent(content);
		topicApplication.setType(PortalConstants.APP_TYPE_CREATE_TOPIC);
		topicApplication.setStatus(PortalConstants.APP_STATUS_PROCESSING);
		topicApplication.setCreateTime(new Date(System.currentTimeMillis()));

		// 存入数据库
		try {
			topicApplication = appService.createTopicApplication(topicApplication);
		} catch (Exception e) {
			log.error("Can not create topic application : {}.", content, e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		// 返回
		return Response.status(Status.CREATED).entity(topicApplication).build();
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
		} else if (StringUtils.isBlank(app.getOwnerName())) {
			pass = false;
			reason = "负责人不能为空！";
		} else if (StringUtils.isBlank(app.getOwnerEmail())) {
			pass = false;
			reason = "负责人邮箱不能为空！";
		}

		return new Pair<Boolean, String>(pass, reason);

	}

	@GET
	@Path("review/{id}")
	public Response getApplication(@PathParam("id") long id) {
		if (id <= 0) {
			throw new RestException("Application id not available", Status.BAD_REQUEST);
		}

		HermesApplication defaultApp = appService.getApplicationById(id);
		return Response.status(Status.OK).entity(defaultApp).build();

	}

	@GET
	@Path("{status}")
	public Response getApplications(@PathParam("status") int status) {
		List<HermesApplication> apps = appService.getApplicationsByStatus(status);
		return Response.status(Status.OK).entity(apps).build();
	}

	@GET
	@Path("generated/{id}")
	public Response getGeneratedApplication(@PathParam("id") long id) {
		if (id <= 0) {
			throw new RestException("Application id not available", Status.BAD_REQUEST);
		}
		HermesApplication app = appService.getApplicationById(id);
		if(HermesApplicationType.CREATE_TOPIC==HermesApplicationType.findByTypeCode(app.getType())){
			TopicView topicView = appService.generageTopicView((TopicApplication)app);
			return Response.status(Status.OK).entity(new Pair<HermesApplication, TopicView>(app, topicView)).build();
		}else{
			throw new RestException("Generate view failed.");
		}
	}
}
