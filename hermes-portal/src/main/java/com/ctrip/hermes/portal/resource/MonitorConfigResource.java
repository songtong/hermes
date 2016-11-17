package com.ctrip.hermes.portal.resource;

import java.util.Date;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.model.ConsumerMonitorConfig;
import com.ctrip.hermes.admin.core.model.ProducerMonitorConfig;
import com.ctrip.hermes.admin.core.monitor.service.MonitorConfigService;
import com.ctrip.hermes.admin.core.service.ConsumerService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/monitor/config/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MonitorConfigResource {
	private static final Logger log = LoggerFactory.getLogger(MonitorConfigResource.class);

	private MonitorConfigService m_mcService = PlexusComponentLocator.lookup(MonitorConfigService.class);

	private ConsumerService m_consumerService = PlexusComponentLocator.lookup(ConsumerService.class);
	
	private TopicService m_topicService = PlexusComponentLocator.lookup(TopicService.class);

	@GET
	@Path("consumer/{topic}/{consumer}")
	public Response getConsumerMonitorConfig( //
	      @PathParam("topic") String topic, @PathParam("consumer") String consumer, //
	      @Context HttpServletRequest req) {
		if (!ensureConsumer(topic, consumer)) {
			return returnNotFound(topic, consumer, req);
		}

		ConsumerMonitorConfig c = null;
		try {
			c = m_mcService.getConsumerMonitorConfig(topic, consumer);
		} catch (Exception e) {
			log.warn("Find consumer monitor config failed: {} [{}]", topic, consumer, e);
			throw new RestException("Find consumer monitor config failed.", e);
		}

		c = c == null ? m_mcService.newDefaultConsumerMonitorConfig(topic, consumer) : c;
		return Response.status(Status.OK).entity(c).build();
	}

	@POST
	@Path("consumer/{topic}/{consumer}")
	public Response setConsumerMonitorConfig( //
	      @PathParam("topic") String topic, @PathParam("consumer") String consumer, //
	      String content, //
	      @QueryParam("ssoUser") @DefaultValue("not-set") String ssoUser, //
	      @QueryParam("ssoMail") @DefaultValue("not-set") String ssoMail, //
	      @Context HttpServletRequest req) {
		if (!ensureConsumer(topic, consumer)) {
			return returnNotFound(topic, consumer, req);
		}

		ConsumerMonitorConfig c = null;
		try {
			c = parseConsumerConfig(topic, consumer, content);
		} catch (Exception e) {
			String msg = String.format("Parse config failed: %s [%s], remote: %s, content: %s", //
			      topic, consumer, req.getRemoteAddr(), content);
			log.warn(msg, e);
			return Response.status(Status.BAD_REQUEST).entity(msg).build();
		}

		if (JSON.parseObject(content).isEmpty() && m_mcService.getConsumerMonitorConfig(topic, consumer) != null) {
			String msg = String.format("Can not set an existed config to NULL: %s [%s], remote: %s, content: %s", //
			      topic, consumer, req.getRemoteAddr(), content);
			log.warn(msg);
			return Response.status(Status.BAD_REQUEST).entity(msg).build();
		}

		c = c == null ? new ConsumerMonitorConfig().setTopic(topic).setConsumer(consumer).setCreateTime(new Date()) : c;

		try {
			m_mcService.setConsumerMonitorConfig(c);
			log.info("Set consumer monitor config success: {} [{}], remote: {}, user: {}, email: {}, new config: {}", //
			      topic, consumer, req.getRemoteAddr(), ssoUser, ssoMail, c);
		} catch (Exception e) {
			log.warn("Set consumer monitor config failed: {} [{}], remote:{}, content: {}", //
			      topic, consumer, req.getRemoteAddr(), content, e);
			throw new RestException("Set consumer monitor config failed.", e);
		}

		return Response.status(Status.OK).entity(c).build();
	}

	private ConsumerMonitorConfig parseConsumerConfig(String topic, String consumer, String jsonStr) {
		ConsumerMonitorConfig cfg = JSON.parseObject(jsonStr, ConsumerMonitorConfig.class);
		cfg.setTopic(topic);
		cfg.setConsumer(consumer);
		cfg.setCreateTime(cfg.getCreateTime() == null ? new Date() : cfg.getCreateTime());
		return cfg;
	}

	private boolean ensureConsumer(String topic, String consumer) {
		try {
			return !StringUtils.isBlank(topic) //
			      && !StringUtils.isBlank(consumer) //
			      && m_consumerService.findConsumerView(topic, consumer) != null;
		} catch (Exception e) {
			log.warn("Ensure consumer {} [{}] failed.", topic, consumer, e);
			return false;
		}
	}

	private Response returnNotFound(String topic, String consumer, HttpServletRequest req) {
		String msg = String.format("Consumer not found: %s [%s], remote: %s", topic, consumer, req.getRemoteAddr());
		log.warn(msg);
		return Response.status(Status.NOT_FOUND).entity(msg).build();
	}
	
	private boolean ensureProducer(String topic) {
		try {
			return topic != null && m_topicService.findTopicViewByName(topic) != null;
		} catch (Exception e) {
			log.warn("Ensure topic {} failed.", topic);
			return false;
		}
	}
	
	@GET
	@Path("topic/{topic}")
	public Response getProudcerMonitorConfig(@PathParam("topic") String topic, @Context HttpServletRequest req) {
		if (!ensureProducer(topic)) {
			return Response.status(Status.NOT_FOUND).entity(String.format("Topic not found: %s, remote: %s", topic, req.getRemoteAddr())).build();
		}
		
		ProducerMonitorConfig config = null;
		try {
			config = m_mcService.getProducerMonitorConfig(topic);
		} catch (Exception e) {
			log.warn("Failed to find producer config from db: {}", topic, e);
			throw new RestException("Failed to find producer config from db.", e);
		}
		
		if (config == null) {
			config = m_mcService.newDefaultProducerMonitorConfig(topic);
		}
		
		return Response.status(Status.OK).entity(config).build();
	}
	
	@POST
	@Path("topic/{topic}")
	public Response setProducerMonitorConfig( //
	      @PathParam("topic") String topic,  //
	      String content, //
	      @QueryParam("ssoUser") @DefaultValue("not-set") String ssoUser, //
	      @QueryParam("ssoMail") @DefaultValue("not-set") String ssoMail, //
	      @Context HttpServletRequest req) {
		if (!ensureProducer(topic)) {
			return Response.status(Status.NOT_FOUND).entity(String.format("Topic not found: %s, remote: %s", topic, req.getRemoteAddr())).build();
		}

		ProducerMonitorConfig config = null;
		try {
			config = parseProducerConfig(topic, content);
		} catch (Exception e) {
			String msg = String.format("Parse config failed: %s , remote: %s, content: %s", //
			      topic, req.getRemoteAddr(), content);
			log.warn(msg, e);
			return Response.status(Status.BAD_REQUEST).entity(msg).build();
		}

		if (JSON.parseObject(content).isEmpty() && m_mcService.getProducerMonitorConfig(topic) != null) {
			String msg = String.format("Can not set an existed config to NULL: %s, remote: %s, content: %s", //
			      topic, req.getRemoteAddr(), content);
			log.warn(msg);
			return Response.status(Status.BAD_REQUEST).entity(msg).build();
		}
		
		if (config == null) {
			config = m_mcService.newDefaultProducerMonitorConfig(topic);
		}
		
		try {
			m_mcService.setProducerMonitorConfig(config);
			log.info("Set producer monitor config success: {}, remote: {}, user: {}, email: {}, new config: {}", //
			      topic, req.getRemoteAddr(), ssoUser, ssoMail, config);
		} catch (Exception e) {
			log.warn("Set producer monitor config failed: {}, remote:{}, content: {}", //
			      topic, req.getRemoteAddr(), content, e);
			throw new RestException("Set producer monitor config failed.", e);
		}

		return Response.status(Status.OK).entity(config).build();
	}
	
	private ProducerMonitorConfig parseProducerConfig(String topic, String content) {
		ProducerMonitorConfig cfg = JSON.parseObject(content, ProducerMonitorConfig.class);
		cfg.setTopic(topic);
		cfg.setCreateTime(cfg.getCreateTime() == null ? new Date() : cfg.getCreateTime());
		return cfg;
	}
}
