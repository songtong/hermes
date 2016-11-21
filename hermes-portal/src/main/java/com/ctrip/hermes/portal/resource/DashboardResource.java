package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.queue.ListUtils;
import com.ctrip.hermes.admin.core.queue.MessagePriority;
import com.ctrip.hermes.admin.core.queue.MessageQueueDao;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.view.TopicView;
import com.ctrip.hermes.core.message.payload.PayloadCodecFactory;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.resource.view.TopicDelayBriefView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.dashboard.DashboardService;
import com.ctrip.hermes.portal.service.dashboard.DefaultDashboardService.DeadletterView;

import io.netty.buffer.Unpooled;

@Path("/dashboard/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class DashboardResource {
	private static final Logger log = LoggerFactory.getLogger(DashboardResource.class);

	private DashboardService m_dashboardService = PlexusComponentLocator.lookup(DashboardService.class);

	private MessageQueueDao m_messageQueueDao = PlexusComponentLocator.lookup(MessageQueueDao.class);

	private TopicService m_topicService = PlexusComponentLocator.lookup(TopicService.class);

	@GET
	@Path("brief/topics")
	public Response getTopics() {
		List<TopicDelayBriefView> list = new ArrayList<TopicDelayBriefView>();
		for (Entry<String, TopicView> entry : m_topicService.getTopicViews().entrySet()) {
			TopicView t = entry.getValue();
			list.add(new TopicDelayBriefView(t.getId(), t.getName(), m_dashboardService.getLatestProduced(t.getName()), 0,
			      t.getStorageType()));
		}

		Collections.sort(list, new Comparator<TopicDelayBriefView>() {
			@Override
			public int compare(TopicDelayBriefView left, TopicDelayBriefView right) {
				int ret = right.getDangerLevel() - left.getDangerLevel();
				return ret = ret == 0 ? left.getTopic().compareTo(right.getTopic()) : ret;
			}
		});

		return Response.status(Status.OK).entity(list).build();
	}

	@GET
	@Path("{topic}/{consumer}/delay")
	public Response getConsumerDelay(@PathParam("topic") String topicName, @PathParam("consumer") String consumerName) {
		Topic topic = m_topicService.findTopicEntityByName(topicName);
		List<DelayDetail> consumerDelay = new ArrayList<>();

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			consumerDelay = m_dashboardService.getDelayDetailForConsumer(topicName, consumerName);
		}

		return Response.status(Status.OK).entity(consumerDelay).build();
	}

	@GET
	@Path("topics/{topic}/latest")
	public Response getTopicLatest(@PathParam("topic") String name) {
		Topic topic = m_topicService.findTopicEntityByName(name);
		if (topic == null) {
			throw new RestException(String.format("Topic %s is not found", name), Status.NOT_FOUND);
		}

		List<MessagePriority> list = new ArrayList<MessagePriority>();
		if (Storage.MYSQL.equals(topic.getStorageType())) {
			@SuppressWarnings("unchecked")
			List<MessagePriority>[] ls = new List[topic.getPartitions().size()];
			for (int i = 0; i < topic.getPartitions().size(); i++) {
				Partition partition = topic.getPartitions().get(i);
				try {
					ls[i] = m_messageQueueDao.getLatestMessages(topic.getName(), partition.getId(), 20);
				} catch (DalException e) {
					log.warn("Find latest messages of {}[{}] failed", topic.getName(), partition.getId(), e);
				}
			}
			list = ListUtils.getTopK(20, MessagePriority.DATE_COMPARATOR_DESC, ls);
		}

		return Response.status(Status.OK).entity(CollectionUtil.collect(list, new Transformer() {
			@Override
			public Object transform(Object obj) {
				return new MessageView((MessagePriority) obj);
			}
		})).build();
	}

	private static class MessageView {
		private MessagePriority m_rawMessage;

		private String m_attributesString;

		private String m_payloadString;

		public MessageView(MessagePriority msg) {
			m_rawMessage = msg;
			HermesPrimitiveCodec codec = new HermesPrimitiveCodec(Unpooled.wrappedBuffer(msg.getAttributes()));
			m_attributesString = JSON.toJSONString(codec.readStringStringMap());
			if (Codec.JSON.equals(msg.getCodecType().split(",")[0])) {
				// m_payloadString = JSON.toJSONString(new JsonPayloadCodec().decode(msg.getPayload(), Object.class));
				m_payloadString = JSON.toJSONString(PayloadCodecFactory.getCodecByType(msg.getCodecType()).decode(
				      msg.getPayload(), Object.class));
			} else if (Codec.AVRO.equals(msg.getCodecType().split(",")[0])) {
				m_payloadString = JSON.toJSONString(PayloadCodecFactory.getCodecByType(msg.getCodecType())
				      .decode(msg.getPayload(), GenericRecord.class).toString());
			}
		}

		@SuppressWarnings("unused")
		public String getAttributesString() {
			return m_attributesString;
		}

		@SuppressWarnings("unused")
		public String getPayloadString() {
			return m_payloadString;
		}

		@SuppressWarnings("unused")
		public MessagePriority getRawMessage() {
			return m_rawMessage;
		}
	}

	@GET
	@Path("top/outdate-topics")
	public Response getTopOutdateTopic(@QueryParam("top") @DefaultValue("100") int top) {
		return Response.status(Status.OK).entity(m_dashboardService.getTopOutdateTopic(top)).build();
	}

	@GET
	@Path("deadletter/latest/{topic}/{consumer}")
	public Response getLatestDeadLetters(@PathParam("topic") String topic, @PathParam("consumer") String consumer)
	      throws DalException {
		if (topic == null || consumer == null) {
			throw new RestException("Topic or consumer can not be null!", Status.BAD_REQUEST);
		}

		Topic theTopic = m_topicService.findTopicEntityByName(topic);
		if (theTopic == null) {
			throw new RestException(String.format("Topic %s not found!", topic), Status.BAD_REQUEST);
		}

		if (theTopic.findConsumerGroup(consumer) == null) {
			throw new RestException(String.format("Consumer %s not found!", consumer), Status.BAD_REQUEST);
		}

		List<DeadletterView> latestDeadLetter = null;
		try {
			latestDeadLetter = m_dashboardService.getLatestDeadLetter(topic, consumer, 20);
		} catch (DalException e) {
			log.error("Find latest deadletter failed!", e);
			throw new RestException("Find latest deadletter failed!", Status.INTERNAL_SERVER_ERROR);
		}

		return Response.ok().entity(latestDeadLetter).build();
	}

	@GET
	@Path("deadletter/download/{topic}/{consumer}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadDeadLetters(@PathParam("topic") final String topic,
	      @PathParam("consumer") final String consumer, @QueryParam("timeStart") long timeStart,
	      @QueryParam("timeEnd") long timeEnd, @QueryParam("sleepInterval") Long sleepInterval) {

		if (topic == null || consumer == null) {
			throw new RestException("Topic or consumer can not be null!", Status.BAD_REQUEST);
		}

		if (timeStart <= 0 || timeEnd <= 0) {
			throw new RestException("End time or start time is illegle!", Status.BAD_REQUEST);
		}

		if (timeStart >= timeEnd) {
			throw new RestException("End time is before start time!", Status.BAD_REQUEST);
		}

		final Topic theTopic = m_topicService.findTopicEntityByName(topic);
		if (theTopic == null) {
			throw new RestException(String.format("Topic %s not found!", topic), Status.BAD_REQUEST);
		}

		if (theTopic.findConsumerGroup(consumer) == null) {
			throw new RestException(String.format("Consumer %s not found!", consumer), Status.BAD_REQUEST);
		}

		StreamingOutput stream = null;
		try {
			if (sleepInterval == null) {
				sleepInterval = 200L;
			}
			stream = m_dashboardService.getDeadLetterStreamByTimespan(topic, consumer, new Date(timeStart), new Date(
			      timeEnd), sleepInterval);
		} catch (Exception e) {
			log.error("Failed to create download stream.", e);
			throw new RestException("Download failed!", Status.INTERNAL_SERVER_ERROR);
		}
		if (stream == null) {
			log.error("Failed to create download stream.");
			throw new RestException("Download failed!", Status.INTERNAL_SERVER_ERROR);
		}

		return Response.ok(stream).header("Content-Disposition", "attachment; filename=deadletter.csv").build();
	}
}
