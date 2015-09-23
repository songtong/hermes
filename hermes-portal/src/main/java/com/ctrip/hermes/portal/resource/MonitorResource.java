package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.portal.assist.ListUtils;
import com.ctrip.hermes.portal.dal.HermesPortalDao;
import com.ctrip.hermes.portal.dal.MessagePriority;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.resource.view.MonitorClientView;
import com.ctrip.hermes.portal.resource.view.TopicDelayBriefView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView.DelayDetail;
import com.ctrip.hermes.portal.service.monitor.MonitorService;

import io.netty.buffer.Unpooled;

@Path("/monitor/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MonitorResource {
	private static final Logger log = LoggerFactory.getLogger(MonitorResource.class);

	private MonitorService m_monitorService = PlexusComponentLocator.lookup(MonitorService.class);

	private PortalMetaService m_metaService = PlexusComponentLocator.lookup(PortalMetaService.class);

	private HermesPortalDao m_portalDao = PlexusComponentLocator.lookup(HermesPortalDao.class);

	@GET
	@Path("brief/topics")
	public Response getTopics() {
		List<TopicDelayBriefView> list = new ArrayList<TopicDelayBriefView>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			Topic t = entry.getValue();
			long delay = m_monitorService.getDelay(t.getName());
			list.add(new TopicDelayBriefView(t.getName(), m_monitorService.getLatestProduced(t.getName()), delay));
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
	@Path("brief/brokers")
	public Response getBrokerBriefs() {
		return Response.status(Status.OK).entity(m_monitorService.getLatestBrokers()).build();
	}

	@GET
	@Path("detail/topics/{topic}/delay")
	public Response getTopicDelay(@PathParam("topic") String name) {
		Topic topic = m_metaService.findTopicByName(name);
		TopicDelayDetailView view = new TopicDelayDetailView(name);

		if (Storage.MYSQL.equals(topic.getStorageType())) {
			view = m_monitorService.getTopicDelayDetail(name);
		}

		return Response.status(Status.OK).entity(view).build();
	}

	@GET
	@Path("topics/{topic}/latest")
	public Response getTopicLatest(@PathParam("topic") String name) {
		Topic topic = m_metaService.findTopicByName(name);
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
					ls[i] = m_portalDao.getLatestMessages(topic.getName(), partition.getId(), 20);
				} catch (DalException e) {
					log.warn("Find latest messages of {}[{}] failed", topic.getName(), partition.getId(), e);
				}
			}
			list = ListUtils.getTopK(15, MessagePriority.DATE_COMPARATOR_DESC, ls);
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
			if (Codec.JSON.equals(msg.getCodecType())) {
				m_payloadString = JSON.toJSONString(new JsonPayloadCodec().decode(msg.getPayload(), Object.class));
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
	@Path("top/delays")
	public Response getTopDelays(@QueryParam("top") @DefaultValue("100") int top) {
		return Response.status(Status.OK).entity(m_monitorService.getTopDelays(top)).build();
	}

	@GET
	@Path("top/outdate-topics")
	public Response getTopOutdateTopic(@QueryParam("top") @DefaultValue("100") int top) {
		return Response.status(Status.OK).entity(m_monitorService.getTopOutdateTopic(top)).build();
	}

	@GET
	@Path("top/broker/qps/received")
	public Response getTopBrokerReceived() {
		return Response.status(Status.OK).entity(m_monitorService.getBrokerReceivedQPS()).build();
	}

	@GET
	@Path("top/broker/qps/delivered")
	public Response getTopBrokerDelivered() {
		return Response.status(Status.OK).entity(m_monitorService.getBrokerDeliveredQPS()).build();
	}

	@GET
	@Path("top/broker/qps/received/{brokerIp}")
	public Response getTopBrokerTopicReceived(@PathParam("brokerIp") String ip) {
		return Response.status(Status.OK).entity(m_monitorService.getBrokerReceivedDetailQPS(ip)).build();
	}

	@GET
	@Path("top/broker/qps/delivered/{brokerIp}")
	public Response getTopBrokerTopicDelivered(@PathParam("brokerIp") String ip) {
		return Response.status(Status.OK).entity(m_monitorService.getBrokerDeliveredDetailQPS(ip)).build();
	}

	@GET
	@Path("clients")
	public Response findClients(@QueryParam("part") String part) {
		return Response.status(Status.OK).entity(m_monitorService.getRelatedClients(part)).build();
	}

	@GET
	@Path("topics/{ip}")
	public Response getDeclaredTopics(@PathParam("ip") String ip) {
		MonitorClientView view = new MonitorClientView(ip);
		view.setProduceTopics(getProduceTopicsList(m_monitorService.getProducerIP2Topics(), ip));
		view.setConsumeTopics(getConsumeTopicsList(m_monitorService.getConsumerIP2Topics(), ip));
		return Response.status(Status.OK).entity(view).build();
	}

	private List<String> getProduceTopicsList(Map<String, Set<String>> map, String key) {
		Set<String> set = map.get(key);
		List<String> list = set == null ? new ArrayList<String>() : new ArrayList<String>(set);
		Collections.sort(list);
		return list;
	}

	private List<Pair<String, List<String>>> getConsumeTopicsList(Map<String, Map<String, Set<String>>> m, String ip) {
		List<Pair<String, List<String>>> list = new ArrayList<Pair<String, List<String>>>();
		Map<String, Set<String>> ms = m.get(ip);
		ms = ms == null ? new HashMap<String, Set<String>>() : ms;
		for (Entry<String, Set<String>> entry : ms.entrySet()) {
			ArrayList<String> l = new ArrayList<String>(entry.getValue());
			Collections.sort(l);
			list.add(new Pair<String, List<String>>(entry.getKey(), l));
		}
		Collections.sort(list, new Comparator<Pair<String, List<String>>>() {
			@Override
			public int compare(Pair<String, List<String>> o1, Pair<String, List<String>> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		return list;
	}

	@GET
	@Path("delay/{topic}/{groupName}")
	// not in use
	public Response getConsumeDelay(@PathParam("topic") String topic, @PathParam("groupName") String groupName) {
		Long delay = m_monitorService.getDelay(topic, groupName);
		if (delay == null) {
			throw new RestException(String.format("Delay [%s, %s] not found.", topic, groupName), Status.NOT_FOUND);
		}
		return Response.status(Status.OK).entity(delay).build();
	}

	@GET
	@Path("delay/{topic}/{groupName}/detail")
	// not in use
	public Response getConsumeDelayDetail(@PathParam("topic") String topic, @PathParam("groupName") String groupName) {
		List<DelayDetail> delayDetails = m_monitorService.getDelayDetailForConsumer(topic, groupName);
		if (delayDetails == null || delayDetails.size() == 0) {
			throw new RestException(String.format("Delay [%s, %s] not found.", topic, groupName), Status.NOT_FOUND);
		}
		return Response.status(Status.OK).entity(delayDetails).build();
	}
}
