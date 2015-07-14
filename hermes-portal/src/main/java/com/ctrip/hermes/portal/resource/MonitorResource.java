package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.PortalMetaService;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.resource.view.MonitorClientView;
import com.ctrip.hermes.portal.resource.view.TopicDelayBriefView;
import com.ctrip.hermes.portal.resource.view.TopicDelayDetailView;
import com.ctrip.hermes.portal.service.monitor.MonitorService;

@Path("/monitor/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MonitorResource {

	private MonitorService m_monitorService = PlexusComponentLocator.lookup(MonitorService.class);

	private PortalMetaService m_metaService = PlexusComponentLocator.lookup(PortalMetaService.class);

	@GET
	@Path("brief/topics")
	public Response getTopics() {
		List<TopicDelayBriefView> list = new ArrayList<TopicDelayBriefView>();
		for (Entry<String, Topic> entry : m_metaService.getTopics().entrySet()) {
			Topic t = entry.getValue();
			int avgDelay = 0;
			for (ConsumerGroup consumer : t.getConsumerGroups()) {
				Pair<Date, Date> delay = m_monitorService.getDelay(t.getName(), consumer.getId());
				avgDelay += delay.getKey().getTime() - delay.getValue().getTime();
			}
			avgDelay /= t.getConsumerGroups().size() == 0 ? 1 : t.getConsumerGroups().size();
			list.add(new TopicDelayBriefView(t.getName(), m_monitorService.getLatestProduced(t.getName()), avgDelay));
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
		for (ConsumerGroup consumer : topic.getConsumerGroups()) {
			for (Entry<Integer, Pair<Date, Date>> e : m_monitorService.getDelayDetails(name, consumer.getId()).entrySet()) {
				int delay = (int) (e.getValue().getKey().getTime() - e.getValue().getValue().getTime());
				view.addDelay(consumer.getName(), e.getKey(), delay);
			}
		}

		return Response.status(Status.OK).entity(view).build();
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
	@Path("delay/{topic}/{groupId}")
	public Response getConsumeDelay(@PathParam("topic") String topic, @PathParam("groupId") int groupId) {
		Pair<Date, Date> delay = m_monitorService.getDelay(topic, groupId);
		if (delay == null) {
			throw new RestException(String.format("Delay [%s, %s] not found.", topic, groupId), Status.NOT_FOUND);
		}
		return Response.status(Status.OK).entity(delay).build();
	}

	@GET
	@Path("delay/{topic}/{groupId}/detail")
	public Response getConsumeDelayDetail(@PathParam("topic") String topic, @PathParam("groupId") int groupId) {
		Map<Integer, Pair<Date, Date>> delayDetails = m_monitorService.getDelayDetails(topic, groupId);
		if (delayDetails == null || delayDetails.size() == 0) {
			throw new RestException(String.format("Delay [%s, %s] not found.", topic, groupId), Status.NOT_FOUND);
		}
		return Response.status(Status.OK).entity(delayDetails).build();
	}
}
