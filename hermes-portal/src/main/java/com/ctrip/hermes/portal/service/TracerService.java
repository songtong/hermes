package com.ctrip.hermes.portal.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.messageLifecycle.entity.Ack;
import com.ctrip.hermes.portal.messageLifecycle.entity.Deliver;
import com.ctrip.hermes.portal.messageLifecycle.entity.Group;
import com.ctrip.hermes.portal.messageLifecycle.entity.MessageLifecycle;
import com.ctrip.hermes.portal.messageLifecycle.entity.Produce;
import com.ctrip.hermes.portal.messageLifecycle.entity.Receive;
import com.ctrip.hermes.portal.messageLifecycle.entity.Save;
import com.ctrip.hermes.portal.messageLifecycle.entity.TryConsume;
import com.ctrip.hermes.portal.messageLifecycle.entity.TryProduce;

@Named
public class TracerService {

	private static final Logger m_logger = LoggerFactory.getLogger(TracerService.class);

	@Inject
	private TracerEsQueryService m_esService;

	@Inject
	private TopicService m_topicService;

	private SimpleDateFormat m_eventTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

	/**
	 * 
	 * @param topicName
	 * @param date
	 * @param refKey
	 * @return Key : list of messageLifecycles; value: list of unUsedEvents
	 */
	public Pair<List<MessageLifecycle>, List<Pair<String, Object>>> trace(String topicName, Date date, String refKey) {
		Topic topic = m_topicService.findTopicEntityByName(topicName);
		if (topic == null) {
			throw new RuntimeException("Topic not found!");
		}

		JsonNode result = m_esService.searchBizByRefKey(topic.getId(), refKey, date);
		List<MessageLifecycle> mlcs = new ArrayList<>();
		List<Pair<String, Object>> unusedEvents = new ArrayList<>();
		handleSearchBizByRefkeyResult(result, mlcs, unusedEvents);
		List<MessageLifecycle> uncompleltedMlcs = new ArrayList<>();
		for (MessageLifecycle mlc : mlcs) {
			// as we only search at most 100 biz records, the mlc may be uncompleted.
			if (mlc.getMsgId() == null) {
				uncompleltedMlcs.add(mlc);
				continue;
			}
			result = m_esService.searchBizByMsgId(mlc.getTopicId(), mlc.getPartition(), mlc.getMsgId(), date);
			List<Long> resendIds = new ArrayList<>();
			handleSearchBizByMsgIdResult(result, mlc, unusedEvents, resendIds);
			if (!resendIds.isEmpty()) {
				result = m_esService.searchBizByResendId(mlc.getTopicId(), mlc.getPartition(), resendIds, date);
				handleSearchBizByResendIdResult(result, mlc, unusedEvents);
			}
		}

		for (MessageLifecycle mlc : uncompleltedMlcs) {
			mlcs.remove(mlc);
		}

		fillTopicAndConsumerNames(mlcs, topic);
		return new Pair<List<MessageLifecycle>, List<Pair<String, Object>>>(mlcs, unusedEvents);
	}

	private void fillTopicAndConsumerNames(List<MessageLifecycle> mlcs, Topic topic) {
		List<ConsumerGroup> consumerGroupEntities = topic.getConsumerGroups();
		for (MessageLifecycle mlc : mlcs) {
			mlc.setTopicName(topic.getName());
			if (!consumerGroupEntities.isEmpty()) {
				for (Group g : mlc.getConsume()) {
					for (ConsumerGroup consumerGroup : consumerGroupEntities) {
						if (consumerGroup.getId().equals(g.getId())) {
							g.setName(consumerGroup.getName());
							break;
						}
					}
					if (g.getName() == null) {
						m_logger.warn("Unknown consumerGroup:id={} in topic:name={}!", g.getId(), topic.getName());
					}
				}
			}
		}
	}

	private void handleSearchBizByResendIdResult(JsonNode data, MessageLifecycle mlc,
	      List<Pair<String, Object>> unusedEvents) {
		Iterator<JsonNode> hits = data.path("hits").path("hits").getElements();

		while (hits.hasNext()) {
			JsonNode hit = hits.next().path("_source");
			handleAckedEvent(hit, mlc, unusedEvents, true);
		}
	}

	private void handleSearchBizByRefkeyResult(JsonNode data, List<MessageLifecycle> mlcs,
	      List<Pair<String, Object>> unusedEvents) {
		Iterator<JsonNode> hits = data.path("hits").path("hits").getElements();

		while (hits.hasNext()) {
			JsonNode hit = hits.next().path("_source");
			switch (hit.path("eventType").asText()) {
			case "RefKey.Transformed":
				handleTransformedEvent(hit, mlcs, unusedEvents);
				break;
			case "Message.Saved":
				handleSavedEvent(hit, mlcs, unusedEvents);
				break;
			case "Message.Received":
				handleReceivedEvent(hit, mlcs, unusedEvents);
				break;
			default:
				m_logger.warn("Unknown eventType:{}", hit.path("eventType").asText());
				unusedEvents.add(new Pair<String, Object>(hit.path("eventType").asText(), hit));
				break;
			}
		}

		for (MessageLifecycle mlc : mlcs) {
			List<TryProduce> tps = mlc.getProduce().getTryProduces();
			for (int i = 0; i < tps.size(); i++) {
				TryProduce tp = tps.get(i);
				tp.setTimes(tps.size() - i);
			}
		}

	}

	private void handleSearchBizByMsgIdResult(JsonNode data, MessageLifecycle mlc,
	      List<Pair<String, Object>> unusedEvents, List<Long> resendIds) {
		Iterator<JsonNode> hits = data.path("hits").path("hits").getElements();
		while (hits.hasNext()) {
			JsonNode hit = hits.next().path("_source");
			;
			switch (hit.path("eventType").asText()) {
			case "Message.Delivered":
				handleDeliveredEvent(hit, mlc, unusedEvents, resendIds);
				break;
			case "Message.Acked":
				handleAckedEvent(hit, mlc, unusedEvents, false);
				break;
			case "RefKey.Transformed":
				break;
			default:
				m_logger.warn("Unknown eventType:{}", hit.path("eventType").asText());
				unusedEvents.add(new Pair<String, Object>(hit.path("eventType").asText(), hit));
				break;
			}
		}
	}

	private void handleAckedEvent(JsonNode hit, MessageLifecycle mlc, List<Pair<String, Object>> unusedEvents,
	      boolean shouldResend) {
		boolean isResend = hit.path("isResend").asBoolean();
		if (shouldResend != isResend) {
			return;
		}

		Ack a = new Ack();
		try {
			a.setEventTime(m_eventTimeFormatter.parse(hit.path("eventTime").asText()));
			a.setBizProcessStartTime(m_eventTimeFormatter.parse(hit.path("BizStart").asText()));
			a.setBizProcessEndTime(m_eventTimeFormatter.parse(hit.path("BizEnd").asText()));
			a.setSuccess(hit.path("ack").asBoolean());
		} catch (ParseException e) {
			m_logger.warn("Parse eventTime:{},bizStartTime:{} or bizEndTime:{} failed for event:{}.", hit
			      .path("eventTime").asText(), hit.path("BizStart").asText(), hit.path("BizEnd").asText(), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Acked", hit));
			return;
		}

		int groupId = hit.path("groupId").asInt();
		String brokerIp = hit.path("host").asText();
		String consumerIp = hit.path("consumerIp").asText();
		long msgId = hit.path("msgId").asLong();
		for (Group g : mlc.getConsume()) {
			if (g.getId().equals(groupId)) {
				for (int i = g.getTryConsumes().size(); i > 0; i--) {
					TryConsume tc = g.getTryConsumes().get(i - 1);
					if (shouldResend) {
						if (tc.getIsResend() && tc.getResendId().equals(msgId) && tc.getBrokerIp().equals(brokerIp)
						      && tc.getConsumerIp().equals(consumerIp)
						      && !tc.getDeliver().getEventTime().after(a.getEventTime())) {
							tc.getAcks().add(a);
							return;
						}
					} else {
						if (!tc.getIsResend() && tc.getBrokerIp().equals(brokerIp) && tc.getConsumerIp().equals(consumerIp)
						      && !tc.getDeliver().getEventTime().after(a.getEventTime())) {
							tc.getAcks().add(a);
							return;
						}
					}
				}
			}
		}

		m_logger.warn("Can not find MessgeLifeCycle for current event:{}", hit);
		unusedEvents.add(new Pair<String, Object>("Message.Acked", hit));

	}

	private void handleDeliveredEvent(JsonNode hit, MessageLifecycle mlc, List<Pair<String, Object>> unusedEvents,
	      List<Long> resendIds) {
		Deliver d = new Deliver();
		try {
			d.setEventTime(m_eventTimeFormatter.parse(hit.path("eventTime").asText()));
		} catch (ParseException e) {
			m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.path("eventTime").asText(), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Delivered", hit));
			return;
		}

		TryConsume tc = new TryConsume();
		tc.setBrokerIp(hit.path("host").asText());
		tc.setConsumerIp(hit.path("consumerIp").asText());
		tc.setIsResend(hit.path("isResend").asBoolean());
		if (tc.isIsResend()) {
			long resendId = hit.path("resendId").asLong();
			tc.setResendId(resendId);
			resendIds.add(resendId);
		}
		tc.setDeliver(d);

		int groupId = hit.path("groupId").asInt();
		for (Group g : mlc.getConsume()) {
			if (g.getId().equals(groupId)) {
				g.getTryConsumes().add(tc);
				tc.setTimes(g.getTryConsumes().size());
				return;
			}
		}

		Group g = new Group();
		g.setId(groupId);
		g.getTryConsumes().add(tc);
		tc.setTimes(g.getTryConsumes().size());
		mlc.addGroup(g);

	}

	private void handleReceivedEvent(JsonNode hit, List<MessageLifecycle> mlcs, List<Pair<String, Object>> unusedEvents) {
		Receive r = new Receive();
		Date bornTime = null;
		try {
			r.setEventTime(m_eventTimeFormatter.parse(hit.path("eventTime").asText()));
			bornTime = m_eventTimeFormatter.parse(hit.path("bornTime").asText());
		} catch (ParseException e) {
			m_logger.warn("Parse eventTime:{} or bornTime:{} failed for event:{}.", hit.path("eventTime").asText(), hit
			      .path("bornTime").asText(), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Received", hit));
			return;
		}

		if (mlcs.isEmpty()) {
			unusedEvents.add(new Pair<String, Object>("Message.Received", hit));
			return;
		}

		TryProduce tp = null;
		for (MessageLifecycle mlc : mlcs) {
			try {
				tp = mlc.getProduce().getTryProduces().get(0);
				if (tp.getReceive() == null && tp.getBrokerIp().equals(hit.path("host").asText())) {
					tp.setReceive(r);
					mlc.getProduce().setBorn(bornTime);
					mlc.getProduce().setProducerIp(hit.path("producerIp").asText());
					return;
				}
			} catch (IndexOutOfBoundsException e) {
				m_logger.warn("No Message.Saved event found in produce-cycle! Current MessageLifeCycle:{}", mlc, e);
				unusedEvents.add(new Pair<String, Object>("Message.Received", r));
				return;
			} catch (Exception e) {
				m_logger.warn("Error occured when handling Message.Received event:{} during MessageLifeCycle:{}", hit, mlc,
				      e);
				unusedEvents.add(new Pair<String, Object>("Message.Received", r));
				return;
			}

		}

		tp = new TryProduce();
		tp.setReceive(r);
		Produce p = mlcs.get(mlcs.size() - 1).getProduce();
		p.getTryProduces().add(tp);
		p.setBorn(bornTime);
		p.setProducerIp(hit.path("producerIp").asText());

	}

	private void handleSavedEvent(JsonNode hit, List<MessageLifecycle> mlcs, List<Pair<String, Object>> unusedEvents) {
		Save s = new Save();
		try {
			s.setEventTime(m_eventTimeFormatter.parse(hit.path("eventTime").asText()));
			s.setSuccess(hit.path("success").asBoolean());
		} catch (ParseException e) {
			m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.path("eventTime").asText(), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Saved", hit));
			return;
		}

		MessageLifecycle mlc = new MessageLifecycle();
		mlc.setPartition(hit.path("partition").asInt());
		mlc.setRefKey(hit.path("refKey").asText());
		mlc.setTopicId(hit.path("topic").asLong());
		mlc.setPriority(hit.path("priority").asInt() == 0 ? true : false);
		mlcs.add(mlc);

		Produce p = new Produce();
		TryProduce tp = new TryProduce();
		tp.setSave(s);
		tp.setBrokerIp(hit.path("host").asText());
		p.addTryProduce(tp);
		mlc.setProduce(p);
	}

	private void handleTransformedEvent(JsonNode hit, List<MessageLifecycle> mlcs,
	      List<Pair<String, Object>> unusedEvents) {
		MessageLifecycle mlc = null;
		int transformedNode = 0;
		while (transformedNode < mlcs.size()) {
			mlc = mlcs.get(transformedNode);
			transformedNode++;
			if (mlc.getMsgId() != null) {
				continue;
			}
			TryProduce tp = mlc.getProduce().getTryProduces().get(0);
			Save s = tp.getSave();
			try {
				if (tp.getBrokerIp().equals(hit.path("host").asText()) && s.isSuccess()
				      && !(s.getEventTime().before(m_eventTimeFormatter.parse(hit.path("eventTime").asText())))) {
					mlc.setMsgId(hit.path("msgId").asLong());
					return;
				}
			} catch (ParseException e) {
				m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.path("eventTime").asText(), hit, e);
				unusedEvents.add(new Pair<String, Object>("RefKey.Transformed", hit));
				return;
			}
		}

		m_logger.warn("Can not find MessgeLifeCycle for current event:{}", hit);
		unusedEvents.add(new Pair<String, Object>("RefKey.Transformed", hit));

	}

}
