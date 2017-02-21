package com.ctrip.hermes.portal.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
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

		JSONObject result = m_esService.searchBizByRefKey(topic.getId(), refKey, date);
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

	private void handleSearchBizByResendIdResult(JSONObject data, MessageLifecycle mlc,
	      List<Pair<String, Object>> unusedEvents) {
		JSONArray hits = data.getJSONObject("hits").getJSONArray("hits");

		for (Object hitObject : hits) {
			JSONObject hit = ((JSONObject) hitObject).getJSONObject("_source");
			handleAckedEvent(hit, mlc, unusedEvents, true);
		}
	}

	private void handleSearchBizByRefkeyResult(JSONObject data, List<MessageLifecycle> mlcs,
	      List<Pair<String, Object>> unusedEvents) {
		JSONArray hits = data.getJSONObject("hits").getJSONArray("hits");
		for (Object hitObject : hits) {
			JSONObject hit = ((JSONObject) hitObject).getJSONObject("_source");
			switch (hit.getString("eventType")) {
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
				m_logger.warn("Unknown eventType:{}", hit.getString("eventType"));
				unusedEvents.add(new Pair<String, Object>(hit.getString("eventType"), hit));
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

	private void handleSearchBizByMsgIdResult(JSONObject data, MessageLifecycle mlc,
	      List<Pair<String, Object>> unusedEvents, List<Long> resendIds) {
		JSONArray hits = data.getJSONObject("hits").getJSONArray("hits");
		for (Object hitObject : hits) {
			JSONObject hit = ((JSONObject) hitObject).getJSONObject("_source");
			switch (hit.getString("eventType")) {
			case "Message.Delivered":
				handleDeliveredEvent(hit, mlc, unusedEvents, resendIds);
				break;
			case "Message.Acked":
				handleAckedEvent(hit, mlc, unusedEvents, false);
				break;
			case "RefKey.Transformed":
				break;
			default:
				m_logger.warn("Unknown eventType:{}", hit.getString("eventType"));
				unusedEvents.add(new Pair<String, Object>(hit.getString("eventType"), hit));
				break;
			}
		}
	}

	private void handleAckedEvent(JSONObject hit, MessageLifecycle mlc, List<Pair<String, Object>> unusedEvents,
	      boolean shouldResend) {
		boolean isResend = hit.getBooleanValue("isResend");
		if (shouldResend != isResend) {
			return;
		}

		Ack a = new Ack();
		try {
			a.setEventTime(hit.getDate(("eventTime")));
			a.setBizProcessStartTime(hit.getDate(("BizStart")));
			a.setBizProcessEndTime(hit.getDate(("BizEnd")));
			a.setSuccess(hit.getBooleanValue("ack"));
		} catch (Exception e) {
			m_logger.warn("Parse eventTime:{},bizStartTime:{} or bizEndTime:{} failed for event:{}.",
			      hit.getString("eventTime"), hit.getString("BizStart"), hit.getString("BizEnd"), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Acked", hit));
			return;
		}

		int groupId = hit.getIntValue("groupId");
		String brokerIp = hit.getString("host");
		String consumerIp = hit.getString("consumerIp");
		long msgId = hit.getLongValue("msgId");
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

	private void handleDeliveredEvent(JSONObject hit, MessageLifecycle mlc, List<Pair<String, Object>> unusedEvents,
	      List<Long> resendIds) {
		Deliver d = new Deliver();
		try {
			d.setEventTime(hit.getDate(("eventTime")));
		} catch (Exception e) {
			m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.getString("eventTime"), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Delivered", hit.toString()));
			return;
		}

		TryConsume tc = new TryConsume();
		tc.setBrokerIp(hit.getString("host"));
		tc.setConsumerIp(hit.getString("consumerIp"));
		tc.setIsResend(hit.getBooleanValue("isResend"));
		if (tc.isIsResend()) {
			long resendId = hit.getLongValue("resendId");
			tc.setResendId(resendId);
			resendIds.add(resendId);
		}
		tc.setDeliver(d);

		int groupId = hit.getIntValue("groupId");
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

	private void handleReceivedEvent(JSONObject hit, List<MessageLifecycle> mlcs, List<Pair<String, Object>> unusedEvents) {
		Receive r = new Receive();
		Date bornTime = null;
		try {
			r.setEventTime(hit.getDate(("eventTime")));
			bornTime = hit.getDate(("bornTime"));
		} catch (Exception e) {
			m_logger.warn("Parse eventTime:{} or bornTime:{} failed for event:{}.", hit.getString("eventTime"),
			      hit.getString("bornTime"), hit, e);
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
				if (tp.getReceive() == null && tp.getBrokerIp().equals(hit.getString("host"))) {
					tp.setReceive(r);
					mlc.getProduce().setBorn(bornTime);
					mlc.getProduce().setProducerIp(hit.getString("producerIp"));
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
		p.setProducerIp(hit.getString("producerIp"));

	}

	private void handleSavedEvent(JSONObject hit, List<MessageLifecycle> mlcs, List<Pair<String, Object>> unusedEvents) {
		Save s = new Save();
		try {
			s.setEventTime(hit.getDate(("eventTime")));
			s.setSuccess(hit.getBooleanValue("success"));
		} catch (Exception e) {
			m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.getString("eventTime"), hit, e);
			unusedEvents.add(new Pair<String, Object>("Message.Saved", hit));
			return;
		}

		MessageLifecycle mlc = new MessageLifecycle();
		mlc.setPartition(hit.getIntValue("partition"));
		mlc.setRefKey(hit.getString("refKey"));
		mlc.setTopicId(hit.getLongValue("topic"));
		mlc.setPriority(hit.getIntValue("priority") == 0 ? true : false);
		mlcs.add(mlc);

		Produce p = new Produce();
		TryProduce tp = new TryProduce();
		tp.setSave(s);
		tp.setBrokerIp(hit.getString("host"));
		p.addTryProduce(tp);
		mlc.setProduce(p);
	}

	private void handleTransformedEvent(JSONObject hit, List<MessageLifecycle> mlcs,
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
				if (tp.getBrokerIp().equals(hit.getString("host")) && s.isSuccess()
				      && !(s.getEventTime().before(hit.getDate(("eventTime"))))) {
					mlc.setMsgId(hit.getLongValue("msgId"));
					return;
				}
			} catch (Exception e) {
				m_logger.warn("Parse eventTime:{} failed for event:{}.", hit.getString("eventTime"), hit, e);
				unusedEvents.add(new Pair<String, Object>("RefKey.Transformed", hit.toString()));
				return;
			}
		}

		m_logger.warn("Can not find MessgeLifeCycle for current event:{}", hit);
		unusedEvents.add(new Pair<String, Object>("RefKey.Transformed", hit.toString()));

	}

}
