package com.ctrip.hermes.monitor.checker.mysql;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfig;
import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.service.MonitorConfigService;
import com.ctrip.hermes.metaservice.queue.MessagePriorityDao;
import com.ctrip.hermes.metaservice.queue.OffsetMessageDao;
import com.ctrip.hermes.metaservice.service.ConsumerService;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.NoticeContent;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;
import com.ctrip.hermes.metaservice.service.notify.SmsNoticeContent;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.DBBasedChecker;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.task.ConsumeBacklogCheckerTask;
import com.ctrip.hermes.monitor.checker.notification.LargeBacklogMailContent;
import com.ctrip.hermes.monitor.utils.MonitorUtils;
import com.ctrip.hermes.monitor.utils.MonitorUtils.Matcher;

import io.netty.util.internal.ConcurrentSet;
import scala.collection.mutable.StringBuilder;

@Component(value = ConsumeLargeBacklogChecker.ID)
public class ConsumeLargeBacklogChecker extends DBBasedChecker {

	private static final Logger log = LoggerFactory.getLogger(ConsumeLargeBacklogChecker.class);

	public static final String ID = "ConsumeLargeBacklogChecker";

	private static final int CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE = 5;

	private static final Pattern PHONE_PATTERN = Pattern.compile("\\d{11}", Pattern.CASE_INSENSITIVE);

	private static final Pattern EMAIL_PATTERN = //
	Pattern.compile("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}", Pattern.CASE_INSENSITIVE);

	private MessagePriorityDao m_msgDao = PlexusComponentLocator.lookup(MessagePriorityDao.class);

	private OffsetMessageDao m_offsetDao = PlexusComponentLocator.lookup(OffsetMessageDao.class);

	private NotifyService m_notifyService = PlexusComponentLocator.lookup(NotifyService.class);

	private ConsumerService m_consumerService = PlexusComponentLocator.lookup(ConsumerService.class);

	private MonitorConfigService m_monitorConfigService = PlexusComponentLocator.lookup(MonitorConfigService.class);

	private static List<Owner> m_hermesAdmins;

	static {
		m_hermesAdmins = new ArrayList<>();
		m_hermesAdmins.add(new Owner("18721960052", "song_t@ctrip.com"));
		m_hermesAdmins.add(new Owner("13661724530", "q_gu@ctrip.com"));
		m_hermesAdmins.add(new Owner("15021290572", "jhliang@ctrip.com"));
		m_hermesAdmins.add(new Owner("15216706100", "lxteng@ctrip.com"));
		m_hermesAdmins.add(new Owner("15267014652", "qingyang@ctrip.com"));
	}

	@Override
	public String name() {
		return ID;
	}

	protected void setMessagePriorityDao(MessagePriorityDao dao) {
		m_msgDao = dao;
	}

	protected void setOffsetMessageDao(OffsetMessageDao dao) {
		m_offsetDao = dao;
	}

	protected Map<Pair<Topic, ConsumerGroup>, Long> parseLimits(Meta meta, String includeString, String excludeString) {
		if (meta == null || StringUtils.isBlank(includeString) || StringUtils.isBlank(excludeString)) {
			return null;
		}
		Map<Pair<Topic, ConsumerGroup>, Long> limits = new HashMap<>();
		Map<String, Map<String, Integer>> includes = JSON.parseObject(includeString,
		      new TypeReference<Map<String, Map<String, Integer>>>() {
		      });
		Map<String, Integer> all = includes.get(".*");
		if (all != null) {
			for (Entry<String, Integer> entry : all.entrySet()) {
				limits.putAll(parseIncludes(meta, ".*", entry.getKey(), entry.getValue()));
			}
		}
		for (Entry<String, Map<String, Integer>> item : includes.entrySet()) {
			if (!item.getKey().equals(".*")) {
				for (Entry<String, Integer> entry : item.getValue().entrySet()) {
					limits.putAll(parseIncludes(meta, item.getKey(), entry.getKey(), entry.getValue()));
				}
			}
		}

		Map<String, List<String>> excludes = JSON.parseObject(excludeString,
		      new TypeReference<Map<String, List<String>>>() {
		      });
		for (Entry<String, List<String>> item : excludes.entrySet()) {
			for (String consumer : item.getValue()) {
				removeExcludes(limits, meta, item.getKey(), consumer);
			}
		}

		return limits;
	}

	private Map<Pair<Topic, ConsumerGroup>, Long> parseIncludes( //
	      Meta meta, final String topicPattern, final String groupPattern, long limit) {
		Map<Pair<Topic, ConsumerGroup>, Long> limits = new HashMap<Pair<Topic, ConsumerGroup>, Long>();
		List<Entry<String, Topic>> includeTopics = MonitorUtils.findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : includeTopics) {
			Topic topic = entry.getValue();
			if (Storage.MYSQL.equals(topic.getStorageType())) {
				List<ConsumerGroup> includeGroups = MonitorUtils.findMatched(topic.getConsumerGroups(),
				      new Matcher<ConsumerGroup>() {
					      @Override
					      public boolean match(ConsumerGroup obj) {
						      return Pattern.matches(groupPattern, obj.getName());
					      }
				      });
				for (ConsumerGroup group : includeGroups) {
					limits.put(new Pair<Topic, ConsumerGroup>(topic, group), limit);
				}
			}
		}
		return limits;
	}

	private void removeExcludes(Map<Pair<Topic, ConsumerGroup>, Long> limits, //
	      Meta meta, final String topicPattern, final String groupPattern) {
		List<Entry<String, Topic>> excludeTopics = MonitorUtils.findMatched(meta.getTopics().entrySet(),
		      new Matcher<Entry<String, Topic>>() {
			      @Override
			      public boolean match(Entry<String, Topic> obj) {
				      return Pattern.matches(topicPattern.trim(), obj.getKey());
			      }
		      });
		for (Entry<String, Topic> entry : excludeTopics) {
			Topic topic = entry.getValue();
			List<ConsumerGroup> excludeGroups = MonitorUtils.findMatched(topic.getConsumerGroups(),
			      new Matcher<ConsumerGroup>() {
				      @Override
				      public boolean match(ConsumerGroup obj) {
					      return Pattern.matches(groupPattern, obj.getName());
				      }
			      });
			for (ConsumerGroup group : excludeGroups) {
				limits.remove(new Pair<Topic, ConsumerGroup>(topic, group));
			}
		}
	}

	Map<Pair<Topic, ConsumerGroup>, Long> enrichLimits(Meta meta, Map<Pair<Topic, ConsumerGroup>, Long> limits) {
		for (ConsumerMonitorConfig cfg : m_monitorConfigService.listConsumerMonitorConfig()) {
			if (cfg.isLargeBacklogEnable() && cfg.getLargeBacklogLimit() > 0) {
				Topic t = meta.findTopic(cfg.getTopic());
				ConsumerGroup c = t.findConsumerGroup(cfg.getConsumer());
				limits.put(new Pair<Topic, ConsumerGroup>(t, c), Long.valueOf(cfg.getLargeBacklogLimit()));
			}
		}
		return limits;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		final CheckerResult result = new CheckerResult();
		ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);
		try {
			Meta meta = fetchMeta();
			Map<Pair<Topic, ConsumerGroup>, Long> limits = enrichLimits(
			      meta, //
			      parseLimits(meta, //
			            m_config.getConsumeBacklogCheckerIncludeTopics(), m_config.getConsumeBacklogCheckerExcludeTopics()));
			ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
			List<Map<Pair<Topic, ConsumerGroup>, Long>> splited = MonitorUtils.splitMap(limits, DB_CHECKER_THREAD_COUNT);
			final CountDownLatch latch = new CountDownLatch(splited.size());
			for (final Map<Pair<Topic, ConsumerGroup>, Long> batchLimits : splited) {
				es.execute(new ConsumeBacklogCheckerTask(batchLimits, m_msgDao, m_offsetDao, result, latch, exceptions));
			}
			if (latch.await(CONSUME_BACKLOG_CHECKER_TIMEOUT_MINUTE, TimeUnit.MINUTES)) {
				result.setRunSuccess(true);
			} else {
				result.setRunSuccess(false);
				result.setErrorMessage("Query consume backlog db timeout, check result is not completely.");
			}
			if (exceptions.size() > 0) {
				result.setRunSuccess(false);
				result.setErrorMessage("Consumer large backlog checker task has exceptions!");
				result.setException(new CompositeException(exceptions));
			}
		} catch (Exception e) {
			result.setErrorMessage("Query consume backlog db failed.");
			result.setException(e);
			result.setRunSuccess(false);
		} finally {
			es.shutdownNow();
		}

		notifyOwnersIfNecessary(result);

		return result;
	}

	private void notifyOwnersIfNecessary(CheckerResult result) {
		Map<Owner, List<MonitorEvent>> aggregatedEvents = new HashMap<>();
		for (MonitorEvent event : result.getMonitorEvents()) {
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent) event;
			for (Owner owner : getOwnersByGroup(e.getTopic(), e.getGroup())) {
				List<MonitorEvent> list = aggregatedEvents.get(owner);
				if (list == null) {
					list = new ArrayList<MonitorEvent>();
					aggregatedEvents.put(owner, list);
				}
				list.add(e);
			}
		}

		for (Entry<Owner, List<MonitorEvent>> entry : aggregatedEvents.entrySet()) {
			doNotifyWithSms(entry.getKey(), entry.getValue());
			doNotifyWithEmail(entry.getKey(), entry.getValue());
		}
	}

	private void doNotifyWithSms(Owner owner, List<MonitorEvent> events) {
		String phoneNumber = cleanPhoneNumber(owner.getPhone());
		if (phoneNumber != null) {
			m_notifyService.notify(new HermesNotice(phoneNumber, generateSmsContent(events)));
		} else {
			log.warn("Can not find phone number of owner: {}", owner);
		}
	}

	private void doNotifyWithEmail(Owner owner, List<MonitorEvent> events) {
		String emailAddress = cleanEmailAddress(owner.getEmail());
		if (emailAddress != null) {
			m_notifyService.notify(new HermesNotice(emailAddress, generateEmailContent(events)));
		} else {
			log.warn("Can not find email address of owner: {}", owner);
		}
	}

	private NoticeContent generateSmsContent(List<MonitorEvent> events) {
		StringBuilder sb = new StringBuilder("消费积压: ");
		for (MonitorEvent event : events) {
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent) event;
			sb.append(String.format("T[%s], C[%s]: [%s] \n", e.getTopic(), e.getGroup(), e.getTotalBacklog()));
		}
		return new SmsNoticeContent(sb.toString());
	}

	private NoticeContent generateEmailContent(List<MonitorEvent> events) {
		return new LargeBacklogMailContent(events);
	}

	private static class Owner {
		private String m_phone;

		private String m_email;

		public Owner(String phone, String email) {
			m_phone = phone;
			m_email = email;
		}

		public String getPhone() {
			return m_phone;
		}

		public String getEmail() {
			return m_email;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_email == null) ? 0 : m_email.hashCode());
			result = prime * result + ((m_phone == null) ? 0 : m_phone.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Owner other = (Owner) obj;
			if (m_email == null) {
				if (other.m_email != null)
					return false;
			} else if (!m_email.equals(other.m_email))
				return false;
			if (m_phone == null) {
				if (other.m_phone != null)
					return false;
			} else if (!m_phone.equals(other.m_phone))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Owner [m_phone=" + m_phone + ", m_email=" + m_email + "]";
		}
	}

	private List<Owner> getOwnersByGroup(String topic, String group) {
		if (!m_config.isConsumeLargeBacklogSmsToOwner()) {
			return m_hermesAdmins;
		}
		List<Owner> owners = new ArrayList<>();
		ConsumerGroupView consumer = m_consumerService.findConsumerView(topic, group);
		if (!addOwner(owners, consumer)) {
			owners.addAll(m_hermesAdmins);
		}
		return owners;
	}

	private boolean addOwner(List<Owner> owners, ConsumerGroupView consumer) {
		boolean hasOwner = false;

		consumer.setPhone1(cleanPhoneNumber(consumer.getPhone1()));
		consumer.setPhone2(cleanPhoneNumber(consumer.getPhone2()));
		consumer.setOwner1(cleanEmailAddress(consumer.getOwner1()));
		consumer.setOwner2(cleanEmailAddress(consumer.getOwner2()));

		if (!StringUtils.isBlank(consumer.getPhone1()) || !StringUtils.isBlank(consumer.getOwner1())) {
			hasOwner |= owners.add(new Owner(consumer.getPhone1(), consumer.getOwner1()));
		}

		if (!StringUtils.isBlank(consumer.getPhone2()) || !StringUtils.isBlank(consumer.getOwner2())) {
			hasOwner |= owners.add(new Owner(consumer.getPhone2(), consumer.getOwner2()));
		}

		return hasOwner;
	}

	private String cleanPhoneNumber(String phone) {
		java.util.regex.Matcher matcher = PHONE_PATTERN.matcher(phone);
		if (matcher.find()) {
			return matcher.group();
		}
		return null;
	}

	private String cleanEmailAddress(String email) {
		java.util.regex.Matcher matcher = EMAIL_PATTERN.matcher(email);
		if (matcher.find()) {
			return matcher.group();
		}
		return null;
	}
}
