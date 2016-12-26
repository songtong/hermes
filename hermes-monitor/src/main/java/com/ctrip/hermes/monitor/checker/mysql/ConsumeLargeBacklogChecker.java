package com.ctrip.hermes.monitor.checker.mysql;

import io.netty.util.internal.ConcurrentSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.model.ConsumerMonitorConfig;
import com.ctrip.hermes.admin.core.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.service.MonitorConfigService;
import com.ctrip.hermes.admin.core.queue.MessagePriorityDao;
import com.ctrip.hermes.admin.core.queue.OffsetMessageDao;
import com.ctrip.hermes.admin.core.service.ConsumerService;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.HermesNoticeContent;
import com.ctrip.hermes.admin.core.service.notify.NotifyService;
import com.ctrip.hermes.admin.core.service.notify.SmsNoticeContent;
import com.ctrip.hermes.admin.core.view.ConsumerGroupView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.monitor.checker.CheckerResult;
import com.ctrip.hermes.monitor.checker.DBBasedChecker;
import com.ctrip.hermes.monitor.checker.exception.CompositeException;
import com.ctrip.hermes.monitor.checker.mysql.task.ConsumeBacklogCheckerTask;
import com.ctrip.hermes.monitor.checker.notification.LargeBacklogMailContent;
import com.ctrip.hermes.monitor.checker.notification.LargeBacklogReportMailContent;
import com.ctrip.hermes.monitor.utils.MonitorUtils;

//@Component(value = ConsumeLargeBacklogChecker.ID)
public class ConsumeLargeBacklogChecker extends DBBasedChecker implements InitializingBean {

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

	private static Owner m_rdHermes = new Owner("", "Rdkjmes@Ctrip.com");

	private long m_lastReportTime = new Date().getTime();

	private AtomicReference<LargeBacklogReportMailContent> m_report = //
	new AtomicReference<>(new LargeBacklogReportMailContent());

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

	protected Map<Pair<Topic, ConsumerGroup>, Long> parseLimits( //
	      Meta meta, Map<Pair<String, String>, ConsumerMonitorConfig> configs) {
		Map<Pair<Topic, ConsumerGroup>, Long> limits = new HashMap<>();
		if (meta != null) {
			for (Entry<String, Topic> entry : meta.getTopics().entrySet()) {
				for (ConsumerGroup consumer : entry.getValue().getConsumerGroups()) {
					if (Storage.MYSQL.equals(entry.getValue().getStorageType())) {
						ConsumerMonitorConfig cfg = configs.get(new Pair<String, String>(entry.getKey(), consumer.getName()));
						if (cfg != null && cfg.isLargeBacklogEnable() && cfg.getLargeBacklogLimit() > 0) {
							limits.put(new Pair<>(entry.getValue(), consumer), (long) cfg.getLargeBacklogLimit());
						}
					}
				}
			}
		}
		return limits;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		final CheckerResult result = new CheckerResult();
		if (m_config.isMonitorCheckerEnable()) {
			ExecutorService es = Executors.newFixedThreadPool(DB_CHECKER_THREAD_COUNT);
			try {
				Meta meta = fetchMeta();
				Map<Pair<String, String>, ConsumerMonitorConfig> cfgs = verifyConsumerMonitorConfigs(meta);
				Map<Pair<Topic, ConsumerGroup>, Long> limits = parseLimits(meta, cfgs);
				ConcurrentSet<Exception> exceptions = new ConcurrentSet<Exception>();
				List<Map<Pair<Topic, ConsumerGroup>, Long>> splited = MonitorUtils
				      .splitMap(limits, DB_CHECKER_THREAD_COUNT);
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

			if (m_config.isMonitorCheckerNotifyEnable()) {
				notifyOwnersIfNecessary(result);
				notifyLargeBacklogReportIfNecessary(result);
			}
		}
		return result;
	}

	private void notifyLargeBacklogReportIfNecessary(CheckerResult result) {
		long now = System.currentTimeMillis();
		for (MonitorEvent event : result.getMonitorEvents()) {
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent) event;
			m_report.get().addMonitorEvent(e, m_consumerService.findConsumerView(e.getTopic(), e.getGroup()));
		}
		if (now - m_lastReportTime >= TimeUnit.MINUTES.toMillis(m_config.getConsumeLargeBacklogReportIntervalMin())) {
			LargeBacklogReportMailContent report = m_report.get();
			if (report.getReports().size() > 0) {
				try {
					m_notifyService.notify(new HermesNotice(getEmails(m_hermesAdmins), m_report.get()));
				} catch (Exception e) {
					log.error("Send large backlog report failed.", e);
				}
			}
			m_report.set(new LargeBacklogReportMailContent());
			m_lastReportTime = now;
		}
	}

	private List<String> getEmails(List<Owner> owners) {
		List<String> list = new ArrayList<String>();
		for (Owner owner : owners) {
			if (!StringUtils.isBlank(owner.getEmail())) {
				list.add(owner.getEmail());
			}
		}
		return list;
	}

	private Map<Pair<String, String>, ConsumerMonitorConfig> verifyConsumerMonitorConfigs(Meta meta) {
		Map<Pair<String, String>, ConsumerMonitorConfig> cfgs = listToMap(
		      m_monitorConfigService.listConsumerMonitorConfig(),
		      new KeyHolder<Pair<String, String>, ConsumerMonitorConfig>() {
			      @Override
			      public Pair<String, String> getKey(ConsumerMonitorConfig item) {
				      return new Pair<String, String>(item.getTopic(), item.getConsumer());
			      }
		      });
		Set<Pair<String, String>> consumers = new HashSet<>();
		for (Entry<String, Topic> entry : meta.getTopics().entrySet()) {
			for (ConsumerGroup c : entry.getValue().getConsumerGroups()) {
				Pair<String, String> consumer = new Pair<String, String>(entry.getKey(), c.getName());
				consumers.add(consumer);
				if (!cfgs.containsKey(consumer)) {
					log.info("Can not find {} in consumer monitor configs, add it.", consumer);
					m_monitorConfigService.setConsumerMonitorConfig(//
					      m_monitorConfigService.newDefaultConsumerMonitorConfig(consumer.getKey(), consumer.getValue()));
				}
			}
		}
		Set<Pair<String, String>> removed = new HashSet<>();
		for (Entry<Pair<String, String>, ConsumerMonitorConfig> entry : cfgs.entrySet()) {
			if (!consumers.contains(entry.getKey())) {
				removed.add(entry.getKey());
			}
		}
		for (Pair<String, String> key : removed) {
			cfgs.remove(key);
			m_monitorConfigService.deleteConsumerMonitorConfig(key.getKey(), key.getValue());
			log.info("Outdated consumer monitor config: {}, delete it.", key);
		}
		return cfgs;
	}

	private static interface KeyHolder<K, T> {
		public K getKey(T item);
	}

	private <T, K> Map<K, T> listToMap(List<T> list, KeyHolder<K, T> keyHolder) {
		Map<K, T> map = new HashMap<>();
		for (T t : list)
			map.put(keyHolder.getKey(t), t);
		return map;
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

	private HermesNoticeContent generateSmsContent(List<MonitorEvent> events) {
		StringBuilder sb = new StringBuilder("消费积压: ");
		for (MonitorEvent event : events) {
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent) event;
			sb.append(String.format("T[%s], C[%s]: [%s] \n", e.getTopic(), e.getGroup(), e.getTotalBacklog()));
		}
		return new SmsNoticeContent(sb.toString());
	}

	private HermesNoticeContent generateEmailContent(List<MonitorEvent> events) {
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

		if (consumer != null) {
			List<Owner> configedOwners = getConfigedOwners(consumer);
			owners.addAll(configedOwners);

			if (!addOwner(owners, consumer) && configedOwners.size() == 0) {
				log.warn("Can not found owner for consumer: {}[{}], please config it.", topic, group);
				owners.addAll(m_hermesAdmins);
			}
		}

		return owners;
	}

	private List<Owner> getConfigedOwners(ConsumerGroupView consumer) {
		List<Owner> owners = new ArrayList<>();
		try {
			ConsumerMonitorConfig cfg = //
			m_monitorConfigService.getConsumerMonitorConfig(consumer.getTopicName(), consumer.getName());
			if (cfg != null && !StringUtils.isBlank(cfg.getAlarmReceivers())) {
				for (Owner owner : JSON.parseObject(cfg.getAlarmReceivers(), new TypeReference<List<Owner>>() {
				})) {
					if (!StringUtils.isBlank(owner.getEmail()) || StringUtils.isBlank(owner.getPhone())) {
						owners.add(owner);
					}
				}
			}
		} catch (Exception e) {
			log.warn("Load configed consumer owner failed: {}", consumer);
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
		if (!StringUtils.isBlank(phone)) {
			java.util.regex.Matcher matcher = PHONE_PATTERN.matcher(phone);
			if (matcher.find()) {
				return matcher.group();
			}
		}
		return null;
	}

	private String cleanEmailAddress(String email) {
		if (!StringUtils.isBlank(email)) {
			java.util.regex.Matcher matcher = EMAIL_PATTERN.matcher(email);
			if (matcher.find()) {
				return matcher.group();
			}
		}
		return null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		m_notifyService.registerRateLimiter(m_rdHermes.getEmail(), 1, TimeUnit.SECONDS);
	}
}
