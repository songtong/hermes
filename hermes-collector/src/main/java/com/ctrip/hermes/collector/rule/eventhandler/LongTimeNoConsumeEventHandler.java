package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.ConsumerGroup;
import com.ctrip.hermes.admin.core.model.ConsumerGroupDao;
import com.ctrip.hermes.admin.core.model.ConsumerGroupEntity;
import com.ctrip.hermes.admin.core.model.ConsumerMonitorConfig;
import com.ctrip.hermes.admin.core.model.ConsumerMonitorConfigDao;
import com.ctrip.hermes.admin.core.model.ConsumerMonitorConfigEntity;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoConsumeEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.HermesNoticeContent;
import com.ctrip.hermes.admin.core.service.notify.HermesNoticeType;
import com.ctrip.hermes.admin.core.service.notify.SmsNoticeContent;
import com.ctrip.hermes.admin.core.service.template.HermesTemplate;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.EPLs;
import com.ctrip.hermes.collector.rule.annotation.Table;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.rule.annotation.Utilities;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.utils.EsperUtils;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy(TPGConsumeState.class)
@Utilities({ EsperUtils.class })
@Table("create table consume_offset ("
		+ "topicId long primary key, topicName string, partitionId int primary key, storageType string, consumerGroupId long primary key, "
		+ "consumerGroup string, offsetPriority long, offsetNonPriority long, offsetPriorityModifiedDate Date, offsetNonPriorityModifiedDate Date, offsetModifiedDate Date, "
		+ "timestamp long)")
@EPLs({
		@EPL("on TPGConsumeState(not sync and (cs.offsetPriorityModifiedDate is not null or cs.offsetNonPriorityModifiedDate is not null)) cs "
				+ "merge into consume_offset co "
				+ "where cs.topicId = co.topicId and cs.partitionId = co.partitionId and cs.consumerGroupId = co.consumerGroupId "
				+ "when matched then update "
				+ "set co.offsetModifiedDate = case when cs.storageType = 'mysql' then maxDate(cs.offsetPriorityModifiedDate, cs.offsetNonPriorityModifiedDate) "
				+ "when cs.offsetPriority + cs.offsetNonPriority - co.offsetPriority - co.offsetNonPriority > 0 then new Date(cs.timestamp) else co.offsetModifiedDate end, "
				+ "co.offsetPriority = cs.offsetPriority, co.offsetNonPriority = cs.offsetNonPriority, co.offsetPriorityModifiedDate = cs.offsetPriorityModifiedDate, "
				+ "co.offsetNonPriorityModifiedDate = cs.offsetNonPriorityModifiedDate, co.timestamp = cs.timestamp "
				+ "when not matched then insert "
				+ "select cs.topicId as topicId, cs.topicName as topicName, cs.partitionId as partitionId, cs.storageType as storageType, cs.consumerGroupId as consumerGroupId, cs.consumerGroup as consumerGroup, "
				+ "cs.offsetPriority as offsetPriority, cs.offsetNonPriority as offsetNonPriority, cs.offsetPriorityModifiedDate as offsetPriorityModifiedDate, "
				+ "cs.offsetNonPriorityModifiedDate as offsetNonPriorityModifiedDate, case when cs.storageType = 'mysql' then maxDate(cs.offsetPriorityModifiedDate, cs.offsetNonPriorityModifiedDate) "
				+ "else new Date(cs.timestamp) end as offsetModifiedDate, cs.timestamp as timestamp"),
		@EPL(name = LongTimeNoConsumeEventHandler.FIND_LONG_TIME_NO_CONSUME, value = "on TPGConsumeState(sync) cs "
				+ "select co.topicId as topicId, co.topicName as topicName, co.partitionId as partitionId, co.storageType as storageType, co.consumerGroupId as consumerGroupId, co.consumerGroup as consumerGroup, "
				+ "co.offsetPriorityModifiedDate as offsetPriorityModifiedDate, co.offsetNonPriorityModifiedDate as offsetNonPriorityModifiedDate, co.offsetModifiedDate as offsetModifiedDate, "
				+ "max(co.offsetModifiedDate) as maxOffsetModifiedDate, co.timestamp as timestamp "
				+ "from consume_offset co "
				+ "where co.storageType = cs.storageType "
				+ "group by co.topicId, co.consumerGroupId "
				+ "order by co.topicId, co.consumerGroupId") })
public class LongTimeNoConsumeEventHandler extends RuleEventHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(LongTimeNoConsumeEventHandler.class);
	public static final String FIND_LONG_TIME_NO_CONSUME = "findLongTimeNoConsume";

	@Autowired
	private CollectorConfiguration m_conf;
	private ConsumerMonitorConfigDao m_consumerMonitorConfigDao = PlexusComponentLocator
			.lookup(ConsumerMonitorConfigDao.class);
	private ConsumerGroupDao m_consumerDao = PlexusComponentLocator
			.lookup(ConsumerGroupDao.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator
			.lookup(TopicDao.class);
	
	private LongTimeNoConsumeEventReportMailContent m_reportContent;
	
	protected void init() {
		super.init();
		m_reportContent = createReportContent();
		scheduleReportTask(new Runnable() {
			public void run() {
				if (m_reportContent.getReports().size() == 0) {
					return;
				}
				
				synchronized(LongTimeNoConsumeEventHandler.this) {
					m_reportContent.setEndTime(System.currentTimeMillis());
					m_reportContent.finish();
					LongTimeNoConsumeEventHandler.this.notify(new HermesNotice(Arrays.asList(m_conf.getNotifierDefaultMail().split(",")), m_reportContent), null);
					m_reportContent = createReportContent();
				}
			}
		}, m_conf.getTimeIntervalInMinutesForReport(), TimeUnit.MINUTES);
	}
	
	private LongTimeNoConsumeEventReportMailContent createReportContent() {
		LongTimeNoConsumeEventReportMailContent content = new LongTimeNoConsumeEventReportMailContent();
		content.setStartTime(System.currentTimeMillis());
		return content;
	}

	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		if (FIND_LONG_TIME_NO_CONSUME.equals(event.getEventSource()) && isRealtimeAlertEnabled()) {
			List<MonitorEvent> events = new ArrayList<>();
			EventBean[] beans = ((Pair<EventBean[], EventBean[]>) event
					.getData()).getFirst();

			String consumerGroup = null;
			String topicName = null;

			int start = 0;
			for (int index = 0; index < beans.length; index++) {
				EventBean bean = beans[index];
				if (topicName == null || consumerGroup == null
						|| !consumerGroup.equals(bean.get("consumerGroup"))
						|| !topicName.equals(bean.get("topicName"))) {
					if (topicName != null) {
						alertIfNecessary(events,
								Arrays.copyOfRange(beans, start, index));
					}

					topicName = (String) bean.get("topicName");
					consumerGroup = (String) bean.get("consumerGroup");
					start = index;
				}
			}

			alertIfNecessary(events,
					Arrays.copyOfRange(beans, start, beans.length));
			return events;
		}

		return null;
	}

	private void alertIfNecessary(List<MonitorEvent> events, EventBean[] beans) {
		if (beans.length == 0) {
			return;
		}
		
		if (!this.isKafkaAlertEnabled() && Storage.KAFKA.equals(beans[0].get("storageType"))) {
			return;
		}
		
		Date maxOffsetModifedDate = (Date)beans[0].get("maxOffsetModifiedDate");
		String topicName = (String)beans[0].get("topicName");
		String consumerGroup = (String)beans[0].get("consumerGroup");
		long timestamp = (long)beans[0].get("timestamp");
		
		if (maxOffsetModifedDate == null) {
			LOGGER.error("Failed to determine offset modified date: {} {}", topicName, consumerGroup);
			return;
		}

		List<ConsumerMonitorConfig> consumerMonitorConfigs = null;
		ConsumerMonitorConfig consumerMonitorConfig = null;
		try {
			consumerMonitorConfigs = m_consumerMonitorConfigDao.findByTopicConsumer(topicName, consumerGroup, ConsumerMonitorConfigEntity.READSET_FULL);
		} catch (DalException e) {
			LOGGER.error("Failed to find ConsumerMonitorConfig from db: {}", e.getMessage());
		}
		
		if (consumerMonitorConfigs != null && consumerMonitorConfigs.size() > 0) {
			consumerMonitorConfig = consumerMonitorConfigs.get(0);
		}
		
		long lag = timestamp - maxOffsetModifedDate.getTime();
		long defaultAlertLimit = getDefaultAlertLimit();
		
		if (consumerMonitorConfig == null && (defaultAlertLimit == -1 || lag < TimeUnit.MINUTES.toMillis(defaultAlertLimit))) {
			return;
		}
		
		if (lag > m_conf.getDefaultLongTimeNoConsumeMaxLimit() || (consumerMonitorConfig != null && (!consumerMonitorConfig.isLongTimeNoConsumeEnable() 
						|| lag < TimeUnit.MINUTES.toMillis(consumerMonitorConfig.getLongTimeNoConsumeLimit())))) {
			return;
		}
		
		Map<Integer, CreationStamp> partition2Timestamp = new HashMap<>();
		for (EventBean bean : beans) {
			int partition = (int)bean.get("partitionId");
			CreationStamp creationStamp = new CreationStamp(partition, (Date)bean.get("offsetModifiedDate"));
			partition2Timestamp.put(partition, creationStamp);
		}
		
		MonitorEvent e = createMonitorEvent(topicName, consumerGroup, partition2Timestamp);
		events.add(e);
		
		long topicId = (long)beans[0].get("topicId");
		ConsumerGroup consumer = null;
		try {
			consumer = m_consumerDao.findByTopicIdAndName(topicId, consumerGroup, ConsumerGroupEntity.READSET_FULL);
		} catch (DalException ex) {
			LOGGER.error("Failed to find consumer group from db.", ex);
		}
		
		synchronized(LongTimeNoConsumeEventHandler.this) {
			if (consumer != null) {
				m_reportContent.addReport(ReportKey.from(topicName, consumerGroup, consumer.getOwner1(), consumer.getOwner2(), consumer.getPhone1(), consumer.getPhone2()), e);
			} else {
				m_reportContent.addReport(ReportKey.from(topicName, consumerGroup, null, null, null, null), e);
			}
		}
		
		// Create category key.
		String categoryKey = String.format("%s-%s", topicName, consumerGroup);
		registerMailCategoryRate(categoryKey, m_conf.getDefaultMailFrequencyInterval(), TimeUnit.MINUTES);
		registerSmsCategoryRate(categoryKey, m_conf.getDefaultSmsFrequencyInterval(), TimeUnit.MINUTES);
		
		notify(Collections.singletonList(e), categoryKey);
	}

	private MonitorEvent createMonitorEvent(String topicName,
			String consumerGroup,
			Map<Integer, CreationStamp> partition2Timestamp) {
		LongTimeNoConsumeEvent e = new LongTimeNoConsumeEvent();
		e.setTopic(topicName);
		e.setConsumer(consumerGroup);
		e.setLatestConsumed(partition2Timestamp);
		e.setCreateTime(new Date());
		e.setShouldNotify(false);
		LOGGER.info("Created LONG_TIME_NO_CONSUME event: {}, {}", e.getTopic(),
				e.getConsumer());
		return e;
	}
	
	private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		Date createTime = ((LongTimeNoConsumeEvent)events.get(0)).getCreateTime();
		builder.append("[长时间未消费]");
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			LongTimeNoConsumeEvent e = (LongTimeNoConsumeEvent)events.get(index);
			Date maxTime = null;
			for (CreationStamp creationStamp : e.getLatestConsumed().values()) {
				if (maxTime == null || (creationStamp.getDate() != null && maxTime.compareTo(creationStamp.getDate()) < 0)) {
					maxTime = creationStamp.getDate();
				}
			}
			builder.append(String.format("T[%s]-C[%s]: %s", e.getTopic(), e.getConsumer(), TimeUtils.toHumanReadableTimeInterval(createTime, maxTime)));
		}
		return new SmsNoticeContent(builder.toString());
	}
	
	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<>();
		
		LongTimeNoConsumeEvent event = (LongTimeNoConsumeEvent) events.get(0);
		LongTimeNoConsumeEventMailContent content = new LongTimeNoConsumeEventMailContent(event.getTopic(), 
				event.getConsumer(), event.getCreateTime().getTime());
		content.setEvents(events);
		
		ConsumerGroup consumerGroup = null;
		try {
        	Topic topic = m_topicDao.findByName(event.getTopic(), TopicEntity.READSET_FULL);
        	consumerGroup = m_consumerDao.findByTopicIdAndName(topic.getId(), event.getConsumer(), ConsumerGroupEntity.READSET_FULL);
	    } catch (Exception ex) {
	    	LOGGER.error("Failed to find consumer group from db.", event);
	    }

		List<String> recipients = this.getDefaultRecipients(HermesNoticeType.EMAIL);
		if (consumerGroup != null) {
			recipients = Utils.getRecipientsList(consumerGroup.getOwner1(), consumerGroup.getOwner2());
		}
		
		notices.add(new HermesNotice(recipients, content));
		
		recipients = this.getDefaultRecipients(HermesNoticeType.SMS);
		if (consumerGroup != null) {
			recipients = Utils.getRecipientsPhones(consumerGroup.getPhone1(), consumerGroup.getPhone2());
		}
		
        notices.add(new HermesNotice(recipients, getSmsNoticeContent(events)));
		return notices;
	}

	@HermesMailDescription(template = HermesTemplate.LONG_TIME_NO_CONSUME)
	public class LongTimeNoConsumeEventMailContent extends
			EventHandlerMailNoticeContent {
		public LongTimeNoConsumeEventMailContent(String topic, String consumerGroup, long timestamp) {
			super(String.format("Topic-Consumer[%s-%s]长时间未消费报警", topic, consumerGroup), 0, timestamp);
		}

	}
	
	@HermesMailDescription(template = HermesTemplate.LONG_TIME_NO_CONSUME_REPORT)
	public class LongTimeNoConsumeEventReportMailContent extends EventHandlerReportMailNoticeContent<ReportKey, MonitorEvent> {
		public LongTimeNoConsumeEventReportMailContent() {
			super("长时间未消费报告");
		}
	}

}
