package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.EPLs;
import com.ctrip.hermes.collector.rule.annotation.Table;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.rule.eventhandler.LongTimeNoProduceEventHandler.LongTimeNoProduceEventMailContent;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.ConsumerGroup;
import com.ctrip.hermes.metaservice.model.ConsumerGroupDao;
import com.ctrip.hermes.metaservice.model.ConsumerGroupEntity;
import com.ctrip.hermes.metaservice.model.ConsumerMonitorConfig;
import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.model.TopicDao;
import com.ctrip.hermes.metaservice.model.TopicEntity;
import com.ctrip.hermes.metaservice.monitor.event.BrokerCommandDropEvent;
import com.ctrip.hermes.metaservice.monitor.event.ConsumerDeadLetterEvent;
import com.ctrip.hermes.metaservice.monitor.event.LongTimeNoProduceEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.monitor.service.MonitorConfigService;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeType;
import com.ctrip.hermes.metaservice.service.notify.SmsNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy(TPGConsumeState.class)
@Table("create table dead_letter "
		+ "(topicId long primary key, topicName string, partitionId int primary key, consumerGroupId long primary key, consumerGroup string, deadLetterCount long, alertCount long, timestamp long)")
@EPLs({
	@EPL(name = DeadLetterEventHandler.FIND_DEAD_LETTER, value = "on TPGConsumeState(sync and storageType='mysql') "
			+ "select dl.topicId as topicId, dl.topicName as topicName, dl.consumerGroupId as consumerGroupId, dl.consumerGroup as consumerGroup, sum(dl.deadLetterCount) as alertCount, max(dl.timestamp) as timestamp "
			+ "from dead_letter as dl "
			+ "group by dl.topicId, dl.topicName, dl.consumerGroupId, dl.consumerGroup "
			+ "having sum(dl.deadLetterCount) > 0 "
			+ "order by dl.topicId, dl.consumerGroupId"
	),
	@EPL(name = DeadLetterEventHandler.UPDATE_DEAD_LETTER, value = "on TPGConsumeState(storageType='mysql') s "
			+ "merge into dead_letter dl "
			+ "where s.topicId = dl.topicId and s.partitionId = dl.partitionId and s.consumerGroupId = dl.consumerGroupId "
			+ "when matched then update set dl.alertCount = s.deadLetterCount - dl.deadLetterCount, dl.deadLetterCount = s.deadLetterCount, dl.timestamp = s.timestamp "
			+ "when not matched then insert select topicId, topicName, partitionId, consumerGroupId, consumerGroup, deadLetterCount, 0 as alertCount, timestamp")
})
public class DeadLetterEventHandler extends RuleEventHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterEventHandler.class);
	public static final String FIND_DEAD_LETTER = "findDeadLetter";
	public static final String UPDATE_DEAD_LETTER = "updateDeadLetter";
	
	private ConsumerGroupDao m_consumerDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private MonitorConfigService m_monitorConfigService = PlexusComponentLocator.lookup(MonitorConfigService.class);
	
	private DeadLetterEventReportMailContent m_reportContent;
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	protected void init() {
		super.init();
		m_reportContent = createReportContent();
		scheduleReportTask(new Runnable() {
			public void run() {
				if (m_reportContent.getReports().size() == 0) {
					return;
				}
				
				synchronized(DeadLetterEventHandler.this) {
					m_reportContent.setEndTime(System.currentTimeMillis());
					m_reportContent.finish();
					DeadLetterEventHandler.this.notify(new HermesNotice(DeadLetterEventHandler.this.getDefaultRecipients(HermesNoticeType.EMAIL), m_reportContent), null);
					m_reportContent = createReportContent();
				}
			}
		}, m_conf.getTimeIntervalInMinutesForReport(), TimeUnit.MINUTES);
	}
	
	private DeadLetterEventReportMailContent createReportContent() {
		DeadLetterEventReportMailContent content = new DeadLetterEventReportMailContent();
		content.setStartTime(System.currentTimeMillis());
		return content;
	}
	
	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		if (event.getEventSource().equals(FIND_DEAD_LETTER) && isRealtimeAlertEnabled()) {
	        EventBean[] beans = ((Pair<EventBean[], EventBean[]>) event.getData()).getFirst();
			List<MonitorEvent> events = new ArrayList<>();
			long defaultAlertLimit = getDefaultAlertLimit();
			for (EventBean bean : beans) {
				ConsumerDeadLetterEvent e = new ConsumerDeadLetterEvent();
				e.setTopic((String)bean.get("topicName"));
				e.setConsumerGroup((String)bean.get("consumerGroup"));
				e.setDeadLetterCount((long)bean.get("alertCount"));
				ConsumerMonitorConfig config = m_monitorConfigService.getConsumerMonitorConfig(e.getTopic(), e.getConsumerGroup());
				
				// Ignore the dead letter that is not up to the limit.
				if ((config == null && (defaultAlertLimit == -1 || e.getDeadLetterCount() < defaultAlertLimit)) || (config != null && (!config.isLargeDeadletterEnable() || e.getDeadLetterCount() < config.getLargeDeadletterLimit()))) {
					continue;
				}
				
				long timestamp = (long)bean.get("timestamp");
				e.setEndTime(new Date(timestamp));
				e.setStartTime(new Date(TimeUtils.before(timestamp, 5L, TimeUnit.MINUTES)));
				e.setCreateTime(new Date(timestamp));
				e.setShouldNotify(false);
				events.add(e);
				LOGGER.info("Created DEAD_LETTER event: {}, {}, {}", e.getTopic(), e.getConsumerGroup(), e.getDeadLetterCount());
				
				long topicId = (long)bean.get("topicId");
				ConsumerGroup consumer = null;
				try {
					consumer = m_consumerDao.findByTopicIdAndName(topicId, e.getConsumerGroup(), ConsumerGroupEntity.READSET_FULL);
				} catch (DalException ex) {
					LOGGER.error("Failed to find consumer group from db.", ex);
				}
				
				synchronized(DeadLetterEventHandler.this) {
					if (consumer != null) {
						m_reportContent.addReport(ReportKey.from(e.getTopic(), e.getConsumerGroup(), consumer.getOwner1(), consumer.getOwner2(), consumer.getPhone1(), consumer.getPhone2()), e);
					} else {
						m_reportContent.addReport(ReportKey.from(e.getTopic(), e.getConsumerGroup(), null, null, null, null), e);
					}
				}
				
				// Create category key.
				String categoryKey = String.format("%s-%s", e.getTopic(), e.getConsumerGroup());
				registerMailCategoryRate(categoryKey, m_conf.getDefaultMailFrequencyInterval(), TimeUnit.MINUTES);
				registerSmsCategoryRate(categoryKey, m_conf.getDefaultSmsFrequencyInterval(), TimeUnit.MINUTES);				

	            notify(Collections.singletonList((MonitorEvent)e), categoryKey);
			}
            return events;
		}
		
		return null;
	}
    
    @Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<>();
		
		ConsumerDeadLetterEvent event = (ConsumerDeadLetterEvent)events.get(0);
        DeadLetterEventMailContent content = new DeadLetterEventMailContent(event.getTopic(), event.getConsumerGroup(), event.getCreateTime().getTime());
        content.setEvents(events);
        
        ConsumerGroup consumerGroup = null;
        try {
        	Topic topic = m_topicDao.findByName(event.getTopic(), TopicEntity.READSET_FULL);
        	consumerGroup = m_consumerDao.findByTopicIdAndName(topic.getId(), event.getConsumerGroup(), ConsumerGroupEntity.READSET_FULL);
        } catch (Exception ex) {
        	LOGGER.error("Failed to find consumer group from db.", event);
        }
        
        List<String> recipients = null;
        if (consumerGroup != null) {
        	recipients = Utils.getRecipientsList(consumerGroup.getOwner1(), consumerGroup.getOwner2());
        }
        
        notices.add(new HermesNotice(recipients, content));
        
        if (consumerGroup != null) {
        	recipients = Utils.getRecipientsPhones(consumerGroup.getPhone1(), consumerGroup.getPhone2());
        }
        
        notices.add(new HermesNotice(recipients, getSmsNoticeContent(events)));
        return notices;
	}
    
    private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		builder.append("[死信]");
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			ConsumerDeadLetterEvent e = (ConsumerDeadLetterEvent)events.get(index);
			builder.append(String.format("T[%s]-C[%s]: %d", e.getTopic(), e.getConsumerGroup(), e.getDeadLetterCount()));
		}
		return new SmsNoticeContent(builder.toString());
	}
    
    @HermesMailDescription(template = HermesTemplate.TOPIC_LARGE_DEADLETTER)
    public class DeadLetterEventMailContent extends EventHandlerMailNoticeContent {
        
    	public DeadLetterEventMailContent(String topic, String consumerGroup, long timestamp) {
    		super(String.format("Topic-Consumer[%s-%s]Dead Letter报警", topic, consumerGroup), 0, timestamp);
    	}
    }
    
    @HermesMailDescription(template = HermesTemplate.TOPIC_LARGE_DEADLETTER_REPORT)
    public class DeadLetterEventReportMailContent extends EventHandlerReportMailNoticeContent<ReportKey, MonitorEvent> {
        
    	public DeadLetterEventReportMailContent() {
    		super(String.format("Dead Letter报告"));
    	}
    }

}
