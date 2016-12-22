package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.admin.core.model.ProducerMonitorConfig;
import com.ctrip.hermes.admin.core.model.ProducerMonitorConfigDao;
import com.ctrip.hermes.admin.core.model.ProducerMonitorConfigEntity;
import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.admin.core.monitor.event.LongTimeNoProduceEvent;
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
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.collector.utils.EsperUtils;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.espertech.esper.client.EventBean;

@Component
@TriggeredBy(TPProduceState.class)
@Utilities({EsperUtils.class})
@Table("create table produce_offset (topicId long primary key, topicName string, partitionId int primary key, storageType string, offsetPriority long, "
		+ "offsetNonPriority long, lastPriorityCreationDate Date, lastNonPriorityCreationDate Date, lastCreationDate Date, timestamp long)")
@EPLs({
	@EPL(name = LongTimeNoProduceEventHandler.UPSERT_PRODUCE, value = "on TPProduceState(not sync) ps "
			+ "merge into produce_offset po "
			+ "where ps.topicId = po.topicId and ps.partitionId = po.partitionId "
			+ "when matched then update "
			+ "set po.lastCreationDate = case when ps.storageType = 'mysql' then maxDate(ps.lastPriorityCreationDate, ps.lastNonPriorityCreationDate) "
			+ "when ps.offsetPriority + ps.offsetNonPriority - po.offsetPriority - po.offsetNonPriority > 0 then new Date(timestamp) else po.lastCreationDate end, "
			+ "po.offsetPriority = ps.offsetPriority, po.offsetNonPriority = ps.offsetNonPriority, po.lastPriorityCreationDate = ps.lastPriorityCreationDate, "
			+ "po.lastNonPriorityCreationDate = ps.lastNonPriorityCreationDate, po.timestamp = ps.timestamp "
			+ "when not matched then insert "
			+ "select ps.topicId as topicId, ps.topicName as topicName, ps.partitionId as partitionId, ps.storageType as storageType, ps.offsetPriority as offsetPriority, "
			+ "ps.offsetNonPriority as offsetNonPriority, ps.lastPriorityCreationDate as lastPriorityCreationDate, "
			+ "ps.lastNonPriorityCreationDate as lastNonPriorityCreationDate, case when ps.storageType = 'mysql' then maxDate(ps.lastPriorityCreationDate, ps.lastNonPriorityCreationDate) "
			+ "else new Date(ps.timestamp) end as lastCreationDate, "
			+ "ps.timestamp as timestamp"),
	@EPL(name = LongTimeNoProduceEventHandler.FIND_LONG_TIME_NO_PRODUCE, value = "on TPProduceState(sync) ps "
			+ "select po.topicId as topicId, po.topicName as topicName, po.partitionId as partitionId, po.storageType as storageType, po.lastPriorityCreationDate as lastPriorityCreationDate, "
			+ "po.lastNonPriorityCreationDate as lastNonPriorityCreationDate, po.lastCreationDate as lastCreationDate, max(po.lastCreationDate) as maxLastCreationDate, "
			+ "po.timestamp as timestamp "
			+ "from produce_offset po "
			+ "where po.storageType = ps.storageType "
			+ "group by po.topicId "
			+ "order by po.topicId")
})
public class LongTimeNoProduceEventHandler extends RuleEventHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(LongTimeNoProduceEventHandler.class);
	public static final String UPSERT_PRODUCE = "upsertProduce";
	public static final String FIND_LONG_TIME_NO_PRODUCE = "findLongTimeNoProduce";
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	private ProducerMonitorConfigDao producerMonitorConfigDao = PlexusComponentLocator.lookup(ProducerMonitorConfigDao.class);

	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private LongTimeNoProduceEventReportMailContent m_reportContent;
	
	protected void init() {
		super.init();
		m_reportContent = createReportContent();
		scheduleReportTask(new Runnable() {
			public void run() {
				if (m_reportContent.getReports().size() == 0) {
					return;
				}
				
				synchronized(LongTimeNoProduceEventHandler.this) {
					m_reportContent.setEndTime(System.currentTimeMillis());
					m_reportContent.finish();
					HermesNotice notice = new HermesNotice(LongTimeNoProduceEventHandler.this.getDefaultRecipients(HermesNoticeType.EMAIL), m_reportContent);
					LongTimeNoProduceEventHandler.this.notify(notice, null);
					LOGGER.info("Generate report sent to {} for long time no produce.", StringUtils.join(notice.getReceivers(), ","));

					m_reportContent = createReportContent();
				}
			}
		}, m_conf.getTimeIntervalInMinutesForReport(), TimeUnit.MINUTES);
	}
	
	private LongTimeNoProduceEventReportMailContent createReportContent() {
		LongTimeNoProduceEventReportMailContent content = new LongTimeNoProduceEventReportMailContent();
		content.setStartTime(System.currentTimeMillis());
		return content;
	}

	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		if (FIND_LONG_TIME_NO_PRODUCE.equals(event.getEventSource()) && isRealtimeAlertEnabled()) {
			List<MonitorEvent> events = new ArrayList<>();
			EventBean[] beans = ((com.espertech.esper.collection.Pair<EventBean[], EventBean[]>)event.getData()).getFirst();
			String topicName = null;
			
			int start = 0; 
			for (int index = 0; index < beans.length; index++) {
				EventBean bean = beans[index];
				if (topicName == null || !topicName.equals(bean.get("topicName"))) {
					if (topicName != null) {
						alertIfNecessary(events, Arrays.copyOfRange(beans, start, index));
					}
					
					topicName = (String)bean.get("topicName");
					start = index;
				}
			}
			
			alertIfNecessary(events, Arrays.copyOfRange(beans, start, beans.length));
			return events;
		}
		return null;
	}
	
	private void alertIfNecessary(List<MonitorEvent> events, EventBean[] beans) {
		if (beans.length == 0) {
			return;
		}
		
		if (!isKafkaAlertEnabled() && Storage.KAFKA.equals(beans[0].get("storageType"))) {
			return;
		}
		
		Date maxLastModifedDate = (Date)beans[0].get("maxLastCreationDate");
		if (maxLastModifedDate == null) {
			LOGGER.warn("Ignore on verify long time no produce on topic {} due to no timestamp.", beans[0].get("topicName"));
			return;
		}
		
		long timestamp = (long)beans[0].get("timestamp");
		String topicName = (String)beans[0].get("topicName");
		
		List<ProducerMonitorConfig> producerMonitorConfigs = null;
		ProducerMonitorConfig producerMonitorConfig = null;
		try {
			producerMonitorConfigs = producerMonitorConfigDao.findByTopic(topicName, ProducerMonitorConfigEntity.READSET_FULL);
		} catch (DalException e) {
			LOGGER.error("Failed to find ProducerMonitorConfig from db: {}", e.getMessage());
		}
		
		if (producerMonitorConfigs != null && producerMonitorConfigs.size() > 0) {
			producerMonitorConfig = producerMonitorConfigs.get(0);
		}
		
		long defaultAlertLimit = getDefaultAlertLimit();
		long lag = timestamp - maxLastModifedDate.getTime();
		
		if (producerMonitorConfig == null && (defaultAlertLimit == -1 || lag < TimeUnit.MINUTES.toMillis(defaultAlertLimit))) {
			return;
		}
		
	
		if (lag > m_conf.getDefaultLongTimeNoProduceMaxLimit() || (producerMonitorConfig != null && (!producerMonitorConfig.isLongTimeNoProduceEnable() 
						|| lag < TimeUnit.MINUTES.toMillis(producerMonitorConfig.getLongTimeNoProduceLimit())))) {
			return;
		}
	
		Map<Integer, Pair<Integer, CreationStamp>> partition2Timestamp = new HashMap<>();
		for (EventBean bean : beans) {
			topicName = (String)bean.get("topicName");
			Integer partition = (Integer)bean.get("partitionId");
			Date creationDate = (Date)bean.get("lastCreationDate");
			partition2Timestamp.put(partition, new Pair<Integer, CreationStamp>(-1, new CreationStamp(partition, creationDate)));
		}
		
		MonitorEvent event = createMonitorEvent(topicName, partition2Timestamp);
		events.add(event);
		
		Topic topic = null;
		try {
			topic = m_topicDao.findByName(topicName, TopicEntity.READSET_FULL);
		} catch (DalException ex) {
			LOGGER.error("Failed to find topic from db.", ex);
		}
		
		synchronized(LongTimeNoProduceEventHandler.this) {
			if (topic != null) {
				m_reportContent.addReport(ReportKey.from(topicName, topic.getOwner1(), topic.getOwner2(), topic.getPhone1(), topic.getPhone2()), event);
			} else {
				m_reportContent.addReport(ReportKey.from(topicName, null, null, null, null), event);
			}
		}

		// Create category key.
		registerMailCategoryRate(topicName, m_conf.getDefaultMailFrequencyInterval(), TimeUnit.MINUTES);
		registerSmsCategoryRate(topicName, m_conf.getDefaultSmsFrequencyInterval(), TimeUnit.MINUTES);	
		
		notify(Collections.singletonList(event), topicName);
	}
	
	private MonitorEvent createMonitorEvent(String topicName, Map<Integer, Pair<Integer, CreationStamp>> partition2Timestamp) {
		LongTimeNoProduceEvent e = new LongTimeNoProduceEvent();
		e.setTopic(topicName);
		e.setLimitsAndStamps(partition2Timestamp);
		e.setCreateTime(new Date());
		e.setShouldNotify(false);
		LOGGER.info("Created LONG_TIME_NO_PRODUCE event: {}", e.getTopic());
		return e;
	}

	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<>();
		LongTimeNoProduceEvent event = (LongTimeNoProduceEvent)events.get(0);
        LongTimeNoProduceEventMailContent content = new LongTimeNoProduceEventMailContent(event.getTopic(), event.getCreateTime().getTime());
        content.setEvents(events);
        
        Topic topic = null;
		try {
			topic = m_topicDao.findByName(event.getTopic(), TopicEntity.READSET_FULL);
		} catch (DalException e1) {
			LOGGER.error("Failed to find topic from db.", event);
		}
		
        List<String> recipients = this.getDefaultRecipients(HermesNoticeType.EMAIL);
		if (topic != null) {
			recipients = Utils.getRecipientsList(topic.getOwner1(), topic.getOwner2());
		}
		
		notices.add(new HermesNotice(recipients, content));
				
		recipients = this.getDefaultRecipients(HermesNoticeType.SMS);
		if (topic != null) {
			recipients = Utils.getRecipientsPhones(topic.getPhone1(), topic.getPhone2());
		}
		
		notices.add(new HermesNotice(recipients, getSmsNoticeContent(events)));
    
        return notices;
	}
	
	private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		Date createTime = ((LongTimeNoProduceEvent)events.get(0)).getCreateTime();
		builder.append("[长时间未生产]");
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			LongTimeNoProduceEvent e = (LongTimeNoProduceEvent)events.get(index);
			Date maxTime = null;
			for (Pair<Integer, CreationStamp> limitAndStamp : e.getLimitsAndStamps().values()) {
				if (maxTime == null || (limitAndStamp.getValue().getDate() != null && maxTime.compareTo(limitAndStamp.getValue().getDate()) < 0)) {
					maxTime = limitAndStamp.getValue().getDate();
				}
			}
			builder.append(String.format("T[%s]: %s", e.getTopic(), TimeUtils.toHumanReadableTimeInterval(createTime, maxTime)));
		}
		return new SmsNoticeContent(builder.toString());
	}
    
    @HermesMailDescription(template = HermesTemplate.LONG_TIME_NO_PRODUCE)
    public class LongTimeNoProduceEventMailContent extends EventHandlerMailNoticeContent {
        public LongTimeNoProduceEventMailContent(String topic, long timestamp) {
        	super(String.format("Topic[%s]长时间未生产报警", topic), 0, timestamp);
        }
    }
    
    @HermesMailDescription(template = HermesTemplate.LONG_TIME_NO_PRODUCE_REPORT)
    public class LongTimeNoProduceEventReportMailContent extends EventHandlerReportMailNoticeContent<ReportKey, MonitorEvent> {
    	public LongTimeNoProduceEventReportMailContent(){
    		super("长时间未生产报告");
    	}
    }

}
