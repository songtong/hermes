package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

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
import com.ctrip.hermes.admin.core.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
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
import com.ctrip.hermes.collector.rule.annotation.Tables;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.TPGConsumeState;
import com.ctrip.hermes.collector.state.impl.TPProduceState;
import com.ctrip.hermes.collector.utils.Utils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Storage;
import com.espertech.esper.client.EPPreparedStatement;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy({TPProduceState.class, TPGConsumeState.class})
@Tables({
	@Table("create table produce_offset (topicId long primary key, topicName string, partitionId int primary key, storageType string, offsetPriority long, offsetNonPriority long, lastPriorityCreationDate Date, lastNonPriorityCreationDate Date)"), 
	@Table("create table consume_offset (topicId long primary key, topicName string, partitionId int primary key, storageType string, consumerGroupId long primary key, consumerGroup string, offsetPriority long, offsetNonPriority long, "
			+ "offsetPriorityModifiedDate Date, offsetNonPriorityModifiedDate Date)"),
	@Table("create table sync_event (storageType string primary key, type string primary key, timestamp long)")
})
//@Window("create window sync_event.std:lastevent() (storageType string, createdTime long)")
@EPLs({
	@EPL(name = "check", value = "select * from State"),
	@EPL(name = ConsumerLargeBacklogEventHandler.PRODUCE_UPSERT, value = "on TPProduceState(not sync) s merge into produce_offset po "
			+ "where s.topicId = po.topicId and s.partitionId = po.partitionId "
			+ "when matched then update "
			+ "set po.offsetPriority = s.offsetPriority, po.offsetNonPriority = s.offsetNonPriority, po.lastPriorityCreationDate = lastPriorityCreationDate, po.lastNonPriorityCreationDate = lastNonPriorityCreationDate "
			+ "when not matched then insert "
			+ "select s.topicId as topicId, s.topicName as topicName, s.partitionId as partitionId, s.storageType as storageType, s.offsetPriority as offsetPriority, s.offsetNonPriority as offsetNonPriority, s.lastPriorityCreationDate as lastPriorityCreationDate, "
			+ "s.lastNonPriorityCreationDate as lastNonPriorityCreationDate"), 
	@EPL(name = ConsumerLargeBacklogEventHandler.CONSUME_UPSERT, value = "on TPGConsumeState(not sync) s merge into consume_offset co "
			+ "where s.topicId = co.topicId and s.partitionId = co.partitionId and s.consumerGroupId = co.consumerGroupId "
			+ "when matched then update "
			+ "set co.offsetPriority = s.offsetPriority, co.offsetNonPriority = s.offsetNonPriority, co.offsetPriorityModifiedDate = s.offsetPriorityModifiedDate, co.offsetNonPriorityModifiedDate = s.offsetNonPriorityModifiedDate "
			+ "when not matched then insert "
			+ "select s.topicId as topicId, s.topicName as topicName, s.partitionId as partitionId, s.storageType as storageType, s.consumerGroupId as consumerGroupId, s.consumerGroup as consumerGroup, s.offsetPriority as offsetPriority, "
			+ "s.offsetNonPriority as offsetNonPriority, s.offsetPriorityModifiedDate as offsetPriorityModifiedDate, s.offsetNonPriorityModifiedDate as offsetNonPriorityModifiedDate"),
	@EPL("on TPProduceState(sync) s "
			+ "merge into sync_event se "
			+ "where s.storageType = se.storageType and se.type = 'produce' "
			+ "when matched then update "
			+ "set se.timestamp = s.timestamp "
			+ "when not matched then insert "
			+ "select 'produce' as type, s.storageType as storageType, s.timestamp as timestamp"),
	@EPL("on TPGConsumeState(sync) s "
			+ "merge into sync_event se "
			+ "where s.storageType = se.storageType and se.type = 'consume' "
			+ "when matched then update "
			+ "set se.timestamp = s.timestamp "
			+ "when not matched then insert "
			+ "select 'consume' as type, s.storageType as storageType, s.timestamp as timestamp"),
	@EPL(name = "sync1", value = "on TPProduceState(sync) s select se.storageType as storageType, se.timestamp as timestamp from sync_event se where se.storageType = s.storageType group by se.storageType, se.timestamp having count(se.*) > 1"),
	@EPL(name = "sync2", value = "on TPGConsumeState(sync) s select se.storageType as storageType, se.timestamp as timestamp from sync_event se where se.storageType = s.storageType group by se.storageType, se.timestamp having count(se.*) > 1"),
	//@EPL(name = ConsumerLargeBacklogEventHandler.FIND_PRODUCE_OFFSETS, value = "select * from produce_offset order by topicId, topic"),
	//@EPL(name = ConsumerLargeBacklogEventHandler.FIND_CONSUME_OFFSETS, value = "")
//	@EPL(name = ConsumerLargeBacklogEventHandler.FIND_LARGE_BACKLOG_CONSUMER, value = "select co.topicId as topicId, co.topicName as topicName, co.partitionId as partitionId, co.storageType as storageType, co.consumerGroupId as consumerGroupId, "
//			+ "co.consumerGroup as consumerGroup, po.offsetPriority - co.offsetPriority as offsetPriority, po.offsetNonPriority - co.offsetNonPriority as offsetNonPriority, "
//			+ "sum(po.offsetPriority - co.offsetPriority) as sumOffsetPriority, sum(po.offsetNonPriority - co.offsetNonPriority) as sumOffsetNonPriority "
//			+ "from sync_event as se unidirectional, produce_offset as po, consume_offset as co "
//			+ "where se.storageType = po.storageType and se.storageType = co.storageType and po.topicId = co.topicId and po.partitionId = co.partitionId "
//			+ "group by co.topicId, co.consumerGroupId "
//			+ "order by co.topicId, co.consumerGroupId"
//			)
})
public class ConsumerLargeBacklogEventHandler extends RuleEventHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerLargeBacklogEventHandler.class);
	private static Logger tracer = LoggerFactory.getLogger("collector-tracer");
	public static final String PRODUCE_UPSERT = "produceUpsert";
	public static final String CONSUME_UPSERT = "consumerUpsert";
	public static final String SYNC_REMOVE = "syncRemove";
	public static final String SYNC = "sync";
	public static final String FIND_PRODUCE_OFFSETS = "select * from produce_offset po where topicId = ? order by partitionId";
	public static final String FIND_CONSUME_OFFSETS = "select * from consume_offset co where topicId = ? and consumerGroupId = ? order by partitionId";
	public static final String FIND_LARGE_BACKLOG_CONSUMER = "findLargeBacklogConsumer";
	
	@Autowired
	private CollectorConfiguration m_conf;
	
	private ConsumerMonitorConfigDao m_consumerMonitorConfigDao = PlexusComponentLocator.lookup(ConsumerMonitorConfigDao.class);
	
	private ConsumerGroupDao m_consumerDao = PlexusComponentLocator.lookup(ConsumerGroupDao.class);
	
	private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
	
	private ConsumerLargeBacklogEventReportMailNoticeContent m_reportContent;
	
	@PostConstruct
	protected void init() {
		super.init();
		m_reportContent = createReportContent();
		scheduleReportTask(new Runnable() {
			public void run() {
				if (m_reportContent.getReports().size() == 0) {
					return;
				}
				
				synchronized(ConsumerLargeBacklogEventHandler.this) {
					m_reportContent.setEndTime(System.currentTimeMillis());
					m_reportContent.finish();
					ConsumerLargeBacklogEventHandler.this.notify(new HermesNotice(Arrays.asList(m_conf.getNotifierDevMail().split(",")), m_reportContent), null);
					m_reportContent = createReportContent();
				}
			}
		}, m_conf.getTimeIntervalInMinutesForReport(), TimeUnit.MINUTES);
	}
	
	private ConsumerLargeBacklogEventReportMailNoticeContent createReportContent() {
		ConsumerLargeBacklogEventReportMailNoticeContent report = new ConsumerLargeBacklogEventReportMailNoticeContent();
		report.setStartTime(System.currentTimeMillis());
		return report;
	}

	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) throws DalException {
		if (event.getEventSource().contains("check")) {
			EventBean[] beans = ((Pair<EventBean[], EventBean[]>) event.getData()).getFirst();
			tracer.info("Bean: " + beans[0].getUnderlying().toString());
		}
		
		if (event.getEventSource().startsWith(SYNC) && isRealtimeAlertEnabled()) {
	        EventBean[] beans = ((Pair<EventBean[], EventBean[]>) event.getData()).getFirst();
        	tracer.info("Got sync event: {}", beans[0].getUnderlying());
	        String storageType = beans[0].get("storageType").toString();
	        if ((this.isKafkaAlertEnabled() && Storage.KAFKA.equals(storageType)) || Storage.MYSQL.equals(storageType)) {
	        	LOGGER.info("Backlog checking on storage type {}.", storageType);
	        	List<MonitorEvent> events = new ArrayList<MonitorEvent>();
				List<Topic> topics = m_topicDao.list(TopicEntity.READSET_FULL);
				for (Topic topic : topics) {
					if (!topic.getStorageType().equals(storageType)) {
						continue;
					}
					List<ConsumerGroup> consumerGroups = m_consumerDao.findByTopicId(topic.getId(), ConsumerGroupEntity.READSET_FULL);
					for (ConsumerGroup consumerGroup : consumerGroups) {
						alertIfNecessary(events, topic, consumerGroup);
					}
				}
				return events;
			}
		}
		return null;
	}
	
	private List<EventBean> findTopicEventBeans(long topicId) {
		List<EventBean> beans = new ArrayList<>();
		EPPreparedStatement preparedStatement = this.getEPAdministrator().prepareEPL(FIND_PRODUCE_OFFSETS);
		preparedStatement.setObject(1, topicId);
		EPStatement statement = this.getEPAdministrator().create(preparedStatement);
		Iterator<EventBean> iter = statement.iterator();
		while (iter.hasNext()) {
			beans.add(iter.next());
		}
		statement.destroy();
		return beans;
	}
	
	private List<EventBean> findTopicConsumerEventBeans(long topicId, long consumerGroupId) {
		List<EventBean> beans = new ArrayList<>();
		EPPreparedStatement preparedStatement = this.getEPAdministrator().prepareEPL(FIND_CONSUME_OFFSETS);
		preparedStatement.setObject(1, topicId);
		preparedStatement.setObject(2, consumerGroupId);
		EPStatement statement = this.getEPAdministrator().create(preparedStatement);
		Iterator<EventBean> iter = statement.iterator();
		while (iter.hasNext()) {
			beans.add(iter.next());
		}
		statement.destroy();
		return beans;
	}
	
	private void alertIfNecessary(List<MonitorEvent> events, Topic topic, ConsumerGroup consumerGroup) {
		List<EventBean> produces = findTopicEventBeans(topic.getId());
		List<EventBean> consumes = findTopicConsumerEventBeans(topic.getId(), consumerGroup.getId());
		
		long sumBacklog = 0;
		if (produces.size() > 0 && produces.size() == consumes.size()) {
			Map<Integer, Long> partition2Backlog = new HashMap<>();
			for (int index = 0; index < consumes.size(); index++) {
				EventBean produce = produces.get(index);
				EventBean consume = consumes.get(index);
				Integer partitionId = (Integer)produce.get("partitionId");
				partition2Backlog.put(partitionId, (long)produce.get("offsetPriority") - (long)consume.get("offsetPriority") 
						+ (long)produce.get("offsetNonPriority") - (long)consume.get("offsetNonPriority"));
				sumBacklog += partition2Backlog.get(partitionId);
			}	
			
			List<ConsumerMonitorConfig> consumerConfigs = null;
			ConsumerMonitorConfig consumerConfig = null;

			try {
				consumerConfigs = m_consumerMonitorConfigDao.findByTopicConsumer(topic.getName(), consumerGroup.getName(), ConsumerMonitorConfigEntity.READSET_FULL);
			} catch (DalException e) {
				LOGGER.error("Failed to find ConsumerMonitorConfig from db: {}", e.getMessage(), e);
			}
			
			if (consumerConfigs != null && consumerConfigs.size() > 0) {
				consumerConfig = consumerConfigs.get(0);
			}
			
			long defaultAlertLimit = getDefaultAlertLimit();
			
			if (consumerConfig == null && (defaultAlertLimit == -1 || sumBacklog < defaultAlertLimit)) {
				return;
			}
			
			if (consumerConfig != null && (!consumerConfig.isLargeBacklogEnable() || sumBacklog < consumerConfig.getLargeBacklogLimit())) {
				return;
			}
			
			MonitorEvent event = createMonitorEvent(topic.getName(), consumerGroup.getName(), sumBacklog, partition2Backlog);
			synchronized(ConsumerLargeBacklogEventHandler.this) {
				m_reportContent.addReport(ReportKey.from(topic.getName(), consumerGroup.getName(), consumerGroup.getOwner1(), consumerGroup.getOwner2(), consumerGroup.getPhone1(), consumerGroup.getPhone2()), event);
			}
			events.add(event);
			
			// Create category key.
			String categoryKey = String.format("%s-%s", topic.getName(), consumerGroup.getName());
			registerMailCategoryRate(categoryKey, m_conf.getDefaultMailFrequencyInterval(), TimeUnit.MINUTES);
			registerSmsCategoryRate(categoryKey, m_conf.getDefaultSmsFrequencyInterval(), TimeUnit.MINUTES);
			
			notify(Collections.singletonList(event), categoryKey);
		}
		
		LOGGER.error("Ignore verify consumer group backlog due to partition records: [{}:{}, {}:{}]", topic.getName(), produces.size(), consumerGroup.getName(), consumes.size());
	}
	
	private MonitorEvent createMonitorEvent(String topicName, String consumerGroup, long sumBacklog, Map<Integer, Long> partition2Backlog) {
		ConsumeLargeBacklogEvent e = new ConsumeLargeBacklogEvent();
		e.setTopic(topicName);
		e.setGroup(consumerGroup);
		e.setTotalBacklog(sumBacklog);
		e.setBacklogDetail(partition2Backlog);
		e.setCreateTime(new Date());
		e.setShouldNotify(false);
		LOGGER.info("Created one CONSUME_LARGE_BACKLOG event: {}, {}", e.getTopic(), e.getGroup());
		return e;
	}
    
    @Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<>();
		
		ConsumeLargeBacklogEvent event = (ConsumeLargeBacklogEvent)events.get(0);
        ConsumerLargeBacklogEventMailContent content = new ConsumerLargeBacklogEventMailContent(event.getTopic(), event.getGroup(), event.getCreateTime().getTime());
        content.setEvents(events);
        
        ConsumerGroup consumerGroup = null;
		try {
			Topic topic = m_topicDao.findByName(event.getTopic(), TopicEntity.READSET_FULL);
	        consumerGroup = m_consumerDao.findByTopicIdAndName(topic.getId(), event.getGroup(), ConsumerGroupEntity.READSET_FULL);
		} catch (DalException ex) {
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
    
    private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		builder.append("[消费积压]");
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent)events.get(index);
			builder.append(String.format("T[%s]-C[%s]: %d", e.getTopic(), e.getGroup(), e.getTotalBacklog()));
		}
		return new SmsNoticeContent(builder.toString());
	}
    
    @HermesMailDescription(template = HermesTemplate.CONSUME_LARGE_BACKLOG)
    public class ConsumerLargeBacklogEventMailContent extends EventHandlerMailNoticeContent {
    	public ConsumerLargeBacklogEventMailContent(String topic, String consumerGroup, long timestamp) {
    		super(String.format("Topic-Consumer[%s-%s]消费积压报警", topic, consumerGroup), 0L, timestamp);
    	}
    }
    
    @HermesMailDescription(template = HermesTemplate.CONSUME_LARGE_BACKLOG_REPORT_V2)
    public static class ConsumerLargeBacklogEventReportMailNoticeContent extends EventHandlerReportMailNoticeContent<ReportKey, MonitorEvent> {
    	public ConsumerLargeBacklogEventReportMailNoticeContent() {
    		super(String.format("消费积压报告"));
    	}
    }
    
}
