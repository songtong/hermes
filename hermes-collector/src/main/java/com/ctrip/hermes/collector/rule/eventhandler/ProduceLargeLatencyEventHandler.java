package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.admin.core.model.Topic;
import com.ctrip.hermes.admin.core.model.TopicDao;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.HermesNoticeContent;
import com.ctrip.hermes.admin.core.service.notify.SmsNoticeContent;
import com.ctrip.hermes.admin.core.service.template.HermesTemplate;
import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.ProduceLatencyState;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy(ProduceLatencyState.class)
@EPL("select * from ProduceLatencyState")
public class ProduceLargeLatencyEventHandler extends RuleEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceLargeLatencyEventHandler.class);
    
    private TopicDao m_topicDao = PlexusComponentLocator.lookup(TopicDao.class);
    
    private ProduceLatencyTooLargeReportEventMailContent m_reportContent;
    
    @Autowired
    private CollectorConfiguration m_conf;
    
    protected void init() {
    	super.init();
    	m_reportContent = createReportContent();
    	Executors.newScheduledThreadPool(1, CollectorThreadFactory.newFactory(ProduceLargeLatencyEventHandler.class)).schedule(new Runnable() {

			@Override
			public void run() {
				if (m_reportContent.getReports().size() == 0) {
					return;
				}
				
				synchronized(ProduceLargeLatencyEventHandler.this) {
					m_reportContent.setEndTime(System.currentTimeMillis());
					m_reportContent.finish();
					ProduceLargeLatencyEventHandler.this.notify(new HermesNotice(Arrays.asList(m_conf.getNotifierDefaultMail().split(",")), m_reportContent), null);
					ProduceLargeLatencyEventHandler.this.notify(new HermesNotice(Arrays.asList(m_conf.getNotifierDevSMSNumber().split(",")), getSmsNoticeContent(m_reportContent)), null);
					m_reportContent = createReportContent();
				}
			}
    	}, m_conf.getTimeIntervalInMinutesForReport(), TimeUnit.MINUTES);
    }
    
    private ProduceLatencyTooLargeReportEventMailContent createReportContent() {
    	ProduceLatencyTooLargeReportEventMailContent content = new ProduceLatencyTooLargeReportEventMailContent();
    	content.setStartTime(System.currentTimeMillis());
    	return content;
    }
    
    @Override
    protected boolean validate(RuleEvent event) {
        return true;
    }

    @Override
    protected List<MonitorEvent> doHandleEvent(RuleEvent event) {
    	if (isRealtimeAlertEnabled()) {
	        EventBean[] beans = ((Pair<EventBean[], EventBean[]>)event.getData()).getFirst();
	        if (beans !=  null) {
	        	List<MonitorEvent> events = new ArrayList<>();
	            for (EventBean bean : beans) {
	                String topicName = (String)bean.get("topic");
	                if ((double)bean.get("ratio") * 100 < this.getDefaultAlertLimit()) {
	                	continue;
	                }
	
	                ProduceLatencyTooLargeEvent e = new ProduceLatencyTooLargeEvent();
	                long timestamp = (long)bean.get("timestamp");
	                e.setTopic(topicName);
	                Topic topic = null;
					try {
						topic = m_topicDao.findByName(e.getTopic(), TopicEntity.READSET_FULL);
						e.setBrokerGroup(topic.getBrokerGroup());
					} catch (DalException ex) {
						LOGGER.error("Failed to find topic from db.", e);
					}
					
	                e.setDate(TimeUtils.formatDate(TimeUtils.before(timestamp, 5, TimeUnit.MINUTES)));
	                e.setLatency((double)bean.get("avg"));
	                e.setCount((long)bean.get("count"));
	                e.setCountAll((long)bean.get("countAll"));
	                e.setRatio((double)bean.get("ratio"));
	                e.setCreateTime(new Date(timestamp));
					e.setShouldNotify(false);
	                events.add(e);
	                m_reportContent.addReport(ReportKey.from(e.getTopic()), e);
	                LOGGER.info("Created ProduceLargeLatencyEvent for topic {}.", e.getTopic());
	            }
	            return events;
	        }
    	}
        return null;
    }
    
	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		return null;
	}
	
	private HermesNoticeContent getSmsNoticeContent (ProduceLatencyTooLargeReportEventMailContent reportContent) {
		StringBuilder builder = new StringBuilder();
		
		for (ReportKey reportKey : reportContent.getReports().keySet()) {
			if (builder.length() > 0) {
				builder.append(",");
			}
			builder.append(String.format("T[%s]", reportKey.getKeys()[0]));
		}
		builder.insert(0, "[发送耗时]");
		return new SmsNoticeContent(builder.toString());
	}
    
    @HermesMailDescription(template = HermesTemplate.PRODUCE_LARGE_LATENCY_REPORT)
    public class ProduceLatencyTooLargeReportEventMailContent extends EventHandlerReportMailNoticeContent<ReportKey, MonitorEvent> {
    	public ProduceLatencyTooLargeReportEventMailContent() {
    		super("发送耗时报告");
    	}
    }

}
