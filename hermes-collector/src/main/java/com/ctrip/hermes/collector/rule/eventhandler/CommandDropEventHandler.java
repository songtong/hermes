package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.rule.RuleEvent;
import com.ctrip.hermes.collector.rule.RuleEventHandler;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.CommandDropState;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.model.Endpoint;
import com.ctrip.hermes.metaservice.model.EndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;
import com.ctrip.hermes.metaservice.monitor.event.BrokerCommandDropEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeType;
import com.ctrip.hermes.metaservice.service.notify.SmsNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy(CommandDropState.class)
@EPL("select * from CommandDropState")
public class CommandDropEventHandler extends RuleEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandDropEventHandler.class);

    @Autowired
    private CollectorConfiguration m_conf;
    
    private EndpointDao m_endpointDao = PlexusComponentLocator.lookup(EndpointDao.class);
    
    private List<Endpoint> m_endpoints;

    protected void init() {
    	super.init();
    	try {
    		m_endpoints = m_endpointDao.list(EndpointEntity.READSET_FULL);
    	} catch (Exception e) {
    		LOGGER.error("Failed to load endpoints from db.", e);
    	}
    }
    
    @Override
    protected boolean validate(RuleEvent event) {
        return true;
    }
    
    private Endpoint findEndpoint(String host) {
    	for (Endpoint endpoint : m_endpoints) {
    		if (endpoint.getHost().equals(host)) {
    			return endpoint;
    		}
    	}
    	return null;
    }

    @Override
    protected List<MonitorEvent> doHandleEvent(RuleEvent event) {
    	if (isRealtimeAlertEnabled()) {
	        EventBean[] beans = ((Pair<EventBean[], EventBean[]>) event.getData()).getFirst();
	        if (beans != null) {
	            List<MonitorEvent> events = new ArrayList<>();
	            for (EventBean bean : beans) {
	                CommandDropState state = (CommandDropState) bean.getUnderlying();
	                BrokerCommandDropEvent e = new BrokerCommandDropEvent();
	                e.setHost(state.getHost());
	                Endpoint endpoint = findEndpoint(state.getHost());
	                if (endpoint != null) {
	                	e.setGroup(endpoint.getGroup());
	                }
	                e.setCommand(state.getCommandType());
	                e.setCount(state.getCount());
	                Calendar calendar = Calendar.getInstance();
	                calendar.setTimeInMillis(TimeUtils.before(state.getTimestamp(), 5, TimeUnit.MINUTES));
	                calendar.set(Calendar.SECOND, 0);
	                calendar.set(Calendar.MINUTE, (short)bean.get("minute"));
	                e.setDate(TimeUtils.formatDate(calendar.getTime()));
	                e.setCreateTime(new Date(state.getTimestamp()));
	                e.setShouldNotify(false);
	                events.add(e);
	                LOGGER.info("Created COMMAND_DROP event: {}", e.getCommand());
	            }
	            notify(events, null);
	            return events;
	        }
    	}
        return null;
    }
    
	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<HermesNotice>();
		BrokerCommandDropEvent e = (BrokerCommandDropEvent) events.get(0);
        CommandDropEventMailContent content = new CommandDropEventMailContent(e.getCreateTime().getTime());
        content.setEvents(events);
        content.setCatTimestamp(TimeUtils.formatCatTimestamp(TimeUtils.before(e.getCreateTime()
                .getTime(), 5, TimeUnit.MINUTES)));
        
        notices.add(new HermesNotice(this.getDefaultRecipients(HermesNoticeType.EMAIL), content));
        notices.add(new HermesNotice(this.getDefaultRecipients(HermesNoticeType.SMS), getSmsNoticeContent(events)));
        
        return notices;
	}
	
	private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("[Broker命令丢失-%s]", ((BrokerCommandDropEvent)events.get(0)).getDate()));
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			BrokerCommandDropEvent e = (BrokerCommandDropEvent)events.get(index);
			builder.append(String.format("[%s(%s)]: %d", e.getCommand(), e.getHost(), e.getCount()));
		}
		return new SmsNoticeContent(builder.toString());
	}

    @HermesMailDescription(template = HermesTemplate.BROKER_COMMAND_DROP)
    public class CommandDropEventMailContent extends EventHandlerMailNoticeContent {
        @ContentField(name = "timestamp")
        private String catTimestamp;
        
        public CommandDropEventMailContent(long timestamp) {
        	super("Broker命令丢弃", 0, timestamp);
        }

        public String getCatTimestamp() {
            return catTimestamp;
        }

        public void setCatTimestamp(String catTimestamp) {
            this.catTimestamp = catTimestamp;
        }

    }
}
