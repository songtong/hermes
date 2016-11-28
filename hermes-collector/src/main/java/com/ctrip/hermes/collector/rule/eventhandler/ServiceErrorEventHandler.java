package com.ctrip.hermes.collector.rule.eventhandler;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;
import com.ctrip.hermes.admin.core.monitor.event.BrokerErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.MetaServerErrorEvent;
import com.ctrip.hermes.admin.core.monitor.event.MonitorEvent;
import com.ctrip.hermes.admin.core.monitor.event.ServerErrorEvent;
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
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.ErrorSample;
import com.ctrip.hermes.collector.state.impl.ServiceErrorState.SourceType;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.Pair;

@Component
@TriggeredBy(ServiceErrorState.class)
@EPL("select * from ServiceErrorState(count > 0)")
public class ServiceErrorEventHandler extends RuleEventHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceErrorEventHandler.class);
	
	@Autowired
	private CollectorConfiguration m_conf;

	@Override
	public boolean validate(RuleEvent event) {
		return true;
	}

	@Override
	public List<MonitorEvent> doHandleEvent(RuleEvent event) {
		if (isRealtimeAlertEnabled()) { 
			List<MonitorEvent> events = new ArrayList<>();
			EventBean[] beans = ((Pair<EventBean[], EventBean[]>)event.getData()).getFirst();
	
			if (beans != null) {
	    		for (EventBean bean : beans) {
	    			ServiceErrorState state = (ServiceErrorState)bean.getUnderlying();
	    			for (Map.Entry<String, Long> countOnHost : state.getCountOnHosts().entrySet()) {
	    				ServerErrorEvent serverErrorEvent = null;
	    				if (state.getSourceType() == SourceType.BROKER) {
	    					serverErrorEvent = new BrokerErrorEvent();
	    				} else {
	    					serverErrorEvent = new MetaServerErrorEvent();
	    				}
	    				serverErrorEvent.setErrorCount(countOnHost.getValue());
	    				serverErrorEvent.setHost(countOnHost.getKey());
	    				serverErrorEvent.setCreateTime(new Date(state.getTimestamp()));
	    				List<org.unidal.tuple.Pair<Long, String>> errors = new ArrayList<>();
	    				for (ErrorSample error : state.getErrorsOnHosts().get(countOnHost.getKey())) {
	        				errors.add(org.unidal.tuple.Pair.from(error.getTimestamp(), error.getMessage()));
	    				}
	    				serverErrorEvent.setErrors(errors);
	    				serverErrorEvent.setShouldNotify(false);
	    				events.add(serverErrorEvent);
	        			LOGGER.info("Created SERVICE_ERROR event: {}, {}", serverErrorEvent.getHost(), serverErrorEvent.getErrorCount());
	    			}
	    		}
	    		notify(events, null);
	    		return events;
			}
		}
		return null;
	}    

	@Override
	public List<HermesNotice> doGenerateNotices(List<MonitorEvent> events) {
		List<HermesNotice> notices = new ArrayList<>();
		ServerErrorEvent event = (ServerErrorEvent)events.get(0);
        ServiceErrorEventMailContent content = new ServiceErrorEventMailContent(event.getType(), event.getCreateTime().getTime());
        content.setEvents(events);
        notices.add(new HermesNotice(this.getDefaultRecipients(HermesNoticeType.EMAIL), content));
        notices.add(new HermesNotice(this.getDefaultRecipients(HermesNoticeType.SMS), getSmsNoticeContent(events)));
		return notices;
	}
	
	private HermesNoticeContent getSmsNoticeContent (List<MonitorEvent> events) {
		StringBuilder builder = new StringBuilder();
		builder.append(String.format("[%s]", ((ServerErrorEvent)events.get(0)).getType().getDisplayName().toUpperCase()));
		for (int index = 0; index < events.size(); index++) {
			if (index > 0) {
				builder.append(",");
			}
			ServerErrorEvent e = (ServerErrorEvent)events.get(index);
			builder.append(String.format("%s: %d", e.getHost(), e.getErrorCount()));
		}
		return new SmsNoticeContent(builder.toString());
	}
    
    @HermesMailDescription(template = HermesTemplate.SERVER_ERROR)
    public class ServiceErrorEventMailContent extends EventHandlerMailNoticeContent {        
    	public ServiceErrorEventMailContent(MonitorEventType eventType, long timestamp) {
    		super(String.format("%s服务器错误", eventType == MonitorEventType.BROKER_ERROR? "Broker" : "Metaserver"), 0, timestamp);
    	}
    }
	
}
