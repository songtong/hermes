package com.ctrip.hermes.collector.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.exception.NoticeException;
import com.ctrip.hermes.collector.hub.NotifierManager;
import com.ctrip.hermes.collector.rule.annotation.Context;
import com.ctrip.hermes.collector.rule.annotation.Contexts;
import com.ctrip.hermes.collector.rule.annotation.EPL;
import com.ctrip.hermes.collector.rule.annotation.EPLs;
import com.ctrip.hermes.collector.rule.annotation.Pattern;
import com.ctrip.hermes.collector.rule.annotation.Patterns;
import com.ctrip.hermes.collector.rule.annotation.Table;
import com.ctrip.hermes.collector.rule.annotation.Tables;
import com.ctrip.hermes.collector.rule.annotation.TriggeredBy;
import com.ctrip.hermes.collector.rule.annotation.Utilities;
import com.ctrip.hermes.collector.rule.annotation.Window;
import com.ctrip.hermes.collector.rule.annotation.Windows;
import com.ctrip.hermes.collector.utils.CollectorThreadFactory;
import com.ctrip.hermes.collector.utils.TimeUtils;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.monitor.dao.MonitorEventStorage;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.KVService;
import com.ctrip.hermes.metaservice.service.KVService.Tag;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeContent;
import com.ctrip.hermes.metaservice.service.notify.HermesNoticeType;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

public abstract class RuleEventHandler implements EventHandler, Visitable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleEventHandler.class);
    private static final String DEFAULT_ALERT_LIMIT_KEY = "%s#DEFAULT_ALERT_LIMIT";
    private static final String KAFKA_ENABLE_ALERT_KEY = "%s#KAFKA";
    private static final String MAX_ALERT_LIMIT_KEY = "%s#MAX_ALERT_LIMIT";
    private static final String MAIL_NOTIFY_CATEGORY_KEY = "MAIL-%s-%s";
    private static final String SMS_NOTIFY_CATEGORY_KEY = "SMS-%s-%s";
    
    private EPAdministrator m_epAdministrator;

    // Handled data types.
    private List<Class<?>> m_handledTypes;
    // Registered statements.
    private Map<String, EPStatement> m_statements = new HashMap<>();
    
    private KVService m_kvService = PlexusComponentLocator.lookup(KVService.class);

    private MonitorEventStorage m_monitorEventStorage = PlexusComponentLocator
            .lookup(MonitorEventStorage.class);
    
    private ScheduledExecutorService m_executor = Executors.newScheduledThreadPool(1, CollectorThreadFactory.newFactory(this.getClass().getName()));

    @Autowired
    protected NotifierManager m_notifierManager;

    @Autowired
    private CollectorConfiguration m_conf;

    @PostConstruct
    protected void init() {
        m_handledTypes = findHandledTypes();
        registerMailCategoryRate(null, m_conf.getDefaultMailFrequencyInterval(), TimeUnit.MINUTES);
		registerSmsCategoryRate(null, m_conf.getDefaultSmsFrequencyInterval(), TimeUnit.MINUTES);

        LOGGER.info("{} initialized done with enabled status: {}", this.getClass().getSimpleName(), this.isRealtimeAlertEnabled());
    }

    public void accept(Visitor visitor) {
        visitor.visit(this);
    }
    
    public EPAdministrator getEPAdministrator() {
		return m_epAdministrator;
	}

	public void setEPAdministrator(EPAdministrator administrator) {
		m_epAdministrator = administrator;
	}

	public List<Class<?>> findHandledTypes() {
        List<Class<?>> handledTypes = new ArrayList<>();

        TriggeredBy triggeredBy = this.getClass().getAnnotation(TriggeredBy.class);
        if (triggeredBy == null) {
            return null;
        }

        for (Class<?> handledType : triggeredBy.value()) {
            handledTypes.add(handledType);
        }

        return handledTypes;
    }

    public List<Context> findContexts() {
        List<Context> contextList = new ArrayList<Context>();
        Contexts contexts = this.getClass().getAnnotation(Contexts.class);
        if (contexts != null) {
            for (Context context : contexts.value()) {
                if (!context.value().equals("")) {
                    contextList.add(context);
                }
            }
        }

        Context context = this.getClass().getAnnotation(Context.class);
        if (context != null && !context.value().equals("")) {
            contextList.add(context);
        }

        return contextList;
    }

    public List<Pattern> findPatterns() {
        List<Pattern> patternList = new ArrayList<Pattern>();
        Patterns patterns = this.getClass().getAnnotation(Patterns.class);
        if (patterns != null && patterns.value().length != 0) {
            for (Pattern pattern : patterns.value()) {
                patternList.add(pattern);
            }
        }

        Pattern pattern = this.getClass().getAnnotation(Pattern.class);
        if (pattern != null && !pattern.value().equals("")) {
            patternList.add(pattern);
        }
        return patternList;
    }

    public List<EPL> findEPLs() {
        List<EPL> eplList = new ArrayList<EPL>();
        EPLs epls = this.getClass().getAnnotation(EPLs.class);
        if (epls != null && epls.value().length != 0) {
            for (EPL epl : epls.value()) {
                eplList.add(epl);
            }
        }

        EPL epl = this.getClass().getAnnotation(EPL.class);
        if (epl != null && !epl.value().equals("")) {
            eplList.add(epl);
        }
        return eplList;
    }

    public List<Table> findTables() {
        List<Table> tableList = new ArrayList<Table>();
        Tables tables = this.getClass().getAnnotation(Tables.class);
        if (tables != null && tables.value().length != 0) {
            for (Table table : tables.value()) {
                tableList.add(table);
            }
        }

        Table table = this.getClass().getAnnotation(Table.class);
        if (table != null && !table.value().equals("")) {
            tableList.add(table);
        }
        return tableList;
    }

    public List<Window> findWindows() {
        List<Window> windowList = new ArrayList<Window>();
        Windows windows = this.getClass().getAnnotation(Windows.class);
        if (windows != null && windows.value().length != 0) {
            for (Window window : windows.value()) {
                windowList.add(window);
            }
        }

        Window window = this.getClass().getAnnotation(Window.class);
        if (window != null && !window.value().equals("")) {
            windowList.add(window);
        }
        return windowList;
    }

    public List<Class<?>> findUtils() {
        Utilities utils = this.getClass().getAnnotation(Utilities.class);
        if (utils != null) {
            return Arrays.asList(utils.value());
        }
        return null;
    }
    
    public void scheduleReportTask(final Runnable runnable, long delay, TimeUnit timeUnit) {
    	m_executor.scheduleAtFixedRate(runnable, 0, delay, timeUnit);
    }

    // Validate rule event.
    protected abstract boolean validate(RuleEvent event);

    protected abstract List<MonitorEvent> doHandleEvent(RuleEvent event) throws Exception;

    public void handleEvent(Event e) {
        RuleEvent ruleEvent = (RuleEvent) e;
        if (validate(ruleEvent)) {
            List<MonitorEvent> events = null;
			try {
				events = doHandleEvent(ruleEvent);
			} catch (Exception ex) {
				LOGGER.error("Failed to handle event: {}", ruleEvent.getEventSource(), ex);
			}
			
            if (events != null && events.size() > 0) {
            	com.dianping.cat.message.Event catEvent = Cat.newEvent("AlertEvent", this.getClass().getSimpleName());
            	catEvent.addData("*count", events.size());
            	catEvent.setStatus(Message.SUCCESS);
            	catEvent.complete();
            	
                for (MonitorEvent event : events) {
                    saveMonitorEvent(event);
                }
            }
        }
    }
    

    public void registerStatement(String name, EPStatement statement) {
        this.m_statements.put(name, statement);
    }

    public EPStatement getStatement(String name) {
        return this.m_statements.get(name);
    }

    public List<Class<?>> getHandledTypes() {
        return m_handledTypes;
    }
    
    protected boolean isKafkaAlertEnabled() {
    	return Boolean.parseBoolean(m_kvService.getValue(String.format(KAFKA_ENABLE_ALERT_KEY, this.getClass().getSimpleName()), Tag.ALERT));
    }
    
    protected boolean isRealtimeAlertEnabled() {
    	return Boolean.parseBoolean(m_kvService.getValue(this.getClass().getSimpleName(), Tag.ALERT));
    }
    
    protected long getDefaultAlertLimit() {
    	String key = String.format(DEFAULT_ALERT_LIMIT_KEY, this.getClass().getSimpleName());
    	try {
    		return Long.parseLong(m_kvService.getValue(key, Tag.ALERT));
    	} catch (Exception e) {
    		LOGGER.error("Failed to parse default alert limit for long value: {}", key);
    		return -1;
    	}
    }
    
    protected long getMaxAlertLimit() {
    	String key = String.format(MAX_ALERT_LIMIT_KEY, this.getClass().getSimpleName());
    	try {
    		return Long.parseLong(m_kvService.getValue(key, Tag.ALERT));
    	} catch (Exception e) {
    		LOGGER.error("Failed to parse max alert limit for long value: {}", key);
    		return -1;
    	} 
    }

    protected void notify(List<MonitorEvent> events, String categoryKey) {
        if (events == null || events.size() == 0) {
            return;
        }
        
        try {
        	List<HermesNotice> notices = generateNotices(events);
        	if (notices != null && notices.size() > 0) {
        		for (HermesNotice notice : notices) {
        			notify(notice, categoryKey);
        		}
        	}
        } catch (NoticeException e) {
            LOGGER.error("Failed to offer notice to notifier manager: {}", e.getMessage(), e);
        }
    }
    
    protected void notify(HermesNotice notice, String categoryKey) {
    	m_notifierManager.offer(notice, String.format(notice.getType() == HermesNoticeType.EMAIL? MAIL_NOTIFY_CATEGORY_KEY: SMS_NOTIFY_CATEGORY_KEY, this.getClass().getName(), categoryKey));
    }
    
    protected void registerMailCategoryRate(String categoryKey, int interval, TimeUnit timeUnit) {
    	m_notifierManager.setCategoryRate(String.format(MAIL_NOTIFY_CATEGORY_KEY, this.getClass().getName(), categoryKey), interval, timeUnit);
    }
    
    protected void registerSmsCategoryRate(String categoryKey, int interval, TimeUnit timeUnit) {
    	m_notifierManager.setCategoryRate(String.format(SMS_NOTIFY_CATEGORY_KEY, this.getClass().getName(), categoryKey), interval, timeUnit);
    }

    protected void saveMonitorEvent(MonitorEvent event) {
        try {
            if (m_conf.isEnableMonitorEvent()) {
                m_monitorEventStorage.addMonitorEvent(event);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to save monitor event due to exception: {}", e.getMessage(), e);
        }
    }
    
    public List<HermesNotice> generateNotices(List<MonitorEvent> events) throws NoticeException {
        List<HermesNotice> notices = doGenerateNotices(events);
        
        if (notices != null && notices.size() > 0) {
        	for (HermesNotice notice : notices) {
        		if (notice.getContent() == null) {
    	            throw new NoticeException("Notice content is required.");
    	        }
        		
        		if (m_conf.isDebugMode() || notice.getReceivers() == null || notice.getReceivers().size() == 0) {
        			notice.setReceivers(getDefaultRecipients(notice.getType()));
        		}
          	}
        }
        return notices;
    }
    
    public abstract List<HermesNotice> doGenerateNotices(List<MonitorEvent> events);

    protected List<String> getDefaultRecipients(HermesNoticeType type) {
        switch (type) {
        case TTS:
            return Arrays.asList(m_conf.getDefaultTTSNumber().split(","));
        case SMS:
            return Arrays.asList(m_conf.getDefaultSMSNumber().split(","));
        case EMAIL:
            return Arrays.asList(m_conf.getNotifierDefaultMail().split(","));
        default:
            return null;
        }
    }
    
    public static class EventHandlerMailNoticeContent extends MailNoticeContent {
    	public static final String TIME_RANGE_SUBJECT_FORMAT = "【Hermes监控】【%s】%s[%s - %s]";
    	public static final String TIME_POINT_SUBJECT_FORMAT = "【Hermes监控】【%s】%s[%s]";
    	@Subject
    	private String m_subject;
    	
    	@ContentField(name = "env")
    	private String m_env;
    	
        @ContentField(name = "events")
        private List<MonitorEvent> m_events;
        
        @ContentField(name = "startTime")
        private long m_startTime;
        
        @ContentField(name = "endTime")
        private long m_endTime;
        
        public EventHandlerMailNoticeContent(String subject) {
    		this.m_env = PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv().toString();
        	this.m_subject = subject;
        }

    	public EventHandlerMailNoticeContent(String subject, long startTime, long endTime) {
    		this.m_env = PlexusComponentLocator.lookup(ClientEnvironment.class).getEnv().toString();
    		if (startTime > 0) {
    			this.m_subject = String.format(TIME_RANGE_SUBJECT_FORMAT, m_env, subject, TimeUtils.formatDate(startTime), TimeUtils.formatDate(endTime));
    		} else {
    			this.m_subject = String.format(TIME_POINT_SUBJECT_FORMAT, m_env, subject, TimeUtils.formatDate(endTime));
    		}
    	}

		public String getSubject() {
			return m_subject;
		}

		public void setSubject(String subject) {
			m_subject = subject;
		}

		public String getEnv() {
			return m_env;
		}

		public void setEnv(String env) {
			m_env = env;
		}

		public List<MonitorEvent> getEvents() {
			return m_events;
		}

		public void setEvents(List<MonitorEvent> events) {
			m_events = events;
		}

		public long getStartTime() {
			return m_startTime;
		}

		public void setStartTime(long startTime) {
			m_startTime = startTime;
		}

		public long getEndTime() {
			return m_endTime;
		}

		public void setEndTime(long endTime) {
			m_endTime = endTime;
		}
    }
    
    public static class EventHandlerReportMailNoticeContent<K, V> extends EventHandlerMailNoticeContent {
    	public static final String TIME_RANGE_REPORT_SUBJECT_FORMAT = "【Hermes监控报告】【%s】%s[%s - %s]";

    	@ContentField(name = "reports")
    	private Map<K, List<V>> m_reports = new HashMap<>();

		public EventHandlerReportMailNoticeContent(String subject) {
			super(subject);
		}

		public Map<K, List<V>> getReports() {
			return m_reports;
		}

		public void setReports(Map<K, List<V>> reports) {
			m_reports = reports;
		}
		
		public void addReport(K key, V value) {
			if (!m_reports.containsKey(key)) {
				m_reports.put(key, new ArrayList<V>());
			}
			m_reports.get(key).add(value);
		}
		
		public void finish() {
			this.setSubject(String.format(TIME_RANGE_REPORT_SUBJECT_FORMAT, this.getEnv(), this.getSubject(), TimeUtils.formatDate(this.getStartTime()), TimeUtils.formatDate(this.getEndTime())));
		}
    }
    
    public static class ReportKey {
    	private Object[] m_keys;
    	private ReportKey() {}
    	public ReportKey(int length) {
    		m_keys = new Object[length];
    	}
    	
    	public void setKey(Object key, int index) {
    		m_keys[index] = key;
    	}

		public Object[] getKeys() {
			return m_keys;
		}

		public void setKeys(Object[] keys) {
			m_keys = keys;
		}
		
		public boolean equals(Object object) {
			ReportKey reportKey = (ReportKey)object;
			
			if (this.getKeys() == null) {
				return reportKey.getKeys() == null? true: false;
			}
			
			if (reportKey.getKeys() == null) {
				return this.getKeys() == null? true: false;
			}
			
			if (reportKey.getKeys().length != this.getKeys().length) {
				return false;
			}
			
			Object[] keys = reportKey.getKeys();
			for (int index = 0; index < this.m_keys.length; index++) {
				if ((keys[index] == null && this.m_keys[index] == null) || (keys[index] != null && this.m_keys[index] != null && keys[index].equals(this.m_keys[index]))) {
					continue;
				}
				return false;
			}
			return true;
		}
		
		public int hashCode() {
			int hashCode = 0;
			for (int index = 0; index < this.m_keys.length; index++) {
				if (this.m_keys[index] == null) {
					continue;
				}
				hashCode += this.m_keys[index].hashCode() << (this.m_keys.length - index); 
			}
			return hashCode;
		}
		
		public static ReportKey from(Object... keys) {
			ReportKey reportKey = new ReportKey();
			reportKey.setKeys(keys);
			return reportKey;
		}
    }

}
