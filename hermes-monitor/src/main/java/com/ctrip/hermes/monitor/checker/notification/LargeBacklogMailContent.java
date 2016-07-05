package com.ctrip.hermes.monitor.checker.notification;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

@HermesMailDescription(template = HermesTemplate.CONSUME_LARGE_BACKLOG)
public class LargeBacklogMailContent extends MailNoticeContent {
	private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@ContentField(name = "events")
	private List<ConsumeLargeBacklogEvent> m_events;

	@Subject
	private String m_title;

	@SuppressWarnings("unchecked")
	public LargeBacklogMailContent(List<? extends MonitorEvent> events) {
		m_events = (List<ConsumeLargeBacklogEvent>) events;
		m_title = String.format("【Hermes监控】消费积压过多 %s", DATE_FMT.format(new Date()));
	}
}
