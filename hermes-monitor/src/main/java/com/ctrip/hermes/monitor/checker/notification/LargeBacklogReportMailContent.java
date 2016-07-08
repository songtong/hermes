package com.ctrip.hermes.monitor.checker.notification;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaservice.monitor.event.ConsumeLargeBacklogEvent;
import com.ctrip.hermes.metaservice.monitor.event.MonitorEvent;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;
import com.ctrip.hermes.metaservice.view.ConsumerGroupView;

@HermesMailDescription(template = HermesTemplate.CONSUME_LARGE_BACKLOG_REPORT)
public class LargeBacklogReportMailContent extends MailNoticeContent {
	private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private Map<Pair<String, String>, Report> reports = new HashMap<>();

	@ContentField(name = "reports")
	private Set<Entry<Pair<String, String>, Report>> reportList = reports.entrySet();

	private Date start;

	@Subject
	private String title;

	public LargeBacklogReportMailContent() {
		start = new Date();
	}

	public static class Report {
		private String o1Email;

		private String o1Phone;

		private String o2Email;

		private String o2Phone;

		private List<Pair<String, Long>> backlogs = new ArrayList<>();

		public String getO1Email() {
			return o1Email;
		}

		public void setO1Email(String o1Email) {
			this.o1Email = o1Email;
		}

		public String getO1Phone() {
			return o1Phone;
		}

		public void setO1Phone(String o1Phone) {
			this.o1Phone = o1Phone;
		}

		public String getO2Email() {
			return o2Email;
		}

		public void setO2Email(String o2Email) {
			this.o2Email = o2Email;
		}

		public String getO2Phone() {
			return o2Phone;
		}

		public void setO2Phone(String o2Phone) {
			this.o2Phone = o2Phone;
		}

		public List<Pair<String, Long>> getBacklogs() {
			return backlogs;
		}

		public void setBacklogs(List<Pair<String, Long>> backlogs) {
			this.backlogs = backlogs;
		}
	}

	public Map<Pair<String, String>, Report> getReports() {
		return reports;
	}

	public void setReports(Map<Pair<String, String>, Report> reports) {
		this.reports = reports;
	}

	public void addMonitorEvent(MonitorEvent event, ConsumerGroupView consumer) {
		title = String.format("【Hermes监控报告】消费积压报告 [%s ~ %s]", DATE_FMT.format(start), DATE_FMT.format(new Date()));
		if (event instanceof ConsumeLargeBacklogEvent) {
			ConsumeLargeBacklogEvent e = (ConsumeLargeBacklogEvent) event;
			Pair<String, String> key = new Pair<String, String>(consumer.getTopicName(), consumer.getName());
			if (!reports.containsKey(key)) {
				reports.put(key, new Report());
			}
			Report r = reports.get(key);
			r.setO1Email(consumer.getOwner1());
			r.setO1Phone(consumer.getPhone1());
			r.setO2Email(consumer.getOwner2());
			r.setO2Phone(consumer.getOwner2());
			r.getBacklogs().add(new Pair<String, Long>(DATE_FMT.format(e.getCreateTime()), e.getTotalBacklog()));
		}
	}
}
