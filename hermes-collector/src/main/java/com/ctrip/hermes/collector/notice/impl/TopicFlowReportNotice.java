package com.ctrip.hermes.collector.notice.impl;

import java.util.Date;
import java.util.List;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.collector.state.impl.TopicFlowDailyReportState.FlowDetail;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

@HermesMailDescription(template = HermesTemplate.TOPIC_FLOW_DAILY_REPORT)
public class TopicFlowReportNotice extends MailNoticeContent {
	@Subject
	public String m_subject;

	@ContentField(name = "total")
	private FlowDetail m_total;

	@ContentField(name = "buProduce")
	private List<Pair<String, Double>> m_buProduce;

	@ContentField(name = "buConsume")
	private List<Pair<String, Double>> m_buConsume;

	@ContentField(name = "buDetails")
	private List<FlowDetail> m_buDetails;

	@ContentField(name = "date")
	private Date m_date;

	public Date getDate() {
		return m_date;
	}

	public void setDate(Date m_date) {
		this.m_date = m_date;
	}

	public TopicFlowReportNotice(String subject) {
		this.m_subject = subject;
	}

	public FlowDetail getTotal() {
		return m_total;
	}

	public void setTotal(FlowDetail m_total) {
		this.m_total = m_total;
	}

	public List<Pair<String, Double>> getBuProduce() {
		return m_buProduce;
	}

	public void setBuProduce(List<Pair<String, Double>> m_buProduce) {
		this.m_buProduce = m_buProduce;
	}

	public List<Pair<String, Double>> getBuConsume() {
		return m_buConsume;
	}

	public void setBuConsume(List<Pair<String, Double>> m_buConsume) {
		this.m_buConsume = m_buConsume;
	}

	public List<FlowDetail> getBuDetails() {
		return m_buDetails;
	}

	public void setBuDetails(List<FlowDetail> m_buDetails) {
		this.m_buDetails = m_buDetails;
	}
}
