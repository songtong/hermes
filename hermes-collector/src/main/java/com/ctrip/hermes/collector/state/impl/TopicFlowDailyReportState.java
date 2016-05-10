package com.ctrip.hermes.collector.state.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.unidal.tuple.Pair;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.notice.Noticeable;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.utils.ApplicationContextUtils;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class TopicFlowDailyReportState extends State implements Noticeable {
	public static final String SUBJECT = "%s[%s]";
	public static final DateFormat SUBJECT_DATE_FORMAT = new SimpleDateFormat("YYYY-MM-dd");
	private FlowDetail total = new FlowDetail("total");
	private Map<String, Long> buProduce;
	private Map<String, Long> buConsume;
	private Map<String, FlowDetail> buDetails = new HashMap<>();

	public FlowDetail getTotal() {
		return total;
	}

	public void setTotal(FlowDetail total) {
		this.total = total;
	}

	public Map<String, Long> getBuProduce() {
		return buProduce;
	}

	public void setBuProduce(Map<String, Long> buProduce) {
		this.buProduce = buProduce;
	}

	public Map<String, Long> getBuConsume() {
		return buConsume;
	}

	public void setBuConsume(Map<String, Long> buConsume) {
		this.buConsume = buConsume;
	}

	public void addToBuProduce(String bu, long count) {
		if (buProduce == null) {
			buProduce = new HashMap<>();
		}
		if (!buProduce.containsKey(bu)) {
			buProduce.put(bu, count);
		} else {
			buProduce.put(bu, buProduce.get(bu) + count);
		}
	}

	public void addToBuConsume(String bu, long count) {
		if (buConsume == null) {
			buConsume = new HashMap<>();
		}
		if (!buConsume.containsKey(bu)) {
			buConsume.put(bu, count);
		} else {
			buConsume.put(bu, buConsume.get(bu) + count);
		}
	}

	public Map<String, FlowDetail> getBuDetails() {
		return buDetails;
	}

	public void setBuDetails(Map<String, FlowDetail> buDetails) {
		this.buDetails = buDetails;
	}

	@JsonIgnore
	public FlowDetail getBuFlowDetail(String bu) {
		if (!buDetails.containsKey(bu)) {
			buDetails.put(bu, new FlowDetail(bu));
		}
		return buDetails.get(bu);
	}

	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub

	}

	public TopicFlowDailyReportState(Object id) {
		super(id);
	}
	
	public Object generateId() {
		return null;
	}

	public class FlowDetail {
		private String type;
		private long totalProduce = 0;
		private long totalConsume = 0;
		private List<Triple<Long, String, Long>> top5Produce;
		private List<Triple<Long, String, Long>> top5Consume;
		private int activeTopicCount = 0;
		private Set<Long> activeTopics = new TreeSet<>();
		private int hasProducedTopicCount = 0;
		private int hasConsumedTopicCount = 0;

		public FlowDetail(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public long getTotalProduce() {
			return totalProduce;
		}

		public void setTotalProduce(long totalProduce) {
			this.totalProduce = totalProduce;
		}

		public void addToTotalProduce(long count) {
			this.totalProduce += count;
		}

		public void addToTotalConsume(long count) {
			this.totalConsume += count;
		}

		public long getTotalConsume() {
			return totalConsume;
		}

		public void setTotalConsume(long totalConsume) {
			this.totalConsume = totalConsume;
		}

		public List<Triple<Long, String, Long>> getTop5Produce() {
			return top5Produce;
		}

		public void setTop5Produce(List<Triple<Long, String, Long>> top5Produce) {
			this.top5Produce = top5Produce;
		}

		public boolean addToTop5Produce(Long id, String name, Long count) {
			if (this.top5Produce == null) {
				this.top5Produce = new ArrayList<>();
			}
			if (top5Produce.size() >= 5) {
				return false;
			}
			this.top5Produce.add(new Triple<Long, String, Long>(id, name, count));
			return true;
		}

		public List<Triple<Long, String, Long>> getTop5Consume() {
			return top5Consume;
		}

		public void setTop5Consume(List<Triple<Long, String, Long>> top5Consume) {
			this.top5Consume = top5Consume;
		}

		public boolean addToTop5Consume(Long id, String name, Long count) {
			if (this.top5Consume == null) {
				this.top5Consume = new ArrayList<>();
			}
			if (top5Consume.size() >= 5) {
				return false;
			}
			this.top5Consume.add(new Triple<Long, String, Long>(id, name, count));
			return true;
		}

		public int getActiveTopicCount() {
			return activeTopicCount;
		}

		public void setActiveTopicCount(int activeTopicCount) {
			this.activeTopicCount = activeTopicCount;
		}

		public void activeTopicCountPlus1() {
			this.activeTopicCount++;
		}

		public Set<Long> getActiveTopics() {
			return activeTopics;
		}

		public void setActiveTopics(Set<Long> activeTopics) {
			this.activeTopics = activeTopics;
		}

		public void addToActiveTopics(long id) {
			this.activeTopics.add(id);
		}

		public int getHasProducedTopicCount() {
			return hasProducedTopicCount;
		}

		public void setHasProducedTopicCount(int hasProducedTopicCount) {
			this.hasProducedTopicCount = hasProducedTopicCount;
		}

		public void hasProducedTopicCountPlus1() {
			this.hasProducedTopicCount++;
		}

		public int getHasConsumedTopicCount() {
			return hasConsumedTopicCount;
		}

		public void setHasConsumedTopicCount(int hasConsumedTopicCount) {
			this.hasConsumedTopicCount = hasConsumedTopicCount;
		}

		public void hasConsumedTopicCountPlus1() {
			this.hasConsumedTopicCount++;
		}
	}

	@Override
	public HermesNotice get() {
		TopicFlowDailyReportMail mail = new TopicFlowDailyReportMail(String.format(SUBJECT, "Hermes流量报表", SUBJECT_DATE_FORMAT.format(this.getTimestamp())));
		mail.setTotal(this.total);
		mail.setBuDetails(new ArrayList<>(getBuDetails().values()));
		mail.setBuProduce(calculateBuPersent(this.buProduce, this.total.getTotalProduce()));
		mail.setBuConsume(calculateBuPersent(this.buConsume, this.total.getTotalConsume()));
		mail.setDate(new Date(this.getTimestamp()));
		CollectorConfiguration conf = ApplicationContextUtils.getBean(CollectorConfiguration.class);
		HermesNotice notice = new HermesNotice(Arrays.asList(conf.getNotifierReportMail().split(",")), mail);

		return notice;

	}

	private List<Pair<String,Double>> calculateBuPersent(Map<String, Long> bus, long sum) {
		ArrayList<Entry<String, Long>> buList = new ArrayList<>(bus.entrySet());
		Collections.sort(buList, new Comparator<Entry<String, Long>>() {

			@Override
			public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}

		});
		List<Pair<String,Double>> percentileBus = new ArrayList<>();
		for (Entry<String, Long> bu : buList) {
			percentileBus.add(new Pair<String, Double>(bu.getKey(), bu.getValue() / (sum * 1.0)));
			//percentileBus.add(new Entry<, bu.getValue() / (sum * 1.0)>);
		}
		return percentileBus;

	}

	@HermesMailDescription(template = HermesTemplate.TOPIC_FLOW_DAILY_REPORT)
	public class TopicFlowDailyReportMail extends MailNoticeContent {
		@Subject
		public String m_subject;

		@ContentField(name = "total")
		private FlowDetail m_total;

		@ContentField(name = "buProduce")
		private List<Pair<String,Double>> m_buProduce;

		@ContentField(name = "buConsume")
		private List<Pair<String,Double>> m_buConsume;

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

		public TopicFlowDailyReportMail(String subject) {
			this.m_subject = subject;
		}

		public FlowDetail getTotal() {
			return m_total;
		}

		public void setTotal(FlowDetail m_total) {
			this.m_total = m_total;
		}

		public List<Pair<String,Double>> getBuProduce() {
			return m_buProduce;
		}

		public void setBuProduce(List<Pair<String,Double>> m_buProduce) {
			this.m_buProduce = m_buProduce;
		}

		public List<Pair<String,Double>> getBuConsume() {
			return m_buConsume;
		}

		public void setBuConsume(List<Pair<String,Double>> m_buConsume) {
			this.m_buConsume = m_buConsume;
		}

		public List<FlowDetail> getBuDetails() {
			return m_buDetails;
		}

		public void setBuDetails(List<FlowDetail> m_buDetails) {
			this.m_buDetails = m_buDetails;
		}
	}
}
