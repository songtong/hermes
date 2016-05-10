package com.ctrip.hermes.collector.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.unidal.tuple.Triple;

import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class TopicFlowDailyReportState extends NotifiedState {
	private FlowDetail total = new FlowDetail("total");
	private Map<String, Long> buProduce;
	private Map<String, Long> buConsume;
	private FlowDetail hotelDetail = new FlowDetail("hotel");
	private FlowDetail flightDetail = new FlowDetail("flight");

	public Map<String, Long> getBuProduce() {
		return buProduce;
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

	public void setBuProduce(Map<String, Long> buProduce) {
		this.buProduce = buProduce;
	}

	public Map<String, Long> getBuConsume() {
		return buConsume;
	}

	public void setBuConsume(Map<String, Long> buConsume) {
		this.buConsume = buConsume;
	}

	public FlowDetail getHotelDetail() {
		return hotelDetail;
	}

	public void setHotelDetail(FlowDetail hotelDetail) {
		this.hotelDetail = hotelDetail;
	}

	public FlowDetail getFlightDetail() {
		return flightDetail;
	}

	public void setFlightDetail(FlowDetail flightDetail) {
		this.flightDetail = flightDetail;
	}

	public FlowDetail getTotal() {
		return total;
	}

	public void setTotal(FlowDetail total) {
		this.total = total;
	}

	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub

	}

	public TopicFlowDailyReportState(Object id) {
		super(id);
	}

	public FlowDetail getBuDetail(String bu) {
		switch (bu) {
		case "flight":
			return flightDetail;
		case "hotel":
			return hotelDetail;
		default:
			return null;
		}
	}

	public List<FlowDetail> listBuDetails() {
		return new ArrayList<FlowDetail>() {
			private static final long serialVersionUID = 1L;
			{
				add(flightDetail);
				add(hotelDetail);
			}
		};
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
	protected void genarateNotice() {
		// TODO Auto-generated method stub
		
	}
	
	@HermesMailDescription(template = HermesTemplate.TOPIC_FLOW_DAILY_REPORT)
	public class TopicFlowDailyReportMail extends MailNoticeContent{
		public String m_subject;
		
		@ContentField(name = "name")
		private String m_name;
		
		
		
		
	}

}
