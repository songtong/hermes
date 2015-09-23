package com.ctrip.hermes.portal.resource.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicDelayDetailView extends TopicDelayBriefView {

	private Map<String, List<DelayDetail>> details = new HashMap<>();

	public TopicDelayDetailView() {
	}

	public TopicDelayDetailView(String topic) {
		setTopic(topic);
	}

	public DelayDetail getDelay(String consumer, int partitionId) {
		List<DelayDetail> delaydetails = details.get(consumer);
		for (DelayDetail currentDetail : delaydetails) {
			if (currentDetail.getPartitionId() == partitionId) {
				return currentDetail;
			}
		}
		return null;
	}

	public void addDelay(DelayDetail delay) {
		if (!details.containsKey(delay.getConsumer())) {
			details.put(delay.getConsumer(), new ArrayList<DelayDetail>());
		}
		details.get(delay.getConsumer()).add(delay);
	}

	public Map<String, List<DelayDetail>> getDetails() {
		return details;
	}

	public void setDetails(Map<String, List<DelayDetail>> details) {
		this.details = details;
	}

	public static class DelayDetail {
		private String consumer;

		private int partitionId;

		private long delay;

		private long priorityMsgId;

		private long nonPriorityMsgId;

		private long priorityMsgOffset;

		private long nonPriorityMsgOffset;

		private String lastConsumedPriorityMsg;

		private String lastConsumedNonPriorityMsg;


		public DelayDetail() {

		}

		public DelayDetail(String consumer, int partitionId) {
			this.consumer = consumer;
			this.partitionId = partitionId;
		}

		public String getConsumer() {
			return consumer;
		}

		public void setConsumer(String consumer) {
			this.consumer = consumer;
		}

		public int getPartitionId() {
			return partitionId;
		}

		public void setPartitionId(int partitionId) {
			this.partitionId = partitionId;
		}


		public long getDelay() {
			return delay;
		}

		public void setDelay(long delay) {
			this.delay = delay;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((consumer == null) ? 0 : consumer.hashCode());
			result = prime * result + partitionId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DelayDetail other = (DelayDetail) obj;
			if (consumer == null) {
				if (other.consumer != null)
					return false;
			} else if (!consumer.equals(other.consumer))
				return false;
			if (partitionId != other.partitionId)
				return false;
			return true;
		}


		public long getPriorityMsgId() {
			return priorityMsgId;
		}

		public void setPriorityMsgId(long priorityMsgId) {
			this.priorityMsgId = priorityMsgId;
		}

		public long getNonPriorityMsgId() {
			return nonPriorityMsgId;
		}

		public void setNonPriorityMsgId(long nonPriorityMsgId) {
			this.nonPriorityMsgId = nonPriorityMsgId;
		}

		public long getPriorityMsgOffset() {
			return priorityMsgOffset;
		}

		public void setPriorityMsgOffset(long priorityMsgOffset) {
			this.priorityMsgOffset = priorityMsgOffset;
		}

		public long getNonPriorityMsgOffset() {
			return nonPriorityMsgOffset;
		}

		public void setNonPriorityMsgOffset(long nonPriorityMsgOffset) {
			this.nonPriorityMsgOffset = nonPriorityMsgOffset;
		}

		public String getLastConsumedPriorityMsg() {
			return lastConsumedPriorityMsg;
		}

		public void setLastConsumedPriorityMsg(String lastConsumedPriorityMsg) {
			this.lastConsumedPriorityMsg = lastConsumedPriorityMsg;
		}

		public String getLastConsumedNonPriorityMsg() {
			return lastConsumedNonPriorityMsg;
		}

		public void setLastConsumedNonPriorityMsg(String lastConsumedNonPriorityMsg) {
			this.lastConsumedNonPriorityMsg = lastConsumedNonPriorityMsg;
		}


		
	}
}
