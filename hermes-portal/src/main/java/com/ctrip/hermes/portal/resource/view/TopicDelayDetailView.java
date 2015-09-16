package com.ctrip.hermes.portal.resource.view;

import java.util.ArrayList;
import java.util.List;

public class TopicDelayDetailView extends TopicDelayBriefView {

	private List<DelayDetail> details = new ArrayList<DelayDetail>();

	public TopicDelayDetailView() {
	}

	public TopicDelayDetailView(String topic) {
		setTopic(topic);
	}

	public void addDelay(String consumer, int partitionId, Long delayInSeconds) {
		details.add(new DelayDetail(consumer, partitionId, delayInSeconds));
	}

	public List<DelayDetail> getDetails() {
		return details;
	}

	public void setDetails(List<DelayDetail> details) {
		this.details = details;
	}

	public static class DelayDetail {
		private String consumer;

		private int partitionId;

		private long delay;

		public DelayDetail() {

		}

		public DelayDetail(String consumer, int partitionId, long delay) {
			this.consumer = consumer;
			this.partitionId = partitionId;
			this.delay = delay;
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

		public void setDelay(int delay) {
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
	}
}
