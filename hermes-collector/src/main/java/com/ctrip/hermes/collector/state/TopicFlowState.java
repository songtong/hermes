package com.ctrip.hermes.collector.state;

import java.util.HashMap;
import java.util.Map;

public class TopicFlowState extends State{
	private int m_topicId;
	private ProducerFlowState m_producerState = new ProducerFlowState();
	private ConsumerFlowState m_consumerState =  new ConsumerFlowState();
	public TopicFlowState(Object id) {
		super(id);
	}
	
	public int getTopicId() {
		return m_topicId;
	}
	public void setTopicId(int topicId) {
		m_topicId = topicId;
	}
	public ProducerFlowState getProducerState() {
		return m_producerState;
	}
	public void setProducerState(ProducerFlowState producerState) {
		m_producerState = producerState;
	}
	public ConsumerFlowState getConsumerState() {
		return m_consumerState;
	}
	public void setConsumerState(ConsumerFlowState consumerState) {
		m_consumerState = consumerState;
	}

	@Override
	protected void doUpdate(State state) {
		TopicFlowState newState = (TopicFlowState)state;
		if (m_producerState != null) {
			m_producerState.update(newState.getProducerState());
		}
		if (m_consumerState != null) {
			m_consumerState.update(newState.getConsumerState());
		}
	}
	
	public class ProducerFlowState extends State {
		private Map<Integer, Long> m_produceCountOnPartitions = new HashMap<Integer, Long>(); 
		private long m_produceCount;
		public ProducerFlowState() {
			super(null);
		}
		public Map<Integer, Long> getProduceCountOnPartitions() {
			return m_produceCountOnPartitions;
		}
		public void setProduceCountOnPartitions(
				Map<Integer, Long> produceCountOnPartitions) {
			m_produceCountOnPartitions = produceCountOnPartitions;
		}
		public long getProduceCount() {
			return m_produceCount;
		}
		public void setProduceCount(long produceCount) {
			this.m_produceCount = produceCount;
		}
		@Override
		protected void doUpdate(State state) {
			ProducerFlowState newState = (ProducerFlowState)state;
			if (newState != null) {
				for (Map.Entry<Integer, Long> entry : newState.getProduceCountOnPartitions().entrySet()) {
					m_produceCountOnPartitions.put(entry.getKey(), m_produceCountOnPartitions.get(entry.getKey()) + entry.getValue());
				}
				this.m_produceCount += newState.getProduceCount();
			}
		}
	}
	
	public class ConsumerFlowState extends State {
		private Map<Integer, Map<Integer, Long>> m_consumerCountOnPartitions = new HashMap<Integer, Map<Integer, Long>>();
		private Map<Integer, Long> m_consumerCountOnGroups = new HashMap<Integer, Long>();
		private long m_consumeCount;
		public ConsumerFlowState() {
			super(null);
		}
		public Map<Integer, Map<Integer, Long>> getConsumerCountOnPartitions() {
			return m_consumerCountOnPartitions;
		}
		public void setConsumerCountOnPartitions(
				Map<Integer, Map<Integer, Long>> consumerCountOnPartitions) {
			m_consumerCountOnPartitions = consumerCountOnPartitions;
		}
		public Map<Integer, Long> getConsumerCountOnGroups() {
			return m_consumerCountOnGroups;
		}
		public void setConsumerCountOnGroups(Map<Integer, Long> consumerCountOnGroups) {
			m_consumerCountOnGroups = consumerCountOnGroups;
		}
		public long getConsumeCount() {
			return m_consumeCount;
		}
		public void setConsumeCount(long consumeCount) {
			m_consumeCount = consumeCount;
		}
		@Override
		protected void doUpdate(State state) {
			ConsumerFlowState newState = (ConsumerFlowState)state;
			if (newState != null) {
				// Update consumer partition consumption counts.
				for (Map.Entry<Integer, Map<Integer, Long>> entry : newState.getConsumerCountOnPartitions().entrySet()) {
					if (!m_consumerCountOnPartitions.containsKey(entry.getKey())) {
						m_consumerCountOnPartitions.put(entry.getKey(), new HashMap<Integer, Long>());
					}
					
					Map<Integer, Long> partitionCounts = new HashMap<Integer, Long>();
					for (Map.Entry<Integer, Long> partitionCount :  entry.getValue().entrySet()) {
						Long count = partitionCounts.get(partitionCount.getKey());
						if (count == null) {
							partitionCounts.put(partitionCount.getKey(), partitionCount.getValue());
						} else {
							partitionCounts.put(partitionCount.getKey(), partitionCount.getValue() + count);
						}
					}
				}
				
				// Update consumer consumption counts.
				for (Map.Entry<Integer, Long> entry : newState.getConsumerCountOnGroups().entrySet()) {
					Long count = m_consumerCountOnGroups.get(entry.getKey());
					if (count == null) {
						m_consumerCountOnGroups.put(entry.getKey(), entry.getValue());
					} else{
						m_consumerCountOnGroups.put(entry.getKey(), entry.getValue() + count);
					} 
				}
				this.m_consumeCount += newState.getConsumeCount();
			}			
		}
	}
}
