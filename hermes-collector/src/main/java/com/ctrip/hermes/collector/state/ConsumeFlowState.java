package com.ctrip.hermes.collector.state;

public class ConsumeFlowState extends State {
	private long m_topicId;
	private long m_consumerId;
	private int m_partitionId;
	private String m_ip;
	private long m_count;

	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub

	}

	public ConsumeFlowState(Object id) {
		super(id);
	}

	public long getTopicId() {
		return m_topicId;
	}

	public void setTopicId(long topicId) {
		this.m_topicId = topicId;
	}

	public int getPartitionId() {
		return m_partitionId;
	}

	public void setPartitionId(int partitionId) {
		this.m_partitionId = partitionId;
	}

	public String getIp() {
		return m_ip;
	}

	public void setIp(String ip) {
		this.m_ip = ip;
	}

	public long getCount() {
		return m_count;
	}

	public void setCount(long count) {
		this.m_count = count;
	}

	public long getConsumerId() {
		return m_consumerId;
	}

	public void setConsumerId(long consumerId) {
		this.m_consumerId = consumerId;
	}
	

}
