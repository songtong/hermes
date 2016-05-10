package com.ctrip.hermes.collector.state;

public class ProduceFlowState extends State {
	private long m_topicId;
	private int m_partitionId;
	private String m_ip;
	private long m_count;

	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub

	}

	public ProduceFlowState(Object id) {
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

	public void setIp(String m_ip) {
		this.m_ip = m_ip;
	}

	public long getCount() {
		return m_count;
	}

	public void setCount(long count) {
		this.m_count = count;
	}

}
