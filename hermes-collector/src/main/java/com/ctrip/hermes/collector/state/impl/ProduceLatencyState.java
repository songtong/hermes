package com.ctrip.hermes.collector.state.impl;

import com.ctrip.hermes.collector.state.State;

public class ProduceLatencyState extends State {
	public static final String ID_FORMAT = "%s-%d";
	private String m_topic;
	private long m_count;
	private double m_min;
	private double m_max;
	private double m_avg;
	private long m_countAll;
	private double m_ratio;
	
	public ProduceLatencyState(String topic, long count, double avg, double min, double max) {
		this.m_topic = topic;
		this.m_count = count;
		this.m_avg = avg;
		this.m_min = min;
		this.m_max = max;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public long getCount() {
		return m_count;
	}

	public void setCount(long count) {
		m_count = count;
	}

	public double getMin() {
		return m_min;
	}

	public void setMin(double min) {
		m_min = min;
	}

	public double getMax() {
		return m_max;
	}

	public void setMax(double max) {
		m_max = max;
	}

	public double getAvg() {
		return m_avg;
	}

	public void setAvg(double avg) {
		m_avg = avg;
	}

	public long getCountAll() {
		return m_countAll;
	}

	public void setCountAll(long countAll) {
		m_countAll = countAll;
	}

	public double getRatio() {
		return m_ratio;
	}

	public void setRatio(double ratio) {
		m_ratio = ratio;
	}

	@Override
	protected void doUpdate(State state) {
		// TODO Auto-generated method stub
		
	}
	
	public Object clone() {
		ProduceLatencyState cloned = new ProduceLatencyState(this.getTopic(), this.getCount(), this.getAvg(), this.getMin(), this.getMax());
		cloned.setIndex(this.getIndex());
		cloned.setTimestamp(this.getTimestamp());
		return cloned;
	}

	@Override
	protected Object generateId() {
		return String.format(ID_FORMAT, this.m_topic, System.currentTimeMillis());
	}
}
