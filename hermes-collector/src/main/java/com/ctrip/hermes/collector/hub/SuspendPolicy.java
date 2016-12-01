package com.ctrip.hermes.collector.hub;

public class SuspendPolicy {
	private long m_sleep = 0;
	private long m_min = 1;
	private long m_max = 3000;
	
	private SuspendPolicy() {}
	
	public void suspend(boolean succeed) {
		if (succeed) {
			success();
		} else {
			fail();
		}
	}
	
	public void success() {
		decrease();
		if (m_sleep > 0) {
			try {
				Thread.sleep(m_sleep);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}
	
	public void fail() {
		increase();
		try {
			Thread.sleep(m_sleep);
		} catch (InterruptedException e) {
			Thread.interrupted();
		}
	}
	
	private void increase() {
		if (m_sleep == 0) {
			m_sleep = m_min;
		} else {
			m_sleep *= 1.5;
		}
		
		if (m_sleep > m_max) {
			m_sleep = m_max;
		}
	}
	
	private void decrease() {
		m_sleep /= 2;
	}
	
	public static SuspendPolicy instance() {
		return new SuspendPolicy();
	}
}
