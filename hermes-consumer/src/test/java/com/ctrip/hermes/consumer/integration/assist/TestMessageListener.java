package com.ctrip.hermes.consumer.integration.assist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class TestMessageListener extends BaseMessageListener<TestObjectMessage> {
	private CountDownLatch m_receiveLatch = null;

	private List<TestObjectMessage> m_receivedMessage = new ArrayList<>();

	private boolean m_throwException = false;

	public TestMessageListener receiveCount(int count) {
		m_receiveLatch = new CountDownLatch(count);
		return this;
	}

	public TestMessageListener withError(boolean shouldFail) {
		m_throwException = shouldFail;
		return this;
	}

	public boolean waitUntilReceivedAllMessage(long timeoutInMillisecond) {
		try {
			return m_receiveLatch.await(timeoutInMillisecond, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			return false;
		}
	}

	public long waitUntilReceivedAllMessage() {
		try {
			long begin = System.currentTimeMillis();
			m_receiveLatch.await();
			return System.currentTimeMillis() - begin;
		} catch (InterruptedException e) {
			return -1;
		}
	}

	public List<TestObjectMessage> getReceivedMessages() {
		return m_receivedMessage;
	}

	@Override
	protected synchronized void onMessage(ConsumerMessage<TestObjectMessage> msg) {
		if (m_receiveLatch != null) {
			if (m_receiveLatch.getCount() == 0) {
				return;
			}
			m_receiveLatch.countDown();
		}
		handleMessage(msg);
	}

	private void handleMessage(ConsumerMessage<TestObjectMessage> msg) {
		m_receivedMessage.add(msg.getBody());
		if (m_throwException) {
			throw new RuntimeException(">>>>> You tell me that I should throw an exception, so I did it.");
		}
	}
}
