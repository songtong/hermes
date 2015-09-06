package com.ctrip.hermes.core.message;

import io.netty.channel.Channel;

import java.util.Iterator;

public class NackDelayedBrokerConsumerMessage<T> implements ConsumerMessage<T>, PropertiesHolderAware,
      BaseConsumerMessageAware<T> {

	private BrokerConsumerMessage<T> m_brokerMsg;

	private int m_resendTimes;

	public NackDelayedBrokerConsumerMessage(BrokerConsumerMessage<T> brokerMsg) {
		m_brokerMsg = brokerMsg;
	}

	public void nack() {
		m_resendTimes++;
		m_brokerMsg.getBaseConsumerMessage().nack();
	}

	public int getResendTimes() {
		return m_resendTimes;
	}

	// below are delegate methods

	public String getGroupId() {
		return m_brokerMsg.getGroupId();
	}

	public void setGroupId(String groupId) {
		m_brokerMsg.setGroupId(groupId);
	}

	public long getCorrelationId() {
		return m_brokerMsg.getCorrelationId();
	}

	public void setCorrelationId(long correlationId) {
		m_brokerMsg.setCorrelationId(correlationId);
	}

	public void setChannel(Channel channel) {
		m_brokerMsg.setChannel(channel);
	}

	public boolean isPriority() {
		return m_brokerMsg.isPriority();
	}

	public void setPriority(boolean priority) {
		m_brokerMsg.setPriority(priority);
	}

	public int getPartition() {
		return m_brokerMsg.getPartition();
	}

	public void setPartition(int partition) {
		m_brokerMsg.setPartition(partition);
	}

	public long getMsgSeq() {
		return m_brokerMsg.getMsgSeq();
	}

	public void setMsgSeq(long msgSeq) {
		m_brokerMsg.setMsgSeq(msgSeq);
	}

	public String getProperty(String name) {
		return m_brokerMsg.getProperty(name);
	}

	public Iterator<String> getPropertyNames() {
		return m_brokerMsg.getPropertyNames();
	}

	public long getBornTime() {
		return m_brokerMsg.getBornTime();
	}

	public String getTopic() {
		return m_brokerMsg.getTopic();
	}

	public String getRefKey() {
		return m_brokerMsg.getRefKey();
	}

	public T getBody() {
		return m_brokerMsg.getBody();
	}

	public void ack() {
		m_brokerMsg.ack();
	}

	public com.ctrip.hermes.core.message.ConsumerMessage.MessageStatus getStatus() {
		return m_brokerMsg.getStatus();
	}

	public void setResend(boolean resend) {
		m_brokerMsg.setResend(resend);
	}

	public boolean isResend() {
		return m_brokerMsg.isResend();
	}

	public int getRemainingRetries() {
		return m_brokerMsg.getRemainingRetries();
	}

	public PropertiesHolder getPropertiesHolder() {
		return m_brokerMsg.getPropertiesHolder();
	}

	public BaseConsumerMessage<T> getBaseConsumerMessage() {
		return m_brokerMsg.getBaseConsumerMessage();
	}

	public long getOffset() {
		return m_brokerMsg.getOffset();
	}

	public int getRetryTimesOfRetryPolicy() {
		return m_brokerMsg.getRetryTimesOfRetryPolicy();
	}

	public void setRetryTimesOfRetryPolicy(int retryTimesOfRetryPolicy) {
		m_brokerMsg.setRetryTimesOfRetryPolicy(retryTimesOfRetryPolicy);
	}

}
