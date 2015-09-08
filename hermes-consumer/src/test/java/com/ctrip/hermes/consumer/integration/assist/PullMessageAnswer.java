package com.ctrip.hermes.consumer.integration.assist;

import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public enum PullMessageAnswer implements Answer<Void> {
	BASIC() {
		@Override
		public Void answer(InvocationOnMock invocation) throws Throwable {
			if (m_answerDelay > 0) {
				waitUntilTrigger();
			}
			m_answeredCount.incrementAndGet();
			PullMessageCommandV2 pullMessageCmd = invocation.getArgumentAt(1, PullMessageCommandV2.class);
			if (pullMessageCmd != null && m_msgCreator != null) {
				PullMessageResultCommandV2 resultCmd = PullMessageResultCreator.createPullMessageResultCommand(
				      pullMessageCmd.getTopic(), Arrays.asList(new Pair<String, String>("hello", "hermes")), 0, 0, false,
				      "hermes-key", m_msgCreator.createRawMessages());
				resultCmd.correlate(pullMessageCmd);

				PlexusComponentLocator.lookup(CommandProcessor.class, CommandType.RESULT_MESSAGE_PULL_V2.toString())
				      .process(new CommandProcessorContext(resultCmd, m_channel));
			}
			return null;
		}
	},

	NO_ANSWER() {
		@Override
		public Void answer(InvocationOnMock invocation) throws Throwable {
			return null;
		}
	};

	private static Channel m_channel;

	private static RawMessageCreator<?> m_msgCreator;

	private static AtomicInteger m_answeredCount = new AtomicInteger(0);

	private static int m_answerDelay = 0;

	/***
	 * @param channel
	 *           Mock channel to write ack/nack when received messages
	 * @return
	 */
	public synchronized PullMessageAnswer channel(Channel channel) {
		m_channel = channel;
		return this;
	}

	/***
	 * @param creator
	 *           Generate mock messages to simulate PullMessageResultCommand
	 * @return
	 */
	public synchronized PullMessageAnswer creator(RawMessageCreator<?> creator) {
		m_msgCreator = creator;
		return this;
	}

	public synchronized static void reset() {
		m_answeredCount.set(0);
		m_channel = null;
		m_msgCreator = null;
	}

	public synchronized PullMessageAnswer withDelay(int delay) {
		m_answerDelay = delay;
		return this;
	}

	private synchronized static void waitUntilTrigger() {
		if (m_answerDelay > 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(m_answerDelay);
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}
}