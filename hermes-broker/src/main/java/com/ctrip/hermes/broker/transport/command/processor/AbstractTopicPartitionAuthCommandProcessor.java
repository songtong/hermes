package com.ctrip.hermes.broker.transport.command.processor;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractTopicPartitionAuthCommandProcessor implements CommandProcessor {

	@Override
	public void process(final CommandProcessorContext ctx) {
		TopicPartition tp = getTopicPartition(ctx);

		if (auth(tp)) {
			doAuthSuccess(ctx);
			doProcess(ctx);
		} else {
			doAuthFail(ctx);
		}
	}

	protected boolean auth(TopicPartition tp) {
		return true;
	}

	protected abstract void doAuthSuccess(CommandProcessorContext ctx);

	protected abstract void doAuthFail(CommandProcessorContext ctx);

	protected abstract void doProcess(CommandProcessorContext ctx);

	protected abstract TopicPartition getTopicPartition(CommandProcessorContext ctx);

	protected static class TopicPartition {
		private String m_topic;

		private int m_partition;

		public TopicPartition(String topic, int partition) {
			m_topic = topic;
			m_partition = partition;
		}

		public String getTopic() {
			return m_topic;
		}

		public int getPartition() {
			return m_partition;
		}

	}
}
