package com.ctrip.hermes.consumer.api;

import java.util.List;

import com.ctrip.hermes.core.message.ConsumerMessage;

public interface MessageStream<T> extends Iterable<ConsumerMessage<T>> {

	/**
	 * 
	 * @return topic name of this MessageStream
	 */
	public String getTopic();

	/**
	 * 
	 * @return partition id of this MessageStream
	 */
	public int getParatitionId();

	/**
	 * 
	 * @param offset
	 *           start offset of messages to fetch, included
	 * @param size
	 *           how many messages to fetch
	 * @return messages from offset
	 * @throws Exception
	 */
	public List<ConsumerMessage<T>> fetchMessages(MessageStreamOffset offset, int size) throws Exception;

	/**
	 * 
	 * @param offsets
	 *           message offsets to fetch
	 * @return messages whose offset is in offsets
	 */
	public List<ConsumerMessage<T>> fetchMessages(List<MessageStreamOffset> offsets) throws Exception;

	public boolean hasNext();

	public ConsumerMessage<T> next();

}
