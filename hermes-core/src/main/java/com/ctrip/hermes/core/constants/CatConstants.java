package com.ctrip.hermes.core.constants;

public interface CatConstants {

	public static final String ROOT_MESSAGE_ID = "RootMessageId";

	public static final String CURRENT_MESSAGE_ID = "CurrentMessageId";

	public static final String SERVER_MESSAGE_ID = "ServerMessageId";

	public static final String TYPE_REMOTE_CALL = "RemoteCall";

	public static final String TYPE_MESSAGE_DELIVER_ELAPSE = "Message.Deliver.Elapse";

	public static final String TYPE_MESSAGE_MISS_RATIO = "Hermes.KPI.Miss.Ratio";

	public static final String TYPE_MESSAGE_CONSUME_LATENCY = "Message.Consume.Latency";

	public static final String TYPE_MESSAGE_CONSUMED = "Message.Consumed";

	public static final String TYPE_MESSAGE_PRODUCE_ERROR = "Message.Produce.Error";

	public static final String TYPE_MESSAGE_PRODUCE_ELAPSE = "Message.Produce.Elapse";

	public static final String TYPE_MESSAGE_PRODUCE_TRANSPORT = "Message.Produce.Transport";

	public static final String TYPE_MESSAGE_PRODUCE_ACKED = "Message.Produce.Acked";

	public static final String TYPE_MESSAGE_PRODUCE_TRIED = "Message.Produce.Tried";

	public static final String TYPE_MESSAGE_BROKER_FLUSH = "Message.Broker.Flush";

	public static final String TYPE_MESSAGE_BROKER_PRODUCE_ELAPSE = "Message.Broker.Produce.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_ACK_TRANSPORT = "Message.Consume.Ack.Transport";
}
