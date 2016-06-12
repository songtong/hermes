package com.ctrip.hermes.core.constants;

public interface CatConstants {

	public static final String ROOT_MESSAGE_ID = "RootMessageId";

	public static final String CURRENT_MESSAGE_ID = "CurrentMessageId";

	public static final String SERVER_MESSAGE_ID = "ServerMessageId";

	public static final String TYPE_REMOTE_CALL = "RemoteCall";

	public static final String TYPE_MESSAGE_DELIVER_ELAPSE = "Message.Deliver.Elapse";

	public static final String TYPE_MESSAGE_MISS_RATIO = "Hermes.KPI.Miss.Ratio";

	public static final String TYPE_MESSAGE_CONSUME_LATENCY = "Message.Consume.Latency";

	public static final String TYPE_MESSAGE_CONSUME_RESEND_LATENCY = "Message.Consume.Resend.Latency";

	public static final String TYPE_MESSAGE_CONSUMED = "Message.Consumed";

	public static final String TYPE_MESSAGE_PRODUCE_ERROR = "Message.Produce.Error";

	public static final String TYPE_MESSAGE_PRODUCE_ELAPSE = "Message.Produce.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_POLL_ELAPSE = "Message.Consume.Poll.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_POLL_TRIED = "Message.Consume.Poll.Tried";

	public static final String TYPE_MESSAGE_CONSUME_COLLECT_ELAPSE = "Message.Consume.Collect.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_COLLECT_TRIED = "Message.Consume.Collect.Tried";

	public static final String TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE = "Message.Produce.Elapse.Large";

	public static final String TYPE_MESSAGE_PRODUCE_TRANSPORT = "Message.Produce.Transport";

	public static final String TYPE_MESSAGE_PRODUCE_ACKED = "Message.Produce.Acked";

	public static final String TYPE_MESSAGE_PRODUCE_TRIED = "Message.Produce.Tried";

	public static final String TYPE_MESSAGE_BROKER_FLUSH = "Message.Broker.Flush";

	public static final String TYPE_MESSAGE_BROKER_PRODUCE_ELAPSE = "Message.Broker.Produce.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_ACK_TRANSPORT = "Message.Consume.Ack.Transport";

	public static final String TYPE_SEND_CMD = "Send.Cmd.V";

	public static final String TYPE_PULL_CMD = "Pull.Cmd.V";

	public static final String TYPE_HERMES_CMD_VERSION = "Hermes.Command.Version";

	public static final String TYPE_CMD_DROP = "Hermes.Command.Drop";
}
