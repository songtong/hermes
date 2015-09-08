package com.ctrip.hermes.core.transport.command;

import java.util.HashMap;
import java.util.Map;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.core.transport.command.v2.AckMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageCommandV2;
import com.ctrip.hermes.core.transport.command.v2.PullMessageResultCommandV2;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public enum CommandType {
	MESSAGE_SEND(101, 1, SendMessageCommand.class), //
	MESSAGE_ACK(102, 1, AckMessageCommand.class), //
	MESSAGE_ACK_V2(102, 2, AckMessageCommandV2.class), //
	MESSAGE_PULL(103, 1, PullMessageCommand.class), //
	MESSAGE_PULL_V2(103, 2, PullMessageCommandV2.class), //
	QUERY_OFFSET(104, 1, QueryOffsetCommand.class), //

	ACK_MESSAGE_SEND(201, 1, SendMessageAckCommand.class), //

	RESULT_MESSAGE_PULL(302, 1, PullMessageResultCommand.class), //
	RESULT_MESSAGE_PULL_V2(302, 2, PullMessageResultCommandV2.class), //
	RESULT_MESSAGE_SEND(301, 1, SendMessageResultCommand.class), //
	RESULT_QUERY_OFFSET(303, 1, QueryOffsetResultCommand.class) //
	;

	private static Map<Pair<Integer, Integer>, CommandType> m_types = new HashMap<>();

	static {
		for (CommandType type : CommandType.values()) {
			m_types.put(new Pair<Integer, Integer>(type.getType(), type.getVersion()), type);
		}
	}

	public static CommandType valueOf(int type, int version) {
		return m_types.get(new Pair<Integer, Integer>(type, version));
	}

	private int m_type;

	private int m_version;

	private Class<? extends Command> m_clazz;

	private CommandType(int type, int version, Class<? extends Command> clazz) {
		m_type = type;
		m_version = version;
		m_clazz = clazz;
	}

	public int getType() {
		return m_type;
	}

	public int getVersion() {
		return m_version;
	}

	public Class<? extends Command> getClazz() {
		return m_clazz;
	}

}
