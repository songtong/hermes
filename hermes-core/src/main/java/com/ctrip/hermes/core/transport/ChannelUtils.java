package com.ctrip.hermes.core.transport;

import io.netty.channel.Channel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ChannelUtils {

	public static void writeAndFlush(Channel channel, Object msg) {
		if (channel.isActive() && channel.isWritable()) {
			channel.writeAndFlush(msg);
		}
	}
}