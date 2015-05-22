package com.ctrip.hermes.core.transport.netty;

import io.netty.channel.Channel;

import java.net.SocketAddress;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class NettyUtils {
	public static String parseChannelRemoteAddr(final Channel channel) {
		if (null == channel) {
			return "";
		}
		final SocketAddress remote = channel.remoteAddress();
		final String addr = remote != null ? remote.toString() : "";

		if (addr.length() > 0) {
			int index = addr.lastIndexOf("/");
			if (index >= 0) {
				return addr.substring(index + 1);
			}

			return addr;
		}

		return "";
	}
}
