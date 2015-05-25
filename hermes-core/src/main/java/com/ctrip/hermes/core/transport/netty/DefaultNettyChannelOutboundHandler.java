package com.ctrip.hermes.core.transport.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultNettyChannelOutboundHandler extends ChannelOutboundHandlerAdapter {
	private static final Logger log = LoggerFactory.getLogger(DefaultNettyChannelOutboundHandler.class);

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error("Exception caught in outbound", cause);
	}

}
