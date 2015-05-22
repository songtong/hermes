package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.netty.AbstractNettyChannelInboundHandler;
import com.ctrip.hermes.core.transport.netty.NettyUtils;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class DefaultServerChannelInboundHandler extends AbstractNettyChannelInboundHandler {
	private static final Logger log = LoggerFactory.getLogger(DefaultServerChannelInboundHandler.class);

	public DefaultServerChannelInboundHandler(CommandProcessorManager cmdProcessorManager) {
		super(cmdProcessorManager);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		log.info("Client connected(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		log.warn("Client disconnected(addr={})", NettyUtils.parseChannelRemoteAddr(ctx.channel()));
		super.channelInactive(ctx);
	}
}
