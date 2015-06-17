package com.ctrip.hermes.core.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.parser.CommandParser;
import com.ctrip.hermes.core.transport.command.parser.DefaultCommandParser;

public class NettyDecoder extends HermesLengthFieldBasedFrameDecoder {
	private static final Logger log = LoggerFactory.getLogger(NettyDecoder.class);

	private CommandParser m_commandParser = new DefaultCommandParser();

	public NettyDecoder() {
		super(Integer.MAX_VALUE, Magic.length(), 4, 0, 4 + Magic.length());
	}

	@Override
	protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
		ByteBuf slicedBuffer = buffer.slice(index, length);
		slicedBuffer.retain();
		return slicedBuffer;
	}

	@Override
	protected void validateMagicNumber(ByteBuf in) {
		Magic.readAndCheckMagic(in);
	}

	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = null;
		Command cmd = null;
		try {
			frame = (ByteBuf) super.decode(ctx, in);
			if (frame == null) {
				return null;
			}

			cmd = m_commandParser.parse(frame);
			return cmd;
		} catch (Exception e) {
			log.error("Exception occured while decoding in netty(client addr={})",
			      NettyUtils.parseChannelRemoteAddr(ctx.channel()), e);
			ctx.channel().close();
		} finally {
			if (null != frame) {
				if (cmd != null && !cmd.getClass().isAnnotationPresent(ManualRelease.class)) {
					frame.release();
				}
			}
		}

		return null;
	}

}
