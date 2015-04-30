package com.ctrip.hermes.core.message.codec;

import io.netty.buffer.ByteBuf;

public class Magic {

	private final static byte[] MAGIC = new byte[] { 'h', 'e', 'm', 's' };

	public static void readAndCheckMagic(ByteBuf buf) {
		byte[] magic = new byte[MAGIC.length];
		buf.readBytes(magic);
		for (int i = 0; i < magic.length; i++) {
			if (magic[i] != MAGIC[i]) {
				// TODO
				throw new IllegalArgumentException();
			}
		}
	}

	public static void writeMagic(ByteBuf buf) {
		buf.writeBytes(MAGIC);
	}

}
