package com.ctrip.hermes.core.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.junit.Test;

public class MagicTest {

	@Test
	public void test() {
		ByteBuf buf = Unpooled.buffer();
		Magic.writeMagic(buf);
		byte[] dst = new byte[Magic.length()];
		buf.readBytes(dst);

		System.out.println(dst.length);
		System.out.println((dst[0] << 24) + (dst[1] << 16) | (dst[2] << 8) + dst[3]);

		buf.readerIndex(0);
		System.out.println(buf.readInt());
	}

}
