package com.ctrip.hermes.spi.internal;

import com.ctrip.hermes.message.MessageContext;
import com.ctrip.hermes.message.MessageValveChain;
import com.ctrip.hermes.spi.MessageValve;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public class TracingMessageValve implements MessageValve {
	public static final String ID = "tracing";

	@Override
	public void handle(MessageValveChain chain, MessageContext ctx) {
		Transaction t = Cat.newTransaction("Message", ctx.getMessage().getTopic());

		try {
			chain.handle(ctx);
		} catch (RuntimeException e) {
			Cat.logError(e);
			t.setStatus(e);
		} finally {
			t.complete();
		}
	}

}
