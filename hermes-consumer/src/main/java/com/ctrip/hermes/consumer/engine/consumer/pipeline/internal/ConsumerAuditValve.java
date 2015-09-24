package com.ctrip.hermes.consumer.engine.consumer.pipeline.internal;

import java.util.List;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.consumer.engine.ConsumerContext;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.dianping.cat.Cat;

@Named(type = Valve.class, value = ConsumerAuditValve.ID)
public class ConsumerAuditValve implements Valve {

	public static final String ID = "consumer-audit";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		Pair<ConsumerContext, List<ConsumerMessage<?>>> pair = (Pair<ConsumerContext, List<ConsumerMessage<?>>>) payload;

		String name = pair.getKey().getAuditAppName();
		Cat.logMetricForCount(name, pair.getValue().size());
		ctx.next(payload);
	}

}
