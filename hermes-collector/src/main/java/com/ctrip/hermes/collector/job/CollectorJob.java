package com.ctrip.hermes.collector.job;

import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.CollectorRegistry;
import com.ctrip.hermes.collector.record.Record;

public abstract class CollectorJob extends AbstractJob {
	public static final String COLLECTOR_CONTEXT = "__collector_context__";
	
	@Autowired
	private CollectorRegistry m_collectorRegistry;
	
	public void setup(JobContext context) {
		// Prepare collector context.
		CollectorContext ctx = createContext(context);
		context.getData().put(COLLECTOR_CONTEXT, ctx);
	}

	// Actually run the job.
	public boolean doRun(JobContext context) throws Exception {
		try {
			CollectorContext ctx = (CollectorContext)context.getData().get(COLLECTOR_CONTEXT);
			collect(context, ctx);
			if (context.isSucceed()) {
				success(context);
			} else {
				fail(context);
			}
		} finally {
			context.getData().remove(COLLECTOR_CONTEXT);
		}
		return context.isSucceed();
	}
	
	public boolean collect(JobContext context, CollectorContext ctx) throws Exception {
		// Reset succeed flag.
		context.setSucceed(false);
		Record<?> record = m_collectorRegistry.find(ctx).collect(ctx);
		context.setRecord(record);
		context.setSucceed(ctx.isSucceed());
		return context.isSucceed();
	}
	
	public abstract CollectorContext createContext(JobContext context);
	
	public abstract void success(JobContext context) throws Exception;
	
	// Method called when the job fails.
	public void fail(JobContext context) throws Exception {
		// Almost has nothing to do.
	}
}