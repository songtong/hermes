package com.ctrip.hermes.collector.job;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;
import com.ctrip.hermes.collector.collector.CollectorRegistry;
import com.ctrip.hermes.collector.record.Record;

public abstract class ScheduledCollectorJob extends ScheduledJob {
	private static final String COLLECTOR_CONTEXT = "collector-context";
	
	public void setup(JobContext context) {
		context.getData().put(COLLECTOR_CONTEXT, createContext(context));
	}

	// Actually run the job.
	public boolean run(JobContext context) throws Exception {
		CollectorContext ctx = (CollectorContext)context.getData().get(COLLECTOR_CONTEXT);
		Record<?> record = getApplicationContext().getBean(CollectorRegistry.class).find(ctx).collect(ctx);
		context.setRecord(record);
		return true;
	}
	
	public abstract CollectorContext createContext(JobContext context);
	
}