package com.ctrip.hermes.monitor.job.partition.finder;

import com.ctrip.hermes.metaservice.queue.CreationStamp;
import com.ctrip.hermes.metaservice.queue.TableContext;

public interface CreationStampFinder {
	public CreationStamp findLatest(TableContext ctx);

	public CreationStamp findOldest(TableContext ctx);

	public CreationStamp findSpecific(TableContext ctx, long id);
}
