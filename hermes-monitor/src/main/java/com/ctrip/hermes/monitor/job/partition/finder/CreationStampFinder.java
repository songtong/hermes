package com.ctrip.hermes.monitor.job.partition.finder;

import com.ctrip.hermes.admin.core.queue.CreationStamp;
import com.ctrip.hermes.admin.core.queue.TableContext;

public interface CreationStampFinder {
	public CreationStamp findLatest(TableContext ctx);

	public CreationStamp findOldest(TableContext ctx);

	public CreationStamp findNearest(TableContext ctx, long id);
}
