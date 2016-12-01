package com.ctrip.hermes.collector.record;

import java.util.Date;


/**
 * @author tenglinxiao
 *
 */
public interface TimeWindow {
	// Set time window starts.
	public void setStartDate(Date startDate);
	
	// Set time windows ends.
	public void setEndDate(Date endDate);
}
