package com.ctrip.hermes.collector.job;

import java.util.Date;


/**
 * @author tenglinxiao
 *
 */
public interface JobExecutor {

	public void execute(Job job, Date scheduledDate);

}
