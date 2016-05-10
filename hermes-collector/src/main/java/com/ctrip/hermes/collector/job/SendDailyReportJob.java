package com.ctrip.hermes.collector.job;

import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.collector.Collector.CollectorContext;

@Component
public class SendDailyReportJob extends ScheduledCollectorJob{

	@Override
	public CollectorContext createContext(JobContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void success(JobContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(JobContext context) {
		// TODO Auto-generated method stub
		
	}

}
