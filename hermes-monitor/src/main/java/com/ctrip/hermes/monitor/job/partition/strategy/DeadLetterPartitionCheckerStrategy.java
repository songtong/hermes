package com.ctrip.hermes.monitor.job.partition.strategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.job.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.job.partition.finder.DeadLetterCreationStampFinder;

@Component
public class DeadLetterPartitionCheckerStrategy extends BasePartitionCheckerStrategy {

	@Autowired
	DeadLetterCreationStampFinder m_finder;

	@Autowired
	private MonitorConfig m_config;

	@Override
	protected CreationStampFinder getCreationStampFinder() {
		return m_finder;
	}

	@Override
	protected MonitorConfig getConfig() {
		return m_config;
	}
}
