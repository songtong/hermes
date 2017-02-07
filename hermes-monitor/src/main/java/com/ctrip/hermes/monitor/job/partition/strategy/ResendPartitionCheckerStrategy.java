package com.ctrip.hermes.monitor.job.partition.strategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.monitor.config.PartitionCheckerConfig;
import com.ctrip.hermes.monitor.job.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.job.partition.finder.ResendCreationStampFinder;

@Component
public class ResendPartitionCheckerStrategy extends BasePartitionCheckerStrategy {
	@Autowired
	private ResendCreationStampFinder m_finder;

	@Autowired
	private PartitionCheckerConfig m_config;

	@Override
	protected CreationStampFinder getCreationStampFinder() {
		return m_finder;
	}

	@Override
	protected PartitionCheckerConfig getConfig() {
		return m_config;
	}
}
