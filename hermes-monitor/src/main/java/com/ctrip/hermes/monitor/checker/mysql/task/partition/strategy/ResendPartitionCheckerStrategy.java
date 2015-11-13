package com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.ResendCreationStampFinder;
import com.ctrip.hermes.monitor.config.MonitorConfig;

@Component
public class ResendPartitionCheckerStrategy extends BasePartitionCheckerStrategy {
	@Autowired
	private ResendCreationStampFinder m_finder;

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