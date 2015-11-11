package com.ctrip.hermes.monitor.checker.mysql.task.partition.strategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.CreationStampFinder;
import com.ctrip.hermes.monitor.checker.mysql.task.partition.finder.MessageCreationStampFinder;

@Component
public class MessagePartitionCheckerStrategy extends BasePartitionCheckerStrategy {
	@Autowired
	private MessageCreationStampFinder m_finder;

	@Override
	protected CreationStampFinder getCreationStampFinder() {
		return m_finder;
	}
}
