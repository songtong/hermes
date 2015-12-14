package com.ctrip.hermes.monitor.job.partition.context;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

public class DeadLetterTableContext extends BaseTableContext {

	public DeadLetterTableContext(Topic topic, Partition partition, int retain, int cordon, int increment) {
		super(topic, partition, retain, cordon, increment);
		setTableName(String.format("%s_%s_dead_letter", topic.getId(), partition.getId()));
	}

	@Override
	public TableType getType() {
		return TableType.DEAD_LETTER;
	}
}
