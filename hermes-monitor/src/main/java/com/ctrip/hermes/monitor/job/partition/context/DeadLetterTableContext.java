package com.ctrip.hermes.monitor.job.partition.context;

import java.util.regex.Pattern;

import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Topic;

public class DeadLetterTableContext extends BaseTableContext {
	public static final Pattern DEAD_TABLE_PATTERN = Pattern.compile("\\d+_\\d+_dead_letter");

	public DeadLetterTableContext(Topic topic, Partition partition, int retain, int cordon, int increment) {
		super(topic, partition, retain, cordon, increment);
		setTableName(String.format("%s_%s_dead_letter", topic.getId(), partition.getId()));
	}

	public static boolean isDeadTable(String tableName) {
		return DEAD_TABLE_PATTERN.matcher(tableName).matches();
	}

	@Override
	public TableType getType() {
		return TableType.DEAD_LETTER;
	}
}
