package com.ctrip.hermes.monitor.job.partition.context;

public class AbandonedTableContext extends BaseTableContext {
	public AbandonedTableContext(String tableName) {
		super(null, null, -1, -1, -1);
		setTableName(tableName);
	}

	@Override
	public TableType getType() {
		return TableType.ABANDONED;
	}
}
