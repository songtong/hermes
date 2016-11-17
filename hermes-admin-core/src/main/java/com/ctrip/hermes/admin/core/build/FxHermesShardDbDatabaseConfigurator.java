package com.ctrip.hermes.admin.core.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class FxHermesShardDbDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();
		defineDaoComponents(all, com.ctrip.hermes.admin.core.queue._INDEX.getDaoClasses());
		return all;
	}
}
