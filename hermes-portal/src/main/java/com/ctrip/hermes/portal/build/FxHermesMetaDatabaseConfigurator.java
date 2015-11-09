package com.ctrip.hermes.portal.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class FxHermesMetaDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();
		defineSimpleTableProviderComponents(all, "fxhermesmetadb",
				com.ctrip.hermes.portal.dal.application._INDEX.getEntityClasses());
		defineDaoComponents(all, com.ctrip.hermes.portal.dal.application._INDEX.getDaoClasses());

		return all;
	}
}
