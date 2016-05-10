package com.ctrip.hermes.collector.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class FxhermesmetadbDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();


      defineSimpleTableProviderComponents(all, "fxhermesmetadb", com.ctrip.hermes.collector.dal.report._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.hermes.collector.dal.report._INDEX.getDaoClasses());
      
      defineSimpleTableProviderComponents(all, "fxhermesmetadb", com.ctrip.hermes.collector.dal.job._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.hermes.collector.dal.job._INDEX.getDaoClasses());

      return all;
   }
}
