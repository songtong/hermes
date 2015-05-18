package com.ctrip.hermes.meta.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.configuration.AbstractJdbcResourceConfigurator;
import org.unidal.lookup.configuration.Component;

final class FxhermesmetadbDatabaseConfigurator extends AbstractJdbcResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();


      defineSimpleTableProviderComponents(all, "fxhermesmetadb", com.ctrip.hermes.meta.dal.meta._INDEX.getEntityClasses());
      defineDaoComponents(all, com.ctrip.hermes.meta.dal.meta._INDEX.getDaoClasses());

      return all;
   }
}
