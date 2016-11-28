package com.ctrip.hermes.collector.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.admin.core.queue.DefaultMessageQueueDao;

public class ComponentsConfigurator extends AbstractResourceConfigurator {
   @Override
   public List<Component> defineComponents() {
      List<Component> all = new ArrayList<Component>();
      
      all.addAll(new FxhermesmetadbDatabaseConfigurator().defineComponents());
      all.add(A(DefaultMessageQueueDao.class));


      return all;
   }

   public static void main(String[] args) {
      generatePlexusComponentsXmlFile(new ComponentsConfigurator());
   }
}
