package com.ctrip.hermes.portal.console;

import org.unidal.web.mvc.Page;
import org.unidal.web.mvc.annotation.ModuleMeta;

public enum ConsolePage implements Page {

   TOPIC("topic", "topic", "Topic", "Topic", true),

   CONSUMER("consumer", "consumer", "Consumer", "Consumer", true),

   DASHBOARD("dashboard", "dashboard", "Dashboard", "Dashboard", true),

   ENDPOINT("endpoint", "endpoint", "Group", "Endpoint", true),

   STORAGE("storage", "storage", "Storage", "Storage", true),

   SUBSCRIPTION("subscription", "subscription", "Subscription", "Subscription", true),

   TRACER("tracer", "tracer", "Tracer", "Tracer", true),

   RESENDER("resender", "resender", "Resender", "Resender", true),

   HOME("home", "home", "Home", "Home", true),

   APPLICATION("application", "application", "Application", "Application", true),

   META("meta", "meta", "Meta", "Meta", true),
   
   TEMPLATE("template", "template", "Template", "Template", true),

   IDC("idc", "idc", "Idc", "Idc", true),

   ZK("zk", "zk", "Zk", "Zk", true);

   private String m_name;

   private String m_path;

   private String m_title;

   private String m_description;

   private boolean m_standalone;

   private ConsolePage(String name, String path, String title, String description, boolean standalone) {
      m_name = name;
      m_path = path;
      m_title = title;
      m_description = description;
      m_standalone = standalone;
   }

   public static ConsolePage getByName(String name, ConsolePage defaultPage) {
      for (ConsolePage action : ConsolePage.values()) {
         if (action.getName().equals(name)) {
            return action;
         }
      }

      return defaultPage;
   }

   public String getDescription() {
      return m_description;
   }

   public String getModuleName() {
      ModuleMeta meta = ConsoleModule.class.getAnnotation(ModuleMeta.class);

      if (meta != null) {
         return meta.name();
      } else {
         return null;
      }
   }

   @Override
   public String getName() {
      return m_name;
   }

   @Override
   public String getPath() {
      return m_path;
   }

   public String getTitle() {
      return m_title;
   }

   public boolean isStandalone() {
      return m_standalone;
   }

   public ConsolePage[] getValues() {
      return ConsolePage.values();
   }
}
