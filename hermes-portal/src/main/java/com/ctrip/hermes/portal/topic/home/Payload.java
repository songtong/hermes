package com.ctrip.hermes.portal.topic.home;

import com.ctrip.hermes.portal.topic.TopicPage;
import org.unidal.web.mvc.ActionContext;
import org.unidal.web.mvc.ActionPayload;
import org.unidal.web.mvc.payload.annotation.FieldMeta;

public class Payload implements ActionPayload<TopicPage, Action> {
   private TopicPage m_page;

   @FieldMeta("op")
   private Action m_action;

   public void setAction(String action) {
      m_action = Action.getByName(action, Action.VIEW);
   }

   @Override
   public Action getAction() {
      return m_action;
   }

   @Override
   public TopicPage getPage() {
      return m_page;
   }

   @Override
   public void setPage(String page) {
      m_page = TopicPage.getByName(page, TopicPage.HOME);
   }

   @Override
   public void validate(ActionContext<?> ctx) {
      if (m_action == null) {
         m_action = Action.VIEW;
      }
   }
}
