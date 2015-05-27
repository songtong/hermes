package com.ctrip.hermes.portal.view;

import com.ctrip.hermes.portal.topic.TopicPage;
import org.unidal.web.mvc.Page;

public class NavigationBar {
   public Page[] getVisiblePages() {
      return new Page[] {
   
      TopicPage.HOME

		};
   }
}
