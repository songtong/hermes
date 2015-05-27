package com.ctrip.hermes.portal.topic;

import org.unidal.web.mvc.AbstractModule;
import org.unidal.web.mvc.annotation.ModuleMeta;
import org.unidal.web.mvc.annotation.ModulePagesMeta;

@ModuleMeta(name = "topic", defaultInboundAction = "home", defaultTransition = "default", defaultErrorAction = "default")
@ModulePagesMeta({

com.ctrip.hermes.portal.topic.home.Handler.class
})
public class TopicModule extends AbstractModule {

}
