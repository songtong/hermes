package com.ctrip.hermes.portal.console;

import org.unidal.web.mvc.AbstractModule;
import org.unidal.web.mvc.annotation.ModuleMeta;
import org.unidal.web.mvc.annotation.ModulePagesMeta;

@ModuleMeta(name = "console", defaultInboundAction = "topic", defaultTransition = "default", defaultErrorAction = "default")
@ModulePagesMeta({

com.ctrip.hermes.portal.console.topic.Handler.class,

com.ctrip.hermes.portal.console.consumer.Handler.class,

com.ctrip.hermes.portal.console.dashboard.Handler.class,

com.ctrip.hermes.portal.console.endpoint.Handler.class,

com.ctrip.hermes.portal.console.storage.Handler.class,

com.ctrip.hermes.portal.console.subscription.Handler.class,

com.ctrip.hermes.portal.console.tracer.Handler.class
})
public class ConsoleModule extends AbstractModule {

}
