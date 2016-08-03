package com.ctrip.hermes.portal.console;

import org.unidal.web.mvc.AbstractModule;
import org.unidal.web.mvc.annotation.ModuleMeta;
import org.unidal.web.mvc.annotation.ModulePagesMeta;

@ModuleMeta(name = "console", defaultInboundAction = "home", defaultTransition = "default", defaultErrorAction = "default")
@ModulePagesMeta({

com.ctrip.hermes.portal.console.topic.Handler.class,

com.ctrip.hermes.portal.console.consumer.Handler.class,

com.ctrip.hermes.portal.console.dashboard.Handler.class,

com.ctrip.hermes.portal.console.endpoint.Handler.class,

com.ctrip.hermes.portal.console.storage.Handler.class,

com.ctrip.hermes.portal.console.subscription.Handler.class,

com.ctrip.hermes.portal.console.tracer.Handler.class,

com.ctrip.hermes.portal.console.resender.Handler.class,

com.ctrip.hermes.portal.console.home.Handler.class,

com.ctrip.hermes.portal.console.application.Handler.class,

com.ctrip.hermes.portal.console.meta.Handler.class,

com.ctrip.hermes.portal.console.template.Handler.class,

com.ctrip.hermes.portal.console.idc.Handler.class
})
public class ConsoleModule extends AbstractModule {

}
