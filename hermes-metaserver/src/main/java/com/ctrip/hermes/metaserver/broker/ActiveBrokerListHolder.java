package com.ctrip.hermes.metaserver.broker;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaserver.commons.BaseActiveClientListHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ActiveBrokerListHolder.class)
public class ActiveBrokerListHolder extends BaseActiveClientListHolder<String> {

}
