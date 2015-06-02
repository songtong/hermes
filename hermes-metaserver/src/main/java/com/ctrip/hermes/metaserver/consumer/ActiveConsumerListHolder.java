package com.ctrip.hermes.metaserver.consumer;

import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaserver.client.BaseActiveClientListHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ActiveConsumerListHolder.class)
public class ActiveConsumerListHolder extends BaseActiveClientListHolder<Pair<String, String>> {

}
