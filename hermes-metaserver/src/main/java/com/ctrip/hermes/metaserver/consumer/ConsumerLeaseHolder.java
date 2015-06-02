package com.ctrip.hermes.metaserver.consumer;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.metaserver.commons.BaseLeaseHolder;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerLeaseHolder.class)
public class ConsumerLeaseHolder extends BaseLeaseHolder<Tpg> {

}
