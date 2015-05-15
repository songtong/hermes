package com.ctrip.hermes.consumer.engine.pipeline;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.core.pipeline.ValveRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ValveRegistry.class, value = BuildConstants.CONSUMER)
public class ConsumerValveRegistry extends AbstractValveRegistry {

}
