package com.ctrip.hermes.consumer.engine.pipeline;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.build.BuildConstants;
import com.ctrip.hermes.consumer.engine.consumer.pipeline.internal.ConsumerAuditValve;
import com.ctrip.hermes.consumer.engine.consumer.pipeline.internal.ConsumerTracingValve;
import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.core.pipeline.ValveRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ValveRegistry.class, value = BuildConstants.CONSUMER)
public class ConsumerValveRegistry extends AbstractValveRegistry implements Initializable {
	@Override
	public void initialize() throws InitializationException {
		doRegister(ConsumerTracingValve.ID, 0);
		doRegister(ConsumerAuditValve.ID, 1);
	}
}
