package com.ctrip.hermes.core.transport.endpoint;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointClient {
	public boolean writeCommand(Endpoint endpoint, Command cmd);

	public void close();

}
