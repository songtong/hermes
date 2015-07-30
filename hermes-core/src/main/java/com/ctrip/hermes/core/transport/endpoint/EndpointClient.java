package com.ctrip.hermes.core.transport.endpoint;

import java.util.concurrent.TimeUnit;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointClient {
	public void writeCommand(Endpoint endpoint, Command cmd);

	public void writeCommand(Endpoint endpoint, Command cmd, long timeout, TimeUnit timeUnit);

	public void close();

}
