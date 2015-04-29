package com.ctrip.hermes.core.transport.endpoint.event;

import com.ctrip.hermes.core.transport.endpoint.EndpointChannel;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class EndpointChannelExceptionCaughtEvent extends EndpointChannelEvent {

	private Throwable m_cause;

	public EndpointChannelExceptionCaughtEvent(Object ctx, Throwable cause, EndpointChannel channel) {
		super(ctx, channel);
		m_cause = cause;
	}

	public Throwable getCause() {
		return m_cause;
	}

}
