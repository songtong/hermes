package com.ctrip.hermes.core.transport.command;

import com.ctrip.hermes.core.transport.command.v4.PullMessageResultCommandV4;

public interface PullMessageResultListener extends Command {
	public void onResultReceived(PullMessageResultCommandV4 ack);
}
