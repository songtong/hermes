package com.ctrip.hermes.core.transport.command;

import com.ctrip.hermes.core.transport.command.v3.PullMessageResultCommandV3;

public interface PullMessageResultListener extends Command {
	public void onResultReceived(PullMessageResultCommandV3 ack);
}
