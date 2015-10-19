package com.ctrip.hermes.core.transport.command.v2;

import com.ctrip.hermes.core.transport.command.Command;

public interface PullMessageResultListener extends Command {
	public void onResultReceived(PullMessageResultCommandV2 ack);
}
