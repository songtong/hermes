package com.ctrip.hermes.metaservice.cmessage;

import com.ctrip.hermes.cmessaging.entity.Cmessaging;

public interface CmessageConfigService {
	public Cmessaging getCmessaging();

	public void updateCmessaging(String cmessagingStr);
}
