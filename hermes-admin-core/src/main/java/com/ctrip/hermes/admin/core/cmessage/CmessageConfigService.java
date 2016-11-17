package com.ctrip.hermes.admin.core.cmessage;

import com.ctrip.hermes.cmessaging.entity.Cmessaging;

public interface CmessageConfigService {
	public Cmessaging getCmessaging();

	public void updateCmessaging(String cmessagingStr);
}
