package com.ctrip.hermes.metaserver.fulltest;

import com.ctrip.hermes.ctrip.env.DefaultClientEnvironment;

public class MockClientEnviroment extends DefaultClientEnvironment {

	@Override
	public String getMetaServerDomainName() {
		return "127.0.0.1";
	}
}
