package com.ctrip.hermes.core.env;

import java.io.IOException;
import java.util.Properties;

import com.ctrip.hermes.Hermes.Env;

public interface ClientEnvironment {

	Properties getProducerConfig(String topic) throws IOException;

	Properties getConsumerConfig(String topic) throws IOException;

	Properties getGlobalConfig();

	Env getEnv();

	boolean isLocalMode();
}
