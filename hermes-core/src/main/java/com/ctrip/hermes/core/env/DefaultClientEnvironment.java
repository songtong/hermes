package com.ctrip.hermes.core.env;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.Hermes;
import com.ctrip.hermes.Hermes.Env;

@Named(type = ClientEnvironment.class)
public class DefaultClientEnvironment extends ContainerHolder implements ClientEnvironment, Initializable {
	private final static String PRODUCER_DEFAULT_FILE = "/hermes-producer.properties";

	private final static String PRODUCER_PATTERN = "/hermes-producer-%s.properties";

	private final static String CONSUMER_DEFAULT_FILE = "/hermes-consumer.properties";

	private final static String CONSUMER_PATTERN = "/hermes-consumer-%s.properties";

	private final static String GLOBAL_DEFAULT_FILE = "/hermes.properties";

	private static final String KEY_IS_LOCAL_MODE = "isLocalMode";

	private ConcurrentMap<String, Properties> m_producerCache = new ConcurrentHashMap<String, Properties>();

	private ConcurrentMap<String, Properties> m_consumerCache = new ConcurrentHashMap<String, Properties>();

	private Properties m_producerDefault;

	private Properties m_consumerDefault;

	private Properties m_globalDefault;

	private static final Logger logger = LoggerFactory.getLogger(DefaultClientEnvironment.class);

	private AtomicReference<Env> m_env = new AtomicReference<Env>();

	private Map<Env, String> m_env2MetaDomain = new HashMap<>();

	// for test only
	private Boolean m_localMode;

	@Override
	public String getMetaServerDomainName() {
		return m_env2MetaDomain.get(getEnv());
	}

	@Override
	public Properties getProducerConfig(String topic) throws IOException {
		Properties properties = m_producerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(PRODUCER_PATTERN, topic), m_producerDefault);
			m_producerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}

	@Override
	public Properties getConsumerConfig(String topic) throws IOException {
		Properties properties = m_consumerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(CONSUMER_PATTERN, topic), m_consumerDefault);
			m_consumerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}

	@Override
	public Properties getGlobalConfig() {
		return m_globalDefault;
	}

	private Properties readConfigFile(String configPath) throws IOException {
		return readConfigFile(configPath, null);
	}

	private Properties readConfigFile(String configPath, Properties defaults) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(configPath);
		logger.info("Reading config from resource {}", configPath);
		if (in == null) {
			// load outside resource under current user path
			Path path = new File(System.getProperty("user.dir") + configPath).toPath();
			if (Files.isReadable(path)) {
				in = new FileInputStream(path.toFile());
				logger.info("Reading config from file {} ", path);
			}
		}
		Properties props = new Properties();
		if (defaults != null) {
			props.putAll(defaults);
		}

		if (in != null) {
			props.load(in);
		}

		StringBuilder sb = new StringBuilder();
		for (Enumeration<String> e = (Enumeration<String>) props.propertyNames(); e.hasMoreElements();) {
			String key = e.nextElement();
			String val = (String) props.getProperty(key);
			sb.append(key).append('=').append(val).append('\n');
		}
		logger.info("Reading properties: \n" + sb.toString());
		return props;
	}

	@Override
	public void initialize() throws InitializationException {
		try {
			m_producerDefault = readConfigFile(PRODUCER_DEFAULT_FILE);
			m_consumerDefault = readConfigFile(CONSUMER_DEFAULT_FILE);
			m_globalDefault = readConfigFile(GLOBAL_DEFAULT_FILE);
		} catch (IOException e) {
			throw new InitializationException("Error read producer default config file", e);
		}

		m_env2MetaDomain.put(Env.LOCAL, m_globalDefault.getProperty("local.domain", "meta.hermes.local"));
		// TODO use real dev&lpt domain when get dev&lpt domain
		m_env2MetaDomain.put(Env.DEV, m_globalDefault.getProperty("dev.domain", "10.3.8.63"));
		m_env2MetaDomain.put(Env.LPT, m_globalDefault.getProperty("lpt.domain", "10.2.5.133"));
		m_env2MetaDomain.put(Env.FAT, m_globalDefault.getProperty("fat.domain", "meta.hermes.fws.qa.nt.ctripcorp.com"));
		m_env2MetaDomain.put(Env.FWS, m_globalDefault.getProperty("fws.domain", "meta.hermes.fws.qa.nt.ctripcorp.com"));
		m_env2MetaDomain
		      .put(Env.UAT, m_globalDefault.getProperty("uat.domain", "meta.hermes.fx.uat.qa.nt.ctripcorp.com"));
		m_env2MetaDomain.put(Env.PROD, m_globalDefault.getProperty("prod.domain", "meta.hermes.fx.ctripcorp.com"));

		logger.info(String.format("Meta server domains: %s", m_env2MetaDomain));
	}

	@Override
	public Env getEnv() {
		if (m_env.get() == null) {
			Env resultEnv = Hermes.getEnv();

			List<EnvProvider> envProviders = lookupList(EnvProvider.class);
			for (EnvProvider p : envProviders) {
				Env newEnv = p.getEnv();
				if (newEnv != null) {
					if (resultEnv == null) {
						resultEnv = newEnv;
					} else {
						if (newEnv != resultEnv) {
							throw new IllegalArgumentException(String.format("Conflict hermes env found '%s' and '%s'",
							      newEnv, resultEnv));
						}
					}
				}
			}

			m_env.compareAndSet(null, resultEnv);
		}

		if (m_env.get() == null) {
			throw new IllegalArgumentException("Hermes env is not set");
		}

		return m_env.get();
	}

	@Override
	public boolean isLocalMode() {
		if (m_localMode != null) {
			return m_localMode;
		} else {
			boolean isLocalMode;
			if (System.getenv().containsKey(KEY_IS_LOCAL_MODE)) {
				isLocalMode = Boolean.parseBoolean(System.getenv(KEY_IS_LOCAL_MODE));
			} else if (getGlobalConfig().containsKey(KEY_IS_LOCAL_MODE)) {
				isLocalMode = Boolean.parseBoolean(getGlobalConfig().getProperty(KEY_IS_LOCAL_MODE));
			} else {
				isLocalMode = false;
			}
			return isLocalMode;
		}
	}

	// for test only!
	public void setLocalMode(Boolean localMode) {
		m_localMode = localMode;
	}

}
