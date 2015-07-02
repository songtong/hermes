package com.ctrip.hermes.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.unidal.lookup.ComponentTestCase;
import org.xml.sax.SAXException;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;
import com.ctrip.hermes.broker.config.BrokerConfig;
import com.ctrip.hermes.broker.queue.storage.MessageQueueStorage;
import com.ctrip.hermes.core.message.partition.PartitioningStrategy;
import com.ctrip.hermes.core.message.payload.PayloadCodecFactory;
import com.ctrip.hermes.core.meta.internal.MetaManager;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.test.broker.TestableMessageQueueStorage;
import com.ctrip.hermes.test.broker.TestableMessageQueueStorage.DataRecord;
import com.ctrip.hermes.test.core.DefaultSettableMetaHolder;
import com.ctrip.hermes.test.core.DummyMetaProxy;
import com.ctrip.hermes.test.core.SettableMetaHolder;
import com.ctrip.hermes.test.core.TestableMetaManager;
import com.google.common.base.Charsets;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class HermesBaseTest extends ComponentTestCase {

	private static final String STORAGE_TEST = "test";

	private TestingServer m_zkServer;

	@Before
	@Override
	public void setUp() throws Exception {
		m_zkServer = new TestingServer(2181);
		setupZKNodes();

		super.setUp();

		setupBrokerMock(DefaultSettableMetaHolder.class, DummyMetaProxy.class);

		lookup(SettableMetaHolder.class).setMeta(loadMeta());
	}

	protected Meta loadMeta() throws Exception {
		String fileName = getClass().getSimpleName() + ".xml";
		InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);

		if (in == null) {
			throw new RuntimeException(String.format("File %s not found in classpath.", fileName));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (SAXException | IOException e) {
				throw new RuntimeException(String.format("Error parse meta file %s", fileName), e);
			}
		}
	}

	@After
	@Override
	public void tearDown() throws Exception {
		if (m_zkServer != null) {
			m_zkServer.stop();
		}

		killAllHermesThread();
		super.tearDown();
		TimeUnit.SECONDS.sleep(1);
	}

	protected void killAllHermesThread() throws Exception {
		ThreadGroup threadGroup = new ThreadGroup("Hermes");
		threadGroup.interrupt();
	}

	protected TestableMessageQueueStorage getMessageStorage() throws Exception {
		return (TestableMessageQueueStorage) lookup(MessageQueueStorage.class, STORAGE_TEST);
	}

	private void setupZKNodes() throws Exception {
		ZookeeperService zkService = PlexusComponentLocator.lookup(ZookeeperService.class);

		zkService.ensurePath(ZKPathUtils.getBaseMetaVersionZkPath());
		zkService.ensurePath(ZKPathUtils.getMetaInfoZkPath());
		zkService.ensurePath(ZKPathUtils.getMetaServerAssignmentRootZkPath());
		zkService.ensurePath(ZKPathUtils.getBrokerAssignmentRootZkPath());
	}

	protected void setupBrokerMock(Class<? extends SettableMetaHolder> metaHolderClazz,
	      Class<? extends MetaProxy> metaProxyClazz) throws Exception {
		defineComponent(MessageQueueStorage.class, STORAGE_TEST, TestableMessageQueueStorage.class);
		defineComponent(SettableMetaHolder.class, metaHolderClazz);
		defineComponent(MetaProxy.class, metaProxyClazz);
		defineComponent(MetaManager.class, TestableMetaManager.class).req(SettableMetaHolder.class).req(MetaProxy.class);
	}

	protected void startBrokerMock() throws Exception {
		lookup(BrokerBootstrap.class).start();
		Producer.getInstance().message("prepare", "prepare", "prepare").sendSync();
	}

	protected void stopBrokerMock() throws Exception {
		try {
			BrokerConfig brokerConfig = lookup(BrokerConfig.class);
			Socket s = new Socket("localhost", brokerConfig.getShutdownRequestPort());
			OutputStream out = s.getOutputStream();
			out.write("shutdown\n".getBytes(Charsets.UTF_8));
			out.flush();
			s.close();
		} catch (Exception e) {
			// ignore it
		}
	}

	protected int calPartition(String topic, String partitionKey) throws Exception {
		int partitionCount = lookup(SettableMetaHolder.class).getMeta().findTopic(topic).getPartitionCount();

		return lookup(PartitioningStrategy.class).computePartitionNo(partitionKey, partitionCount);
	}

	protected <T> T decodeBody(DataRecord msg, Class<T> clazz) {
		return PayloadCodecFactory.getCodecByType(msg.getBodyCodecType()).decode(msg.getBody(), clazz);
	}

	protected String getDurableAppProperty(DataRecord msg, String key) {
		return msg.getDurableProperties().get("APP." + key);
	}
}
