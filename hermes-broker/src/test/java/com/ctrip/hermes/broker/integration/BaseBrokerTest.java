package com.ctrip.hermes.broker.integration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.datasource.DataSourceProvider;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourceDef;
import org.unidal.dal.jdbc.datasource.model.entity.DataSourcesDef;
import org.unidal.dal.jdbc.datasource.model.entity.PropertiesDef;
import org.unidal.dal.jdbc.mapping.TableProvider;
import org.unidal.dal.jdbc.test.TableMaker;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityDao;
import com.ctrip.hermes.broker.dal.hermes.MessagePriorityEntity;
import com.ctrip.hermes.broker.queue.storage.mysql.dal.HermesTableProvider;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.env.DefaultClientEnvironment;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.PropertiesHolder;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.meta.internal.LocalMetaProxy;
import com.ctrip.hermes.core.meta.internal.MetaLoader;
import com.ctrip.hermes.core.meta.internal.MetaProxy;
import com.ctrip.hermes.core.meta.remote.RemoteMetaLoader;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.Header;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.parser.DefaultCommandParser;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.SettableFuture;

public abstract class BaseBrokerTest extends MockitoComponentTestCase {

	@Mock
	protected DataSourceProvider m_dsProvider;

	@Mock
	protected MetaLoader m_metaLoader;

	@Mock
	protected MetaProxy m_metaProxy;

	@Mock
	protected Channel m_channel;

	@Mock
	private ChannelFuture m_channelFuture;

	private CommandHandler m_cmdHandler;

	@Override
	protected String getDefaultDataSource() {
		return "ds0";
	}

	@Before
	public final void before() throws Exception {
		((DefaultClientEnvironment) lookup(ClientEnvironment.class)).setLocalMode(true);

		createTables("hermes.xml");

		DataSourcesDef defs = new DataSourcesDef();
		DataSourceDef def = new DataSourceDef("ds0");
		PropertiesDef props = new PropertiesDef();
		props.setDriver("org.h2.Driver");
		props.setUrl("jdbc:h2:mem:ds0");
		def.setProperties(props);
		defs.addDataSource(def);
		when(m_dsProvider.defineDatasources()).thenReturn(defs);
		defineMockitoComponent(DataSourceProvider.class, "message", m_dsProvider);

		Meta meta = new LocalMetaLoader().load();
		when(m_metaLoader.load()).thenReturn(meta);
		defineMockitoComponent(MetaLoader.class, LocalMetaLoader.ID, m_metaLoader);
		defineMockitoComponent(MetaLoader.class, RemoteMetaLoader.ID, m_metaLoader);
		defineMockitoComponent(MetaProxy.class, LocalMetaProxy.ID, m_metaProxy);
		defineMockitoComponent(MetaProxy.class, RemoteMetaProxy.ID, m_metaProxy);

		List<TableProvider> providers = lookupList(TableProvider.class);
		for (TableProvider p : providers) {
			if (p instanceof HermesTableProvider) {
				((HermesTableProvider) p).setTablePrefix("h2_");
			}
		}

		when(m_channel.writeAndFlush(any())).thenAnswer(new Answer<ChannelFuture>() {

			private DefaultCommandParser cmdParser = new DefaultCommandParser();

			@Override
			public ChannelFuture answer(InvocationOnMock invocation) throws Throwable {
				if (m_cmdHandler != null) {
					if (invocation.getArguments()[0] instanceof Command) {
						Command cmd = (Command) invocation.getArguments()[0];
						ByteBuf buf = Unpooled.buffer();
						cmd.toBytes(buf);

						Command decodedCmd = cmdParser.parse(buf);
						m_cmdHandler.handle(decodedCmd);
					}
				}

				return m_channelFuture;
			}
		});

		doBefore();
	}

	protected abstract void doBefore() throws Exception;

	protected void setCommandHandler(CommandHandler handler) {
		m_cmdHandler = handler;
	}

	protected void createTables(String xmlFile) throws Exception {
		InputStream in = getClass().getResourceAsStream(xmlFile);

		if (in == null) {
			throw new IllegalArgumentException(String.format("Resource(%s) not found!", xmlFile));
		}

		TableMaker maker = lookup(TableMaker.class);

		maker.make(getDefaultDataSource(), in);
	}

	protected SendMessageCommand sendMessage(String topic, List<ProducerMessage<String>> pmsgs) {
		SendMessageCommand cmd = new SendMessageCommand(topic, 0);

		for (ProducerMessage<String> pmsg : pmsgs) {
			SettableFuture<SendResult> future = SettableFuture.create();
			cmd.addMessage(pmsg, future);
		}

		ByteBuf buf = Unpooled.buffer();
		cmd.toBytes(buf);

		SendMessageCommand decodedCmd = new SendMessageCommand();
		Header header = new Header();
		header.parse(buf);
		decodedCmd.parse(buf, header);

		CommandProcessorContext ctx = new CommandProcessorContext(decodedCmd, m_channel);
		CommandProcessorManager cmdProcessorMgr = lookup(CommandProcessorManager.class);
		cmdProcessorMgr.offer(ctx);

		return cmd;
	}

	private List<MessagePriority> attachPP(List<MessagePriority> rows, int partition, int priority) {
		for (MessagePriority row : rows) {
			row.setPartition(partition);
			row.setPriority(priority);
		}

		return rows;
	}

	private List<MessagePriority> findIdAfter(String topic, int partition, int priority) throws DalException {
		MessagePriorityDao dao = lookup(MessagePriorityDao.class);
		return attachPP(
		      dao.findIdAfter(topic, partition, priority, 0, Integer.MAX_VALUE, MessagePriorityEntity.READSET_FULL),
		      partition, priority);
	}

	protected List<MessagePriority> dumpMessagesInDB(String topic) throws DalException {
		MetaService metaService = lookup(MetaService.class);
		List<Partition> partitions = metaService.findTopicByName(topic).getPartitions();

		List<MessagePriority> result = new ArrayList<>();
		for (Partition p : partitions) {
			result.addAll(findIdAfter(topic, p.getId(), 0));
			result.addAll(findIdAfter(topic, p.getId(), 1));
		}
		return result;
	}

	protected List<MessagePriority> dumpMessagesInDB(String topic, int partition) throws DalException {
		List<MessagePriority> result = new ArrayList<>();
		result.addAll(findIdAfter(topic, partition, 0));
		result.addAll(findIdAfter(topic, partition, 1));
		return result;
	}

	protected List<MessagePriority> dumpMessagesInDB(String topic, int partition, boolean isPriority)
	      throws DalException {
		List<MessagePriority> result = new ArrayList<>();
		if (isPriority) {
			result.addAll(findIdAfter(topic, partition, 0));
		} else {
			result.addAll(findIdAfter(topic, partition, 1));
		}
		return result;
	}

	protected List<Pair<String, String>> props(String... kvParts) {
		List<Pair<String, String>> result = new ArrayList<>();
		if (kvParts.length == 0) {// generate random properties
			result.add(new Pair<>(uuid(), uuid()));
		} else {

			if (kvParts.length % 2 != 0) {
				throw new IllegalArgumentException("Should pass even number of arguments");
			}

			for (int i = 0; i < kvParts.length; i += 2) {
				result.add(new Pair<>(kvParts[i], kvParts[i + 1]));
			}
		}
		return result;
	}

	protected ProducerMessage<String> createProducerMessage(String topic, String body, String key, int partition,
	      long bornTime, boolean isPriority) {

		return createProducerMessage(topic, body, key, partition, bornTime, isPriority, null, null, null);

	}

	protected ProducerMessage<String> createProducerMessage(String topic, String body, String key, int partition,
	      long bornTime, boolean isPriority, List<Pair<String, String>> appProperites,
	      List<Pair<String, String>> sysProperites, List<Pair<String, String>> volatileProperites) {
		ProducerMessage<String> msg = new ProducerMessage<String>(topic, body);
		msg.setBornTime(bornTime);
		msg.setKey(key);
		msg.setPartition(partition);
		msg.setPriority(isPriority);
		PropertiesHolder propertiesHolder = new PropertiesHolder();
		if (appProperites != null && !appProperites.isEmpty()) {
			for (Pair<String, String> appProperty : appProperites) {
				propertiesHolder.addDurableAppProperty(appProperty.getKey(), appProperty.getValue());
			}
		}
		if (sysProperites != null && !sysProperites.isEmpty()) {
			for (Pair<String, String> sysProperty : sysProperites) {
				propertiesHolder.addDurableSysProperty(sysProperty.getKey(), sysProperty.getValue());
			}
		}
		if (volatileProperites != null && !volatileProperites.isEmpty()) {
			for (Pair<String, String> volatileProperty : volatileProperites) {
				propertiesHolder.addVolatileProperty(volatileProperty.getKey(), volatileProperty.getValue());
			}
		}
		msg.setPropertiesHolder(propertiesHolder);
		return msg;
	}

	protected void assertMessageEqual(ProducerMessage<String> pmsg, MessagePriority saved) {
		assertEquals(JSON.toJSONString(pmsg.getBody()), new String(saved.getPayload(), Charsets.UTF_8));
		assertEquals(pmsg.getBornTime(), saved.getCreationDate().getTime());
		assertEquals(pmsg.getKey(), saved.getRefKey());
		assertEquals(pmsg.getPartition(), saved.getPartition());
		assertEquals(pmsg.isPriority() ? 0 : 1, saved.getPriority());
	}

	protected String uuid() {
		return UUID.randomUUID().toString();
	}

	public interface CommandHandler {
		void handle(Command cmd);
	}

}
