package com.ctrip.hermes.core.transport.endpoint;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.service.SystemClockService;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.netty.DefaultNettyChannelOutboundHandler;
import com.ctrip.hermes.core.transport.netty.MagicNumberPrepender;
import com.ctrip.hermes.core.transport.netty.NettyDecoder;
import com.ctrip.hermes.core.transport.netty.NettyEncoder;
import com.ctrip.hermes.core.transport.netty.NettyUtils;
import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = EndpointClient.class)
public class DefaultEndpointClient implements EndpointClient, Initializable {
	private static final Logger log = LoggerFactory.getLogger(DefaultEndpointClient.class);

	private ConcurrentMap<Endpoint, EndpointChannel> m_channels = new ConcurrentHashMap<>();

	private EventLoopGroup m_eventLoopGroup;

	private ScheduledExecutorService m_writerThreadPool;

	@Inject
	private CoreConfig m_config;

	@Inject
	private CommandProcessorManager m_commandProcessorManager;

	@Inject
	private SystemClockService m_systemClockService;

	@Inject
	private MetaService m_metaService;

	private AtomicBoolean m_started = new AtomicBoolean(false);

	@Override
	public void writeCommand(Endpoint endpoint, Command cmd) {
		writeCommand(endpoint, cmd, m_config.getEndpointChannelDefaultWrtieTimeout(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void writeCommand(Endpoint endpoint, Command cmd, long timeout, TimeUnit timeUnit) {
		if (m_started.compareAndSet(false, true)) {
			scheduleWriterTask();
		}

		getChannel(endpoint).write(cmd, timeout, timeUnit);
	}

	private EndpointChannel getChannel(Endpoint endpoint) {
		switch (endpoint.getType()) {
		case Endpoint.BROKER:
			if (!m_channels.containsKey(endpoint)) {
				synchronized (m_channels) {
					if (!m_channels.containsKey(endpoint)) {
						m_channels.put(endpoint, creatChannel(endpoint));
					}
				}
			}

			return m_channels.get(endpoint);

		default:
			throw new IllegalArgumentException(String.format("Unknow endpoint type: %s", endpoint.getType()));
		}
	}

	void removeChannel(Endpoint endpoint, EndpointChannel endpointChannel) {
		EndpointChannel removedChannel = null;
		if (Endpoint.BROKER.equals(endpoint.getType()) && m_channels.containsKey(endpoint)) {
			synchronized (m_channels) {
				if (m_channels.containsKey(endpoint)) {
					EndpointChannel tmp = m_channels.get(endpoint);
					if (tmp == endpointChannel) {
						if (tmp.isClosed()) {
							m_channels.remove(endpoint);
						} else if (!tmp.isFlushing() && !tmp.hasUnflushOps()) {
							m_channels.remove(endpoint);
							removedChannel = endpointChannel;
						}
					}
				}
			}
		}

		if (removedChannel != null) {
			log.info("Closing idle connection to broker({}:{})", endpoint.getHost(), endpoint.getPort());
			removedChannel.close();
		}
	}

	private EndpointChannel creatChannel(Endpoint endpoint) {
		EndpointChannel endpointChannel = new EndpointChannel();
		connect(endpoint, endpointChannel);
		return endpointChannel;
	}

	void connect(final Endpoint endpoint, final EndpointChannel endpointChannel) {
		ChannelFuture channelFuture = createBootstrap(endpoint, endpointChannel).connect(endpoint.getHost(),
		      endpoint.getPort());

		channelFuture.addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!endpointChannel.isClosed()) {
					if (!future.isSuccess()) {
						endpointChannel.setChannelFuture(null);
						
						if (m_metaService.containsEndpoint(endpoint)) {
							final EventLoop loop = future.channel().eventLoop();
							loop.schedule(new Runnable() {
								@Override
								public void run() {
									log.info("Reconnecting to broker({}:{})", endpoint.getHost(), endpoint.getPort());
									connect(endpoint, endpointChannel);
								}
							}, m_config.getEndpointChannelAutoReconnectDelay(), TimeUnit.SECONDS);
						}
					} else {
						endpointChannel.setChannelFuture(future);
					}
				} else {
					if (future.isSuccess()) {
						future.channel().close();
					}
				}
			}

		});
	}

	@Override
	public void initialize() throws InitializationException {
		m_writerThreadPool = Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create(
		      "EndpointChannelWriter", false));

		m_eventLoopGroup = new NioEventLoopGroup(1, HermesThreadFactory.create("NettyWriterEventLoop", false));

	}

	private Bootstrap createBootstrap(final Endpoint endpoint, final EndpointChannel endpointChannel) {
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(m_eventLoopGroup);
		bootstrap.channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true)//
		      .option(ChannelOption.TCP_NODELAY, true)//
		      .option(ChannelOption.SO_SNDBUF, m_config.getNettySendBufferSize())//
		      .option(ChannelOption.SO_RCVBUF, m_config.getNettyReceiveBufferSize());

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {

				ch.pipeline().addLast(
				      //
				      new DefaultNettyChannelOutboundHandler(),//
				      new NettyDecoder(), //
				      new MagicNumberPrepender(), //
				      new LengthFieldPrepender(4), //
				      new NettyEncoder(), //
				      new IdleStateHandler(0, 0, m_config.getEndpointChannelMaxIdleTime()),//
				      new DefaultClientChannelInboundHandler(m_commandProcessorManager, endpoint, endpointChannel,
				            DefaultEndpointClient.this, m_config));
			}
		});

		return bootstrap;
	}

	private void scheduleWriterTask() {
		m_writerThreadPool.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {
					for (EndpointChannel endpointChannel : m_channels.values()) {
						if (!endpointChannel.isClosed()) {
							endpointChannel.flush();
						}
					}

				} catch (Exception e) {
					log.warn("Exception occurred in EndpointChannelWriter loop", e);
				}
			}
		}, 0, m_config.getEndpointChannelWriterCheckInterval(), TimeUnit.MILLISECONDS);
	}

	class EndpointChannel {

		private AtomicReference<ChannelFuture> m_channelFuture = new AtomicReference<>(null);

		private BlockingQueue<WriteOp> m_opQueue = new LinkedBlockingQueue<>(m_config.getEndpointChannelSendBufferSize());

		private AtomicReference<WriteOp> m_flushingOp = new AtomicReference<>(null);

		private AtomicBoolean m_flushing = new AtomicBoolean(false);

		private AtomicBoolean m_closed = new AtomicBoolean(false);

		public void setChannelFuture(ChannelFuture channelFuture) {
			if (!isClosed()) {
				m_channelFuture.set(channelFuture);
			}
		}

		public boolean hasUnflushOps() {
			return !m_opQueue.isEmpty() || m_flushingOp.get() != null;
		}

		public boolean isFlushing() {
			return m_flushing.get();
		}

		public boolean isClosed() {
			return m_closed.get();
		}

		public void close() {
			if (m_closed.compareAndSet(false, true)) {
				ChannelFuture channelFuture = m_channelFuture.get();
				if (channelFuture != null) {
					channelFuture.channel().close();
				}
			}
		}

		public void flush() {
			if (!isClosed()) {
				ChannelFuture channelFuture = m_channelFuture.get();

				if (channelFuture != null) {
					Channel channel = channelFuture.channel();
					if (channel != null && channel.isActive() && channel.isWritable() && !m_opQueue.isEmpty()) {
						if (m_flushing.compareAndSet(false, true)) {
							if (m_flushingOp.compareAndSet(null, m_opQueue.peek())) {
								m_opQueue.poll();
								doFlush(channel, m_flushingOp.get());
							}
						}
					}
				}
			}
		}

		private void doFlush(Channel channel, final WriteOp op) {

			if (op != null && !op.isExpired()) {

				ChannelFuture future = channel.writeAndFlush(op.getCmd());

				future.addListener(new ChannelFutureListener() {

					@Override
					public void operationComplete(final ChannelFuture future) throws Exception {

						if (future.isSuccess()) {
							m_flushingOp.set(null);
							m_flushing.set(false);
						} else {
							if (!isClosed()) {
								future.channel().eventLoop().schedule(new Runnable() {

									@Override
									public void run() {
										doFlush(future.channel(), op);
									}
								}, m_config.getEndpointChannelWriteRetryDealy(), TimeUnit.MILLISECONDS);
							}
						}

					}
				});
			} else {
				m_flushingOp.set(null);
				m_flushing.set(false);
			}
		}

		public void write(Command cmd, long timeout, TimeUnit timeUnit) {
			if (!isClosed() && !m_opQueue.offer(new WriteOp(cmd, timeout, timeUnit))) {
				ChannelFuture channelFuture = m_channelFuture.get();
				Channel channel = null;
				if (channelFuture != null) {
					channel = channelFuture.channel();
				}
				log.warn("Send buffer of endpoint channel {} is full",
				      channel == null ? "null" : NettyUtils.parseChannelRemoteAddr(channel));
			}
		}

		private class WriteOp {
			private Command m_cmd;

			private long m_expireTime;

			public WriteOp(Command cmd, long timeout, TimeUnit timeUnit) {
				m_cmd = cmd;
				m_expireTime = m_systemClockService.now() + timeUnit.toMillis(timeout);
			}

			public Command getCmd() {
				return m_cmd;
			}

			public boolean isExpired() {
				return m_expireTime < m_systemClockService.now();
			}

		}
	}
}
