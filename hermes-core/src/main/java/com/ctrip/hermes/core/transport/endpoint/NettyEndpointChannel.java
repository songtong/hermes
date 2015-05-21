package com.ctrip.hermes.core.transport.endpoint;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.hermes.core.transport.ManualRelease;
import com.ctrip.hermes.core.transport.command.Ack;
import com.ctrip.hermes.core.transport.command.AckAware;
import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelActiveEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelExceptionCaughtEvent;
import com.ctrip.hermes.core.transport.endpoint.event.EndpointChannelInactiveEvent;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class NettyEndpointChannel extends SimpleChannelInboundHandler<Command> implements EndpointChannel {

	private AtomicReference<Channel> m_channel = new AtomicReference<>(null);

	private AtomicBoolean m_writerStarted = new AtomicBoolean(false);

	private Map<Long, AckAware<Ack>> m_pendingCommands = new HashMap<>();

	private Object m_pendingCmdsLock = new Object();

	private AtomicBoolean m_pendingCmdsHouseKeeperStarted = new AtomicBoolean(false);

	private Thread m_writer;

	private ScheduledExecutorService m_pendingCmdsHouseKepper;

	// TODO config size
	private BlockingQueue<Command> m_writeQueue = new LinkedBlockingQueue<Command>();

	protected CommandProcessorManager m_cmdProcessorManager;

	protected List<EndpointChannelEventListener> m_listeners = new CopyOnWriteArrayList<>();

	protected AtomicBoolean m_closed = new AtomicBoolean(false);

	public NettyEndpointChannel(CommandProcessorManager cmdProcessorManager) {
		m_cmdProcessorManager = cmdProcessorManager;

		m_pendingCmdsHouseKepper = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "PendingCmdsHouseKeeper");
				t.setDaemon(true);
				return t;
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public void writeCommand(Command command) {
		if (m_writerStarted.compareAndSet(false, true)) {
			startWriter();
		}

		if (m_pendingCmdsHouseKeeperStarted.compareAndSet(false, true)) {
			startPendingCmdHouseKeeper();
		}

		if (command instanceof AckAware) {
			synchronized (m_pendingCmdsLock) {
				m_pendingCommands.put(command.getHeader().getCorrelationId(), (AckAware<Ack>) command);
			}
		}

		// TODO if full?
		m_writeQueue.offer(command);
	}

	private void startPendingCmdHouseKeeper() {
		m_pendingCmdsHouseKepper.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try {
					long now = System.currentTimeMillis();
					synchronized (m_pendingCmdsLock) {
						for (Map.Entry<Long, AckAware<Ack>> entry : m_pendingCommands.entrySet()) {
							AckAware<Ack> ackAware = entry.getValue();
							Long correlationId = entry.getKey();
							if (ackAware.getExpireTime() < now) {
								ackAware.onFail();
								m_pendingCommands.remove(correlationId);
							}
						}
					}
				} catch (Exception e) {
					// TODO
				}
			}
		}, 3, 3, TimeUnit.SECONDS);
	}

	private void startWriter() {
		m_writer = new Thread(new NettyWriter());

		// TODO
		m_writer.setDaemon(true);
		m_writer.setName("NettyWriter");
		m_writer.start();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
		if (command instanceof Ack) {
			synchronized (m_pendingCmdsLock) {
				long correlationId = command.getHeader().getCorrelationId();
				AckAware<Ack> reqCommand = m_pendingCommands.remove(correlationId);
				if (reqCommand != null) {
					reqCommand.onAck((Ack) command);
				} else {
					if (command.getClass().isAnnotationPresent(ManualRelease.class)) {
						command.release();
					}
				}
			}
		} else {
			m_cmdProcessorManager.offer(new CommandProcessorContext(command, this));
		}

	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Writablity changed..." + ctx.channel().isWritable());
		super.channelWritabilityChanged(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel inactive...");
		Channel channel = m_channel.getAndSet(null);
		if (channel != null) {
			channel.close();
		}
		clearAllPendings();
		notifyListener(new EndpointChannelInactiveEvent(ctx, this));
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO log
		System.out.println("Channel active...");
		m_channel.set(ctx.channel());
		notifyListener(new EndpointChannelActiveEvent(ctx, this));
		super.channelActive(ctx);

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		// TODO
		cause.printStackTrace();
		Channel channel = m_channel.getAndSet(null);
		if (channel != null) {
			channel.close();
		}
		clearAllPendings();
		notifyListener(new EndpointChannelExceptionCaughtEvent(ctx, cause, this));
		super.exceptionCaught(ctx, cause);
	}

	@Override
	public void addListener(EndpointChannelEventListener... listeners) {
		if (listeners != null) {
			m_listeners.addAll(Arrays.asList(listeners));
		}
	}

	private void clearAllPendings() {
		synchronized (m_pendingCmdsLock) {
			for (AckAware<Ack> ackAware : m_pendingCommands.values()) {
				ackAware.onFail();
			}
			m_pendingCommands.clear();
		}
	}

	protected void notifyListener(EndpointChannelEvent event) {
		for (EndpointChannelEventListener listener : m_listeners) {
			try {
				listener.onEvent(event);
			} catch (Exception e) {
				// TODO
			}
		}
	}

	@Override
	public boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public void close() {
		m_closed.set(true);
		Channel channel = m_channel.get();
		if (channel != null) {
			// TODO notify?
			// TODO do we need to wait for all wQueue flushed
			channel.close();
		}
	}

	@Override
	public String getHost() {
		return m_channel.get().remoteAddress().toString();
	}

	private class NettyWriter implements Runnable {

		@Override
		public void run() {

			Command cmd = null;
			while (!m_closed.get() && !Thread.currentThread().isInterrupted()) {
				try {

					if (cmd == null) {
						cmd = m_writeQueue.poll(1, TimeUnit.SECONDS);
					}

					Channel channel = m_channel.get();

					if (cmd != null && channel != null && channel.isActive() && channel.isWritable()) {
						ChannelFuture future = channel.writeAndFlush(cmd).sync();
						if (future.isSuccess()) {
							cmd = null;
						}
					}

				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					// TODO

					try {
						TimeUnit.MILLISECONDS.sleep(50);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		}

	}
}
