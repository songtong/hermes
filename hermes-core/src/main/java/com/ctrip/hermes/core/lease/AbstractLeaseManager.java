package com.ctrip.hermes.core.lease;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads;
import org.unidal.tuple.Pair;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractLeaseManager<T extends SessionIdAware> implements LeaseManager<T>, Initializable {

	protected ConcurrentMap<T, LeaseAcquisitionContext> m_pendings = new ConcurrentHashMap<>();

	protected ConcurrentMap<T, Pair<Lease, LeaseAcquisitionContext>> m_existingLeases = new ConcurrentHashMap<>();

	private static final long RETRY_DELAY = 500L;

	// TODO
	protected ExecutorService m_listenerNotifierThreadPool = Executors.newFixedThreadPool(10, new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			// TODO extract common ThreadFactory
			Thread t = new Thread(r);
			t.setDaemon(true);
			t.setName("LeaseMonitor-listenerNotifier");
			return t;
		}
	});

	// TODO
	protected ListeningExecutorService m_leaseAcquireOrRenewThreadPool = MoreExecutors.listeningDecorator(Executors
	      .newFixedThreadPool(10, new ThreadFactory() {

		      @Override
		      public Thread newThread(Runnable r) {
			      // TODO extract common ThreadFactory
			      Thread t = new Thread(r);
			      t.setDaemon(true);
			      t.setName("LeaseMonitor-leaseOp");
			      return t;
		      }
	      }));

	@Override
	public void registerAcquisition(T key, LeaseAcquisitionListener listener) {
		m_pendings.putIfAbsent(key, new LeaseAcquisitionContext(listener, new AtomicLong(System.currentTimeMillis())));
	}

	@Override
	public void initialize() throws InitializationException {
		Threads.forGroup().start(new Runnable() {

			@Override
			public void run() {

				while (!Thread.currentThread().isInterrupted()) {
					tryAcquireNewLeases();

					tryRenewExistingAndRemoveExpiredLeases();

					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO
						Thread.currentThread().interrupt();
					}
				}
			}

		}, true);
	}

	protected long getRenewTimeBeforeExpire() {
		return 1000L * 3;
	}

	protected abstract LeaseAcquireResponse tryAcquireLease(T key);

	protected abstract LeaseAcquireResponse tryRenewLease(T key, Lease lease);

	private void tryAcquireNewLeases() {
		final Phaser phaser = new Phaser(1);

		final long now = System.currentTimeMillis();
		for (final Map.Entry<T, LeaseAcquisitionContext> pending : m_pendings.entrySet()) {
			T key = pending.getKey();
			LeaseAcquisitionContext context = pending.getValue();
			AtomicLong nextAcquireTime = context.getNextAcquireTime();
			if (!m_existingLeases.containsKey(key)) {
				if (nextAcquireTime.get() > now) {
					continue;
				}

				phaser.register();
				try {
					// TODO
					acquireNewLease(phaser, now, key, context);
				} catch (Exception e) {
					// TODO
				}
			}
		}

		phaser.arriveAndAwaitAdvance();
	}

	private void acquireNewLease(final Phaser phaser, final long now, final T key, final LeaseAcquisitionContext context) {

		ListenableFuture<LeaseAcquireResponse> future = m_leaseAcquireOrRenewThreadPool
		      .submit(new Callable<LeaseAcquireResponse>() {

			      @Override
			      public LeaseAcquireResponse call() throws Exception {
				      return tryAcquireLease(key);
			      }
		      });

		Futures.addCallback(future, new FutureCallback<LeaseAcquireResponse>() {

			@Override
			public void onSuccess(final LeaseAcquireResponse response) {
				phaser.arriveAndDeregister();

				if (response != null) {
					if (response.isAcquired()) {
						final Lease lease = response.getLease();
						// 对于已经获得的lease，下一次尝试renew的时间会比真正到期时间提前一点
						context.getNextAcquireTime().set(lease.getExpireTime() - getRenewTimeBeforeExpire());
						m_existingLeases.put(key, new Pair<>(lease, context));
						m_listenerNotifierThreadPool.submit(new Runnable() {
							@Override
							public void run() {
								context.getListener().onAcquire(lease);
							}
						});
					} else {
						context.getNextAcquireTime().set(response.getNextTryTime());
					}
				} else {
					// TODO
					context.getNextAcquireTime().set(now + RETRY_DELAY);
				}
			}

			@Override
			public void onFailure(Throwable t) {
				phaser.arriveAndDeregister();
				// do nothing
			}
		});
	}

	private void tryRenewExistingAndRemoveExpiredLeases() {
		final Phaser phaser = new Phaser(1);
		final long now = System.currentTimeMillis();
		for (Map.Entry<T, Pair<Lease, LeaseAcquisitionContext>> existingLease : m_existingLeases.entrySet()) {
			try {
				T key = existingLease.getKey();
				Lease lease = existingLease.getValue().getKey();
				LeaseAcquisitionContext context = existingLease.getValue().getValue();
				AtomicLong nextRenewTime = context.getNextAcquireTime();

				if (lease.getExpireTime() < now) {
					removeExpiredLease(key);
				} else {
					if (now >= nextRenewTime.get()) {
						phaser.register();
						renewExistingLease(phaser, now, key, lease, context);

					}
				}
			} catch (Exception e) {
				// TODO
			}
		}

		phaser.arriveAndAwaitAdvance();
	}

	private void renewExistingLease(final Phaser phaser, final long now, final T key, final Lease lease,
	      final LeaseAcquisitionContext context) {
		ListenableFuture<LeaseAcquireResponse> future = m_leaseAcquireOrRenewThreadPool
		      .submit(new Callable<LeaseAcquireResponse>() {

			      @Override
			      public LeaseAcquireResponse call() throws Exception {
				      return tryRenewLease(key, lease);
			      }
		      });

		Futures.addCallback(future, new FutureCallback<LeaseAcquireResponse>() {

			@Override
			public void onSuccess(final LeaseAcquireResponse response) {
				phaser.arriveAndDeregister();
				if (response != null) {
					if (response.isAcquired()) {
						final Lease renewLease = response.getLease();
						if (renewLease != null && renewLease.getExpireTime() > now) {
							lease.setExpireTime(renewLease.getExpireTime());
							lease.setId(renewLease.getId());
							context.getNextAcquireTime().set(lease.getExpireTime() - getRenewTimeBeforeExpire());
						}
					} else {
						// TODO 500毫秒后再去尝试renew
						context.getNextAcquireTime().set(now + RETRY_DELAY);
					}
				} else {
					// TODO
					context.getNextAcquireTime().set(now + RETRY_DELAY);
				}
			}

			@Override
			public void onFailure(Throwable t) {
				phaser.arriveAndDeregister();
				// do nothing
			}
		});
	}

	private void removeExpiredLease(final T key) {
		m_existingLeases.remove(key);
		final LeaseAcquisitionListener listener = m_pendings.get(key).getListener();
		if (listener != null) {
			m_listenerNotifierThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					listener.onExpire();
				}
			});
		}
	}

	private static class LeaseAcquisitionContext {
		private LeaseAcquisitionListener m_listener;

		private AtomicLong m_nextAcquireTime;

		public LeaseAcquisitionContext(LeaseAcquisitionListener listener, AtomicLong nextAcquireTime) {
			m_listener = listener;
			m_nextAcquireTime = nextAcquireTime;
		}

		public LeaseAcquisitionListener getListener() {
			return m_listener;
		}

		public AtomicLong getNextAcquireTime() {
			return m_nextAcquireTime;
		}

	}
}
