package com.ctrip.hermes.core.lease;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.helper.Threads;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class AbstractLeaseManager<T> implements LeaseManager<T>, Initializable {

	protected ConcurrentMap<T, LeaseAcquisitionListener> m_pendings = new ConcurrentHashMap<>();

	protected ConcurrentMap<T, Lease> m_existingLeases = new ConcurrentHashMap<>();

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
		m_pendings.putIfAbsent(key, listener);
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
						Thread.sleep(5);
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

	protected abstract Lease tryAcquireLease(T key);

	protected abstract boolean tryRenewLease(T key, Lease lease);

	private void tryAcquireNewLeases() {
		final Phaser phaser = new Phaser(1);

		for (Map.Entry<T, LeaseAcquisitionListener> pending : m_pendings.entrySet()) {
			final T key = pending.getKey();
			final LeaseAcquisitionListener listener = pending.getValue();
			if (!m_existingLeases.containsKey(key)) {
				phaser.register();
				try {

					ListenableFuture<Lease> future = m_leaseAcquireOrRenewThreadPool.submit(new Callable<Lease>() {

						@Override
						public Lease call() throws Exception {
							return tryAcquireLease(key);
						}
					});

					Futures.addCallback(future, new FutureCallback<Lease>() {

						@Override
						public void onSuccess(Lease lease) {
							phaser.arriveAndDeregister();
							if (lease != null && lease.getExpireTime() > System.currentTimeMillis()) {
								m_existingLeases.put(key, lease);
								m_listenerNotifierThreadPool.submit(new Runnable() {
									@Override
									public void run() {
										listener.onAcquire();
									}
								});
							}
						}

						@Override
						public void onFailure(Throwable t) {
							phaser.arriveAndDeregister();
							// do nothing
						}
					});

				} catch (Exception e) {
					// TODO
				}
			}
		}

		phaser.arriveAndAwaitAdvance();
	}

	private void tryRenewExistingAndRemoveExpiredLeases() {
		final Phaser phaser = new Phaser(1);
		for (Map.Entry<T, Lease> existingLease : m_existingLeases.entrySet()) {
			try {
				final T key = existingLease.getKey();
				final Lease lease = existingLease.getValue();

				if (lease.getExpireTime() < System.currentTimeMillis()) {
					m_existingLeases.remove(key);
					final LeaseAcquisitionListener listener = m_pendings.get(key);
					if (listener != null) {
						m_listenerNotifierThreadPool.submit(new Runnable() {
							@Override
							public void run() {
								listener.onClose();
							}
						});
					}
				} else {
					if (lease.getExpireTime() - getRenewTimeBeforeExpire() <= System.currentTimeMillis()) {
						phaser.register();
						ListenableFuture<Boolean> future = m_leaseAcquireOrRenewThreadPool.submit(new Callable<Boolean>() {

							@Override
							public Boolean call() throws Exception {
								return tryRenewLease(key, lease);
							}
						});

						future.addListener(new Runnable() {

							@Override
							public void run() {
								phaser.arriveAndDeregister();
							}
						}, MoreExecutors.sameThreadExecutor());

					}
				}
			} catch (Exception e) {
				// TODO
			}
		}

		phaser.arriveAndAwaitAdvance();
	}

}
