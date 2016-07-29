/**
 * 
 */
package com.ctrip.hermes.core.selector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Pair;

/**
 * @author marsqing
 *
 *         Jun 20, 2016 4:09:57 PM
 */
public class DefaultSelector<T> implements Selector<T> {

	private final static Logger log = LoggerFactory.getLogger(DefaultSelector.class);

	private final ConcurrentMap<T, ObserveContext> ctxes = new ConcurrentHashMap<>();

	private final ExecutorService es;

	private final int slotCount;

	private final long maxWriteOffsetTtlMillis;

	private final OffsetLoader<T> offsetLoader;

	private InitialLastUpdateTime initialLastUpdateTime;

	public DefaultSelector(ExecutorService es, int slotCount, long maxWriteOffsetTtlMillis, OffsetLoader<T> offsetLoader,
			InitialLastUpdateTime initialLastUpdateTime) {
		this.es = es;
		this.slotCount = slotCount;
		this.maxWriteOffsetTtlMillis = maxWriteOffsetTtlMillis;
		this.offsetLoader = offsetLoader;
		this.initialLastUpdateTime = initialLastUpdateTime;
	}

	@Override
	public void register(T key, ExpireTimeHolder expireTimeHolder, SelectorCallback callback, Slot... slots) {
		if (key == null || slots == null) {
			return;
		}

		ObserveContext ctx = findOrCreateObserveContext(key, initialLastUpdateTime);

		synchronized (ctx) {
			ctx.addObserver(expireTimeHolder, callback, slots);
		}
	}

	@Override
	public void update(T key, boolean refreshUpdateTimeWhenNoOb, Slot... slots) {
		if (key == null || slots == null) {
			return;
		}

		ObserveContext ctx = findOrCreateObserveContext(key, InitialLastUpdateTime.NEWEST);

		synchronized (ctx) {
			ctx.updateMaxWriteOffset(refreshUpdateTimeWhenNoOb, slots);
		}
	}

	@Override
	public void updateAll(boolean refreshUpdateTimeWhenNoOb, Slot... slots) {
		for (Entry<T, ObserveContext> entry : ctxes.entrySet()) {
			update(entry.getKey(), refreshUpdateTimeWhenNoOb, slots);
		}
	}

	private ObserveContext findOrCreateObserveContext(T key, InitialLastUpdateTime initialLastUpdateTime) {
		ObserveContext ctx = ctxes.get(key);
		if (ctx == null) {
			long lastUpdateTime = initialLastUpdateTime == InitialLastUpdateTime.NEWEST ? System.currentTimeMillis() : 0;
			ctxes.putIfAbsent(key, new ObserveContext(key, lastUpdateTime));
			return ctxes.get(key);
		} else {
			return ctx;
		}
	}

	private boolean isSlotIndexValid(Slot slot) {
		if (slot != null && slot.getIndex() < slotCount) {
			return true;
		} else {
			log.error("slot {} is null or index is incompatible with slotCount {}", slot, slotCount);
			return false;
		}
	}

	protected boolean isSlotMinFireIntervalValid(Slot slot, long lastUpdateTime) {
		return System.currentTimeMillis() - lastUpdateTime >= slot.getMinFireInterval();
	}

	private void loadMaxWriteOffsetAsync(T key) {
		offsetLoader.loadAsync(key);
	}

	protected boolean isLongTimeNoUpdate(long lastUpdateTime) {
		return System.currentTimeMillis() - lastUpdateTime > maxWriteOffsetTtlMillis;
	}

	protected long newLastUpdateTime() {
		return System.currentTimeMillis();
	}

	private class ObserveContext {
		private T key;
		private AtomicLong lastUpdateTime;
		private List<AtomicLong> maxWriteOffsets = new ArrayList<>();
		private List<Observer> obs = new LinkedList<>();

		public ObserveContext(T key, long lastUpdateTime) {
			this.key = key;
			this.lastUpdateTime = new AtomicLong(lastUpdateTime);
			for (int i = 0; i < slotCount; i++) {
				maxWriteOffsets.add(new AtomicLong(-1L));
			}
		}

		public void addObserver(ExpireTimeHolder expireTimeHolder, SelectorCallback callback, Slot[] slots) {
			log.debug("addObserver {} {}", key, slots);
			obs.add(new Observer(expireTimeHolder, callback, slots));
			fire();
		}

		public void updateMaxWriteOffset(boolean refreshUpdateTimeWhenNoOb, Slot[] slots) {
			boolean anyOffsetUpdated = false;
			for (Slot slot : slots) {
				if (isSlotIndexValid(slot) && isSlotMinFireIntervalValid(slot, lastUpdateTime.get())) {
					int index = slot.getIndex();
					maxWriteOffsets.get(index).set(Math.max(slot.getOffset(), maxWriteOffsets.get(index).get()));
					anyOffsetUpdated = true;
				} else {
					log.debug("wont fire due to invalid slot fire interval");
				}
			}

			if (anyOffsetUpdated) {
				// remove expired observers first to avoid refresh
				// lastUpdateTime when no valid observer exists
				removeExpiredObservers();

				if (obs.isEmpty()) {
					if (refreshUpdateTimeWhenNoOb) {
						lastUpdateTime.set(newLastUpdateTime());
					}
				} else {
					lastUpdateTime.set(newLastUpdateTime());
					fire();
				}
			}
		}

		private void removeExpiredObservers() {
			for (Iterator<Observer> iter = obs.iterator(); iter.hasNext();) {
				Observer ob = iter.next();
				if (ob.isExpired()) {
					iter.remove();
				}
			}
		}

		private void fire() {
			if (!obs.isEmpty()) {
				boolean longTimeNoUpdate = isLongTimeNoUpdate(lastUpdateTime.get());
				if (longTimeNoUpdate) {
					log.debug("long time no update");
					lastUpdateTime.set(newLastUpdateTime());
					loadMaxWriteOffsetAsync(key);
				}

				for (Iterator<Observer> iter = obs.iterator(); iter.hasNext();) {
					final Observer ob = iter.next();
					if (ob.isExpired()) {
						iter.remove();
					} else {
						final Pair<Boolean, SlotMatchResult[]> matchResult = ob.tryMatch(maxWriteOffsets);
						if (longTimeNoUpdate || matchResult.getKey()) {
							iter.remove();
							execCallback(ob, matchResult.getValue());
						}
					}
				}
			}
		}

		private void execCallback(final Observer ob, final SlotMatchResult[] slotMatchResults) {
			final CallbackContext ctx = new CallbackContext(slotMatchResults);
			es.submit(new Runnable() {
				public void run() {
					ob.getCallback().onReady(ctx);
				}
			});
		}

	}

	private class Observer {
		private ExpireTimeHolder expireTimeHolder;
		private List<Long> awaitingOffsets;
		private SelectorCallback callback;

		public Observer(ExpireTimeHolder expireTimeHolder, SelectorCallback callback, Slot[] slots) {
			this.expireTimeHolder = expireTimeHolder;
			awaitingOffsets = new ArrayList<>(slotCount);

			for (int i = 0; i < slotCount; i++) {
				awaitingOffsets.add(Long.MAX_VALUE);
			}

			for (Slot slot : slots) {
				if (isSlotIndexValid(slot)) {
					awaitingOffsets.set(slot.getIndex(), slot.getOffset());
				}
			}

			this.callback = callback;
		}

		public boolean isExpired() {
			return System.currentTimeMillis() > expireTimeHolder.currentExpireTime();
		}

		public Pair<Boolean, SlotMatchResult[]> tryMatch(List<AtomicLong> maxWriteOffsets) {
			boolean match = false;
			SlotMatchResult[] slotMatchResults = new SlotMatchResult[slotCount];

			for (int i = 0; i < slotCount; i++) {
				long writeOffset = maxWriteOffsets.get(i).get();
				long awaitingOffset = awaitingOffsets.get(i);
				boolean thisMatch = false;

				if (writeOffset >= awaitingOffset) {
					log.debug("match {} {}", writeOffset, awaitingOffset);
					thisMatch = true;
					match = true;
				}
				slotMatchResults[i] = new SlotMatchResult(i, thisMatch, awaitingOffset, writeOffset);
			}

			return new Pair<>(match, slotMatchResults);
		}

		public SelectorCallback getCallback() {
			return callback;
		}

	}

	@Override
	public int getSlotCount() {
		return slotCount;
	}

}
