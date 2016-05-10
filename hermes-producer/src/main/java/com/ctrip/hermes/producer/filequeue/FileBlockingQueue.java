package com.ctrip.hermes.producer.filequeue;

import java.io.File;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.message.Persistable;
import com.ctrip.hermes.core.message.Persistable.PersistableType;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.producer.config.ProducerConfig;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

public class FileBlockingQueue extends AbstractQueue<Persistable> implements BlockingQueue<Persistable>, java.io.Serializable {
	private static Logger log = LoggerFactory.getLogger(FileBlockingQueue.class);

	private IBigQueue m_queue;
	
	private ReentrantLock m_lock = new ReentrantLock();
	
	private Condition m_notFull = m_lock.newCondition();
	
	private Condition m_notEmpty = m_lock.newCondition();
	
	private AtomicLong m_takeCount = new AtomicLong(0);
	
	private long m_gcIntervalByCount;
				
	public FileBlockingQueue(String directory, String queueName) throws IOException {			
		File queueDir = new File(directory);
		if (!queueDir.exists()) {
			queueDir.mkdir();
		}
		m_queue = new BigQueueImpl(directory, queueName);
		m_gcIntervalByCount = PlexusComponentLocator.lookup(ProducerConfig.class).getFileQueueGcIntervalByCount();
		m_queue.gc();
	}
	
	private void checkNotNull(Persistable p) {
		if (p == null) {
			throw new NullPointerException("Producer worker context can NOT be null.");
		}
	}
	
	private void runGcIfNecessary() {
		if (m_takeCount.incrementAndGet() % m_gcIntervalByCount == 0) {
			try {
				m_queue.gc();
			} catch (Exception e) {
				log.error("File queue GC failed.", e);
			}
		}
	}
	
	@Override
	public Persistable poll() {
		try {
			m_lock.lock();
			Persistable p = PersistableType.createPersistable(m_queue.dequeue());
			m_notFull.notifyAll();
			return p;
		} catch (IOException e) {
			log.error("Failed to poll data from file queue.", e);
			return null;
		} finally {
			runGcIfNecessary();
			m_lock.unlock();
		}
	}

	@Override
	public Persistable peek() {
		try {
			m_lock.lock();
			return PersistableType.createPersistable(m_queue.peek());
		} catch(IOException e) {
			log.error("Failed to peek data from file queue.", e);
			return null;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public boolean offer(Persistable p) {
		checkNotNull(p);
		try {
			m_lock.lock();
			m_queue.enqueue(p.toBytes());
			m_notEmpty.notifyAll();
			return true;
		} catch (IOException e) {
			log.error("Failed to offer data into file queue.", e);
			return false;
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public void put(Persistable p)
			throws InterruptedException {
		checkNotNull(p);
		try {
			m_lock.lock();
			while (true) {
				try {
					m_queue.enqueue(p.toBytes());
					m_notEmpty.notifyAll();
					break;
				} catch (IOException e) {
					try {
						m_queue.gc();
						m_queue.enqueue(p.toBytes());
						m_notEmpty.notifyAll();
						break;
					} catch (IOException ex) {
						log.error("Failed to put msg on file queue after gc.", ex);
						m_notFull.await();
					}
				}
			}
		} finally {
			m_lock.unlock();
		}
	}

	@Override
	public boolean offer(Persistable p, long timeout,
			TimeUnit unit) throws InterruptedException {
		checkNotNull(p);
        long nanos = unit.toNanos(timeout);
		m_lock.lockInterruptibly();
        try {
        	while (true) {
	        	try {
					m_queue.enqueue(p.toBytes());
					m_notEmpty.notifyAll();
					return true;
				} catch (IOException e) {
					log.error("Failed to offer data to file queue.", e);
					if (nanos <= 0) {
						return false;
					}
					nanos = m_notFull.awaitNanos(nanos);
				}
        	}
        } finally {
            m_lock.unlock();
        }
	}

	@Override
	public Persistable take() throws InterruptedException {
		m_lock.lockInterruptibly();
        try {
            while (m_queue.size() == 0) {
                m_notEmpty.await();
            }
            Persistable p = PersistableType.createPersistable(m_queue.dequeue());
            m_notFull.notifyAll();
            return p;
        } catch (IOException e) {
        	log.error("Failed to take data from file queue.", e);
        	return null;
        } finally {
        	runGcIfNecessary();
            m_lock.unlock();
        }
	}

	@Override
	public Persistable poll(long timeout, TimeUnit unit)
			throws InterruptedException {
		long nanos = unit.toNanos(timeout);
        m_lock.lockInterruptibly();
        try {
            while (m_queue.size() == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = m_notEmpty.awaitNanos(nanos);
            }
            Persistable p = PersistableType.createPersistable(m_queue.dequeue());
            m_notFull.notifyAll();
            return p;
        } catch (IOException e) {
        	log.error("Failed to poll data from file queue.", e);
        	return null;
        } finally {
        	runGcIfNecessary();
            m_lock.unlock();
        }
	}

	@Override
	public int remainingCapacity() {
		throw new UnsupportedOperationException("Not known to file queue remaining capacity, which dependes on available disk space.");
	}

	@Override
	public int drainTo(Collection<? super Persistable> c) {
		throw new UnsupportedOperationException("Not supported to drain out all the data in file queue.");
	}

	@Override
	public int drainTo(Collection<? super Persistable> c,
			int maxElements) {
		if (c == null) {
			throw new NullPointerException("Collection container for queue data is not allowed be null.");
		}
		
        if (c == this) {
            throw new IllegalArgumentException();
        }
        
        int count = maxElements;
        
        m_lock.lock();
        try {
            while (count > 0) {
            	Persistable p = PersistableType.createPersistable(m_queue.dequeue());
            	if (p == null) {
            		break;
            	}
            	c.add(p);
            	count--;
            	runGcIfNecessary();
            }
            return maxElements - count;
        } catch (IOException e) {
        	log.error("Failed to finish draining data to collection container.", e);
        	return maxElements - count;
        } finally {
        	m_lock.unlock();
        }
	}

	@Override
	public Iterator<Persistable> iterator() {
		throw new UnsupportedOperationException("Iterator is not supported for file blocking queue.");
	}

	@Override
	public int size() {
		m_lock.lock();
		try {
			if (m_queue.size() >= Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			}
			return (int)m_queue.size();
		} finally {
			m_lock.unlock();
		}
	}
	
	public long actualSize() {
		m_lock.lock();
		try {
			return m_queue.size();
		} finally {
			m_lock.unlock();
		}
	}
}
