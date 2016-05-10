package com.ctrip.hermes.collector.state;

import java.lang.reflect.Method;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

public class ProxiedState implements MethodInterceptor{
	private ReentrantReadWriteLock m_readWriteLock = new ReentrantReadWriteLock(); 
	private State m_state;
	private Lock m_lock;
	
	private ProxiedState(State state) {
		this.m_state = state;
	}
	
	@Override
	public Object intercept(Object obj, Method method, Object[] args,
			MethodProxy methodProxy) throws Throwable {
		try {
			if (method.getName().startsWith("get")) {
				m_lock = m_readWriteLock.readLock();
			} else {
				m_lock = m_readWriteLock.writeLock();
			}
			m_lock.lock();
			return methodProxy.invoke(m_state, args);
		} finally {
			m_lock.unlock();
		}
	}
	
	public static State getProxiedState(State state) { 
		ProxiedState proxiedState = new ProxiedState(state);
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(state.getClass());
		enhancer.setCallback(proxiedState);
		return (State)enhancer.create();
	} 
}
