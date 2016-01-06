package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;

import org.unidal.dal.jdbc.DalException;

public interface CachedDao<K, T> {
	public T findByPK(final K key) throws DalException;

	public Collection<T> list() throws DalException;
}
