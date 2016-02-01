package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;

import com.google.common.cache.CacheStats;

public interface CachedDao<K, T> {
	public T findByPK(final K key) throws DalException;

	public Collection<T> list(boolean fromDB) throws DalException;

	public Map<String, CacheStats> getStats();

	public void invalidateAll();
}
