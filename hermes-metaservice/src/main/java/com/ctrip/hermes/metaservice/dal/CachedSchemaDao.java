package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Schema;
import com.ctrip.hermes.metaservice.model.SchemaDao;
import com.ctrip.hermes.metaservice.model.SchemaEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;

@Named
public class CachedSchemaDao extends SchemaDao implements CachedDao<Long, Schema> {

	private Cache<Long, Schema> cache = CacheBuilder.newBuilder().maximumSize(500).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<Long, Schema>() {

		      @Override
		      public Schema load(Long key) throws Exception {
			      return findByPK(key, SchemaEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Schema proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Schema findByPK(final Long keyId) throws DalException {
		try {
			return cache.get(keyId, new Callable<Schema>() {

				@Override
				public Schema call() throws Exception {
					return findByPK(keyId, SchemaEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Schema proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	public Collection<Schema> list() throws DalException {
		if (isNeedReload) {
			List<Schema> models = list(SchemaEntity.READSET_FULL);
			for (Schema model : models) {
				cache.put(model.getKeyId(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Schema proto, Updateset<Schema> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedSchemaDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

}
