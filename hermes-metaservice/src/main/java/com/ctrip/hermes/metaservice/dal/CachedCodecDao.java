package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Codec;
import com.ctrip.hermes.metaservice.model.CodecDao;
import com.ctrip.hermes.metaservice.model.CodecEntity;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

@Named
public class CachedCodecDao extends CodecDao implements CachedDao<String, Codec> {

	private int max_size = 5;

	private LoadingCache<String, Codec> cache = CacheBuilder.newBuilder().maximumSize(max_size).recordStats()
	      .refreshAfterWrite(10, TimeUnit.MINUTES).build(new CacheLoader<String, Codec>() {

		      @Override
		      public Codec load(String key) throws Exception {
			      return findByPK(key, CodecEntity.READSET_FULL);
		      }

	      });

	private volatile boolean isNeedReload = true;

	@Override
	public int deleteByPK(Codec proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.deleteByPK(proto);
	}

	public Codec findByPK(final String keyType) throws DalException {
		try {
			return cache.get(keyType);
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Map<String, CacheStats> getStats() {
		Map<String, CacheStats> result = new HashMap<>();
		result.put(CachedCodecDao.class.getSimpleName() + "_cache", cache.stats());
		return result;
	}

	public int insert(Codec proto) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.insert(proto);
	}

	@Override
	public void invalidateAll() {
		cache.invalidateAll();
		isNeedReload = true;
	}

	public Collection<Codec> list(boolean fromDB) throws DalException {
		if (isNeedReload || fromDB) {
			List<Codec> models = list(CodecEntity.READSET_FULL);
			for (Codec model : models) {
				cache.put(model.getKeyType(), model);
			}
			isNeedReload = false;
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Codec proto, Updateset<Codec> updateset) throws DalException {
		cache.invalidateAll();
		isNeedReload = true;
		return super.updateByPK(proto, updateset);
	}

}
