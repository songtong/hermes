package com.ctrip.hermes.metaservice.dal;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Updateset;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Codec;
import com.ctrip.hermes.metaservice.model.CodecDao;
import com.ctrip.hermes.metaservice.model.CodecEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedCodecDao extends CodecDao implements CachedDao<String, Codec> {

	private Cache<String, Codec> cache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).maximumSize(5)
	      .build();

	@Override
	public int deleteByPK(Codec proto) throws DalException {
		cache.invalidateAll();
		return super.deleteByPK(proto);
	}

	public Codec findByPK(final String keyType) throws DalException {
		try {
			return cache.get(keyType, new Callable<Codec>() {

				@Override
				public Codec call() throws Exception {
					return findByPK(keyType, CodecEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public int insert(Codec proto) throws DalException {
		cache.invalidateAll();
		return super.insert(proto);
	}

	public Collection<Codec> list() throws DalException {
		if (cache.size() == 0) {
			List<Codec> models = list(CodecEntity.READSET_FULL);
			for (Codec model : models) {
				cache.put(model.getKeyType(), model);
			}
		}
		return cache.asMap().values();
	}

	@Override
	public int updateByPK(Codec proto, Updateset<Codec> updateset) throws DalException {
		cache.invalidateAll();
		return super.updateByPK(proto, updateset);
	}

}
