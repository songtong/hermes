package com.ctrip.hermes.metaservice.dal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.model.Producer;
import com.ctrip.hermes.metaservice.model.ProducerDao;
import com.ctrip.hermes.metaservice.model.ProducerEntity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Named
public class CachedProducerDao extends ProducerDao implements CachedDao<Long, Producer> {

	private Cache<Long, List<Producer>> topicCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
	      .maximumSize(5).build();

	public List<Producer> findByTopic(final Long keyType) throws DalException {
		try {
			return topicCache.get(keyType, new Callable<List<Producer>>() {

				@Override
				public List<Producer> call() throws Exception {
					return findByTopicId(keyType, ProducerEntity.READSET_FULL);
				}

			});
		} catch (ExecutionException e) {
			throw new DalException(null, e.getCause());
		}
	}

	public Collection<Producer> list() throws DalException {
		return new ArrayList<Producer>();
	}

	@Override
	public Producer findByPK(Long key) throws DalException {
		return new Producer();
	}

}
