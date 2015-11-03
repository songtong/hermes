package com.ctrip.hermes.metaservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.model.MetaDao;
import com.ctrip.hermes.metaservice.model.MetaEntity;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaService.class)
public class DefaultMetaService implements MetaService {
	protected static final Logger m_logger = LoggerFactory.getLogger(DefaultMetaService.class);

	@Inject
	protected MetaDao m_metaDao;

	@Override
	public Meta findLatestMeta() throws DalException {
		try {
			return JSON.parseObject(m_metaDao.findLatest(MetaEntity.READSET_FULL).getValue(), Meta.class);
		} catch (DalNotFoundException e) {
			return new Meta().addStorage(new Storage(Storage.MYSQL)).addStorage(new Storage(Storage.KAFKA)).setVersion(0L);
		}
	}

}
