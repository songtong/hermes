package com.ctrip.hermes.metaservice.service;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MetaService {

	Meta getMetaEntity();

	Meta refreshMeta() throws DalException;

}
