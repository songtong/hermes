package com.ctrip.hermes.metaservice.service;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.Meta;

public interface PortalMetaService extends MetaService {

	public Meta buildNewMeta() throws DalException;

	public Meta previewNewMeta() throws DalException;

}
