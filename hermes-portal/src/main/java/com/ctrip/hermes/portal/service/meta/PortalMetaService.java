package com.ctrip.hermes.portal.service.meta;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.service.MetaService;
import com.ctrip.hermes.portal.util.MetaDiffer.MetaDiff;

public interface PortalMetaService extends MetaService {

	public Meta buildNewMeta() throws DalException;

	public Meta previewNewMeta() throws DalException;

	public MetaDiff getMetaDiff() throws DalException, Exception;

}
