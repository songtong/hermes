package com.ctrip.hermes.portal.dal;

import java.util.Date;

import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

public interface HermesPortalDao {
	/***
	 * @return Pair<Date, Date> key: latest message date, value: latest consumed date
	 */
	public Pair<Date, Date> getDelayTime(String topic, int partition, int groupId) throws DalException;

	public Date getLatestProduced(String topic, int partition) throws DalException;

	public Date getLatestConsumed(String topic, int partition, int group) throws DalException;

}
