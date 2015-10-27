package com.ctrip.hermes.monitor.checker;

import java.util.Date;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface Checker {

	public String name();

	public CheckerResult check(Date toDate, int minutesBefore);
}
