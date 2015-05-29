package com.ctrip.hermes.metaserver.meta;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.utils.HermesThreadFactory;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.metaservice.service.MetaService;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = MetaHolder.class)
public class MetaHolder implements Initializable {

	private static final Logger log = LoggerFactory.getLogger(MetaHolder.class);

	@Inject
	private MetaService m_metaService;

	private AtomicReference<Meta> m_metaCache = new AtomicReference<>();

	public Meta getMeta() {
		return m_metaCache.get();
	}

	@Override
	public void initialize() throws InitializationException {
		refreshMeta();
		Executors.newSingleThreadScheduledExecutor(HermesThreadFactory.create("RefreshMeta", true))
		      .scheduleWithFixedDelay(new Runnable() {

			      @Override
			      public void run() {
				      try {
					      refreshMeta();
				      } catch (RuntimeException e) {
					      log.warn("refresh meta failed", e);
				      }
			      }

			      // TODO config
		      }, 1, 1, TimeUnit.SECONDS);
	}

	private synchronized void refreshMeta() {
		try {
			Meta meta = m_metaService.findLatestMeta();
			if (meta != null) {
				m_metaCache.set(meta);
			}
		} catch (DalException e) {
			throw new RuntimeException(e);
		}
	}
}
