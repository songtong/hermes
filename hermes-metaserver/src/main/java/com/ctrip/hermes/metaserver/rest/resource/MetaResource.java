package com.ctrip.hermes.metaserver.rest.resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaserver.meta.MetaHolder;
import com.ctrip.hermes.metaserver.rest.commons.RestException;

@Path("/meta/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class MetaResource {

	private static final Logger logger = LoggerFactory.getLogger(MetaResource.class);

	private MetaHolder m_metaHolder = PlexusComponentLocator.lookup(MetaHolder.class);

	private static SerializeConfig jsonConfig = new SerializeConfig();

	@GET
	@Path("complete")
	public Response getCompleteMeta(@QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) {
		logger.debug("get meta, version {}", version);
		Meta meta = null;
		try {
			meta = m_metaHolder.getMeta();
			if (meta == null) {
				throw new RestException("Meta not found", Status.NOT_FOUND);
			}
			if (!isMetaModified(version, hashCode, meta)) {
				return Response.status(Status.NOT_MODIFIED).build();
			}
		} catch (Exception e) {
			logger.warn("get meta failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(meta).build();
	}

	@GET
	public Response getMeta(@QueryParam("version") @DefaultValue("0") long version,
	      @QueryParam("hashCode") @DefaultValue("0") long hashCode) {
		Meta meta = m_metaHolder.getMeta();
		if (meta == null) {
			throw new RestException("Meta not found", Status.NOT_FOUND);
		}
		if (!isMetaModified(version, hashCode, meta)) {
			return Response.status(Status.NOT_MODIFIED).build();
		}

		Storage storage = meta.findStorage(Storage.MYSQL);
		final List<Datasource> dss = storage.getDatasources();

		// pass empty jsonConfig to make JSON.toJSONString use this overloaded function
		// to skip serializer.config(SerializerFeature.WriteDateUseDateFormat, true);
		// to make Date field serialize to timestamp instead of date string
		String json = JSON.toJSONString(meta, jsonConfig, new ValueFilter() {
			@Override
			@SuppressWarnings("unchecked")
			public Object process(Object object, String name, Object value) {
				if (object instanceof Datasource && value instanceof Map) {
					Datasource ds = (Datasource) object;
					for (Datasource d : dss) {
						if (d.getId().equals(ds.getId())) {
							Map<String, Property> nps = new HashMap<String, Property>();
							for (Entry<String, Property> entry : ((Map<String, Property>) value).entrySet()) {
								nps.put(entry.getKey(), new Property(entry.getKey()).setValue("**********"));
							}
							return nps;
						}
					}
				}
				return value;
			}
		});

		return Response.status(Status.OK).entity(json).build();
	}

	private boolean isMetaModified(long version, long hashCode, Meta meta) {
		return !(version > 0 && meta.getVersion().equals(version)) || (hashCode > 0 && meta.hashCode() == hashCode);
	}
}
