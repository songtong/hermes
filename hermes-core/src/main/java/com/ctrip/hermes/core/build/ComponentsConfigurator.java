package com.ctrip.hermes.core.build;

import java.util.ArrayList;
import java.util.List;

import org.unidal.lookup.configuration.AbstractResourceConfigurator;
import org.unidal.lookup.configuration.Component;

import com.ctrip.hermes.HermesCoreModule;
import com.ctrip.hermes.core.config.CoreConfig;
import com.ctrip.hermes.core.env.DefaultClientEnvironment;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.message.partition.HashPartitioningStrategy;
import com.ctrip.hermes.core.message.payload.CMessagingPayloadCodec;
import com.ctrip.hermes.core.message.payload.JsonPayloadCodec;
import com.ctrip.hermes.core.meta.internal.DefaultMetaManager;
import com.ctrip.hermes.core.meta.internal.DefaultMetaService;
import com.ctrip.hermes.core.meta.internal.LocalMetaLoader;
import com.ctrip.hermes.core.meta.internal.LocalMetaProxy;
import com.ctrip.hermes.core.meta.internal.RemoteMetaLoader;
import com.ctrip.hermes.core.meta.remote.DefaultMetaServerLocator;
import com.ctrip.hermes.core.meta.remote.RemoteMetaProxy;
import com.ctrip.hermes.core.service.DefaultSystemClockService;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorManager;
import com.ctrip.hermes.core.transport.command.processor.DefaultCommandProcessorRegistry;
import com.ctrip.hermes.core.transport.endpoint.DefaultClientEndpointChannelManager;
import com.ctrip.hermes.core.transport.endpoint.DefaultEndpointManager;

public class ComponentsConfigurator extends AbstractResourceConfigurator {

	@Override
	public List<Component> defineComponents() {
		List<Component> all = new ArrayList<Component>();

		all.add(A(HermesCoreModule.class));

		// partition algo
		all.add(A(HashPartitioningStrategy.class));

		// meta
		all.add(A(LocalMetaLoader.class));
		all.add(A(RemoteMetaLoader.class));
		all.add(A(DefaultMetaManager.class));
		all.add(A(DefaultMetaService.class));
		all.add(A(LocalMetaProxy.class));
		all.add(A(RemoteMetaProxy.class));
		all.add(A(DefaultMetaServerLocator.class));

		// endpoint manager
		all.add(A(DefaultEndpointManager.class));

		// endpoint channel
		all.add(A(DefaultClientEndpointChannelManager.class));

		// command processor
		all.add(A(CommandProcessorManager.class));
		all.add(A(DefaultCommandProcessorRegistry.class));

		// codec
		all.add(A(DefaultMessageCodec.class));
		all.add(A(JsonPayloadCodec.class));
		all.add(A(CMessagingPayloadCodec.class));

		// env
		all.add(A(DefaultClientEnvironment.class));

		all.add(A(CoreConfig.class));
		all.add(A(DefaultSystemClockService.class));

		return all;
	}

	public static void main(String[] args) {
		generatePlexusComponentsXmlFile(new ComponentsConfigurator());
	}
}
