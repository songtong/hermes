package com.ctrip.hermes.collector.hub;

import javax.annotation.PostConstruct;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.TopicFlowState;
import com.ctrip.hermes.collector.utils.CacheSerializer;

@Component
public class CacheHub {
	public static final String CACHE_NAME = "state_cache";
	 
	@PostConstruct
	protected void init() {
//		ResourcePools pools = ResourcePoolsBuilder.newResourcePoolsBuilder().disk(1, MemoryUnit.MB).build();
//		CacheConfiguration<String, State> conf = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, State.class, pools)
//				.withValueSerializer(CacheSerializer.class)
//				.build();
//		CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().withCache(CACHE_NAME, conf)
//				.with(CacheManagerBuilder.persistence("/Users/tenglinxiao/Documents/hermes/hermes-collector/store"))
//				.build(true);
//
//		
//		Cache<String, State> cache = cacheManager.getCache(CACHE_NAME, String.class, State.class);
//		cache.put("dd", new TopicFlowState());
//		System.out.println(cache.get("dd"));	
		
	}
	
	public static void main(String args[]) {
		new CacheHub().init();
	}
}
