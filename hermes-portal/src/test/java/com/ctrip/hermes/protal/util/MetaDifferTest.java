/**
 * 
 */
package com.ctrip.hermes.protal.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.unidal.helper.Files.IO;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;

/**
 * @author marsqing
 *
 *         Aug 10, 2016 4:27:34 PM
 */
public class MetaDifferTest {

	@Test
	public void test() throws Exception {
		File resourceDir = new File("src/test/resources/com/ctrip/hermes/protal/util");

		/**
		 * classpath resource need to be refreshed whenever modified, so read
		 * from file
		 */
		Meta oldMeta = JSON.parseObject(IO.INSTANCE.readFrom(new File(resourceDir, "old.json")), Meta.class);
		Meta newMeta = JSON.parseObject(IO.INSTANCE.readFrom(new File(resourceDir, "new.json")), Meta.class);

		Pair<Set<String>, Set<String>> result = new MetaDiffer().compare(oldMeta, newMeta);

		Set<String> nonTopicProperties = result.getKey();
		Set<String> topics = result.getValue();

		assertEquals(2, nonTopicProperties.size());
		assertTrue(nonTopicProperties.contains("storages"));
		assertTrue(nonTopicProperties.contains("codecs"));

		assertEquals(3, topics.size());
		assertTrue(topics.contains("added.topic"));
		assertTrue(topics.contains("modified.topic"));
		assertTrue(topics.contains("deleted.topic"));

	}

}
