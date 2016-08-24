/**
 * 
 */
package com.ctrip.hermes.protal.util;

import java.io.File;

import org.junit.Test;
import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.portal.util.MetaDiffer;
import com.ctrip.hermes.portal.util.MetaDiffer.MetaDiff;

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

		MetaDiff result = new MetaDiffer().compare(oldMeta, newMeta);

		System.out.println(result);
		
	}

}
