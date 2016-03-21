package com.ctrip.hermes;

import java.io.ByteArrayOutputStream;
import java.io.File;

import org.unidal.helper.Codes;
import org.unidal.helper.Files.IO;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

public class Test {

	public static void main(String[] args) {
		String src = "~{e4d2170683b617b11200a395839594962047a0a7f420d073d5d950d0d1d4c6a8801f3}";
		System.out.println(Codes.forDecode().decode(src.substring(2, src.length() - 1)));
   }
	
	@SuppressWarnings("unused")
	public static void main2(String[] args) throws Exception {
		String json = IO.INSTANCE.readFrom(new File("/Users/marsqing/Desktop/meta.json"), "utf-8");

		Meta meta = JSON.parseObject(json, Meta.class);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();

		long start = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			 String xml = meta.toString();
//			ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(bout));
//			out.writeObject(meta);
//			out.close();
//			 String bytes = JSON.toJSONString(meta);
			// Meta meta2 = (Meta) meta.clone();
			// Meta meta2 = cloneMeta(meta);
//			Meta meta2 = new Meta();
//			BeanUtils.copyProperties(meta2, meta);
//			System.out.println(meta.toString());
//			System.out.println(meta2.toString());
			System.out.println(System.currentTimeMillis() - start);

			// Map<String, Server> s1 = meta.getServers();
			// Map<String, Endpoint> e1 = meta.getEndpoints();
			// meta2.getServers().clear();
			// meta2.getEndpoints().clear();
			// Map<String, Server> s2 = meta.getServers();
			// Map<String, Endpoint> e2 = meta.getEndpoints();
			// System.out.println(s1.equals(s2));
			// System.out.println(e1.equals(e2));
			// System.out.println(meta.toString().equals(meta2.toString()));

			start = System.currentTimeMillis();
			 DefaultSaxParser.parse(xml);
//			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
//			Meta meta2 = (Meta) in.readObject();
//			 JSON.parseObject(bytes, Meta.class);
			System.out.println(System.currentTimeMillis() - start);
//			System.out.println(meta.toString().equals(meta2.toString()));
		}
	}

	private static Meta cloneMeta(Meta m) {
		Meta cloned = new Meta();

		cloned.getApps().putAll(m.getApps());
		cloned.getCodecs().putAll(m.getCodecs());
		cloned.getEndpoints().putAll(m.getEndpoints());
		cloned.getServers().putAll(m.getServers());
		cloned.getStorages().putAll(m.getStorages());
		cloned.getTopics().putAll(m.getTopics());
		cloned.setVersion(m.getVersion());

		return cloned;
	}

}
