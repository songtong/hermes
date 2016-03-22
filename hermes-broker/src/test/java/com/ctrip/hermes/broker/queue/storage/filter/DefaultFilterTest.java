package com.ctrip.hermes.broker.queue.storage.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.codehaus.plexus.util.StringUtils;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.tuple.Pair;

public class DefaultFilterTest extends ComponentTestCase {
	public static final Random m_rand = new Random();

	public static String randomString(int maxLen) {
		StringBuilder sb = new StringBuilder(maxLen);
		int len = m_rand.nextInt(maxLen - 1) + 1;
		for (int i = 0; i < len; i++) {
			sb.append((char) ('a' + m_rand.nextInt(25)));
		}
		return sb.toString();
	}

	public static List<String> randomStrings(int count, int maxLen) {
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < count; i++) {
			list.add(randomString(maxLen));
		}
		return list;
	}

	public static String generateTag(String pattern) {
		StringBuilder sb = new StringBuilder();
		String[] parts = pattern.split("\\.");
		for (String part : parts) {
			if (!StringUtils.isBlank(part)) {
				if ("*".equals(part)) {
					sb.append(randomString(10) + ".");
				} else if ("#".equals(part)) {
					for (int i = 0; i < m_rand.nextInt(4) + 1; i++) {
						sb.append(randomString(10) + ".");
					}
				} else {
					sb.append(part + ".");
				}
			}
		}
		return sb.substring(0, sb.length() - 1);
	}

	private List<String> generateTags(String pattern, int count) {
		List<String> tags = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			tags.add(generateTag(pattern));
		}
		return tags;
	}

//	private Pair<String, Map<String, String>> generateSource(boolean isMatch, Map<String, List<String>> patterns) {
//		
//	}

	private String[] patterns = new String[] { //
	"abc.*.ddss.#", "*.hello.world", "hello.world.*", "diors.#", "#.diors", "a.*.b.#.c" };

	@Test
	public void testFilter() {
		int topicCount = 5000;
		int loopCount = 100000, loop = 100;
		int maxTagCount = 20000;
		List<String> topics = randomStrings(topicCount, 256);
		Filter filter = lookup(Filter.class);
		Map<String, List<String>> pattern2Tags = new HashMap<>();
		for (String pattern : patterns) {
			pattern2Tags.put(pattern, generateTags(pattern, m_rand.nextBoolean() ? //
			m_rand.nextBoolean() ? m_rand.nextInt(maxTagCount) : m_rand.nextInt(500)
			      : 1));
		}
		for (int i = 0; i < loop; i++) {
			long begin = System.currentTimeMillis();
			for (int j = 0; j < loopCount; j++) {
//				 filter.isMatch(topics.get(m_rand.nextInt(topicCount)), filter, )
			}
			System.out.println("cost: " + (System.currentTimeMillis() - begin));
		}
	}
}
