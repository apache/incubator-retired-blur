package com.nearinfinity.blur.analysis;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class BlurAnalyzer extends PerFieldAnalyzerWrapper {

	private static final String SEP = ".";
	private static final String FIELDS = "fields";
	private static final String DEFAULT = "default";
	
	public static void main(String[] args) throws Exception {
		String s = "{" +
		"\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
		"\"fields\":{" +
			"\"a\":{" +
				"\"b\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"" +
			"}," +
			"\"b\":{" +
				"\"c\":[" +
					"\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
					"{\"sub1\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}," +
					"{\"sub2\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}" +
				"]" +
			"}" +
		"}" +
		"}";
		System.out.println(s);
		BlurAnalyzer analyzer = BlurAnalyzer.create(s);
		System.out.println(analyzer);
	}

	public static BlurAnalyzer create(String s) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = mapper.readTree(s);
		return create(jsonNode);
	}

	public BlurAnalyzer(Analyzer defaultAnalyzer, Map<String, Analyzer> fieldAnalyzers) {
		super(defaultAnalyzer, fieldAnalyzers);
	}

	public BlurAnalyzer(Analyzer defaultAnalyzer) {
		super(defaultAnalyzer);
	}
	
	public static BlurAnalyzer create(JsonNode jsonNode) throws Exception {
		Analyzer defaultAnalyzer = getAnalyzer(jsonNode.get(DEFAULT));
		BlurAnalyzer analyzer = new BlurAnalyzer(defaultAnalyzer);
		populate(analyzer, "", jsonNode.get(FIELDS));
		return analyzer;
	}

	private static void populate(BlurAnalyzer analyzer, String name, JsonNode jsonNode) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if (jsonNode == null) {
			return;
		}
		if (jsonNode.isObject()) {
			for (String key : it(jsonNode.getFieldNames())) {
				populate(analyzer, getName(name,key.toString()), jsonNode.get(key));
			}
		} else if (jsonNode.isArray()) {
			int size = jsonNode.size();
			for (int i = 0; i < size; i++) {
				populate(analyzer, name, jsonNode.get(i));
			}
		} else {
			Analyzer a = getAnalyzerByClassName(jsonNode.getValueAsText());
			System.out.println("Adding [" + name + "] [" + a.getClass().getName() + "]");
			analyzer.addAnalyzer(name, a);
		}
	}

	private static <T> Iterable<T> it(final Iterator<T> iterator) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return iterator;
			}
		};
	}

	private static String getName(String baseName, String newName) {
		if (baseName.isEmpty()) {
			return newName;
		}
		return baseName + SEP + newName;
	}

	private static Analyzer getAnalyzer(Object o) throws Exception {
		if (o == null) {
			throw new NullPointerException();
		}
		String cn;
		if (o instanceof JsonNode) {
			JsonNode jsonNode = (JsonNode) o;
			cn = jsonNode.getTextValue();
		} else {
			cn = o.toString();
		}
		return getAnalyzerByClassName(cn);
	}

	private static Analyzer getAnalyzerByClassName(String className) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		Class<?> clazz = Class.forName(className);
		try {
			return (Analyzer) clazz.newInstance();
		} catch (Exception e) {
			Constructor<?> constructor = clazz.getConstructor(new Class[]{Version.class});
			return (Analyzer) constructor.newInstance(Version.LUCENE_CURRENT);
		}
	}

}
