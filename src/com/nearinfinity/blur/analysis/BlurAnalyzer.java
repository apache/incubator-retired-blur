package com.nearinfinity.blur.analysis;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.util.Version;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class BlurAnalyzer extends PerFieldAnalyzerWrapper {

	private static final String SEP = ".";
	private static final String FIELDS = "fields";
	private static final String DEFAULT = "default";
	
	public static void main(String[] args) throws Exception {
		BlurAnalyzer analyzer = BlurAnalyzer.create("{" +
				"\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
				"\"fields\":{" +
					"\"a\":{" +
						"\"b\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"" +
					"}" +
					"\"b\":{" +
						"\"c\":[" +
							"\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
							"{\"sub1\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}," +
							"{\"sub2\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}" +
						"]" +
					"}" +
				"}" +
				"}");
		System.out.println(analyzer);
	}

	public static BlurAnalyzer create(String s) throws Exception {
		return create((JSONObject) new JSONParser().parse(s));
	}

	public BlurAnalyzer(Analyzer defaultAnalyzer, Map<String, Analyzer> fieldAnalyzers) {
		super(defaultAnalyzer, fieldAnalyzers);
	}

	public BlurAnalyzer(Analyzer defaultAnalyzer) {
		super(defaultAnalyzer);
	}
	
	public static BlurAnalyzer create(JSONObject object) throws Exception {
		Analyzer defaultAnalyzer = getAnalyzer(object.get(DEFAULT));
		BlurAnalyzer analyzer = new BlurAnalyzer(defaultAnalyzer);
		populate(analyzer,"",object.get(FIELDS));
		return analyzer;
	}

	private static void populate(BlurAnalyzer analyzer, String name, Object object) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if (object == null) {
			return;
		} 
		if (object instanceof JSONObject) {
			JSONObject jsonObject = (JSONObject) object;
			for (Object key : jsonObject.keySet()) {
				Object value = jsonObject.get(key);
				populate(analyzer, getName(name,key.toString()), value);
			}
		} else if (object instanceof JSONArray) {
			JSONArray jsonArray = (JSONArray) object;
			int size = jsonArray.size();
			for (int i = 0; i < size; i++) {
				Object o = jsonArray.get(i);
				populate(analyzer, name, o);
			}
		} else {
			Analyzer a = getAnalyzerByClassName(object.toString());
			System.out.println("Adding [" + name + "] [" + a.getClass().getName() + "]");
			analyzer.addAnalyzer(name, a);
		}
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
		return getAnalyzerByClassName(o.toString());
	}

	private static Analyzer getAnalyzerByClassName(String className) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		Class<?> clazz = Class.forName(className);
		Constructor<?> constructor = clazz.getConstructor(new Class[]{Version.class});
		return (Analyzer) constructor.newInstance(Version.LUCENE_CURRENT);
	}

}
