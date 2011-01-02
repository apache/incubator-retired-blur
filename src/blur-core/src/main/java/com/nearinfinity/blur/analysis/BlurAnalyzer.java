package com.nearinfinity.blur.analysis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class BlurAnalyzer extends PerFieldAnalyzerWrapper {

	private static final String SEP = ".";
	private static final String FIELDS = "fields";
	private static final String DEFAULT = "default";
	
    public static BlurAnalyzer create(Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        return create(fileSystem.open(path));
    }
	
    public static BlurAnalyzer create(File file) throws IOException {
        return create(new FileInputStream(file));
    }
	
	public static BlurAnalyzer create(InputStream input) throws IOException {
	    return create(toString(input));
	}

	private static String toString(InputStream input) {
	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	    byte[] buffer = new byte[1024];
	    int numb = 0;
	    try {
            while ((numb = input.read(buffer)) != -1) {
                outputStream.write(buffer, 0, numb);
            }
            outputStream.close();
            input.close();
            return new String(outputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static BlurAnalyzer create(String s) throws IOException {
		if (s == null || s.trim().isEmpty()) {
			return new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30),"");
		}
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = mapper.readTree(s);
		return create(jsonNode,s);
	}

    private String originalJsonStr;

	private BlurAnalyzer(Analyzer defaultAnalyzer, String jsonStr) {
		super(defaultAnalyzer);
		this.originalJsonStr = jsonStr;
	}
	
	private static BlurAnalyzer create(JsonNode jsonNode, String jsonStr) {
	    try {
    		Analyzer defaultAnalyzer = getAnalyzer(jsonNode.get(DEFAULT));
    		BlurAnalyzer analyzer = new BlurAnalyzer(defaultAnalyzer, jsonStr);
    		populate(analyzer, "", jsonNode.get(FIELDS));
    		return analyzer;
	    } catch (Exception e) {
	        throw new RuntimeException(e);
        }
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

	private static Analyzer getAnalyzer(Object o) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
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
			return (Analyzer) constructor.newInstance(Version.LUCENE_30);
		}
	}

    @Override
    public String toString() {
        return originalJsonStr;
    }

}
