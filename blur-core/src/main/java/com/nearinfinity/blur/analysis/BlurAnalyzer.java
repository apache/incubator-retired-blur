package com.nearinfinity.blur.analysis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.nearinfinity.blur.utils.BlurConstants;

public class BlurAnalyzer extends PerFieldAnalyzerWrapper implements BlurConstants {

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
    private Map<String,Store> storeMap = new HashMap<String, Store>();
    private Map<String,Set<String>> subIndexNameLookups = new HashMap<String, Set<String>>();
    
    public void addSubField(String name) {
        int lastIndexOf = name.lastIndexOf('.');
        String mainFieldName = name.substring(0,lastIndexOf);
        Set<String> set = subIndexNameLookups.get(mainFieldName);
        if (set == null) {
            set = new TreeSet<String>();
            subIndexNameLookups.put(mainFieldName, set);
        }
        set.add(name);
    }
    
    public Set<String> getSubIndexNames(String indexName) {
        return subIndexNameLookups.get(indexName);
    }

	public BlurAnalyzer(Analyzer defaultAnalyzer, String jsonStr) {
		super(defaultAnalyzer);
		this.originalJsonStr = jsonStr;
		addAnalyzer(ID, new WhitespaceAnalyzer());
		addAnalyzer(SUPER_KEY, new WhitespaceAnalyzer());
		addAnalyzer(PRIME_DOC, new WhitespaceAnalyzer());
	}
	
	private static BlurAnalyzer create(JsonNode jsonNode, String jsonStr) {
	    try {
	        Map<String,Class<? extends Analyzer>> aliases = getAliases(jsonNode.get("aliases"));
    		Analyzer defaultAnalyzer = getAnalyzer(jsonNode.get(DEFAULT),aliases);
    		BlurAnalyzer analyzer = new BlurAnalyzer(defaultAnalyzer, jsonStr);
    		populate(analyzer, "", jsonNode.get(FIELDS),Store.YES,aliases);
    		return analyzer;
	    } catch (Exception e) {
	        throw new RuntimeException(e);
        }
	}

	private static Map<String, Class<? extends Analyzer>> getAliases(JsonNode jsonNode) throws ClassNotFoundException {
	    Map<String, Class<? extends Analyzer>> map = new HashMap<String, Class<? extends Analyzer>>();
	    if (jsonNode == null) {
            return map;
	    }
	    if (jsonNode.isArray()) {
	        Iterator<JsonNode> elements = jsonNode.getElements();
	        while (elements.hasNext()) {
	            JsonNode node = elements.next();
	            if (node.isObject()) {
	                addAlias(map,node);
	            }
	        }
	        return map;
	    }
	    throw new IllegalArgumentException("aliases has to be an array.");
    }

    @SuppressWarnings("unchecked")
    private static void addAlias(Map<String, Class<? extends Analyzer>> map, JsonNode jsonNode) throws ClassNotFoundException {
        Iterator<String> fieldNames = jsonNode.getFieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            JsonNode value = jsonNode.get(name);
            String className = value.getValueAsText();
            Class<? extends Analyzer> clazz = (Class<? extends Analyzer>) Class.forName(className);
            map.put(name, clazz);
        }
    }

    private static void populate(BlurAnalyzer analyzer, String name, JsonNode jsonNode, Store store, Map<String, Class<? extends Analyzer>> aliases) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if (jsonNode == null) {
			return;
		}
		if (jsonNode.isObject()) {
			for (String key : it(jsonNode.getFieldNames())) {
				populate(analyzer, getName(name,key.toString()), jsonNode.get(key), store, aliases);
			}
		} else if (jsonNode.isArray()) {
			int size = jsonNode.size();
			for (int i = 0; i < size; i++) {
				JsonNode node = jsonNode.get(i);
				if (node.isObject()) {
				    populate(analyzer, name, node, Store.NO, aliases);
				} else {
				    populate(analyzer, name, node, Store.YES, aliases);
				}
			}
		} else {
			Analyzer a = getAnalyzerByClassName(jsonNode.getValueAsText(), aliases);
			analyzer.addAnalyzer(name, a);
			analyzer.putStore(name,store);
			if (store == Store.NO) {
			    analyzer.addSubField(name);
			}
		}
	}

	public void putStore(String name, Store store) {
	    storeMap.put(name, store);
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

	private static Analyzer getAnalyzer(Object o, Map<String, Class<? extends Analyzer>> aliases) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
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
		return getAnalyzerByClassName(cn, aliases);
	}

	@SuppressWarnings("unchecked")
    private static Analyzer getAnalyzerByClassName(String className, Map<String, Class<? extends Analyzer>> aliases) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
	    Class<? extends Analyzer> clazz = aliases.get(className);
	    if (clazz == null) {
	        clazz = (Class<? extends Analyzer>) Class.forName(className);
	    }
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

    public Store getStore(String indexName) {
        Store store = storeMap.get(indexName);
        if (store == null) {
            return Store.YES;
        }
        return store;
    }
    
    public Index getIndex(String indexName) {
        return Index.ANALYZED_NO_NORMS;
    }

}
