package com.nearinfinity.blur.search.cache.acl;

import org.apache.lucene.search.Filter;

import com.nearinfinity.blur.search.AbstractCachedQueryManager;

public class AclFilterManager extends AbstractCachedQueryManager<Filter> {

	private static final String ACL = "Acl-";

	public AclFilterManager(String name, boolean auto) {
		super(ACL + name, auto);
	}

	@Override
	public Filter create(String... names) {
		return null;
	}

}
