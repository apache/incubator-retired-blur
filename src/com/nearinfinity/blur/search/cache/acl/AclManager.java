package com.nearinfinity.blur.search.cache.acl;

import org.apache.lucene.search.Filter;

import com.nearinfinity.blur.search.cache.AbstractCachedQueryManager;

public class AclManager extends AbstractCachedQueryManager<Filter> {

	private static final String ACL = "Acl-";

	public AclManager(String name, boolean auto) {
		super(ACL + name, auto);
	}

	@Override
	public Filter create(String... names) {
		return null;
	}

}
