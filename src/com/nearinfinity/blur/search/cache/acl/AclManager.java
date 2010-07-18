package com.nearinfinity.blur.search.cache.acl;

import com.nearinfinity.blur.search.cache.AbstractCachedQueryManager;

public class AclManager extends AbstractCachedQueryManager<Acl> {

	private static final String ACL = "Acl-";

	public AclManager(String name, boolean auto) {
		super(ACL + name, auto);
	}

	@Override
	public Acl create(String... names) {
		return new Acl(getCompressedBitSetsByName(names),names);
	}

}
