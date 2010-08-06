package com.nearinfinity.blur.lucene.search.cache;


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
