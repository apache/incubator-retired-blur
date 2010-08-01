package com.nearinfinity.blur.lucene.search.cache;


public class FacetManager extends AbstractCachedQueryManager<Facet> {
	
	private static final String FACET = "Facet-";

	public FacetManager(String name, boolean auto) {
		super(FACET + name, auto);
	}

	@Override
	public Facet create(String... names) {
		return new Facet(getCompressedBitSetsByName(names),names);
	}
	

}
