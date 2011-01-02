package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.manager.hits.HitsComparator;
import com.nearinfinity.blur.manager.hits.HitsPeekableIteratorComparator;
import com.nearinfinity.blur.manager.hits.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Hit;


public interface BlurConstants {
	
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final Comparator<? super ColumnFamily> COLUMN_FAMILY_COMPARATOR = new ColumnFamilyComparator();
	public static final Comparator<? super Column> COLUMN_COMPARATOR = new ColumnComparator();
    public static final Comparator<? super PeekableIterator<Hit>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new HitsPeekableIteratorComparator();
    public static final Comparator<? super Hit> HITS_COMPARATOR = new HitsComparator();

    public static final String PRIME_DOC = "_prime_";
    public static final String PRIME_DOC_VALUE = "true";
    public static final String ID = "_id_";
    public static final String SUPER_KEY = "_superkey_";
    public static final String SEP = ".";
}
