package it.unimi.di.big.mg4j.index.cluster;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.util.BloomFilter;

/** A lexical clustering strategy that uses a chain of responsability to choose the local index:
 * {@linkplain StringMap term maps} out of a given list are inquired
 * until one contains the given term.
 * 
 * <p>If the index cluster has Bloom filters, they will be used to reduce useless accesses to
 * term maps.
 * 
 * <p>The intended usage of this class is memory/disk lexical partitioning. Note that a serialised version
 * of this class is <em>empty</em>. It acts just like a placeholder, so that loaders now that they
 * must generate a new instance depending on the indices contained in the cluster.
 * 
 * @author Sebastiano Vigna
 */


public class ChainedLexicalClusteringStrategy implements LexicalClusteringStrategy {
	static final long serialVersionUID = 0;
	/** The array of indices to inquiry. */
	private transient final StringMap<? extends CharSequence>[] termMap;
	/** An array of optional Bloom filters to reduce term map access, or <code>null</code>. */
	private transient final BloomFilter<Void>[] termFilter;
	
	/** Creates a new chained lexical clustering strategy using additional Bloom filters.
	 * 
	 * <p>Note that the static type of the parameter <code>index</code> is 
	 * an array of {@link Index}, but the elements of the array must be
	 * {@linkplain DiskBasedIndex disk-based indices}, or an exception will be thrown.
	 * 
	 * @param index an array of disk-based indices, from which term maps will be extracted.
	 * @param termFilter an array, parallel to <code>index</code>, of Bloom filter representing the terms contained in each local index. 
	 */
	public ChainedLexicalClusteringStrategy( final Index[] index, final BloomFilter<Void>[] termFilter ) {
		this.termMap = new StringMap<?>[ index.length ];
		for( int i = index.length; i-- != 0; ) 
			if ( ( termMap[ i ] = index[ i ].termMap ) == null )
				throw new IllegalArgumentException( "Index " + index[ i ] + " has no term map" );
		this.termFilter = termFilter;
	}

	/** Creates a new chained lexical clustering strategy.
	 * 
	 * <p>Note that the static type of the parameter <code>index</code> is 
	 * an array of {@link Index}, but the elements of the array must be
	 * {@linkplain DiskBasedIndex disk-based indices}, or an exception will be thrown.
	 * 
	 * @param index an array of disk-based indices, from which term maps will be extracted.
	 */
	public ChainedLexicalClusteringStrategy( final Index[] index ) {
		this( index, null );
	}

	public int numberOfLocalIndices() {
		return termMap.length;
	}

	public int localIndex( final CharSequence term ) {
		for( int i = 0; i < termMap.length; i++ ) 
			if ( ( termFilter == null || termFilter[ i ].contains( term ) ) && termMap[ i ].getLong( term ) != -1 ) return i;
		return -1;
	}

	public long globalNumber( int localIndex, long localNumber ) {
		throw new UnsupportedOperationException();
	}
}
