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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.TooManyTermsException;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ClassUtils;

/** A abstract class representing a cluster of local indices containing separate 
 * set of documents from the same collection.
 * 
 * <p>This class stores the strategy and possibly the {@linkplain BloomFilter Bloom filters}
 * associated with this documental cluster. 
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public abstract class DocumentalCluster extends IndexCluster {
	private static final long serialVersionUID = 1L;

	public static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
	
	/** Whether this documental cluster is concatenated. */
	public final boolean concatenated;
	/** Whether this documental cluster is flat; in this case, all local indices have the same term list. */
	public final boolean flat;
	/** An Array containing the numbers from 0 to the number of local indices (excluded). Used to implement {@link IndexReader#documents(long)} more
	 * efficiently in flat indices. */
	public final int[] allIndices;

	/** The clustering strategy. */
	protected final DocumentalClusteringStrategy strategy;

	/** Creates a new documental index cluster. */
	
	public DocumentalCluster( final Index[] localIndex, final DocumentalClusteringStrategy strategy, final boolean flat, final BloomFilter<Void>[] termFilter, final int numberOfDocuments, final int numberOfTerms, 
			final long numberOfPostings, final long numberOfOccurences, final int maxCount, final Payload payload, final boolean hasCounts, final boolean hasPositions,
			final TermProcessor termProcessor, final String field, final IntBigList sizes, final Properties properties ) {
		super( localIndex, termFilter, numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurences, maxCount, payload, hasCounts, hasPositions, termProcessor, field, sizes, properties );
		this.strategy = strategy;
		this.flat = flat;
		this.concatenated = getClass().isAssignableFrom( DocumentalConcatenatedCluster.class );
		this.allIndices = new int[ localIndex.length ];
		for( int i = allIndices.length; i-- != 0; ) allIndices[ i ] = i;
	}

	@Override
	public DocumentalClusterIndexReader getReader( final int bufferSize ) throws IOException {
		return new DocumentalClusterIndexReader( this, bufferSize == -1 ? DEFAULT_BUFFER_SIZE : bufferSize );
	}

	@Override
	public IndexIterator documents( final CharSequence prefix, final int limit ) throws IOException, TooManyTermsException {
		final ArrayList<DocumentIterator> iterators = new ArrayList<DocumentIterator>( localIndex.length );
		final IntArrayList usedIndices = new IntArrayList();

		IndexIterator documentIterator;
		for ( int i = 0; i < localIndex.length; i++ ) {
			// TODO: check for limit globally
			documentIterator = localIndex[ i ].documents( prefix, limit );
			if ( documentIterator.mayHaveNext() ) {
				iterators.add( documentIterator );
				usedIndices.add( i );
			}
		}
		// TODO: test that this clustered multiterm does work
		final IndexIterator result = concatenated ?
				new DocumentalConcatenatedClusterIndexIterator( (DocumentalClusterIndexReader)getReader(), iterators.toArray( IndexIterators.EMPTY_ARRAY ), usedIndices.toIntArray() ) :
					new DocumentalMergedClusterIndexIterator( (DocumentalClusterIndexReader)getReader(), iterators.toArray( IndexIterators.EMPTY_ARRAY ), usedIndices.toIntArray() );
		result.term( prefix );
		return result;
		
	}
	
	public String toString() {
		return ClassUtils.getShortClassName( this, null ) + Arrays.toString( localIndex );
	}
}
