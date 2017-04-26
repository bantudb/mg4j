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
import it.unimi.di.big.mg4j.index.MultiTermIndexIterator;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.TooManyTermsException;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.util.ArrayList;

/** A cluster exhibiting local indices referring to the same collection, but 
 * containing different set of terms, as a single index. 
 *  
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */
public class LexicalCluster extends IndexCluster {

	private static final long serialVersionUID = 1L;
	
	/** The strategy to be used.*/
	protected final LexicalClusteringStrategy strategy;
	/** The strategy, cast to a partition strategy, or <code>null</code>. */
	protected final LexicalPartitioningStrategy partitioningStrategy;
	
	/** Creates a new lexical index cluster. */
	
	public LexicalCluster( final Index[] localIndex, final LexicalClusteringStrategy strategy, final BloomFilter<Void>[] termFilter, final int numberOfDocuments, final int numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final boolean hasCounts, final boolean hasPositions,
			final TermProcessor termProcessor, final String field, final IntBigList sizes, final Properties properties ) {
		super( localIndex, termFilter, numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, hasCounts, hasPositions, termProcessor, field, sizes, properties );
		this.strategy = strategy;
		this.partitioningStrategy = strategy instanceof LexicalPartitioningStrategy ? ((LexicalPartitioningStrategy)strategy) : null;
	}

	public IndexReader getReader( final int bufferSize ) throws IOException {
		return new LexicalClusterIndexReader( this, bufferSize );
	}

	public IndexIterator documents( final CharSequence prefix, final int limit ) throws IOException, TooManyTermsException {
		final ArrayList<DocumentIterator> iterators = new ArrayList<DocumentIterator>( localIndex.length );

		DocumentIterator documentIterator;
		for ( int i = 0; i < localIndex.length; i++ ) {
			// TODO: check for limit globally
			documentIterator = localIndex[ i ].documents( prefix, limit );
			if ( documentIterator.mayHaveNext() ) iterators.add( documentIterator );
		}
		// TODO: test that this multiterm-of-multiterm actually works.
		return MultiTermIndexIterator.getInstance( this, iterators.toArray( IndexIterators.EMPTY_ARRAY ) );
	}
}
