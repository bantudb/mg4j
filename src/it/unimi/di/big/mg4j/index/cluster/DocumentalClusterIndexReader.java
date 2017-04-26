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

import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An index reader for a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster}. It dispatches
 * the correct {@link it.unimi.di.big.mg4j.index.IndexReader} depending on the concrete subclass of
 * {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster}. 
 *  
 * @author Sebastiano Vigna
 */

public class DocumentalClusterIndexReader extends AbstractIndexClusterIndexReader {
	private static final Logger LOGGER = LoggerFactory.getLogger( DocumentalClusterIndexReader.class );
	private static final boolean DEBUG = false;
	
	/** The index this reader refers to. */
	protected final DocumentalCluster index;
	
	public DocumentalClusterIndexReader( DocumentalCluster index, int bufferSize ) throws IOException {
		super( index, bufferSize );
		this.index = index;
	}

	public IndexIterator documents( long term ) throws IOException {
		if ( ! index.flat ) throw new UnsupportedOperationException( "Only flat documental clusters allow access by term number" );

		final IndexIterator[] iterator = new IndexIterator[ indexReader.length ];
		for ( int i = 0; i < indexReader.length; i++ ) iterator[ i ] = indexReader[ i ].documents( term );

		final IndexIterator indexIterator =
			index.concatenated ? 
					new DocumentalConcatenatedClusterIndexIterator( this, iterator, index.allIndices ) :
						new DocumentalMergedClusterIndexIterator( this, iterator, index.allIndices ) ;
					
		return indexIterator;
	}

	public IndexIterator documents( final CharSequence term ) throws IOException {
		final ArrayList<IndexIterator> iterators = new ArrayList<IndexIterator>( indexReader.length );
		final IntArrayList usedIndices = new IntArrayList();
		for ( int i = 0; i < indexReader.length; i++ ) {
			if ( index.termFilter == null || index.termFilter[ i ].contains( term ) ) {
				IndexIterator it = indexReader[ i ].documents( term );
				if ( it.mayHaveNext() ) {
					iterators.add( it );
					usedIndices.add( i );
				}
			}
		}

		if ( DEBUG ) LOGGER.debug( "Indices used for " + term + ": " + usedIndices );

		if ( iterators.isEmpty() ) return index.getEmptyIndexIterator( term );
		final IndexIterator indexIterator =
			index.concatenated ? 
					new DocumentalConcatenatedClusterIndexIterator( this, iterators.toArray( IndexIterators.EMPTY_ARRAY ), usedIndices.toIntArray() ) :
						new DocumentalMergedClusterIndexIterator( this, iterators.toArray( IndexIterators.EMPTY_ARRAY ), usedIndices.toIntArray() ) ;
					
		indexIterator.term( term );
		return indexIterator;
	}
}
