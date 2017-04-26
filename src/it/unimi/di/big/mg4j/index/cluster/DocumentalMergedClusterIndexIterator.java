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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;

import java.io.IOException;


/** An index iterator merging iterators from local indices.
 *  
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public class DocumentalMergedClusterIndexIterator extends DocumentalMergedClusterDocumentIterator implements IndexIterator {
	/** A cached copy of the iterators, to avoid type casts. */
	protected final IndexIterator[] indexIterator;
	/** The precomputed frequency. */
	protected final long frequency;
	/** The reference index. */
	protected final DocumentalMergedCluster index;
	/** The term associated with this index iterator. */
	protected String term;
	/** The id associated with this index iterator. */
	protected int id;
	
	public DocumentalMergedClusterIndexIterator( final DocumentalClusterIndexReader indexReader, final IndexIterator[] indexIterator, final int[] usedIndex ) throws IOException {
		super( indexReader, indexIterator, usedIndex );
		this.index = (DocumentalMergedCluster)indexReader.index;
		this.indexIterator = indexIterator;
		long t = 0;
		for ( int i = indexIterator.length; i-- != 0; ) t += indexIterator[ i ].frequency();
		frequency = t;
	}

	public DocumentalMergedClusterIndexIterator term( final CharSequence term ) {
		this.term = term == null ? null : term.toString();
		return this;
	}

	public String term() { 
		return term;
	}

	public DocumentalMergedClusterIndexIterator id( final int id ) {
		this.id = id;
		return this;
	}
	
	public int id() {
		return id;
	}
	
	public Index index() {
		return index;
	}
	
	public long frequency() {
		return frequency;
	}

	public Payload payload() throws IOException {
		if ( currentIterator < 0 ) throw new IllegalStateException( "There is no current payload: nextDocument() has never been called" );
		return indexIterator[ currentIterator ].payload();
	}
	
	public int count() throws IOException {
		if ( currentIterator < 0 ) throw new IllegalStateException( "There is no current count: nextDocument() has never been called" );
		return indexIterator[ currentIterator ].count();
	}

	public int nextPosition() throws IOException {
		if ( currentIterator < 0 ) throw new IllegalStateException( "There are no current positions: nextDocument() has never been called" );
		return indexIterator[ currentIterator ].nextPosition();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		return visitor.visit( this );
	}
	
	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		return visitor.visit( this );
	}
	
	public long termNumber() {
		// ALERT: to be implemented
		throw new UnsupportedOperationException( "To be implemented" );
	}

	public IndexIterator weight( final double weight ) {
		super.weight( weight );
		return this;
	}

}
