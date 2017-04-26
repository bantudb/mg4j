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
import it.unimi.di.big.mg4j.search.AbstractDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;

/** A document iterator merging iterators from local indices.
 * 
 * @author Sebastiano Vigna
 */

public class DocumentalMergedClusterDocumentIterator extends AbstractDocumentIterator implements DocumentIterator {
	/** The component document iterators. */
	final protected DocumentIterator[] documentIterator;
	/** The number of component iterators. */
	final protected int n;
	/** The indices corresponding to each underlying document iterator. */
	protected final int[] usedIndex;
	/** The cached strategy of the index we refer to. */
	protected final DocumentalClusteringStrategy strategy;
	/** The queue of document iterator indices (offsets into {@link #documentIterator} and {@link #usedIndex}). */
	protected final LongHeapSemiIndirectPriorityQueue queue;
	/** The reference array for the queue (containing <em>global</em> document pointers). */
	protected final long[] globalDocumentPointer;
	/** The set of indices involved in this iterator. */
	protected final ReferenceSet<Index> indices = new ReferenceArraySet<Index>();

	/** The underlying index reader. */
	private final DocumentalClusterIndexReader indexReader;

	/** The current iterator. */
	protected int currentIterator = -1;
	
	/** Creates a new document iterator for a documental cluster.
	 * 
	 * <p>This constructor uses an array of document iterators that it is not required to be full.
	 * This is very useful with rare terms.
	 * 
	 * @param indexReader the underlying index reader.
	 * @param documentIterator an array of document iterators.
	 * @param usedIndex an array parallel to <code>documentIterator</code> containing the ordinal numbers
	 * of the indices corresponding to the iterators.
	 */
	
	public DocumentalMergedClusterDocumentIterator( final DocumentalClusterIndexReader indexReader, final DocumentIterator[] documentIterator, int[] usedIndex ) throws IOException {
		this.documentIterator = documentIterator;
		this.n = documentIterator.length;
		this.indexReader = indexReader;
		this.usedIndex = usedIndex;
		
		strategy = indexReader.index.strategy;
		globalDocumentPointer = new long[ n ];
		queue = new LongHeapSemiIndirectPriorityQueue( globalDocumentPointer, n );
		
		long result;
		for( int i = n; i-- != 0; ) {
			if ( ( result = documentIterator[ i ].nextDocument() ) != END_OF_LIST ) {
				indices.addAll( documentIterator[ i ].indices() );
				globalDocumentPointer[ i ] = strategy.globalPointer( usedIndex[ i ], result );
				queue.enqueue( i );
			}
		}
		
		if ( queue.isEmpty() ) curr = END_OF_LIST;
	}

	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		return documentIterator[ currentIterator ].intervalIterator();
	}
	
	public IntervalIterator intervalIterator( Index index ) throws IOException {
		ensureOnADocument();
		if ( ! indices.contains( index ) ) return IntervalIterators.FALSE;
		return documentIterator[ currentIterator ].intervalIterator( index );
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		ensureOnADocument();
		return documentIterator[ currentIterator ].intervalIterators();
	}

	public ReferenceSet<Index> indices() {
		return indices;
	}

	public long skipTo( final long p ) throws IOException {
		if ( curr >= p ) return curr;
		if ( p >= indexReader.index.numberOfDocuments ) return curr = END_OF_LIST;
		int first;
		long d;
		
		//System.err.println( "Advancing to " + n  + " doc: " + Arrays.toString( doc ) + " first: " + queue.first() );
		while( ! queue.isEmpty() && globalDocumentPointer[ first = queue.first() ] < p ) {
			d = documentIterator[ first ].skipTo( strategy.localPointer( p ) );
			if ( d == END_OF_LIST ) queue.dequeue();
			else {
				globalDocumentPointer[ first ] = strategy.globalPointer( usedIndex[ first ], d );
				if ( globalDocumentPointer[ first ] < p ) queue.dequeue(); // This covers the case of getting to the end of list without finding p 
				else queue.changed();
			}
		}
		
		if ( queue.isEmpty() ) return curr = END_OF_LIST;
		return curr = globalDocumentPointer[ currentIterator = queue.first() ];
	}

	public long nextDocument() throws IOException {
		if ( curr == END_OF_LIST ) return END_OF_LIST;
		if ( curr != -1 ) {
			final long result;
			if ( ( result = documentIterator[ currentIterator ].nextDocument() ) != END_OF_LIST ) {
				globalDocumentPointer[ currentIterator ] = strategy.globalPointer( usedIndex[ currentIterator ], result );
				queue.changed();
			}
			else queue.dequeue();
		}
		return curr = queue.isEmpty() ? END_OF_LIST : globalDocumentPointer[ currentIterator = queue.first() ];
	}
	
	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator[ currentIterator ].accept( visitor );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator[ currentIterator ].acceptOnTruePaths( visitor );
	}

	public void dispose() throws IOException {			
		indexReader.close();
	}
}
