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
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;
import java.util.Arrays;

/** A document iterator concatenating iterators from local indices.
 *  
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public class DocumentalConcatenatedClusterDocumentIterator extends AbstractDocumentIterator implements DocumentIterator {
	private static final boolean DEBUG = false;

	/** The component document iterators. */
	protected final DocumentIterator[] documentIterator;
	/** The number of component iterators. */
	protected final int n;
	/** The indices corresponding to each underlying document iterator. */
	protected final int[] documentIteratorIndex;
	/** The cached strategy of the index we refer to. */
	protected final ContiguousDocumentalStrategy strategy;
	/** The current iterator (an index into {@link #documentIterator}). If it is equal to {@link #n},
	 * it means that we hit the end of list on the last document iterator. Otherwise, {@link #curr}
	 * contains the last document ever returned or reached by {@link #skipTo(long)}. */
	protected int currentIterator;
	/** The last iterator to ever return something (an index into {@link #documentIterator}).*/
	protected int lastIterator = -1;
	/** The underlying index reader. */
	private final DocumentalClusterIndexReader indexReader;
	/** The set of indices involved in this iterator. */
	private final ReferenceArraySet<Index> indices = new ReferenceArraySet<Index>();

	/** Creates a new document iterator for a documental cluster.
	 * 
	 * <p>This constructor uses an array of document iterators that it is not required to be full.
	 * This is very useful with rare terms.
	 * 
	 * @param indexReader the underlying index reader.
	 * @param documentIterator an array of document iterators.
	 * @param usedIndex an array parallel to <code>documentIterator</code> containing the number
	 * of the indices corresponding to the iterators.
	 */
	
	public DocumentalConcatenatedClusterDocumentIterator( final DocumentalClusterIndexReader indexReader, final DocumentIterator[] documentIterator, int[] usedIndex ) {
		this.documentIterator = documentIterator;
		this.n = documentIterator.length;
		this.indexReader = indexReader;
		this.documentIteratorIndex = usedIndex;
		this.strategy = (ContiguousDocumentalStrategy)indexReader.index.strategy;
		for( int i = n; i-- != 0; ) {
			if ( ! documentIterator[ i ].mayHaveNext() ) throw new IllegalArgumentException( "All component document iterators must be nonempty" ); 
			indices.addAll( documentIterator[ i ].indices() );		
		}
	}

	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		return documentIterator[ lastIterator ].intervalIterator();
	}

	public IntervalIterator intervalIterator( Index index ) throws IOException {
		ensureOnADocument();
		if ( ! indices.contains( index ) ) return IntervalIterators.FALSE;
		return documentIterator[ lastIterator ].intervalIterator( index );
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		ensureOnADocument();
		return documentIterator[ lastIterator ].intervalIterators();
	}
	
	
	public ReferenceSet<Index> indices() {
		return indices;
	}

	public long skipTo( final long p ) throws IOException {
		if ( DEBUG ) System.err.println( this + ": Requested to skip to " + p + "..." );
		// In this case we are already beyond p
		if ( curr >= p ) return curr;
		// In this case, we are already beyond the last iterator
		if ( currentIterator == n || p >= indexReader.index.numberOfDocuments ) return curr = END_OF_LIST;
		
		// Otherwise, first we recover the local index that contains p
		final int k = strategy.localIndex( p );
		
		if ( DEBUG ) System.err.println( this + ": Moving to local index " + k );
		assert k >= documentIteratorIndex[ currentIterator ];
		
		// Them we advance currentIterator until we get to index k.
		while( currentIterator < n && documentIteratorIndex[ currentIterator ] < k ) currentIterator++;

		// If currentIterator == n, we have been requested to skip to a cluster that does not contain pointers
		long globalResult = END_OF_LIST;
		if ( currentIterator < n ) {
			// Now we skip to p inside the only index that might contain it.
			globalResult = documentIterator[ currentIterator ].skipTo( strategy.localPointer( p ) );
			if ( DEBUG ) System.err.println( this + ": Skipped to local pointer " + strategy.localPointer( p ) + " in iterator " + currentIterator + "; result: " + globalResult );

			// 	If we got to the end of list, the first document beyond p is the first document of the next iterator (if any).
			if ( globalResult == END_OF_LIST && ++currentIterator < n ) globalResult = documentIterator[ currentIterator ].nextDocument();
		}

		lastIterator = globalResult == END_OF_LIST ? -1 : currentIterator;
		curr  = globalResult == END_OF_LIST ? END_OF_LIST : strategy.globalPointer( documentIteratorIndex[ currentIterator ], globalResult );
		if ( DEBUG ) System.err.println( this + ": Will return " + curr + " (lastIterator=" + lastIterator + ")" );
		return curr;
	}

	public long nextDocument() throws IOException {
		if ( curr == END_OF_LIST ) return END_OF_LIST;
		if ( DEBUG ) System.err.println( this + ".nextDocument()" );
		final long result = documentIterator[ currentIterator ].nextDocument();
		if ( result != END_OF_LIST ) return curr = strategy.globalPointer( documentIteratorIndex[ lastIterator = currentIterator ], result );
		currentIterator++;
		/* Note that we are heavily exploiting the fact that only nonempty
		 * iterators are present. */ 
		return curr = currentIterator < n ? strategy.globalPointer( documentIteratorIndex[ currentIterator ], documentIterator[ lastIterator = currentIterator ].nextDocument() ) : END_OF_LIST;
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator[ lastIterator ].accept( visitor );
	}
	
	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator[ lastIterator ].acceptOnTruePaths( visitor );
	}
	
	public void dispose() throws IOException {
		indexReader.close();
	}

	public String toString() {
		return this.getClass().getSimpleName() + Arrays.toString( documentIterator );
	}
}
