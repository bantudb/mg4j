package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Paolo Boldi and Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;


/** A document iterator that returns documents <em>not</em> returned by its underlying iterator,
 * and returns just {@link it.unimi.di.big.mg4j.search.IntervalIterators#TRUE} on all interval iterators.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.9
 */

public class NotDocumentIterator extends AbstractDocumentIterator {
	@SuppressWarnings("unused")
	private final static boolean DEBUG = false;
	private final static boolean ASSERTS = false;

	/** The underlying iterator. */
	private final DocumentIterator documentIterator;
	/** If not <code>null</code>, the sole index involved in this iterator. */
	private final Index soleIndex;
	/** The number of documents. */
	private final long numberOfDocuments;
	/** A map mapping all indices in {@link #indices()} to {@link IntervalIterators#TRUE}, and the others to {@link IntervalIterators#FALSE}. */
	private final Index2IntervalIteratorMap intervalIterators;
	/** An unmodifiable copy of {@link #intervalIterators}. */
	private final Reference2ReferenceMap<Index,IntervalIterator> unmodifiableIntervalIterators;
	/** The next document that must <em>not</em> be returned, or {@link #numberOfDocuments}
	 * if the underlying iterator is exhausted. {@link #next} is always less than or equal to
	 * this field. */
	private long nextToSkip;
	/** The next document that will be considered; it might be returned or not depending on whether it is returned
	 * by {@link #documentIterator}. */
	private long nextCandidate;

	/** Creates a new NOT document iterator over a given iterator.
	 * @param documentIterator an iterator.
	 * @param numberOfDocuments the number of documents.
	 */
	protected NotDocumentIterator( final DocumentIterator documentIterator, final long numberOfDocuments ) throws IOException {
		this.documentIterator = documentIterator;
		this.numberOfDocuments = numberOfDocuments;
		
		if ( ( nextToSkip = documentIterator.nextDocument() ) == END_OF_LIST ) nextToSkip = numberOfDocuments;
		final int n = documentIterator.indices().size();
		
		soleIndex = n == 1 ? indices().iterator().next() : null;
		intervalIterators = new Index2IntervalIteratorMap( n );
		for( Index i: indices() ) intervalIterators.put( i, IntervalIterators.TRUE );
		unmodifiableIntervalIterators = Reference2ReferenceMaps.unmodifiable( intervalIterators );	
	}

	/** Returns a document iterator computing the NOT of the given iterator.
	 * @param it an iterator.
	 * @param numberOfDocuments the number of documents.
	 */
	public static NotDocumentIterator getInstance( final DocumentIterator it, final long numberOfDocuments ) throws IOException {
		return new NotDocumentIterator( it, numberOfDocuments );
	}
	
	public ReferenceSet<Index> indices() {
		return documentIterator.indices();
	}

	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		
		nextCandidate = n;
		nextToSkip = documentIterator.skipTo( n );
		if ( nextToSkip == END_OF_LIST ) nextToSkip = numberOfDocuments;
		nextDocument();
		return curr;
	}

	public long nextDocument() throws IOException {
		for(;;) {
			if ( nextCandidate >= numberOfDocuments ) return	curr = END_OF_LIST;
			if ( nextCandidate < nextToSkip ) return curr = nextCandidate++;
			if ( ASSERTS ) assert nextCandidate == nextToSkip;
			nextCandidate++;
			nextToSkip = documentIterator.nextDocument();
			if ( nextToSkip == -1 ) nextToSkip = numberOfDocuments;
		}
	}

	public boolean mayHaveNext() {
		return nextCandidate < numberOfDocuments;
	}

	public void dispose() throws IOException {
		documentIterator.dispose();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( documentIterator.accept( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = documentIterator.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) {
		if ( ! visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public String toString() {
	   return getClass().getSimpleName() + "(" + documentIterator + ")";
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() {
		return unmodifiableIntervalIterators;
	}

	public IntervalIterator intervalIterator() {
		if ( soleIndex == null ) throw new IllegalStateException();
		return IntervalIterators.TRUE;
	}

	public IntervalIterator intervalIterator( final Index index ) {
		final IntervalIterator intervalIterator = intervalIterators.get( index );
		return intervalIterator == null ? IntervalIterators.FALSE : intervalIterator;
	}
}
