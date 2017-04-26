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
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;

import java.io.IOException;

/** An abstract document iterator helping in the implementation of {@link it.unimi.di.big.mg4j.search.ConsecutiveDocumentIterator}
 * and {@link it.unimi.di.big.mg4j.search.OrderedAndDocumentIterator}. 
 */

public abstract class AbstractOrderedIntervalDocumentIterator extends AbstractIntersectionDocumentIterator {
	private final static boolean DEBUG = false;
	
	/** The iterator returned for the current document, if any, or <code>null</code>. */
	private IntervalIterator currentIterator;

	/** Creates a new abstract document iterator.
	 * @param arg an argument that will be passed to {@link #getIntervalIterator(Index, int, boolean, Object)}.
	 * @param documentIterator the underlying document iterators (at least one). The must all return the same
	 * singleton set as {@link DocumentIterator#indices()}.
	 */
	protected AbstractOrderedIntervalDocumentIterator( final Object arg, final DocumentIterator[] documentIterator ) {
		super( arg, documentIterator );
		if ( soleIndex == null ) throw new IllegalArgumentException();
	}

	
	@Override
	public long nextDocument() throws IOException {
		assert curr != END_OF_LIST;
		do currentIterator = null; while( ( curr = align( lastIterator.nextDocument() ) ) != END_OF_LIST && intervalIterator() == IntervalIterators.FALSE );
		return curr;
	}

	@Override
	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;		
		currentIterator = null;
		if ( ( curr = align( lastIterator.skipTo( n ) ) ) != END_OF_LIST && intervalIterator() == IntervalIterators.FALSE ) nextDocument();
		return curr;
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		currentIterators.put( soleIndex, intervalIterator() );
		return unmodifiableCurrentIterators;
	}
	
	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );

		// If the iterator has been created and it's ready, we just return it.		
		if ( currentIterator != null ) return currentIterator;

		IntervalIterator intervalIterator;
		int t = 0, f = 0;
			
		/* We count the number of TRUE and FALSE iterators. In the case of index iterators, we can avoid 
		 * the check and just rely on the index internals.
		 * 
		 * If all iterators are FALSE, we return FALSE. Else if all remaining iterators are TRUE
		 * we return TRUE.
		 */
		if ( indexIterator == null ) {
			for( int i = n; i-- != 0; ) {
				intervalIterator = documentIterator[ i ].intervalIterator();
				if ( intervalIterator == IntervalIterators.TRUE ) t++;
				if ( intervalIterator == IntervalIterators.FALSE ) f++;
			}
			// Note that we cannot optimise the case n - t - f == 1 because of gaps in ConsecutiveDocumentIterator.
			if ( f == n ) intervalIterator = IntervalIterators.FALSE;
			else if ( t + f == n ) intervalIterator = IntervalIterators.TRUE;
			else intervalIterator = soleIntervalIterator.reset();
		}
		else {
			if ( indexIteratorsWithoutPositions == n ) intervalIterator = IntervalIterators.TRUE;
			else intervalIterator = soleIntervalIterator.reset();
		}

		return currentIterator = intervalIterator;	
	}

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );
		return index == soleIndex ? intervalIterator() : IntervalIterators.FALSE;
	}
}
