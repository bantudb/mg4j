package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2008-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;


/** A document iterator that aligns the results of two iterators over
 * different indices.
 *
 * <p>This class is an example of cross-index computation. As in the case of an
 * {@link AndDocumentIterator}, we intersect the posting lists. However, once
 * we get to the index level, we actually return just intervals that appear in
 * <em>all</em> component iterators. Of course, this is meaningful only if all
 * indices represent different views on the same data, a typical example being 
 * semantic tagging. 
 * 
 * <p>An instance of this class exposes a single interval iterator associated with
 * the index of the <em>first</em> component iterator, as all interval iterators
 * are exhausted during the computation of their intersection.
 * Correspondingly, a call to {@link IntervalIterator#intervalTerms(LongSet)} just
 * returns the terms related to the <em>first</em> component iterator.
 */

public class AlignDocumentIterator extends AbstractIntervalDocumentIterator {
	private final static boolean DEBUG = false;

	/** The first operand, to be aligned. */
	protected final DocumentIterator firstIterator;
	/** The second operand, to be used to align the first operand. */
	protected final DocumentIterator secondIterator;
	/** {@link #firstIterator}, if it is an {@link IndexIterator}. */
	protected final IndexIterator firstIndexIterator;
	/** {@link #secondIterator}, if it is an {@link IndexIterator}. */
	protected final IndexIterator secondIndexIterator;
	/** The sole index involved in this iterator. */
	protected final Index index;
	/** The iterator returned for the current document, if any, or <code>null</code>. */
	private IntervalIterator currentIterator;

	/** Returns a document iterator that aligns the first iterator to the second.
	 * 
	 * @param firstIterator the iterator to be aligned.
	 * @param secondIterator the iterator used to align <code>firstIterator</code>.
	 * 
	 * @return a document iterator that computes the alignment of <code>firstIterator</code> on <code>secondIterator</code>. 
	 */
	public static DocumentIterator getInstance( final DocumentIterator firstIterator, final DocumentIterator secondIterator ) {
		return new AlignDocumentIterator( firstIterator, secondIterator );
	}

	protected AlignDocumentIterator( final DocumentIterator firstIterator, final DocumentIterator secondIterator ) {
		super( 2, firstIterator.indices(), allIndexIterators( firstIterator, secondIterator ), null );
		this.firstIterator = firstIterator;
		this.secondIterator = secondIterator;
		if ( firstIterator instanceof IndexIterator && secondIterator instanceof IndexIterator ) {
			firstIndexIterator = (IndexIterator)firstIterator; 
			secondIndexIterator = (IndexIterator)secondIterator;
		}
		else firstIndexIterator = secondIndexIterator = null;
		if ( firstIterator.indices().size() != 1 || secondIterator.indices().size() != 1 ) throw new IllegalArgumentException( "You can align single-index iterators only" );
		index = firstIterator.indices().iterator().next();
	}

	protected IntervalIterator getIntervalIterator( final Index unused, final int n, final boolean allIndexIterators, final Object unusedArg ) {
		return allIndexIterators ? new AlignIndexIntervalIterator() : new AlignIntervalIterator();
	}
	
	private long align( long first ) throws IOException {
		if ( first == END_OF_LIST ) return END_OF_LIST;
		long second = -1;  // This forces a call to secondIterator.skipTo( first ).

		for( ;; ) {
			if ( first < second ) {
				if ( ( first = firstIterator.skipTo( second ) ) == END_OF_LIST ) return curr = END_OF_LIST;
			}
			else if ( second < first ) {
				if ( ( second = secondIterator.skipTo( first ) ) == END_OF_LIST ) return curr = END_OF_LIST;
			}
			else {
				curr = first;
				if ( intervalIterator() != IntervalIterators.FALSE ) return first; 
				currentIterator = null;
				if ( ( first = firstIterator.nextDocument() ) == END_OF_LIST ) return curr = END_OF_LIST;
			}
		}
	}
	
	public long nextDocument() throws IOException {
		currentIterator = null;
		return curr = align( firstIterator.nextDocument() );
	}
	
	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		currentIterator = null;
		return curr = align( firstIterator.skipTo( n ) );
	}

	public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() throws IOException {
		currentIterators.put( index, intervalIterator() );
		return unmodifiableCurrentIterators;
	}

	public IntervalIterator intervalIterator() throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );
		ensureOnADocument();
		
		// If the iterator has been created and it's ready, we just return it.		
		if ( currentIterator != null ) return currentIterator;
		
		final IntervalIterator firstIntervalIterator = firstIterator.intervalIterator(), secondIntervalIterator = secondIterator.intervalIterator();
		
		// TODO: could be better optimizied for the index iterator case.
		if ( secondIntervalIterator == IntervalIterators.FALSE ) return currentIterator = IntervalIterators.FALSE; 
		if ( secondIntervalIterator == IntervalIterators.TRUE ) return currentIterator = firstIntervalIterator == IntervalIterators.TRUE ? IntervalIterators.TRUE : IntervalIterators.FALSE;
		if ( firstIntervalIterator == IntervalIterators.TRUE ) return currentIterator = IntervalIterators.FALSE;

		return currentIterator = soleIntervalIterator.reset();
	}

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );
		ensureOnADocument();
		if ( index != this.index ) return IntervalIterators.FALSE;
		return intervalIterator();
	}
	
	public void dispose() throws IOException {
		firstIterator.dispose();
		secondIterator.dispose();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 2 );
		if ( a == null ) {
			if ( firstIterator.accept( visitor ) == null ) return null;
			if ( secondIterator.accept( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = firstIterator.accept( visitor ) ) == null ) return null;
			if ( ( a[ 1 ] = secondIterator.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( firstIterator.acceptOnTruePaths( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = firstIterator.acceptOnTruePaths( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}
	
	/** An interval iterator returning the intersection of the component interval iterators. */
	
	protected class AlignIntervalIterator implements IntervalIterator {
		/** The interval iterator of the first iterator. */
		private IntervalIterator firstIntervalIterator;
		/** The interval iterator of the second iterator. */
		private IntervalIterator secondIntervalIterator;
		/** The first result, cached. */
		private Interval first;

		public IntervalIterator reset() throws IOException {
			firstIntervalIterator = firstIterator.intervalIterator();
			secondIntervalIterator = secondIterator.intervalIterator();
			first = null;
			return ( first = nextInterval() ) == null ? IntervalIterators.FALSE : this;
		}

		public void intervalTerms( final LongSet terms ) {
			firstIntervalIterator.intervalTerms( terms );
		}

		public Interval nextInterval() throws IOException {
			if ( first != null ) {
				final Interval result = first;
				first = null;
				return result;
			}

			Interval firstInterval = firstIntervalIterator.nextInterval();
			Interval secondInterval = secondIntervalIterator.nextInterval();
			
			if ( firstInterval == null || secondInterval == null ) return null;
			
			while ( ! firstInterval.equals( secondInterval ) ) {
				if ( firstInterval.left <= secondInterval.left ) { 
					if ( ( firstInterval = firstIntervalIterator.nextInterval() ) == null ) return null;
				}
				else if ( ( secondInterval = secondIntervalIterator.nextInterval() ) == null ) return null;
			}

			return firstInterval;
		}

		@Override
		public int extent() {
			return firstIntervalIterator.extent();
		}

		public String toString() {
			return getClass().getSimpleName() + "(" + firstIterator + ", " + secondIterator + ")";
		}
	}


	/** An interval iterator returning the intersection of the component interval iterators. */
	
	protected class AlignIndexIntervalIterator implements IntervalIterator {
		/** The position of the first iterator. */
		private int firstCurr;
		/** The position of the second iterator. */
		private int secondCurr;
		/** The first result, cached. */
		private Interval first;

		@Override
		public IntervalIterator reset() throws IOException {
			first = null;
			return ( first = nextInterval() ) == null ? IntervalIterators.FALSE : this;
		}
		
		@Override
		public void intervalTerms( final LongSet terms ) {
			terms.add( firstIndexIterator.termNumber() );
		}

		@Override
		public Interval nextInterval() throws IOException {
			if ( first != null ) {
				final Interval result = first;
				first = null;
				return result;
			}

			firstCurr = firstIndexIterator.nextPosition();
			secondCurr = secondIndexIterator.nextPosition();
			
			if ( firstCurr == IndexIterator.END_OF_POSITIONS || secondCurr == IndexIterator.END_OF_POSITIONS ) return null;
			
			while ( firstCurr != secondCurr ) {
				if ( firstCurr < secondCurr ) { 
					if ( ( firstCurr = firstIndexIterator.nextPosition() ) == IndexIterator.END_OF_POSITIONS ) return null;
				}
				else if ( ( secondCurr = secondIndexIterator.nextPosition() ) == IndexIterator.END_OF_POSITIONS ) return null;
			}

			return Interval.valueOf( secondCurr );
		}

		@Override
		public int extent() {
			return 1;
		}

		public String toString() {
		   return getClass().getSimpleName() + "(" + firstIterator + ", " + secondIterator + ")";
		}
	}
	
	public String toString() {
	   return getClass().getSimpleName() + "(" + firstIterator + ", " + secondIterator + ")";
	}
}
