package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;


/** A document iterator that computes the Brouwerian difference between two given document iterators.
 * 
 * <p>In the lattice of interval antichains, the Brouwerian difference is obtained by deleting from
 * the first operand (the minuend) all intervals that contain some interval of the second operand (the subtrahend). Thus,
 * Brouwerian difference can be fruitfully employed to kill intervals containing a term or, even
 * more fruitfully, to change at query time the granularity of an index by subtracting from the
 * results of a query those length-two intervals 
 * that cross the cutpoints between the desired parts of the index.
 * 
 * <p>Additionally, this class provides <em>interval enlargement</em>&mdash;by using a suitable
 * {@linkplain #getInstance(DocumentIterator, DocumentIterator, int, int) factory method} each interval
 * returned by the subtrahend will be enlarged to the left and to the right by the given amount (e.g.,
 * if the left margin is 1 and the right margin is 2 the interval [2..3] will turn into [1..5]).
 * 
 * <p>Note that while {@link #accept(DocumentIteratorVisitor)} will recursively visit both
 * the minuend and the subtrahend, {@link #acceptOnTruePaths(DocumentIteratorVisitor)} will
 * visit <em>only the minuend</em>.
 * 
 * @author Sebastiano Vigna
 * @since 1.2
 */

public class DifferenceDocumentIterator extends AbstractIntervalDocumentIterator implements DocumentIterator {
	private static final boolean DEBUG = false;
	private final static boolean ASSERTS = false;
	
	/** The first operand (the minuend). */
	protected final DocumentIterator minuendIterator;
	/** The second operand (the subtrahend). */
	protected final DocumentIterator subtrahendIterator;
	/** A margin that will be added to the left of each interval returned by the {@linkplain #subtrahendIterator subtrahend}. */
	protected final int leftMargin;
	/** A margin that will be added to the right of each interval returned by the {@linkplain #subtrahendIterator subtrahend}. */
	protected final int rightMargin;
	/** If true, for the current document we have intervals for the minuend but not for the {@linkplain #subtrahendIterator subtrahend}. */
	private boolean noSubtrahend;
	/** If true, the {@linkplain #subtrahendIterator subtrahend} is nonempty. */
	private boolean maybeNonEmptySubtrahend;

	/** Creates a new difference document iterator given a minuend and a subtrahend iterator.
	 * @param minuendIterator the minuend.
	 * @param subtrahendIterator the subtrahend.
	 */
	protected DifferenceDocumentIterator( final DocumentIterator minuendIterator, final DocumentIterator subtrahendIterator, final int leftMargin, final int rightMargin ) {
		super( 2, minuendIterator.indices(), allIndexIterators( minuendIterator, subtrahendIterator ), null );
		if ( leftMargin < 0 || rightMargin < 0 ) throw new IllegalArgumentException( "Illegal margins: " + leftMargin + ", " + rightMargin );
		this.minuendIterator = minuendIterator;
		this.subtrahendIterator = subtrahendIterator;
		this.leftMargin = leftMargin;
		this.rightMargin = rightMargin;

		// If the subtrahend is empty, the result is equal to the minuend.
		maybeNonEmptySubtrahend = subtrahendIterator.mayHaveNext();
	}

	/** Returns a new difference document iterator given a minuend and a subtrahend iterator.
	 * @param minuendIterator the minuend.
	 * @param subtrahendIterator the subtrahend.
	 */
	public static DocumentIterator getInstance( final DocumentIterator minuendIterator, final DocumentIterator subtrahendIterator ) {
		return getInstance( minuendIterator, subtrahendIterator, 0, 0 );
	}

	/** Returns a new difference document iterator given a minuend, a subtrahend iterator and an enlargement.
	 * @param minuendIterator the minuend.
	 * @param subtrahendIterator the subtrahend.
	 * @param leftMargin a margin that will be added to the left of each interval.
	 * @param rightMargin a margin that will be added to the right of each interval.
	 */
	public static DocumentIterator getInstance( final DocumentIterator minuendIterator, final DocumentIterator subtrahendIterator, final int leftMargin, final int rightMargin ) {
		return new DifferenceDocumentIterator( minuendIterator, subtrahendIterator, leftMargin, rightMargin );
	}

	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object arg ) {
		return new DifferenceIntervalIterator( index );
	}

	@Override
	public long nextDocument() throws IOException {
		do currentIterators.clear(); while( ( curr = minuendIterator.nextDocument() ) != END_OF_LIST && maybeNonEmptySubtrahend && noIntervals() );
		return curr;
	}

	@Override
	public boolean mayHaveNext() {
		return minuendIterator.mayHaveNext();
	}
	
	@Override
	public long skipTo( final long n ) throws IOException {
		assert n == END_OF_LIST || n < indices().iterator().next().numberOfDocuments;
		if ( curr >= n ) return curr;		
		currentIterators.clear();
		if ( ( curr = minuendIterator.skipTo( n ) ) != END_OF_LIST && noIntervals() ) nextDocument();
		return curr;
	}

	private boolean noIntervals() throws IOException {
		// An easy optimisation for the case in which the subtrahend does not include the current document.
		if ( noSubtrahend = ( subtrahendIterator.skipTo( curr ) != curr ) ) return false;
		
		/* The policy here is that a difference iterator is valid is at least one of the underlying
		 * interval iterators would return at least one interval. */
		if ( soleIndex != null ) return intervalIterator() == IntervalIterators.FALSE;
		
		for( Index index: indices() ) if ( intervalIterator( index ) != IntervalIterators.FALSE ) return false;
		return true;
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		for( Index index : indices() ) intervalIterator( index );
		return unmodifiableCurrentIterators;
	}

	public IntervalIterator intervalIterator() throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );
		ensureOnADocument();
		final Index soleIndex = this.soleIndex;
		
		IntervalIterator intervalIterator;
		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( soleIndex ) ) != null ) return intervalIterator;

		intervalIterator = minuendIterator.intervalIterator();
		if ( intervalIterator == IntervalIterators.FALSE ) return IntervalIterators.FALSE;

		IntervalIterator subtrahendIntervalIterator;

		if ( maybeNonEmptySubtrahend && ! noSubtrahend ) {
			subtrahendIntervalIterator = subtrahendIterator.intervalIterator( soleIndex );
			if ( subtrahendIntervalIterator == IntervalIterators.TRUE ) intervalIterator = IntervalIterators.FALSE;
			else if ( intervalIterator != IntervalIterators.TRUE && subtrahendIntervalIterator != IntervalIterators.FALSE ) intervalIterator = soleIntervalIterator.reset();
		}

		currentIterators.put( soleIndex, intervalIterator );
		
		if ( DEBUG ) System.err.println( "Returning interval iterator " + intervalIterator );
		return intervalIterator;
	}

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );
		ensureOnADocument();

		IntervalIterator intervalIterator;
		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( index ) ) != null ) return intervalIterator;

		intervalIterator = minuendIterator.intervalIterator( index );
		if ( intervalIterator == IntervalIterators.FALSE ) return IntervalIterators.FALSE;

		IntervalIterator subtrahendIntervalIterator;

		if ( maybeNonEmptySubtrahend && ! noSubtrahend ) {
			subtrahendIntervalIterator = subtrahendIterator.intervalIterator( index );
			if ( subtrahendIntervalIterator == IntervalIterators.TRUE ) intervalIterator = IntervalIterators.FALSE;
			else if ( intervalIterator != IntervalIterators.TRUE && subtrahendIntervalIterator != IntervalIterators.FALSE ) intervalIterator = intervalIterators.get( index ).reset();
		}

		currentIterators.put( index, intervalIterator );
		
		if ( DEBUG ) System.err.println( "Returning interval iterator " + intervalIterator );
		return intervalIterator;
	}

	public void dispose() throws IOException {
		minuendIterator.dispose();
		subtrahendIterator.dispose();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 2 );
		if ( a == null ) {
			if ( minuendIterator.accept( visitor ) == null ) return null;
			if ( subtrahendIterator.accept( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = minuendIterator.accept( visitor ) ) == null ) return null;
			if ( ( a[ 1 ] = subtrahendIterator.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( minuendIterator.acceptOnTruePaths( visitor )  == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = minuendIterator.acceptOnTruePaths( visitor ) ) == null ) return null;
		}

		return visitor.visitPost( this, a );
	}
	
	public String toString() {
		return getClass().getSimpleName() + "(" + minuendIterator + ( leftMargin == 0 && rightMargin == 0 ? " - " : " - [[" + leftMargin + "," + rightMargin + "]] " ) + subtrahendIterator + ")";
	}
	
	protected class DifferenceIntervalIterator implements IntervalIterator {
		/** The index of this iterator. */
		final Index index;
		/** The underlying minuend interval iterator. */
		private IntervalIterator minuendIntervalIterator;
		/** The underlying subtrahend interval iterator. */
		private IntervalIterator subtrahendIntervalIterator;
		/** The last interval returned by {@link #subtrahendIntervalIterator}. */
		private Interval subtrahendInterval;
		/** The first result, cached. */
		private Interval first;
		
		protected DifferenceIntervalIterator( final Index index ) {
			this.index = index;
		}

		@Override
		public IntervalIterator reset() throws IOException {
			subtrahendInterval = Intervals.MINUS_INFINITY;
			minuendIntervalIterator = minuendIterator.intervalIterator( index );
			subtrahendIntervalIterator = subtrahendIterator.intervalIterator( index );
			if ( ASSERTS ) assert minuendIntervalIterator != IntervalIterators.TRUE;
			if ( ASSERTS ) assert minuendIntervalIterator != IntervalIterators.FALSE;
			if ( ASSERTS ) assert subtrahendIntervalIterator != IntervalIterators.TRUE;
			if ( ASSERTS ) assert subtrahendIntervalIterator != IntervalIterators.FALSE;
			first = null;
			return ( first = nextInterval() ) == null ? IntervalIterators.FALSE : this;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			// Just delegate to minuend
			minuendIntervalIterator.intervalTerms( terms );
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			if ( first != null ) {
				final Interval result = first;
				first = null;
				return result;
			}

			if ( subtrahendInterval == Intervals.MINUS_INFINITY ) subtrahendInterval = subtrahendIntervalIterator.nextInterval();
			
			Interval minuendInterval;
			while( ( minuendInterval = minuendIntervalIterator.nextInterval() ) != null ) {
					while(	subtrahendInterval != null && 
							subtrahendInterval.left - leftMargin < minuendInterval.left &&
							subtrahendInterval.right + rightMargin < minuendInterval.right ) 
						subtrahendInterval = subtrahendIntervalIterator.nextInterval();
				if ( subtrahendInterval == null || 
						subtrahendInterval.left - leftMargin < minuendInterval.left ||
						subtrahendInterval.right + rightMargin > minuendInterval.right ) return minuendInterval;
			}
            
			return null;
		}
		
		@Override
		public int extent() {
			return minuendIntervalIterator.extent();
		}
		
		public String toString() {
		   return getClass().getSimpleName() + "(" + minuendIntervalIterator + ( leftMargin == 0 && rightMargin == 0 ? " - " : " -[" + leftMargin + "," + rightMargin + "] " ) + subtrahendIntervalIterator + ")";
		}
	}
}
