package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2013-2016 Sebastiano Vigna 
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

import java.io.IOException;


/** A document iterator that computes the containement between two given document iterators.
 * 
 * <p>Additionally, this class provides <em>interval enlargement</em>&mdash;by using a suitable
 * {@linkplain #getInstance(DocumentIterator, DocumentIterator, int, int) factory method} each interval
 * returned by the first operand will be enlarged to the left and to the right by the given amount (e.g.,
 * if the left margin is 1 and the right margin is 2 the interval [2..3] will turn into [1..5]).
 * 
 * <p>Note that while {@link #accept(DocumentIteratorVisitor)} will recursively visit both
 * operands, {@link #acceptOnTruePaths(DocumentIteratorVisitor)} will visit <em>only the first one</em>.
 * 
 * @author Sebastiano Vigna
 * @since 1.2
 */

public class ContainmentDocumentIterator extends AbstractIntervalDocumentIterator implements DocumentIterator {
	private static final boolean DEBUG = false;
	/** The first operand. */
	protected final DocumentIterator first;
	/** The second operand. */
	protected final DocumentIterator second;
	/** A margin that will be added to the left of each interval returned by the first operand. */
	protected final int leftMargin;
	/** A margin that will be added to the right of each interval returned by the first operand. */
	protected final int rightMargin;
	/** The sole index involved in this iterator. */
	protected final Index index;
	/** The iterator returned for the current document, if any, or <code>null</code>. */
	private IntervalIterator currentIterator;

	/** Creates a new containement document iterator given two operands.
	 * @param first the first operand.
	 * @param second the second operand.
	 */
	protected ContainmentDocumentIterator( final DocumentIterator first, final DocumentIterator second, final int leftMargin, final int rightMargin ) {
		super( 2, first.indices(), allIndexIterators( first, second ), null );
		if ( leftMargin < 0 || rightMargin < 0 ) throw new IllegalArgumentException( "Illegal margins: " + leftMargin + ", " + rightMargin );
		this.first = first;
		this.second = second;
		this.leftMargin = leftMargin;
		this.rightMargin = rightMargin;
		if ( first.indices().size() != 1 || second.indices().size() != 1 ) throw new IllegalArgumentException( "You can compute the containment of single-index iterators only" );
		index = first.indices().iterator().next();
	}

	/** Returns a new containment document iterator given two operands.
	 * @param first the first operand.
	 * @param second the second operand.
	 */
	public static DocumentIterator getInstance( final DocumentIterator first, final DocumentIterator second ) {
		return getInstance( first, second, 0, 0 );
	}

	/** Returns a new containment document iterator given two operands and an enlargement.
	 * @param first the first operand.
	 * @param second the second operand.
	 * @param leftMargin a margin that will be added to the left of each interval returned by the first operand.
	 * @param rightMargin a margin that will be added to the right of each interval returned by the first operand.
	 */
	public static DocumentIterator getInstance( final DocumentIterator first, final DocumentIterator second, final int leftMargin, final int rightMargin ) {
		return new ContainmentDocumentIterator( first, second, leftMargin, rightMargin );
	}

	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object arg ) {
		return new ContainmentIntervalIterator();
	}

	protected final long align( long to ) throws IOException {
		long f = to;
		long s = -1;
		while( f != s ) {
			if ( f < s ) {
				if ( ( f = first.skipTo( s ) ) == END_OF_LIST ) return curr = END_OF_LIST;
			}
			else if ( ( s = second.skipTo( f ) ) == END_OF_LIST ) return curr = END_OF_LIST;
		}
		
		return curr = f;
	}
	
	@Override
	public long nextDocument() throws IOException {
		long f;
		do {
			// TODO: optimize skipping by frequency
			if ( ( f = first.nextDocument() ) == END_OF_LIST ) return curr = END_OF_LIST;
			currentIterator = null;
			if ( align( f ) == END_OF_LIST ) return END_OF_LIST;
		} while( noIntervals() );
		return curr;
	}

	@Override
	public boolean mayHaveNext() {
		return first.mayHaveNext();
	}
	
	@Override
	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		currentIterator = null;
		if ( ( curr = first.skipTo( n ) ) != END_OF_LIST && align( curr ) != END_OF_LIST && noIntervals() ) nextDocument();
		return curr;
	}

	private boolean noIntervals() throws IOException {
		/* The policy here is that a difference iterator is valid is at least one of the underlying
		 * interval iterators would return at least one interval. */
		if ( soleIndex != null ) return intervalIterator() == IntervalIterators.FALSE;
		
		for( Index index: indices() ) if ( intervalIterator( index ) != IntervalIterators.FALSE ) return false;
		return true;
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		currentIterators.put( index, intervalIterator() );
		return unmodifiableCurrentIterators;
	}

	public IntervalIterator intervalIterator() throws IOException {
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );
		ensureOnADocument();
		
		// If the iterator has been created and it's ready, we just return it.		
		if ( currentIterator != null ) return currentIterator;

		final IntervalIterator firstIntervalIterator = first.intervalIterator();
		if ( firstIntervalIterator == IntervalIterators.FALSE ) return currentIterator = IntervalIterators.FALSE;

		final IntervalIterator secondIntervalIterator = second.intervalIterator();
		if ( secondIntervalIterator == IntervalIterators.FALSE ) return currentIterator = IntervalIterators.FALSE;
		else if ( secondIntervalIterator == IntervalIterators.TRUE ) return currentIterator = firstIntervalIterator;
		else if ( firstIntervalIterator == IntervalIterators.TRUE ) return currentIterator = IntervalIterators.FALSE;
		else return currentIterator = soleIntervalIterator.reset();
	}


	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		ensureOnADocument();
		if ( index != this.index ) return IntervalIterators.FALSE;
		return intervalIterator();
	}

	public void dispose() throws IOException {
		first.dispose();
		second.dispose();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 2 );
		if ( a == null ) {
			if ( first.accept( visitor ) == null ) return null;
			if ( second.accept( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = first.accept( visitor ) ) == null ) return null;
			if ( ( a[ 1 ] = second.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( first.acceptOnTruePaths( visitor )  == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = first.acceptOnTruePaths( visitor ) ) == null ) return null;
		}

		return visitor.visitPost( this, a );
	}
	
	public String toString() {
		return getClass().getSimpleName() + "(" + first + ( leftMargin == 0 && rightMargin == 0 ? " <- " : " <- [[" + leftMargin + "," + rightMargin + "]] " ) + second + ")";
	}
	
	protected class ContainmentIntervalIterator implements IntervalIterator {
		/** The first underlying interval iterator. */
		private IntervalIterator firstIntervalIterator;
		/** The second underlying interval iterator. */
		private IntervalIterator secondIntervalIterator;
		/** The last interval returned by {@link #secondIntervalIterator}. */
		private Interval lastSecondInterval;
		/** The first result, cached. */
		private Interval firstInterval;
		
		@Override
		public IntervalIterator reset() throws IOException {
			firstIntervalIterator = first.intervalIterator();
			secondIntervalIterator = second.intervalIterator();
			lastSecondInterval = secondIntervalIterator.nextInterval();
			assert firstIntervalIterator != IntervalIterators.TRUE;
			assert firstIntervalIterator != IntervalIterators.FALSE;
			assert secondIntervalIterator != IntervalIterators.TRUE;
			assert secondIntervalIterator != IntervalIterators.FALSE;
			firstInterval = null;
			return ( firstInterval = nextInterval() ) == null ? IntervalIterators.FALSE : this;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			// Just delegate to the first operator
			firstIntervalIterator.intervalTerms( terms );
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			if ( firstInterval != null ) {
				final Interval result = firstInterval;
				firstInterval = null;
				return result;
			}

			for( Interval interval; ( interval = firstIntervalIterator.nextInterval() ) != null; ) {
				while( lastSecondInterval.left < interval.left - leftMargin )
					if ( ( lastSecondInterval = secondIntervalIterator.nextInterval() ) == null ) return null;
				
				if ( lastSecondInterval.right <= interval.right + rightMargin ) return interval;
			}
            
			return null;
		}
		
		@Override
		public int extent() {
			return firstIntervalIterator.extent();
		}
		
		public String toString() {
		   return getClass().getSimpleName() + "(" + firstIntervalIterator + ( leftMargin == 0 && rightMargin == 0 ? " <- " : " <- [[" + leftMargin + "," + rightMargin + "]] " ) + secondIntervalIterator + ")";
		}
	}
}
