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
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;
import java.util.Arrays;

/** An iterator returning documents containing nonoverlapping intervals in query order 
 * satisfying the underlying queries.
 * 
 * <p>In practice, this iterator implements <em>strictly ordered AND</em>, which is
 * satisfied when the subqueries are satisfied by nonoverlapping intervals in query order.
 */

public class OrderedAndDocumentIterator extends AbstractOrderedIntervalDocumentIterator {

	private static final boolean ASSERTS = false;
	
	/** Returns a document iterator that computes the ordered AND of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>it</code> has zero length.
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the ordered AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final Index index, final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) return TrueDocumentIterator.getInstance( index );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return new OrderedAndDocumentIterator( null, documentIterator );
	}
	
	/** Returns a document iterator that computes the ordered AND of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be joined (at least one).
	 * @return a document iterator that computes the ordered AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		return getInstance( null, documentIterator );
	}
	
	protected OrderedAndDocumentIterator( final Object arg, final DocumentIterator[] documentIterator ) {
		super( arg, documentIterator );
	}

	protected IntervalIterator getIntervalIterator( final Index unused, final int n, final boolean allIndexIterators, final Object unusedArg ) {
		if ( ASSERTS ) assert unused == soleIndex;
		if ( ASSERTS ) assert unusedArg == null;
		return allIndexIterators ? new OrderedAndIndexIntervalIterator( n ) : new OrderedAndIntervalIterator( n );
	}
		
	protected class OrderedAndIntervalIterator extends AbstractCompositeIntervalIterator {
		private final static boolean DEBUG = false;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** The index of the next list to be aligned (from 0 to {@link #m}). */
		private int toBeAligned;
		/** The number of non-{@link IntervalIterators#TRUE} interval iterator. Only
		 * elements with index smaller than this value are valid in {@link AbstractCompositeIntervalIterator#intervalIterator}. */
		private int m;
		/** The left extreme of the next interval to be returned, or -1 if a new interval should be computed. */
		private int nextLeft;
		/** The right extreme of the next interval to be returned, if {@link #nextLeft} is not -1. */
		private int nextRight;

		protected OrderedAndIntervalIterator( final int n ) {
			super( n );
		}
		
		public IntervalIterator reset() throws IOException {
			m = 0;
			toBeAligned = 1;
			endOfProcess = false;

			for( int i = 0; i < n; i++ ) {
				intervalIterator[ m ] = documentIterator[ i ].intervalIterator();
				if ( intervalIterator[ m ] != IntervalIterators.TRUE ) {
					if ( ASSERTS ) assert intervalIterator[ m ] != IntervalIterators.FALSE;
					curr[ m++ ] = Intervals.MINUS_INFINITY;
				}
			}

			if ( m == 0 ) throw new IllegalStateException();
			endOfProcess = ( curr[ 0 ] = intervalIterator[ 0 ].nextInterval() ) == null;
			advance();
			return nextLeft == -1 ? IntervalIterators.FALSE : this;
		}

		public void intervalTerms( final LongSet terms ) {
			for( int i = n; i-- != 0; ) intervalIterator[ i ].intervalTerms( terms );
		}

		public void advance() throws IOException {
			final Interval[] curr = this.curr;
			final IntervalIterator[] intervalIterator = this.intervalIterator;
			final int m = this.m;
			// We have to decrease leftOfLast to avoid overflows. Do not test it against Integer.MAX_VALUE.
			int nextLeft = Integer.MAX_VALUE, nextRight = Integer.MAX_VALUE, leftOfLast = Integer.MAX_VALUE - 1;

			int i = toBeAligned;

			for(;;) {
				if ( DEBUG ) System.err.println( "Current candidate: " + Interval.valueOf( nextLeft, nextRight ) );

				for(;;) {

					if ( curr[ i - 1 ].right >= leftOfLast - ( m - i - 1 ) ) {
						// If we're here the last interval we obtained is aligned, but it cannot completed to an alignment smaller than [nextLeft..nextRight]
						toBeAligned = i;
						if ( ASSERTS ) assert nextLeft != Integer.MAX_VALUE;
						this.nextLeft = nextLeft;
						this.nextRight = nextRight;
						return;
					}

					if ( i == m || curr[ i ].left > curr[ i - 1 ].right ) break;

					do { 
						if ( curr[ i ].right >= leftOfLast - ( m - i - 2 ) || ( curr[ i ] = intervalIterator[ i ].nextInterval() ) == null ) {
							toBeAligned = i;
							endOfProcess = curr[ i ] == null;
							if ( nextLeft == Integer.MAX_VALUE ) {
								this.nextLeft = -1;
								return;
							}
							this.nextLeft = nextLeft;
							this.nextRight = nextRight;
							return;
						}
					} while ( curr[ i ].left <= curr[ i - 1 ].right );
					
					i++;
				}
				
				nextLeft = curr[ 0 ].left;
				nextRight = curr[ m - 1 ].right;
				leftOfLast = curr[ m - 1 ].left;
				i = 1;
				
				if ( ( curr[ 0 ] = intervalIterator[ 0 ].nextInterval() ) == null ) {
					endOfProcess = true;
					this.nextLeft = nextLeft;
					this.nextRight = nextRight;
					return;
				}
			}
		}

		@Override
		public Interval nextInterval() throws IOException {
			if ( nextLeft == -1 ) {
				if ( endOfProcess ) return null;
				advance();
			}

			if ( nextLeft == -1 ) return null;
			final Interval result = Interval.valueOf( nextLeft, nextRight );
			nextLeft = -1;
			return result;
		}
		
		@Override
		public int extent() {
			int s = 0;
			for ( int i = m; i-- != 0; ) s += intervalIterator[ i ].extent();
			return s;
		}
	}
	
	protected class OrderedAndIndexIntervalIterator extends AbstractCompositeIndexIntervalIterator {
		private static final boolean DEBUG = false;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** The index of the next list to be aligned. */
		private int toBeAligned;
		/** The left extreme of the next interval to be returned, or -1 if a new interval should be computed. */
		private int nextLeft;
		/** The right extreme of the next interval to be returned, if {@link #nextLeft} is not -1. */
		private int nextRight;
		protected OrderedAndIndexIntervalIterator( final int n ) {
			super( n );
		}
		
		public IntervalIterator reset() throws IOException {
			final int[] curr = this.curr;

			Arrays.fill( curr, Integer.MIN_VALUE );
			toBeAligned = 1;
			endOfProcess = false;
			curr[ 0 ] = indexIterator[ 0 ].nextPosition();
			advance();
			return nextLeft == -1 ? IntervalIterators.FALSE : this;
		}
	
		public void intervalTerms( final LongSet terms ) {
			for( int i = n; i-- != 0; ) terms.add( indexIterator[ i ].termNumber() );
		}

		public void advance() throws IOException {
			// We have to decrease nextRight to avoid overflows. Do not test it against Integer.MAX_VALUE.
			int nextLeft = Integer.MAX_VALUE, nextRight = Integer.MAX_VALUE - 1;
			final int[] curr = this.curr;
			final int n = OrderedAndDocumentIterator.this.n;
			
			int i = toBeAligned;

			for(;;) {
				if ( DEBUG ) System.err.println( "Current candidate: " + Interval.valueOf( nextLeft, nextRight ) );
				for(;;) {
					if ( curr[ i - 1 ] >= nextRight - ( n - i - 1 ) ) {
						// If we're here the last position we obtained is aligned, but it cannot completed to an alignment smaller than [nextLeft..nextRight]
						toBeAligned = i;
						if ( ASSERTS ) assert nextLeft != Integer.MAX_VALUE;
						this.nextLeft = nextLeft;
						this.nextRight = nextRight;
						return;
					}

					// Note that in this particular case we must check that this is not the first iteration of the external loop
					if ( i == n || curr[ i ] > curr[ i - 1 ] ) break;
					
					do {
						// For singletons, curr[ i ] >= nextRight - ( n - i - 2 ) is always false here.
						if ( ASSERTS ) assert curr[ i ] < nextRight - ( n - i - 2 );
						if ( ( curr[ i ] = indexIterator[ i ].nextPosition() ) == IndexIterator.END_OF_POSITIONS ) {
							endOfProcess = true;
							if ( nextLeft == Integer.MAX_VALUE ) {
								this.nextLeft = -1;
								return;
							}
							this.nextLeft = nextLeft;
							this.nextRight = nextRight;
							return;
						}
					} while ( curr[ i ] <= curr[ i - 1 ] );
					
					i++;
				}
				
				nextLeft = curr[ 0 ];
				nextRight = curr[ n - 1 ];
				i = 1;
				
				if ( ( curr[ 0 ] = indexIterator[ 0 ].nextPosition() ) == IndexIterator.END_OF_POSITIONS ) {
					endOfProcess = true;
					this.nextLeft = nextLeft;
					this.nextRight = nextRight;
					return;
				}
			}
		}

		@Override
		public Interval nextInterval() throws IOException {
			if ( nextLeft == -1 ) {
				if ( endOfProcess ) return null;
				advance();
			}

			if ( nextLeft == -1 ) return null;
			final Interval result = Interval.valueOf( nextLeft, nextRight );
			nextLeft = -1;
			return result;
		}

		@Override
		public int extent() {
			return n;
		}
	}
}
