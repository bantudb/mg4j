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

import static it.unimi.di.big.mg4j.index.IndexIterator.END_OF_POSITIONS;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;
import java.util.Arrays;

/** An iterator returning documents containing consecutive intervals (in query order) 
 * satisfying the underlying queries.
 * 
 * <p>As an additional service, this class makes it possible to specify <em>gaps</em> between
 * intervals. If gaps are specified, a match will satisfy the condition
 * that the left extreme of the first interval is larger than or equal to the
 * first gap, the left extreme of the second interval is equal to
 * the right extreme of the first interval plus the second gap plus one, 
 * the left extreme of the third interval is equal to the right extreme
 * of the second interval plus the third gap plus one and so on.  The standard
 * semantics corresponds thus to the everywhere zero gap array. That
 * the returned intervals <em>will contain the leftmost gap</em>, too. 
 * 
 * <p>This semantics
 * makes it possible to perform phrasal searches &ldquo;with holes&rdquo;, typically
 * because of stopwords that have not been indexed. Note that it is possible to specify
 * a gap <em>before</em> the first interval, but not <em>after</em> the last interval,
 * as in general the document length is not known at this level of query resolution.
 * 
 * <p>This class will handle correctly {@link IntervalIterators#TRUE TRUE} iterators; in this
 * case, the semantics is defined as follows: an interval is in the output if it is formed by the union of disjoint intervals,
 * one from each input list, and each gap of value <var>k</var> corresponds to <var>k</var> iterators
 * returning all document positions as singleton intervals. Since {@link IntervalIterators#TRUE TRUE} represents a list containing just
 * the empty interval, the result is equivalent to dropping {@link IntervalIterators#TRUE TRUE} iterators from the input; as
 * a consequence, the gap of a {@link IntervalIterators#TRUE TRUE} iterator is merged with that of the following iterator.
 * 
 * <p><strong>Warning</strong>: In case gaps are specified, the mathematically correct semantics would require that
 * gaps before {@link IntervalIterators#TRUE TRUE} iterators that are not followed by any non-{@link IntervalIterators#TRUE TRUE} iterators
 * have the effect of enlarging the resulting intervals <em>on the right side</em>. However,
 * this behaviour is very difficult to implement at this level because document lengths are not known. For this
 * reason, if one or more {@link IntervalIterators#TRUE TRUE} iterators appear a the end of the component iterator list they will be simply dropped.  
 */

public class ConsecutiveDocumentIterator extends AbstractOrderedIntervalDocumentIterator {
	/** Returns a document iterator that computes the consecutive AND of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>it</code> has zero length.
	 * @param documentIterator the iterators to be composed.
	 * @return a document iterator that computes the consecutive AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final Index index, final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) return TrueDocumentIterator.getInstance( index );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return new ConsecutiveDocumentIterator( documentIterator, null );
	}
	
	/** Returns a document iterator that computes the consecutive AND of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be composed (at least one).
	 * @return a document iterator that computes the consecutive AND of <code>documentIterator</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return getInstance( null, documentIterator );
	}
	
	/** Returns a document iterator that computes the consecutive AND of the given nonzero-length array of iterators, adding
	 * gaps between intervals.
	 * 
	 * <p>A match will satisfy the condition
	 * that the left extreme of the first interval is larger than or equal to the
	 * first gap, the left extreme of the second interval is larger than 
	 * the right extreme of the first interval plus the second gap, and so on. This semantics
	 * makes it possible to perform phrasal searches &ldquo;with holes&rdquo;, typically
	 * because of stopwords that have not been indexed.
	 * 
	 * @param documentIterator the iterators to be composed (at least one).
	 * @param gap an array of gaps parallel to <code>documentIterator</code>, or <code>null</code> for no gaps. 
	 * @return a document iterator that computes the consecutive AND of <code>documentIterator</code> using the given gaps.  
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final DocumentIterator documentIterator[], final int gap[] ) throws IOException {
		if ( gap != null && gap.length != documentIterator.length ) throw new IllegalArgumentException( "The number of gaps (" + gap.length + ") is not equal to the number of document iterators (" + documentIterator.length +")" );
		if ( documentIterator.length == 1 && ( gap == null || gap[ 0 ] == 0 ) ) return documentIterator[ 0 ];
		return new ConsecutiveDocumentIterator( documentIterator, gap );
	}
	
	protected ConsecutiveDocumentIterator( final DocumentIterator[] documentIterator, final int[] gap ) {
		super( gap, documentIterator );
	}

	protected IntervalIterator getIntervalIterator( final Index unused, final int n, final boolean allIndexIterators, final Object arg ) {
		// We need an actual gap array, even in the case of no gap, only for interval iterators.
		final int[] gap;
		if ( arg == null ) {
			if ( ! allIndexIterators ) gap = new int[ n ];
			else gap = null;
		}
		else gap = ((int[])arg).clone();
		if ( ! allIndexIterators ) return new ConsecutiveIntervalIterator( n, gap );
		// In this case, gap must be made cumulative
		if ( gap != null ) for( int i = 1; i < n; i++ ) gap[ i ] += gap[ i - 1 ] + 1;
		return new ConsecutiveIndexIntervalIterator( n, gap );
	}

	
	protected class ConsecutiveIntervalIterator extends AbstractCompositeIntervalIterator {
		private static final boolean DEBUG = false;
		/** A cached reference to the gap array. */
		private final int[] gap;
		/** The actual gaps. They depend on whether some {@link IntervalIterators#TRUE} iterator reduces the iterator array. */
		private final int[] actualGap;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** The number of non-{@link IntervalIterators#TRUE} interval iterator. */
		private int m;
		/** Whether the first result is ready. */
		private boolean firstReady;

		protected ConsecutiveIntervalIterator( final int n, final int[] gap ) {
			super( n );
			this.gap = gap;
			// The enlargement is made necessary by the filling long in reset().
			this.actualGap = new int[ n + 1 ];
		}

		public IntervalIterator reset() throws IOException {
			final int[] actualGap = this.actualGap;
			final int[] gap = this.gap;
			final IntervalIterator[] intervalIterator = this.intervalIterator;
			
			actualGap[ m = 0 ] = -1; // The first interval has actual gap zero if it has gap zero, so we compensate here for the increment below.

			int i;
			for( i = 0; i < n; i++ ) {
				actualGap[ m ] += gap[ i ]; // Accumulate gap
				
				if ( ( intervalIterator[ m ] = documentIterator[ i ].intervalIterator() ) != IntervalIterators.TRUE ) {
					actualGap[ m ]++; // If this interval iterator is real, add one.
					curr[ m ] = Intervals.MINUS_INFINITY;
					actualGap[ ++m ] = 0; // Prepare next gap.
				}
			}

			if ( m == 0 ) throw new IllegalStateException();			

			do	{
				curr[ 0 ] = intervalIterator[ 0 ].nextInterval(); 
			} while( curr[ 0 ] != null && curr[ 0 ].left < actualGap[ 0 ] );
			return ( ! ( endOfProcess = curr[ 0 ] == null ) ) && ( firstReady = align() ) ? this : IntervalIterators.FALSE;
		}
		
		public void intervalTerms( final LongSet terms ) {
			for( int i = m; i-- != 0; ) intervalIterator[ i ].intervalTerms( terms );
		}
		
		private boolean align() throws IOException {
			if ( DEBUG ) System.err.println( this + ".align()" );
			
			final Interval[] curr = this.curr;
			final int[] actualGap = this.actualGap;
			final IntervalIterator[] intervalIterator = this.intervalIterator;
			
			if ( DEBUG ) System.err.println( java.util.Arrays.asList( curr ) );
			int k = 0;

			while( k < m ) {
				for ( k = 1; k < m; k++ ) {
					while ( curr[ k ].left < curr[ k - 1 ].right + actualGap[ k ] )
						if ( ( curr[ k ] = intervalIterator[ k ].nextInterval() ) == null ) {
							endOfProcess = true;
							return false;
						}

					if ( curr[ k ].left > curr[ k - 1 ].right + actualGap[ k ] ) {
						if ( endOfProcess = ( ( curr[ 0 ] = intervalIterator[ 0 ].nextInterval() ) == null ) ) return false;
						break;
					} 
				}
			}
			
			return true;
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			if ( ! firstReady ) {
				if ( endOfProcess ) return null;

				if ( ( curr[ 0 ] = intervalIterator[ 0 ].nextInterval() ) == null ) {
					endOfProcess = true;
					return null;
				}

				if ( ! align() ) return null;
			}
			else firstReady = false;

			return Interval.valueOf( curr[ 0 ].left - actualGap[ 0 ], curr[ m - 1 ].right );
		}
		
		@Override
		public int extent() {
			int s = 0;
			for ( int i = m; i-- != 0; ) s += intervalIterator[ i ].extent() + actualGap[ i ];
			return s - m + 1;
		}
		
	}


	protected class ConsecutiveIndexIntervalIterator extends AbstractCompositeIndexIntervalIterator {
		private static final boolean DEBUG = false;
		/** A cached reference to the gap array. If <code>null</code>, there are no gaps, and we use a faster algorithm. */
		private final int[] gap;
		/** The first element of {@link #gap}, if {@link #gap} is not <code>null</code>; zero, otherwise. */
		private final int gap0;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** Whether the first result is ready. */
		private boolean firstReady;
		
		protected ConsecutiveIndexIntervalIterator( final int n, final int[] gap ) {
			super( n );
			this.gap = gap;
			this.gap0 = gap != null ? gap[ 0 ] : 0;
		}
		
		@Override
		public IntervalIterator reset() throws IOException {
			final int[] curr = this.curr;
			Arrays.fill( curr,  -1 );
			curr[ 0 ] = indexIterator[ 0 ].nextPosition() - gap0;
			firstReady = endOfProcess = false;
			
			if ( gap0 != 0 ) {
				// Go beyond the 0-th gap. This must be done just once.
				while ( curr[ 0 ] < 0 && ( curr[ 0 ] = indexIterator[ 0 ].nextPosition() ) != END_OF_POSITIONS ) curr[ 0 ] -= gap0;
				if ( endOfProcess = curr[ 0 ] == END_OF_POSITIONS ) return IntervalIterators.FALSE;
			}

			return ( firstReady = align() ) ? this : IntervalIterators.FALSE;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			for( int i = n; i-- != 0; ) terms.add( indexIterator[ i ].termNumber() );
		}
		
		private boolean align() throws IOException {
			if ( DEBUG ) System.err.println( this + ".align()" );
			if ( n == 1 ) return true;
			
			final int[] curr = this.curr;
			final IndexIterator[] indexIterator = ConsecutiveDocumentIterator.this.indexIterator;
			
			if ( gap != null ) {
				final int[] gap = this.gap;

				int k = 1, l = n <= 2 ? 0 : 2; // This is actually ( k + 1 ) % n
				int start = curr[ 0 ];
				for(;;) {
					// First, we try to align the k-th term.
					while ( ( curr[ k ] = indexIterator[ k ].nextPosition() ) < start + gap[ k ] );
					// If we exhaust the term positions, it's all over.
					if ( curr[ k ] == END_OF_POSITIONS ) {
						endOfProcess = true;
						return false;
					}
					curr[ k ] -= gap[ k ];
					// If we went beyond start + k, we must update start.
					if ( curr[ k ] > start ) start = curr[ k ];
					// All current normalised positions (curr[ x ] - gap[ x ]) are squeezed between start and curr[ l ].
					else if ( curr[ l ] == start ) return true; 
					k = l;

					if ( ++l == n ) l = 0;
				}
			}
			else {
				int k = 1, l = n <= 2 ? 0 : 2; // This is actually ( k + 1 ) % n
				int start = curr[ 0 ];
				for(;;) {
					// First, we try to align the k-th term.
					while ( ( curr[ k ] = indexIterator[ k ].nextPosition() ) < start + k );
					// If we exhaust the term positions, it's all over.
					if ( curr[ k ] == END_OF_POSITIONS ) {
						endOfProcess = true;
						return false;
					}
					curr[ k ] -= k;
					// If we went beyond start + k, we must update start.
					if ( curr[ k ] > start ) start = curr[ k ];
					// All current normalised positions (curr[ x ] - x) are squeezed between start and curr[ l ].
					else if ( curr[ l ] == start ) return true; 
					k = l;

					if ( ++l == n ) l = 0;
				}
			}
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			if ( firstReady ) firstReady = false;
			else {
				if ( endOfProcess ) return null;

				if ( ( curr[ 0 ] = indexIterator[ 0 ].nextPosition() ) != END_OF_POSITIONS ) curr[ 0 ] -= gap0;
				else {
					endOfProcess = true;
					return null;
				}
				
				if ( ! align() ) return null;
			}
			
			return gap != null ? Interval.valueOf( curr[ 0 ], curr[ 0 ] + gap[ n - 1 ] ) : Interval.valueOf( curr[ 0 ], curr[ 0 ] + n - 1 );
		}

		@Override
		public int extent() {
			return gap == null ? n : gap[ n - 1 ] + 1;
		}
	}
}
