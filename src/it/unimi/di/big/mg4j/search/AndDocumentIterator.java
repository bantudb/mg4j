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
import it.unimi.dsi.fastutil.ints.IntHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;


/** A document iterator that returns the AND of a number of document iterators.
 *
 * <P>This class adds to {@link it.unimi.di.big.mg4j.search.AbstractIntersectionDocumentIterator}
 * an interval iterator generating the AND of the intervals returned for each of the documents involved. 
 */

public class AndDocumentIterator extends AbstractIntersectionDocumentIterator {
	private static final boolean ASSERTS = false;
	private final static boolean DEBUG = false;
	
	/** Returns a document iterator that computes the AND of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>documentIterator</code> has zero length.
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the AND of <code>documentIterator</code>. 
	 */
	public static DocumentIterator getInstance( final Index index, final DocumentIterator... documentIterator ) {
		if ( documentIterator.length == 0 ) return TrueDocumentIterator.getInstance( index );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return new AndDocumentIterator( documentIterator );
	}

	/** Returns a document iterator that computes the AND of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be joined (at least one).
	 * @return a document iterator that computes the AND of <code>documentIterator</code>. 
	 */
	public static DocumentIterator getInstance( final DocumentIterator... documentIterator ) {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		return getInstance( null, documentIterator );
	}

	protected AndDocumentIterator( final DocumentIterator[] documentIterator ) {
		super( null, documentIterator );
	}

	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object unused ) {
		return allIndexIterators ? new AndIndexIntervalIterator( index, n ) : new AndIntervalIterator( index, n );	
	}

	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		currentIterators.clear();
		return curr = align( lastIterator.skipTo( n ) );
	}
	
	public long nextDocument() throws IOException {
		/* Due to our superclass setting curr to END_OF_LIST when
		 * an iterator returns false on mayHaveNext(), we might 
		 * have still to return END_OF_LIST even if curr is END_OF_LIST. */
		assert lastIterator.document() != END_OF_LIST;
		currentIterators.clear();
		return curr = align( lastIterator.nextDocument() );
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		final Iterator<Index> i = indices.iterator();
		while( i.hasNext() ) intervalIterator( i.next() );
		return unmodifiableCurrentIterators;
	}

	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );

		IntervalIterator intervalIterator;

		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( soleIndex ) ) != null ) return intervalIterator;

		int t = 0, f = 0;
			
		/* We count the number of TRUE and FALSE iterators. In the case of index iterators, we can avoid 
		 * the check and just rely on the index internals.
		 * 
		 * If all iterators are FALSE, we return FALSE. Else if all remaining iterators are TRUE
		 * we return TRUE. 
		 */
		IntervalIterator soleComponentIterator = null;
		if ( indexIterator == null ) {
			for( int i = n; i-- != 0; ) {
				intervalIterator = documentIterator[ i ].intervalIterator();
				if ( intervalIterator == IntervalIterators.TRUE ) t++;
				else if ( intervalIterator == IntervalIterators.FALSE ) f++;
				else soleComponentIterator = intervalIterator;
			}
			if ( f == n ) intervalIterator = IntervalIterators.FALSE;
			else if ( t + f == n ) intervalIterator = IntervalIterators.TRUE;
			else if ( t + f < n - 1 ) intervalIterator = soleIntervalIterator.reset();
			else intervalIterator = soleComponentIterator;
		}
		else {
			if ( indexIteratorsWithoutPositions == n ) intervalIterator = IntervalIterators.TRUE;
			else intervalIterator = soleIntervalIterator.reset();
		}

		currentIterators.put( soleIndex, intervalIterator );	
		return intervalIterator;
	}

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );

		if ( ! indices.contains( index ) ) return IntervalIterators.FALSE;

		IntervalIterator intervalIterator;

		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( index ) ) != null ) return intervalIterator;

		int t = 0, f = 0;
			
		/* We count the number of TRUE and FALSE iterators. In the case of index iterators, we can avoid 
		 * the check and just rely on the index internals.
		 * 
		 * If all iterators are FALSE, we return FALSE. Else if all remaining iterators are TRUE
		 * we return TRUE. 
		 */
		if ( indexIterator == null ) 
			for( int i = n; i-- != 0; ) {
				intervalIterator = documentIterator[ i ].intervalIterator( index );
				if ( intervalIterator == IntervalIterators.TRUE ) t++;
				if ( intervalIterator == IntervalIterators.FALSE ) f++;
			}
		else
			for( int i = n; i-- != 0; ) {
				final Index indexIteratorIndex = indexIterator[ i ].index();
				if ( indexIteratorIndex != index ) f++;
				else if ( ! indexIterator[ i ].index().hasPositions ) t++;
			}
		
		// Note that we cannot optimise the case n - t - f == 1 because of gaps in ConsecutiveDocumentIterator.
		if ( f == n ) intervalIterator = IntervalIterators.FALSE;
		else if ( t + f == n ) intervalIterator = IntervalIterators.TRUE;
		else intervalIterator = intervalIterators.get( index ).reset();

		currentIterators.put( index, intervalIterator );	
		return intervalIterator;
	}

	/** An interval iterator returning the AND (in the Clarke&minus;Cormack&minus;Burkowski lattice) of the component interval iterators. */
	
	protected class AndIntervalIterator extends AbstractCompositeIntervalIterator implements IntervalIterator {
		/** The index of this iterator. */
		final private Index index;
		/** A heap-based indirect priority queue used to keep track of the currently scanned intervals. */
		final private ObjectHeapSemiIndirectPriorityQueue<Interval> queue;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** The left extreme of the last returned interval, or {@link Integer#MIN_VALUE} after a {@link #reset()}. */
		private int lastLeft;
		/** The maximum right extreme currently in the queue. */
		private int maxRight;

		/** Creates a new AND interval iterator.
		 * 
		 * @param index the index of the iterator.
		 * @param n the number of component iterators.
		 */
		protected AndIntervalIterator( final Index index, final int n ) {
			super( n );
			// We just set up some internal data, but we perform no initialisation.
			this.index = index;
			queue = new ObjectHeapSemiIndirectPriorityQueue<Interval>( curr, Intervals.STARTS_BEFORE_OR_PROLONGS );
		}


		public IntervalIterator reset() throws IOException {
			Arrays.fill( curr, null );
			queue.clear();
			maxRight = Integer.MIN_VALUE;
			lastLeft = Integer.MIN_VALUE;
			endOfProcess = false;
			
			for ( int i = 0; i < n; i++ ) {
				intervalIterator[ i ] = documentIterator[ i ].intervalIterator( index );
				// TRUE and FALSE iterators are simply skipped.
				if ( intervalIterator[ i ] != IntervalIterators.TRUE && intervalIterator[ i ] != IntervalIterators.FALSE ) {
					curr[ i ] = intervalIterator[ i ].nextInterval();
					queue.enqueue( i );
					maxRight = Math.max( maxRight, curr[ i ].right );
				}
			}

			if ( ASSERTS ) assert ! queue.isEmpty();
			return this;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			for( int i = n; i-- != 0; ) intervalIterator[ i ].intervalTerms( terms );
		}


		@Override
		public Interval nextInterval() throws IOException {
			if ( endOfProcess ) return null;

			int first;
			while ( curr[ first = queue.first() ].left == lastLeft ) {
				if ( ( curr[ first ] = intervalIterator[ first ].nextInterval() ) == null ) {
					endOfProcess = true;
					return null;
				}
				maxRight = Math.max( maxRight, curr[ first ].right );
				queue.changed();
			}

			int nextLeft, nextRight;
			
			do {
				nextLeft = curr[ first = queue.first() ].left;
				nextRight = maxRight;
				if ( DEBUG ) System.err.println( this + " is saving interval " + Interval.valueOf( nextLeft, nextRight ) );

				/* We check whether the current top is equal to the span of the queue, and
				 * whether the top interval iterator is exhausted. In both cases, the
				 * current span is guaranteed to be a minimal interval. */
				if ( curr[ first ].right == maxRight || ( endOfProcess = ( curr[ first ] = intervalIterator[ first ].nextInterval() ) == null ) ) break; 
				maxRight = Math.max( maxRight, curr[ first ].right );
				queue.changed();
			} while ( maxRight == nextRight );
			
			return Interval.valueOf( lastLeft = nextLeft, nextRight );
		}
		
		@Override
		public int extent() {
			int s = 0;
			for ( int i = n; i-- != 0; ) s += intervalIterator[ i ].extent();
			return s;
		}
	}


	/** An interval iterator returning the AND (in the Clarke&minus;Cormack&minus;Burkowski lattice) of the component interval iterators. */
	
	protected class AndIndexIntervalIterator extends AbstractCompositeIndexIntervalIterator implements IntervalIterator {
		/** The index of this iterator. */
		final private Index index;
		/** A heap-based indirect priority queue used to keep track of the currently scanned positions. */
		final private IntHeapSemiIndirectPriorityQueue queue;
		/** Whether the scan is over. */
		private boolean endOfProcess;
		/** The left extreme of the last returned interval, or {@link Integer#MIN_VALUE} after a {@link #reset()}. */
		private int lastLeft;
		/** The maximum right extreme currently in the queue. */
		private int maxRight;

		/** Creates a new AND interval iterator.
		 * 
		 * @param index the index of the iterator.
		 * @param n the number of component iterators.
		 */
		protected AndIndexIntervalIterator( final Index index, final int n ) {
			super( n );
			// We just set up some internal data, but we perform no initialisation.
			this.index = index;
			queue = new IntHeapSemiIndirectPriorityQueue( curr );
		}

		@Override
		public IntervalIterator reset() throws IOException {
			queue.clear();
			maxRight = Integer.MIN_VALUE;
			lastLeft = Integer.MIN_VALUE;
			endOfProcess = false;
			
			for ( int i = 0; i < n; i++ ) {
				// The case != index is identical to the TRUE/FALSE case in AndIntervalIterator.
				final Index indexIteratorIndex = indexIterator[ i ].index();
				if ( indexIteratorIndex == index && indexIteratorIndex.hasPositions ) {
					curr[ i ] = indexIterator[ i ].nextPosition();
					queue.enqueue( i );
					maxRight = Math.max( maxRight, curr[ i ] );
				}
			}

			if ( ASSERTS ) assert ! queue.isEmpty();
			return this;
		}
		
		@Override
		public void intervalTerms( final LongSet terms ) {
			for( int i = n; i-- != 0; ) terms.add( indexIterator[ i ].termNumber() );
		}

		@Override
		public Interval nextInterval() throws IOException {
			if ( endOfProcess ) return null;

			int first;
			while ( curr[ first = queue.first() ] == lastLeft ) {
				if ( ( curr[ first ] = indexIterator[ first ].nextPosition() ) == IndexIterator.END_OF_POSITIONS ) {
					endOfProcess = true;
					return null;
				}
				maxRight = Math.max( maxRight, curr[ first ] );
				queue.changed();
			}
		
			int nextLeft, nextRight;
			
			do {
				nextLeft = curr[ first = queue.first() ];
				nextRight = maxRight;
				if ( DEBUG ) System.err.println( this + " is saving interval " + Interval.valueOf( nextLeft, nextRight ) );

				/* We check whether all iterators are on the same position, and
				 * whether the top interval iterator is exhausted. In both cases, the
				 * current span is guaranteed to be a minimal interval. */
				if ( curr[ first ] == maxRight || ( endOfProcess = ( curr[ first ] = indexIterator[ first ].nextPosition() ) == IndexIterator.END_OF_POSITIONS ) ) break; 

				if ( maxRight < curr[ first ] ) maxRight = curr[ first ];
				queue.changed();
			} while ( maxRight == nextRight );
			
			return Interval.valueOf( lastLeft = nextLeft, nextRight );
		}

		@Override
		public int extent() {
			return n;
		}
	}
}
