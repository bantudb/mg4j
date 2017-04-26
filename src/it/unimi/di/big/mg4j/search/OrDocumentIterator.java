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
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;

/** An iterator on documents that returns the OR of a number of document iterators.
*
* <P>This class adds to {@link it.unimi.di.big.mg4j.search.AbstractUnionDocumentIterator}
* an interval iterator generating the OR of the intervals returned for each of the documents involved. 
*/

public class OrDocumentIterator extends AbstractUnionDocumentIterator implements DocumentIterator {
	private final static boolean DEBUG = false;

	
	/** Returns a document iterator that computes the OR of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>documentIterator</code> has zero length.
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the OR of <code>documentIterator</code>. 
	 */
	public static DocumentIterator getInstance( final Index index, DocumentIterator... documentIterator  ) {
		if ( documentIterator.length == 0 ) return FalseDocumentIterator.getInstance( index );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return new OrDocumentIterator( documentIterator );
	}

	/** Returns a document iterator that computes the OR of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the OR of <code>documentIterator</code>. 
	 */
	public static DocumentIterator getInstance( DocumentIterator... documentIterator  ) {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		return getInstance( null, documentIterator );
	}

	protected OrDocumentIterator( final DocumentIterator... documentIterator ) {
		super( documentIterator );
	}

	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object unused ) {
		return allIndexIterators ? new OrIndexIntervalIterator( index, n ) : new OrIntervalIterator( index, n );
	}

	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );

		IntervalIterator intervalIterator;

		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( soleIndex ) ) != null ) return intervalIterator;

		int t = 0, f = 0, c = computeFront();
	
		/* We count the number of TRUE and FALSE iterators. In the case of index iterators, we can avoid 
		 * the check and just rely on the index internals.
		 * 
		 * If all iterators are FALSE, we return FALSE. Else if all remaining iterators are TRUE
		 * we return TRUE. 
		 */
		IntervalIterator soleComponentIterator = null;
		if ( indexIterator == null ) { 
			for( int i = c; i -- != 0; ) {
				intervalIterator = documentIterator[ front[ i ] ].intervalIterator();
				if ( intervalIterator == IntervalIterators.TRUE ) t++;
				else if ( intervalIterator == IntervalIterators.FALSE ) f++;
				else soleComponentIterator = intervalIterator;
			}
			
			if ( f == c ) intervalIterator = IntervalIterators.FALSE;
			else if ( f + t == c ) intervalIterator = IntervalIterators.TRUE;
			else if ( f + t < c - 1 ) intervalIterator = soleIntervalIterator.reset();
			else intervalIterator = soleComponentIterator;
		}
		else {
			for( int i = c; i -- != 0; ) {
				final IndexIterator ii = indexIterator[ front[ i ] ];
				if ( ! ii.index().hasPositions ) t++;
				else soleComponentIterator = ii.intervalIterator();
			}
			if ( t == c ) intervalIterator = IntervalIterators.TRUE;
			else if ( f + t < c - 1 ) intervalIterator = soleIntervalIterator.reset();
			else intervalIterator = soleComponentIterator;

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

		int t = 0, f = 0, c = computeFront();
	
		/* We count the number of TRUE and FALSE iterators. In the case of index iterators, we can avoid 
		 * the check and just rely on the index internals.
		 * 
		 * If all iterators are FALSE, we return FALSE. Else if all remaining iterators are TRUE
		 * we return TRUE. 
		 */
		IntervalIterator soleComponentIterator = null;
		if ( indexIterator == null ) 
			for( int i = c; i -- != 0; ) {
				intervalIterator = documentIterator[ front[ i ] ].intervalIterator( index );
				if ( intervalIterator == IntervalIterators.TRUE ) t++;
				else if ( intervalIterator == IntervalIterators.FALSE ) f++;
				else soleComponentIterator = intervalIterator;
			}
		else
			for( int i = c; i -- != 0; ) {
				final IndexIterator ii = indexIterator[ front[ i ] ];
				if ( ii.index() != index ) f++;
				else if ( ! ii.index().hasPositions ) t++;
				else soleComponentIterator = ii.intervalIterator( index );
			}

		if ( f == c ) intervalIterator = IntervalIterators.FALSE;
		else if ( f + t == c ) intervalIterator = IntervalIterators.TRUE;
		else if ( f + t < c - 1 ) intervalIterator = intervalIterators.get( index ).reset();
		else intervalIterator = soleComponentIterator;
				
		currentIterators.put( index, intervalIterator );	
		return intervalIterator;
	}

	protected class OrIntervalIterator extends AbstractCompositeIntervalIterator {
		/** The index of this iterator. */
		final Index index;
		/** A heap-based indirect priority queue used to keep track of the currently scanned intervals. */
		private ObjectHeapSemiIndirectPriorityQueue<Interval> intervalQueue;
		/** The left extreme of the last returned interval, or {@link Integer#MIN_VALUE} after a {@link #reset()}. */
		private int lastLeft;
		/** An array to hold the front of the interval queue. */
		private final int[] intervalFront;

		protected OrIntervalIterator( final Index index, final int n ) {
			super( n );
			// We just set up some internal data, but we perform no initialisation.
			this.index = index;
			intervalQueue = new ObjectHeapSemiIndirectPriorityQueue<Interval>( curr, Intervals.ENDS_BEFORE_OR_IS_SUFFIX );
			intervalFront = new int[ n ];
		}

		public IntervalIterator reset() throws IOException {
			lastLeft = Integer.MIN_VALUE;
			intervalQueue.clear();

			for ( int i = computeFront(), k; i-- != 0; ) {
				k = front[ i ];
				intervalIterator[ k ] = documentIterator[ k ].intervalIterator( index );
				if ( intervalIterator[ k ] != IntervalIterators.TRUE && ( curr[ k ] = intervalIterator[ k ].nextInterval() ) != null ) intervalQueue.enqueue( k );
			}
			
			return this;
		}

		public void intervalTerms( final LongSet terms ) {
			final int frontSize = intervalQueue.front( intervalFront );
			final int[] intervalFront = this.intervalFront;
			for( int i = frontSize; i-- != 0; ) intervalIterator[ intervalFront[ i ] ].intervalTerms( terms );
		}
		
		public Interval nextInterval () throws IOException {
			if ( intervalQueue.isEmpty() ) return null;

			int first;

			while ( curr[ first = intervalQueue.first() ].left <= lastLeft ) {
				if ( ( curr[ first ] = intervalIterator[ first ].nextInterval() ) != null ) intervalQueue.changed();
				else {
					intervalQueue.dequeue();
					if ( intervalQueue.isEmpty() ) return null;
				}
			} 

			lastLeft = curr[ first ].left;
			return curr[ first ];
		}

		@Override
		public int extent() {
			int e = Integer.MAX_VALUE;
			for ( int i = computeFront(), k; i-- != 0; ) {
				k = front[ i ];
				if ( curr[ k ] != null ) e = Math.min( e, intervalIterator[ k ].extent() );
			}
			return e;
		}
	}
	
	protected class OrIndexIntervalIterator extends AbstractCompositeIndexIntervalIterator implements IntervalIterator {
		@SuppressWarnings({ "hiding" })
		private final static boolean DEBUG = false;
		private final static boolean ASSERTS = false;

		/** The index of this iterator. */
		final Index index;
		/** A heap-based semi-indirect priority queue used to keep track of the currently scanned positions. */
		private final IntHeapSemiIndirectPriorityQueue positionQueue;
		/** An array to hold the front of the position queue. */
		private final int[] positionFront;
		
		protected OrIndexIntervalIterator( final Index index, final int n ) {
			super( n );
			this.index = index;
			positionQueue = new IntHeapSemiIndirectPriorityQueue( curr );
			positionFront = new int[ n ]; 
		}

		public IntervalIterator reset() throws IOException {
			positionQueue.clear();

			for ( int i = computeFront(), k; i-- != 0; ) {
				k = front[ i ];
				if ( indexIterator[ k ].index() == index && indexIterator[ k ].index().hasPositions ) {
					curr[ k ] = indexIterator[ k ].nextPosition();
					if ( DEBUG ) System.err.println( this + ".reset(): enqueueing " + k );
					positionQueue.enqueue( k );
				}
			}
			
			if ( ASSERTS ) assert ! positionQueue.isEmpty();
			return this;
		}

		public void intervalTerms( final LongSet terms ) {
			final int frontSize = positionQueue.front( positionFront );
			final int[] positionFront = this.positionFront;
			for( int i = frontSize; i-- != 0; ) terms.add( indexIterator[ positionFront[ i ] ].termNumber() );
		}
		
		public Interval nextInterval() throws IOException {
			if ( positionQueue.isEmpty() ) return null;
			
			int first = positionQueue.first();
			final int result = curr[ first ];
			
			do
				if ( ( curr[ first ] = indexIterator[ first ].nextPosition() ) == IndexIterator.END_OF_POSITIONS ) positionQueue.dequeue();
				else positionQueue.changed();
			while ( ! positionQueue.isEmpty() && curr[ first = positionQueue.first() ] == result );

			if ( DEBUG ) System.err.println( this + ".nextInterval() => " + Interval.valueOf( result ) );
			return Interval.valueOf( result );
		}

		@Override
		public int extent() {
			return 1;
		}
 	}

	public void dispose() throws IOException {
		for( DocumentIterator d: documentIterator ) d.dispose();
	}
}
