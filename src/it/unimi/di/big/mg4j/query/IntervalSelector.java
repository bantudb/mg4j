package it.unimi.di.big.mg4j.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.SelectedInterval.IntervalType;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;

/** A strategy for selecting reasonable intervals to be shown to the user.
 * 
 * <p>MG4J returns for each query and each document a list of minimal intervals satisfying the query.
 * Due to overlaps and long intervals, this list is not always the best way to show the result of
 * a query to the user. Instances of this class select intervals using two parameters 
 * (maximum number of intervals and maximum interval length) and the following algorithm: intervals enqueued in 
 * a queue ordered by length;
 * then, they are extracted from the queue and added greedily to the result set as long as they do not
 * overlap any other interval already in the result set, they are not longer than the maximum length,
 * and the result set contains less intervals than the maximum allowed.
 * 
 * <p>If all intervals are longer than the maximum allowed length, then from the shorter interval
 * we extract two new intervals long as half of the maximum allowed length and 
 * sharing the left and right extreme, respectively, with the original interval.
 *
 * <p><strong>Warning</strong>: implementations of this class are not required
 * to be thread-safe, but they provide {@linkplain it.unimi.dsi.lang.FlyweightPrototype flyweight copies}
 * (actually, just copies, as no internal state is shared, but we implement the interface for consistency
 * with the rest of the components used by a {@link it.unimi.di.big.mg4j.query.QueryEngine}).
 * The {@link #copy()} method is strengthened so to return an object implementing this interface.
 */
public class IntervalSelector implements FlyweightPrototype<IntervalSelector> {
	/** An array containing the sentinels for {@link #leftOrderedIntervals}. */
	private final static SelectedInterval[] INIT = { new SelectedInterval( Interval.valueOf( -1 ), null), new SelectedInterval( Interval.valueOf( Integer.MAX_VALUE ), null ) };

	/** Maximum number of text intervals that will be selected. */
	private final int maxIntervals;
	/** Maximum length of a marked interval. */
	private final int intervalMaxLength;
	/** A map used to order the intervals by their left extreme. Intervals in this set are always pairwise disjoint. 
	 * Two fake intervals that are outside the document interval range are used as sentinel to reduce the
	 * number of special cases. This map must be kept empty. */
	private final ObjectRBTreeSet<SelectedInterval> leftOrderedIntervals = new ObjectRBTreeSet<SelectedInterval>();
	/** A list used to pour iterators. */
	private final ObjectArrayList<Interval> intervals = new ObjectArrayList<Interval>();
	
	/** Creates a new selector that selects all intervals. */
	public IntervalSelector() {
		this( Integer.MIN_VALUE, Integer.MIN_VALUE );
	}
		
	/** Creates a new selector.
	 * 
	 * @param maxIntervals the maximum number of intervals returned by the selector.
	 * @param intervalMaxLength the maximum length of an interval returned by the selector.
	 */
	public IntervalSelector( final int maxIntervals, final int intervalMaxLength ) {
		this.maxIntervals = maxIntervals;
		this.intervalMaxLength = intervalMaxLength;
	}
	
	public IntervalSelector copy() {
		return new IntervalSelector( maxIntervals, intervalMaxLength );
	}


	/** Selects intervals from an interval iterator.
	 * 
	 * @param intervalIterator an iterator returning intervals.
	 * @return an array containing the selected intervals; the special empty arrays {@link SelectedInterval#TRUE_ARRAY}
	 * and {@link SelectedInterval#FALSE_ARRAY} are returned for {@link IntervalIterators#TRUE}
	 * and {@link IntervalIterators#FALSE}, respectively.
	 */
	public SelectedInterval[] select( final IntervalIterator intervalIterator ) throws IOException {
		if ( intervalIterator == IntervalIterators.TRUE ) return SelectedInterval.TRUE_ARRAY;
		if ( intervalIterator == IntervalIterators.FALSE ) return SelectedInterval.FALSE_ARRAY;

		/* First of all, we pour all intervals in a list, and use the elements 
		 * of the list to initialise a queue ordered by interval length. */
		intervals.clear();
		for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) intervals.add( interval );

		// Special case--we let out all intervals.
		if ( maxIntervals == Integer.MIN_VALUE && intervalMaxLength == Integer.MIN_VALUE ) {
			SelectedInterval result[] = new SelectedInterval[ intervals.size() ];
			for( int i = intervals.size(); i-- != 0; ) result[ i ] = new SelectedInterval( intervals.get( i ), IntervalType.WHOLE );
			return result;
		}

		ObjectHeapPriorityQueue<Interval> shortIntervals = new ObjectHeapPriorityQueue<Interval>( intervals.toArray( Intervals.EMPTY_ARRAY ), intervals.size(), Intervals.LENGTH_COMPARATOR );
		
		/* We reset the interval set, add the sentinels and the (certainly existing) first interval. 
		 * If the first interval is too long, we shorten it. */
		leftOrderedIntervals.add( INIT[ 0 ] );
		leftOrderedIntervals.add( INIT[ 1 ] );

		Interval interval;
		interval = shortIntervals.dequeue();
		if ( interval.length() < intervalMaxLength ) leftOrderedIntervals.add( new SelectedInterval( interval, IntervalType.WHOLE ) );
		else {
			leftOrderedIntervals.add( new SelectedInterval( Interval.valueOf( interval.left, interval.left + intervalMaxLength / 2 ), IntervalType.PREFIX ) );
			leftOrderedIntervals.add( new SelectedInterval( Interval.valueOf( interval.right - intervalMaxLength / 2, interval.right ), IntervalType.SUFFIX ) );
		}

		/* We now iteratively extract intervals from the queue, check that they do not overlap
		 * any interval already chosen, and in case add it to the set of chosen intervals. */
		//System.err.println( "Starting with " + shortIntervals.size() + " intervals");
		
		ObjectBidirectionalIterator<SelectedInterval> iterator;
		SelectedInterval left, right;
		while( leftOrderedIntervals.size() - INIT.length < maxIntervals && ! shortIntervals.isEmpty() ) {
			//System.err.println( "Map is now: " + leftOrderedIntervals ); 
			interval = shortIntervals.dequeue();
			// If all remaining intervals are too large, stop iteration.
			if ( interval.length() > intervalMaxLength ) break;
			
			// This iterator falls exactly in the middle of the intervals preceding and following interval.
			iterator = leftOrderedIntervals.iterator( new SelectedInterval( interval, null ) );
			iterator.previous();
			left = iterator.next();
			right = iterator.next();
			//System.err.println( "Testing " + interval + " against " + left + " and " + right );
			if ( interval.left > left.interval.right && interval.right < right.interval.left ) leftOrderedIntervals.add( new SelectedInterval( interval, IntervalType.WHOLE ) );
			//System.err.println( "Completed test; Map is now: " + leftOrderedIntervals ); 
		}
		
		iterator = leftOrderedIntervals.iterator();
		iterator.next();
		SelectedInterval[] result = new SelectedInterval[ leftOrderedIntervals.size() - INIT.length ];
		ObjectIterators.unwrap( iterator, result );
		leftOrderedIntervals.clear();
		return result;
	}

	/** Selects intervals from a document iterator.
	 * 
	 * <p>Intervals will be gathered using the interval iterators returned
	 * by the document iterator for the current document.
	 * 
	 * @param documentIterator a document iterator positioned over a document, with
	 * callable {@link DocumentIterator#intervalIterator(Index)} methods for all indices.
	 * @param index2Interval a map that will be cleared and fill with associations from
	 * indices to arrays of selected intervals; the special empty arrays {@link SelectedInterval#TRUE_ARRAY}
	 * and {@link SelectedInterval#FALSE_ARRAY} are returned for {@link IntervalIterators#TRUE}
	 * and {@link IntervalIterators#FALSE}, respectively.
	 * @return <code>index2Interval</code>.
	 * @throws IOException 
	 */
	public Reference2ObjectMap<Index,SelectedInterval[]> select( final DocumentIterator documentIterator, final Reference2ObjectMap<Index,SelectedInterval[]> index2Interval ) throws IOException {
		index2Interval.clear();
		IntervalIterator intervalIterator;
		for( Index index : documentIterator.indices() ) {
			if ( index.hasPositions ) {
				intervalIterator = documentIterator.intervalIterator( index );
				if ( intervalIterator == IntervalIterators.TRUE ) index2Interval.put( index, SelectedInterval.TRUE_ARRAY );
				else index2Interval.put( index, select( documentIterator.intervalIterator( index ) ) );
			}
		}
		return index2Interval;
	}
}
