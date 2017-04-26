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


import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;
import java.util.Collections;
                                                                                                                                                           
/** A class providing static methods and objects that do useful things with interval iterators. */

public class IntervalIterators {
	private IntervalIterators() {}
	
	/** An iterator used to instantiate singleton iterators such as {@link #TRUE} and {@link #FALSE}. The extent is 0.	 */
	protected static class FakeIterator implements IntervalIterator {
		private final boolean nonEmpty;
		private FakeIterator( final boolean nonEmpty ) { this.nonEmpty = nonEmpty; }
		@Override
		public IntervalIterator reset() { return this; }
		@Override
		public Interval nextInterval() { if ( ! nonEmpty ) new UnsupportedOperationException(); return null; }
		@Override
		public int extent() { return 0; } // TODO: this is not correct for FALSE (but it is unlikely to be a problem)
		@Override
		public void intervalTerms( final LongSet terms ) {} 
		@Override
		public String toString() { return this.getClass().getName() + "." + ( nonEmpty ? "TRUE" : "FALSE" ); }
	}
                                                                 
	/** A singleton iterator representing maximum truth.
	 * 
	 * <p>This iterator is a placeholder for an iterator returning just {@link Intervals#EMPTY_INTERVAL}.
	 * The antichain formed by the empty interval is the top element of the lattice of antichains, and
	 * thus represents the highest truth. Since, however, <code>EMPTY_INTERVAL</code>
	 * is a singleton that slightly violates the {@link Interval} invariants, an iterator actually
	 * returning <code>EMPTY_INTERVAL</code> would cause severe problems in all algorithms manipulating
	 * intervals. Rather, {@link #TRUE} is treated separately and is never actually used in
	 * an algorithm on interval antichains (also because, albeit it claims to have elements,
	 * it will return <code>null</code> on {@link IntervalIterator#nextInterval()}).
	 * 
	 * <P>A most natural appearance of {@link #TRUE} is due to negation: all documents satisfying
	 * a negative query return {@link #TRUE} as interval iterator, as the query is true, but you don't know where.
	 * 
	 * <P><strong>Warning</strong>: Before 4.0, an {@link IndexIterator} by convention would have returned {@link #TRUE} 
	 * when {@link DocumentIterator#intervalIterator(it.unimi.di.big.mg4j.index.Index)} 
	 * was called with an argument that was not the {@linkplain IndexIterator#index() key index}.
	 * Now it returns {@link #FALSE}.
	 */

	public final static IntervalIterator TRUE = new FakeIterator( true );
	
	/** A singleton empty interval iterator.
	 * 
	 * <P>The main usefulness of this iterator is as a singleton: in some circumstances you have
	 * to return an empty iterator, and since it is by definition stateless, it is a pity
	 * to create a new object (the same considerations led to {@link Collections#emptySet()}).
	 * 
	 * <P>This iterator is used by {@linkplain DocumentIterator document iterators} as a placeholder
	 * whenever {@link DocumentIterator#intervalIterator(it.unimi.di.big.mg4j.index.Index)} is called
	 * on an index for which there are not intervals.  For instance, an {@link IndexIterator} by convention returns {@link #FALSE} 
	 * when {@link DocumentIterator#intervalIterator(it.unimi.di.big.mg4j.index.Index)} 
	 * is called with an argument that is not the {@linkplain IndexIterator#index() key index}.
	 * Before 4.0, the same placeholder role was held by {@link #TRUE} instead.
	 */
	public final static IntervalIterator FALSE = new FakeIterator( false );

	/** Returns a set containing the intervals enumerated by the specified interval iterators: iteration
	 * on the set is guarantee to return the intervals in the same order in which they were enumerated.
	 * 
	 * @param intervalIterator an interval iterator.
	 * @return a set containing the intervals enumerated by {@code intervalIterator}. 
	 */
	public static ObjectSet<Interval> pour( final IntervalIterator intervalIterator ) throws IOException {
		ObjectLinkedOpenHashSet<Interval> set = new ObjectLinkedOpenHashSet<Interval>();
		for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) set.add( interval );
		return set;
	}
}
