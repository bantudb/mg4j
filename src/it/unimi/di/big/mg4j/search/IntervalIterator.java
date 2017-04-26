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

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
                                                                                                                                                    
/** An iterator over {@linkplain Interval intervals}.
 * 
 * <p>An interval iterator is lazy&mdash;it has just a {@link #nextInterval()} method and 
 * no {@code hasNext()} method.
 *  
 * <P>This interface specifies a method {@link #extent()} returning
 * a positive integer that is supposed to approximate the minimum possible
 * length of an interval returned by this iterator. This method returns -1
 * if this value cannot be computed.
 */

public interface IntervalIterator {

	/** Resets the internal state of this iterator for a new document. 
	 *
	 * <P>To reduce object creation, interval iterators are usually created in a lazy
	 * fashion by document iterator when they are needed. However, this implies that
	 * every time the document iterator is moved, some internal state of the interval iterator must be reset
	 * (e.g., because on the new document some of the component interval iterators are now
	 * {@link IntervalIterators#TRUE}). The semantics of this method is largely implementation dependent,
	 * with the important exception that the return value must be {@link IntervalIterators#FALSE} if 
	 * the first call to {@link #nextInterval()} will return {@code null}.
	 * 
	 * @return {@link IntervalIterators#FALSE} if this interval iterator will return {@code null} at the
	 * first call to {@link #nextInterval()}; this interval iterator, otherwise.
	 */
	public IntervalIterator reset() throws IOException;       
	
	/** Returns an approximation of a lower bound for the length of an interval
	 *  returned by this iterator.
 	 *
	 *  @return an approximation of a lower bound for the length of an interval.
	 */
	public int extent();
	
	/** Returns the next interval provided by this interval iterator, or <code>null</code> if no more intervals are available.
	 * 
	 * @return the next interval, or <code>null</code> if no more intervals are available.
	 */
	public Interval nextInterval() throws IOException;

	/** Provides the set of terms that span the current interval.
	 * 
	 * <p>For each interval returned by MG4J, there is a set of terms that caused the interval to be returned.
	 * The terms appear inside the interval, and certainly at its extremes.
	 * 
	 * <p>Note that the results of this method must be taken with a grain of salt: there might be different sets of terms
	 * causing the current interval, and only one will be returned. 
	 * 
	 * @param terms a set of integers that will be filled with the terms spanning the current interval.
	 */
	public void intervalTerms( LongSet terms );
}
