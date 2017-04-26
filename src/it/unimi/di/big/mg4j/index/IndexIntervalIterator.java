package it.unimi.di.big.mg4j.index;

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

import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** An interval iterator returning the positions of the current document as singleton intervals. */
public final class IndexIntervalIterator implements IntervalIterator {
	private final IndexIterator indexIterator;
	
	public IndexIntervalIterator( final IndexIterator indexIterator ) {
		this.indexIterator = indexIterator;
	}

	/** A no-op.
	 * 
	 * @return this iterator.
	 */
	@Override
	public IndexIntervalIterator reset() throws IOException {
		return this;
	}
	
	@Override
	public void intervalTerms( final LongSet terms ) {
		terms.add( indexIterator.termNumber() );
	}
	
	@Override
	public Interval nextInterval() throws IOException {
		final int nextPosition = indexIterator.nextPosition();
		return nextPosition == IndexIterator.END_OF_POSITIONS ? null : Interval.valueOf( nextPosition );
	}
	
	@Override
	public int extent() {
		return 1;
	}
}
