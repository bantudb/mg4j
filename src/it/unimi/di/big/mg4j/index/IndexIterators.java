package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.util.NoSuchElementException;

/** A class providing static methods and objects that do useful things with {@linkplain IndexIterator index iterators}. */

public class IndexIterators {

	protected IndexIterators() {}
	
	final public static IndexIterator[] EMPTY_ARRAY = {};
	
	/** Returns the positions of an index iterator as an array.
	 * 
	 * <p>Note that this method just iterates {@link IndexIterator#nextPosition()}. If the
	 * method has already been called, the positions that have been already returned will be missing.
	 * 
	 * @return an array containing the positions obtained by repeatedly calling {@link IndexIterator#nextPosition()}.
	 */
	public static int[] positionArray( final IndexIterator indexIterator ) throws IOException {
		IntArrayList l = new IntArrayList();
		for( int p; ( p = indexIterator.nextPosition() ) != IndexIterator.END_OF_POSITIONS; ) l.add( p );
		return l.toIntArray();
	}

	/** Writes the positions of an index iterator into an array; if the array is not large enough, it
	 * will be reallocated using {@link IntArrays#grow(int[], int)}.
	 * 
	 * <p>Note that this method just iterates {@link IndexIterator#nextPosition()}. If the
	 * method has already been called, the positions that have been already returned will be missing.
	 * 
	 * @return {@code position}, if it was large enough to contain all positions; a new, larger array otherwise.
	 */
	public static int[] positionArray( final IndexIterator indexIterator, int[] position ) throws IOException {
		for( int i = 0, p; ( p = indexIterator.nextPosition() ) != IndexIterator.END_OF_POSITIONS; ) {
			if ( i == position.length ) position = IntArrays.grow( position, position.length + 1 );
			position[ i++ ] = p;
		}
		
		return position;
	}

	protected static final class PositionsIterator extends AbstractIntIterator {
		private final IndexIterator indexIterator;
		private int next = -1;

		protected PositionsIterator( IndexIterator indexIterator ) {
			this.indexIterator = indexIterator;
		}

		@Override
		public boolean hasNext() {
			try {
				if ( next == -1 ) next = indexIterator.nextPosition();
				return next != IndexIterator.END_OF_POSITIONS;
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
		}

		public int nextInt() {
			if ( ! hasNext() ) throw new NoSuchElementException();
			final int result = next;
			next = -1;
			return result;
		}
	}

	/** Returns an {@link IntIterator} view of the positions of an index iterator.
	 * 
	 * <p>Note that this method just offers of a view of {@link IndexIterator#nextPosition()}. If the
	 * method has already been called, the positions that have been already returned will be missing.
	 * 
	 * @return an {@link IntIterator} view of the positions obtained by repeatedly calling {@link IndexIterator#nextPosition()}.
	 */
	public static IntIterator positionIterator( final IndexIterator indexIterator ) {
		return new PositionsIterator( indexIterator );
	}
}
