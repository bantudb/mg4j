package it.unimi.di.big.mg4j.mock.search;

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
import it.unimi.di.big.mg4j.search.DocumentIterator;



public class TrueDocumentIterator extends MockDocumentIterator {

	private final Index index;

	/** Creates a NOT document iterator over a given iterator.
	 *
	 * @param index the reference index.
	 */
	protected TrueDocumentIterator( final Index index ) {
		this.index = index;
		indices.add( index );
		for ( int i = 0; i < index.numberOfDocuments; i++ ) addTrueIteratorDocument( i, index );
		start( true );
	}

	/** Returns a TRUE document iterator over a given iterator.
	 * @param index the reference index.
	 */
	public static DocumentIterator getInstance( final Index index ) {
		return new TrueDocumentIterator( index );
	}

	public String toString() {
	   return this.getClass().getSimpleName() + "(" + index + ")";
	}
}
