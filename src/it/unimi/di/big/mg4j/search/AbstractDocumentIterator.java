package it.unimi.di.big.mg4j.search;

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


/** An abstract iterator on documents that and 
 * provides support for the {@link DocumentIterator#weight()}/{@link DocumentIterator#weight(double)} methods.
 *
 * <p>Instances of this class expect implementation to keep track of the {@linkplain #curr current document}
 * of the iterator. The special value -1 denotes an iterator that has not still been accessed,
 * and the special value {@link DocumentIterator#END_OF_LIST} denotes an iterator that has been exhausted. 
 * 
 * <p>Methods performing actions depending on the last document returned should throw an {@link IllegalStateException}
 * if called when {@link #curr} is -1 or {@link DocumentIterator#END_OF_LIST}. You just need to call {@link #ensureOnADocument()}.
 */

public abstract class AbstractDocumentIterator implements DocumentIterator {
	/** The current document of the iterator. 
	 * @see AbstractDocumentIterator */
	protected long curr = -1;
	/** The weight of this iterator. */
	protected double weight = 1;
	
	public boolean mayHaveNext() {
		return curr != END_OF_LIST;
	}

	public double weight() {
		return weight;
	}
	
	public DocumentIterator weight( final double weight ) {
		this.weight = weight;
		return this;
	}

	protected final void ensureOnADocument() {
		// This catches curr == END_OF_LIST || curr == -1.
		if ( ( curr | 0x80000000 ) == -1 ) throw new IllegalStateException();
	}
	
	/** Returns the current document.
	 * 
	 * @return {@link #curr}.
	 */
	public long document() {
		return curr; 
	}
}
