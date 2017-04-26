package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2004-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.DocumentIterator;

import java.io.IOException;

/** An iterator over an inverted list.
 *
 * <P>An index iterator scans the inverted list of an indexed term. Each
 * integer returned by {@link DocumentIterator#nextDocument() nextDocument()} 
 * is the index of a document containing the
 * term. If the index contains counts, they can be obtained after each call to
 * {@link #nextDocument()} using {@link #count()}. Then, if the index contains
 * positions they can be obtained by calling {@link #nextPosition()}.
 * 
 * <p><strong>Warning</strong>: from MG4J 5.0, the plethora of non-lazy methods
 * to access positions ({@code positionArray()}, etc.) has been replaced by
 * static methods in {@link IndexIterators}.
 *
 * <P>Note that this interface extends {@link it.unimi.di.big.mg4j.search.DocumentIterator}.
 * The intervals returned for a document are exactly length-one intervals
 * corresponding to the positions returned by {@link #nextPosition()}. If the index
 * to which an instance of this class refers does not contain positions
 * an {@link UnsupportedOperationException} will be thrown.
 * 
 * <p>Additionally, this interface strengthens {@link DocumentIterator#weight(double)} so that
 * it {@linkplain #weight(double) returns an index iterator}.
 * 
 */

public interface IndexIterator extends DocumentIterator {

	/** Returns the index over which this iterator is built. 
	 * 
	 * @return the index over which this iterator is built.
	 */
	public Index index();
	
	/** Returns the number of the term whose inverted list is returned by this index iterator.
	 * 
	 * <p>Usually, the term number is automatically set by {@link IndexReader#documents(CharSequence)} or {@link IndexReader#documents(long)}.
	 * 
	 * @return the number of the term over which this iterator is built. 
	 * @throws IllegalStateException if no term was set when the iterator was created.
	 * @see #term()
	 */
	public long termNumber();
	
	/** Returns the term whose inverted list is returned by this index iterator.
	 * 
	 * <p>Usually, the term is automatically set by {@link IndexReader#documents(CharSequence)} or {@link IndexReader#documents(long)}, but you can
	 * supply your own term with {@link #term(CharSequence)}.
	 * 
	 * @return the term over which this iterator is built, as a compact mutable string.
	 * @throws IllegalStateException if no term was set when the iterator was created.
	 * @see #termNumber()
	 */
	public String term();
	
	/** Sets the term whose inverted list is returned by this index iterator.
	 * 
	 * <p>Usually, the term is automatically set by {@link Index#documents(CharSequence)}
	 * or by {@link IndexReader#documents(CharSequence)}, but you can
	 * use this method to ensure that {@link #term()} doesn't throw
	 * an exception.
	 * 
	 * @param term a character sequence (that will be defensively copied)
	 * that will be assumed to be the term whose inverted list is returned by this index iterator.
	 * @return this index iterator.
	 */
	public IndexIterator term( CharSequence term );

	/** Returns the frequency, that is, the number of documents that will be returned by this iterator.
	 *
	 * @return the number of documents that will be returned by this iterator.
	 */

	public long frequency() throws IOException;

	/** Returns the payload, if any, associated with the current document. 
	 * 
	 * @return the payload associated with the current document. 
	 */
	public Payload payload() throws IOException;
	
	/** Returns the count, that is, the number of occurrences of the term in the current document.
	 *
	 * @return the count (number of occurrences) of the term in the current document.
	 * @throws UnsupportedOperationException if the index of this iterator does not contain counts.
	 */
	public int count() throws IOException;

	/** A special value denoting that the end of the position list has been reached. */
	public int END_OF_POSITIONS = Integer.MAX_VALUE;

	/** Returns the next position at which the term appears in the current document. 
	 *
	 * @return the next position of the current document in which the current term appears,
	 * or {@link #END_OF_POSITIONS} if there are no more positions.
	 * @throws UnsupportedOperationException if the index of this iterator does not contain positions.
	 */
	public int nextPosition() throws IOException;

	/** Sets the id of this index iterator.
	 * 
	 * <p>The <em>id</em> is an integer associated with each index iterator. It has
	 * no specific semantics, and can be used differently in different contexts.
	 * A typical usage pattern, for instance, is using it to assign a unique number to
	 * the index iterators contained in a composite document iterator (say,
	 * numbering consecutively the leaves of the composite).
	 * 
	 * @param id the new id for this index iterator.
	 * @return this index iterator.
	 */
	public IndexIterator id( int id );
	
	/** Returns the id of this index iterator.
	 * 
	 * @see #id(int)
	 * @return the id of this index iterator.
	 */
	public int id();
	
	/** Returns the weight of this index iterator. 
	 * 
	 * @see DocumentIterator#weight(double) 
	 */	
	public IndexIterator weight( double weight );
}
