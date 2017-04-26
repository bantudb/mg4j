package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.io.SafelyCloseable;

import java.io.IOException;

/** Provides access to an inverted index.
*
* <P>An {@link it.unimi.di.big.mg4j.index.Index} contains global read-only metadata. To get actual data
* from an index, you need to get an index reader <i>via</i> a call to {@link Index#getReader()}. Once
* you have an index reader, you can ask for the {@linkplain #documents(CharSequence) documents matching a term}.
* 
* <p>Alternatively, you can perform a <em>read-once scan</em> of the index calling {@link #nextIterator()},
* which will return in order the {@linkplain IndexIterator index iterators} of all terms of the underlying index.
* More generally, {@link #nextIterator()} returns an iterator positioned at the start of the inverted
* list of the term after the current one. When called just after the reader creation, it returns an
* index iterator for the first term.
* 
* <p><strong>Warning:</strong> An index reader is exactly what it looks like&mdash;a <em>reader</em>. It
* cannot be used by many threads at the same time, and all its access methods are exclusive: if you
* obtain a {@linkplain #documents(long) document iterator}, the previous one is no longer valid. However,
* you can generate many readers, and use them concurrently.
* 
* <p><strong>Warning:</strong> Invoking the {@link it.unimi.di.big.mg4j.search.DocumentIterator#dispose()} method
* on iterators returned by an instance of this class will invoke {@link #close()} on the instance, thus
* making the instance no longer accessible. This behaviour is necessary to handle cases in which a
* reader is created on-the-fly just to create an iterator.
*
* @author Paolo Boldi 
* @author Sebastiano Vigna 
* @since 1.0
*/

public interface IndexReader extends SafelyCloseable {

	/** Returns a document iterator over the documents containing a term.
	 * 
	 * <p>Note that the index iterator returned by this method will
	 * return <code>null</code> on a call to {@link IndexIterator#term() term()}.
	 * 
	 * 	<p>Note that it is <em>always</em> possible
	 * to call this method with argument 0, even if the underlying index
	 * does not provide random access.
	 * 
	 * @param termNumber the number of a term.
	 * @throws UnsupportedOperationException if this index reader is not accessible by term
	 * number.
	 */
	public IndexIterator documents( long termNumber ) throws IOException;

	/** Returns an index iterator over the documents containing a term; the term is
	 *  given explicitly.
	 * 
	 * <p>Unless the {@linkplain Index#termProcessor term processor} of
	 * the associated index is <code>null</code>, words coming from a query will
	 * have to be processed before being used with this method.
	 * 
	 * <p>Note that the index iterator returned by this method will
	 * return <code>term</code> on a call to {@link IndexIterator#term() term()}.
	 *
	 * @param term a term (the term will be downcased if the index is case insensitive).
	 * @throws UnsupportedOperationException if the {@linkplain StringMap term map} is not available for the underlying index.
	 */
	public IndexIterator documents( CharSequence term ) throws IOException;
	
	/** Returns an {@link IndexIterator} on the term after the current one (optional operation).
	 * 
	 * <p>Note that after creation there is no current term. Thus, the first call to this
	 * method will return an {@link IndexIterator} on the first term. As a consequence, repeated
	 * calls to this method provide a way to scan sequentially an index.
	 * 
	 * @return the index iterator of the next term, or <code>null</code> if there are no more terms
	 * after the current one.
	 */
	
	public IndexIterator nextIterator() throws IOException;
}
