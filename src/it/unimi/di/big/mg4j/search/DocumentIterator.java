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
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;
import java.util.Map;

/** An iterator over documents (pointers) and their intervals.
 *
 * <p><strong>Warning</strong>: from MG4J 5.0, this class does not implement
 * {@link IntIterator}. Moreover, {{@link #nextDocument()} no longer returns -1
 * to denote end of iteration, but rather {@link #END_OF_LIST}. 
 *
 * <P>Each call to {@link #nextDocument()}
 * will return a document pointer, or {@link #END_OF_LIST} if no more documents are available. Just
 * after the call to {@link #nextDocument()}, {@link #intervalIterator(Index)} will return an interval iterator
 * enumerating intervals in the last returned document for the specified index. The latter method may return, as a special result, a
 * special {@link it.unimi.di.big.mg4j.search.IntervalIterators#TRUE TRUE} value: this means that 
 * albeit the current document satisfies the query, there is only a generic
 * empty witness to prove it (see {@link it.unimi.di.big.mg4j.search.IntervalIterators#TRUE TRUE} for some elaboration).
 * 
 * <p>A document iterator is usually structured as composite,
 * with operators as internal nodes and {@link it.unimi.di.big.mg4j.index.IndexIterator}s
 * as leaves. The methods {@link #accept(DocumentIteratorVisitor)} 
 * and {@link #acceptOnTruePaths(DocumentIteratorVisitor)} implement the visitor pattern.
 * 
 * <p>The {@link #dispose()} method is intended to recursively release all resources associated
 * to a composite document iterator. Note that this is not always what you want, as you might
 * be, say, pooling {@linkplain it.unimi.di.big.mg4j.index.IndexReader index readers} to reduce the number
 * of file open/close operations. For this reason, we intentionally avoid calling the method &ldquo;close&rdquo;.
 * 
 * <p><strong>Warning</strong>: interval enumeration can be carried out only just after a call
 * to {@link #nextDocument()}. Subsequent calls to {@link #nextDocument()}
 * will reset the internal state of the iterator.
 */

public interface DocumentIterator {


	/** Returns the interval iterator of this document iterator for single-index queries.
	 * 
	 * <P>This is a commodity method that can be used only for queries
	 * built over a single index.
	 * 
	 * @return an interval iterator.
	 * @see #intervalIterator(Index) 
	 * @throws IllegalStateException if this document iterator is not built on a single index.
	 */
	public IntervalIterator intervalIterator() throws IOException;


	/** Returns the interval iterator of this document iterator for the given index.
	 * 
	 * <P>After a call to {@link #nextDocument()}, this iterator
	 * can be used to retrieve the intervals in the current document (the
	 * one returned by {@link #nextDocument()}) for 
	 * the index <code>index</code>.
	 *  
	 * <P>Note that if all indices have positions, 
	 * it is guaranteed that at least one index will return an interval.
	 * However, for disjunctive queries it cannot be guaranteed that <em>all</em>
	 * indices will return an interval.
	 * 
	 * <p>Indices without positions always return {@link IntervalIterators#TRUE}.
	 * Thus, in presence of indices without positions it is possible that no
	 * intervals at all are available.
	 * 
	 * @param index an index (must be one over which the query was built).
	 * @return an interval iterator over the current document in <code>index</code>.
	 */

	public IntervalIterator intervalIterator( Index index ) throws IOException;

	/** Returns an unmodifiable map from indices to interval iterators.
	 * 
	 * <P>After a call to {@link #nextDocument()}, this map
	 * can be used to retrieve the intervals in the current document. An invocation of {@link Map#get(java.lang.Object)}
	 * on this map with argument <code>index</code> yields the same result as
	 * {@link #intervalIterator(Index) intervalIterator(index)}.
	 *  
	 * @return a map from indices to interval iterators over the current document.
	 * @throws UnsupportedOperationException if this index does not contain positions.
	 * @see #intervalIterator(Index)
	 */

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException;
	
	/** Returns the set of indices over which this iterator is built.
	 * 
	 * @return the set of indices over which this iterator is built.
	 */

	public ReferenceSet<Index> indices();
	
	/** Returns the next document provided by this document iterator, or {@link #END_OF_LIST} if no more documents are available.
	 * 
	 * @return the next document, or {@link #END_OF_LIST} if no more documents are available.
	 */
	public long nextDocument() throws IOException;

	/** Returns whether there may be a next document, possibly with false positives.
	 * 
	 * @return true there may be a next document; false if certainly there is no next document.
	 */
	
	public boolean mayHaveNext();

	/** Returns the last document returned by {@link #nextDocument()}.
	 * 
	 * @return the last document returned by {@link #nextDocument()}, -1 if no document has been returned yet, and
	 * {@link #END_OF_LIST} if the list of results has been exhausted.
	 */

	public long document();

	/** A special value denoting that the end of the list has been reached. */
	public static final long END_OF_LIST = Long.MAX_VALUE;
	
	/** Skips all documents smaller than <code>n</code>.
	 * 
	 * <P>Define the <em>current document</em> <code>k</code> associated with this document iterator
	 * as follows:
	 * <ul>
	 * <li>-1, if {@link #nextDocument()} and this method have never been called;
	 * <li>{@link #END_OF_LIST}, if a call to this method or to
	 * {@link #nextDocument()} returned {@link #END_OF_LIST};
	 * <li>the last value returned by a call to {@link #nextDocument()} or this method, otherwise. 
	 * </ul>
	 * 
	 * <p>If <code>k</code> is larger than or equal to <code>n</code>, then
	 * this method does nothing and returns <code>k</code>. Otherwise, a 
	 * call to this method is equivalent to 
	 * <pre>
	 * while( ( k = nextDocument() ) < n );
	 * return k;
	 * </pre>
	 *
	 * <P>Thus, when a result <code>k</code> &ne; {@link #END_OF_LIST}
	 * is returned, the state of this iterator
	 * will be exactly the same as after a call to {@link #nextDocument()} 
	 * that returned <code>k</code>.
	 * In particular, the first document larger than or equal to <code>n</code> (when returned
	 * by this method) will <em>not</em> be returned by the next call to 
	 * {@link #nextDocument()}.
	 *
	 * @param n a document pointer.
	 * @return a document pointer larger than or equal to <code>n</code> if available, {@link #END_OF_LIST}
	 * otherwise.
	 */

	long skipTo( long n ) throws IOException;
	
	/** Accepts a visitor.
	 * 
	 * <p>A document iterator is usually structured as composite,
	 * with operators as internal nodes and {@link it.unimi.di.big.mg4j.index.IndexIterator}s
	 * as leaves. This method implements the visitor pattern.
	 * 
	 * @param visitor the visitor.
	 * @return an object resulting from the visit, or <code>null</code> if the visit was interrupted.
	 */
	<T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException;

	/** Accepts a visitor after a call to {@link #nextDocument()}, 
	 * limiting recursion to true paths.
	 * 
	 * <p>After a call to {@link #nextDocument()}, a document iterator
	 * is positioned over a document. This call is equivalent to {@link #accept(DocumentIteratorVisitor)},
	 * but visits only along <em>true paths</em>. 
	 * 
	 * <p>We define a <em>true path</em> as a path from the root of the composite that passes only through 
	 * nodes whose associated subtree is positioned on the same document of the root. Note that {@link OrDocumentIterator}s
	 * detach exhausted iterators from the composite tree, so true paths define the subtree that is causing
	 * the current document to satisfy the query represented by this document iterator.
	 * 
	 * <p>For more elaboration, and the main application of this method, see {@link it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor}.
	 * 
	 * @param visitor the visitor.
	 * @return an object resulting from the visit, or <code>null</code> if the visit was interrupted.
	 * @see #accept(DocumentIteratorVisitor)
	 * @see it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor
	 */
	<T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException;

	/** Returns the weight associated with this iterator.
	 * 
	 * <p>The number returned by this method has no fixed semantics: different {@linkplain Scorer scorers}
	 * might choose different interpretations, or even ignore it.
	 * 
	 * @return the weight associated with this iterator.
	 */
	double weight();
	
	/** Sets the weight of this index iterator. 
	 * 
	 * @param weight the weight of this index iterator.
	 * @return this document iterator.
	 */
	DocumentIterator weight( double weight );

	/** Disposes this document iterator, releasing all resources.
	 * 
	 * <p>This method should propagate down to the underlying index iterators, where it should release resources
	 * such as open files and network connections. If you're doing your own resource tracking and pooling,
	 * then you do not need to call this method.
	 */
	void dispose() throws IOException;
}
