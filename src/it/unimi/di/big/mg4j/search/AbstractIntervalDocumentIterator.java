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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

/** An abstract iterator on documents that provides basic support for handling interval iterators. 
 * 
 * <p>Implementing subclasses must override {@link #getIntervalIterator(Index, int, boolean, Object)} so
 * that it returns the correct interval iterator. Note that because of delays in class initialization, the
 * {@linkplain #AbstractIntervalDocumentIterator(int, ReferenceSet, boolean, Object) constructor} of this
 * class takes a number of apparently disparate parameters: they are necessary to initialize various kinds
 * of interval iterators. This class provides static methods that help in building the correct value for the parameters
 * of the constructor.
 */

public abstract class AbstractIntervalDocumentIterator extends AbstractDocumentIterator {
	/** The set of indices involved in this iterator. */
	protected final ReferenceSet<Index> indices;
	/** If not <code>null</code>, the sole index involved in this document iterator. */
	protected final Index soleIndex;
	/** If not <code>null</code>, the sole interval iterator involved in this document iterator. */
	protected final IntervalIterator soleIntervalIterator;
	/** A map from indices to interval iterators. */
	protected final Index2IntervalIteratorMap intervalIterators;
	/** A map from indices to the iterators returned for the current document. The key set may
	 * not contain an index because the related iterator has never been requested. Note that implementing
	 * subclasses might use this map just to store the result of {@link #intervalIterators()}, leaving the
	 * tracking of current iterators to some other mechanism. */
	protected final Index2IntervalIteratorMap currentIterators;
	/** An unmodifiable wrapper around {@link #currentIterators}. */
	protected final Reference2ReferenceMap<Index,IntervalIterator> unmodifiableCurrentIterators;

	/** Creates a new instance.
	 *  @param n the number of underlying iterators.
	 *  @param indices the set of indices appearing in the underlying iterators.
	 *  @param allIndexIterators whether all underlying iterators are {@linkplain IndexIterator index iterators}.
	 *  @param arg an argument that will be passed to {@link #getIntervalIterator(Index, int, boolean, Object)}.
	 */
	protected AbstractIntervalDocumentIterator( final int n, final ReferenceSet<Index> indices, final boolean allIndexIterators, final Object arg ) {
		this.indices = indices;
		intervalIterators = new Index2IntervalIteratorMap( indices.size() );
		currentIterators = new Index2IntervalIteratorMap( indices.size() );
		unmodifiableCurrentIterators = Reference2ReferenceMaps.unmodifiable( currentIterators );

		if ( indices.size() == 1 ) {
			soleIndex = indices.iterator().next();
			soleIntervalIterator = getIntervalIterator( soleIndex, n, allIndexIterators, arg );
			intervalIterators.add( soleIndex,  soleIntervalIterator );
		}
		else {
			soleIndex = null;
			soleIntervalIterator = null;
			for( Index in: indices ) if ( in.hasPositions ) intervalIterators.add( in, getIntervalIterator( in, n, allIndexIterators, arg ) );
		}
	}

	/** Creates an interval iterator suitable for this {@link AbstractIntervalDocumentIterator}.
	 * 
	 * @param index the reference index for the iterator, or {@code null}.
	 * @param n the number of underlying or component iterators.
	 * @param allIndexIterators whether all underlying or component iterators are {@linkplain IndexIterator index iterators}.
	 * @param arg an optional argument.
	 * @return an interval iterator suitable for this {@link AbstractIntervalDocumentIterator}.
	 */
	protected abstract IntervalIterator getIntervalIterator( Index index, int n, boolean allIndexIterators, Object arg );

	public ReferenceSet<Index> indices() {
		return indices;
	}
	
	/** A commodity static methods that computes the union of the indices of the given document iterators.
	 * 
	 * @param index an index that will be passed to {@link AbstractCompositeDocumentIterator#AbstractCompositeDocumentIterator(Index, Object, DocumentIterator...)}.
	 * @param documentIterator a list of document iterators.
	 * @return a set containing the union of the indices of the given document iterators.
	 */
	protected static ReferenceArraySet<Index> indices( final Index index, final DocumentIterator... documentIterator ) {
		final ReferenceArraySet<Index> indices = new ReferenceArraySet<Index>();
		if ( index != null ) indices.add( index );
		else for( DocumentIterator d: documentIterator ) indices.addAll( d.indices() );
		return indices;
	}
	
	/** A commodity static methods that checks whether all specified {@linkplain DocumentIterator document iterators} are actually {@linkplain IndexIterator index iterators}.
	 * 
	 * @param documentIterator a list of document iterators.
	 * @return true if all elements of {@code documentIterator} are actually {@linkplain IndexIterator index iterators}.
	 */
	protected static boolean allIndexIterators( final DocumentIterator... documentIterator ) {
		for( DocumentIterator d: documentIterator ) if ( ! ( d instanceof IndexIterator ) ) return false;
		return true;
	}
}
