package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** A decorator that caches the intervals produced by the underlying document iterator.
 * 
 * <P>Often, scores exhaust the intervals produced by a document iterator to compute their
 * result. However, often you also need those intervals for other purposes (maybe just
 * because you are aggregating several interval-based scorers). Decorating a document
 * iterator with an instance of this class you get again a document iterator, but its intervals can
 * be retrieved several times by calling {@link #intervalIterator(Index)}, {@link #intervalIterator()} and
 * {@link #intervalIterators()}.
 * 
 * <strong>Important</strong>: calls are <em>not nestable</em>: when you require again an iterator, 
 * the one previously returned is no longer valid, and when the current document changes (e.g.,
 * because of a call to {@link #nextDocument()}) the previously returned interval iterators are invalidated.
 * 
 * @author Sebastiano Vigna
 * @since 0.9.1
 */
public class CachingDocumentIterator implements DocumentIterator {

	/** The underlying document iterator. */
	private final DocumentIterator documentIterator;

	/** If not <code>null</code>, the sole index involved in this iterator. */
	private final Index soleIndex;

	/** A map from indices to caching iterators. We reuse iterators, and we create
	 * them on demand, so we must keep track of them separately from the set of current 
	 * iterators (which will be returned to the user). */
	private final Index2IntervalIteratorMap cachingIterators;

	/** A map from indices to the iterators already returned for the current document. The key set may
	 * not contain an index because the related iterator has never been requested. */
	private final Index2IntervalIteratorMap currentIterators;
	
	/** An unmodifiable wrapper around {@link #currentIterators}. */
	private final Reference2ReferenceMap<Index,IntervalIterator> unmodifiableCurrentIterators;
	
	public CachingDocumentIterator( final DocumentIterator documentIterator ) {
		this.documentIterator = documentIterator;
		final int n = documentIterator.indices().size();
		soleIndex = n == 1 ? indices().iterator().next() : null;

		this.currentIterators = new Index2IntervalIteratorMap( n );
		this.cachingIterators = new Index2IntervalIteratorMap( n );
		this.unmodifiableCurrentIterators = Reference2ReferenceMaps.unmodifiable( currentIterators );
	}
	
	public long document() {
		return documentIterator.document();
	}
	
	public ReferenceSet<Index> indices() {
		return documentIterator.indices();
	}
	
	/** An instance of this class caches lazily the intervals returning by the underlying interval iterator, and can
	 *  return them from the start after a call to {@link #restart()}. */
	
	private final static class CachingIntervalIterator implements IntervalIterator {
		/** The list of cached intervals (must be emitted before the ones returned by the iterator). */
		private final ObjectArrayList<Interval> cachedIntervals = new ObjectArrayList<Interval>();

		/** The actual iterator (might be partially exhausted). */
		private IntervalIterator intervalIterator;

		/** The current position in the cached intervals array. */
		private int pos = 0;
	
		/** Sets the underlying iterator.
		 * 
		 * <P>Most MG4J classes reuse interval iterators, but in principle a document
		 * iterator might return a different interval iterator at each call. This
		 * method is used to set it.
		 * 
		 * @param intervalIterator the new underlying iterator.
		 */
		public void wrap( final IntervalIterator intervalIterator ) {
			this.intervalIterator = intervalIterator;
		}
		
		@Override
		public int extent() {
			return intervalIterator.extent();
		}

		@Override
		public Interval nextInterval() throws IOException {
			if ( pos < cachedIntervals.size() ) return cachedIntervals.get( pos++ );
			else {
				final Interval next = intervalIterator.nextInterval();
				if ( next == null ) return null;
				cachedIntervals.add( next );
				pos++;
				return next;
			}
		}

		public IntervalIterator reset() {
			cachedIntervals.clear();			
			restart();
			return this;
		}

		public void restart() {
			pos = 0;
		}

		public void intervalTerms( final LongSet terms ) {
			intervalIterator.intervalTerms( terms );
		}
		
	};

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		if ( ! documentIterator.indices().contains( index ) ) return IntervalIterators.FALSE;
		CachingIntervalIterator result = (CachingIntervalIterator)cachingIterators.get( index );

		if ( currentIterators.containsKey( index ) ) {
			// the interval iterator for index is result
			result.restart();
			return result;
		}
		
		final IntervalIterator intervalIterator = documentIterator.intervalIterator( index );
		if ( intervalIterator == IntervalIterators.TRUE || intervalIterator == IntervalIterators.FALSE ) return intervalIterator; 

		// We instantiate caching iterators on demand
		if ( result == null ) cachingIterators.put( index, result = new CachingIntervalIterator() );
		result.wrap( intervalIterator );
		result.reset();
		currentIterators.put( index, result );
		
		return result;
	}
	
	public IntervalIterator intervalIterator() throws IOException {
		if ( soleIndex == null ) throw new IllegalStateException();
		return intervalIterator( soleIndex );
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		for( Index i : indices() ) intervalIterator( i );
		return unmodifiableCurrentIterators;
	}
	
	public long nextDocument() throws IOException {
		currentIterators.clear();
		return documentIterator.nextDocument();
	}
		
	public boolean mayHaveNext() {
		return documentIterator.mayHaveNext();
	}
	
	public long skipTo( final long n ) throws IOException {
		currentIterators.clear();
		return documentIterator.skipTo( n );
	}
	
	public void dispose() throws IOException {
		documentIterator.dispose();
	}

	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator.accept( visitor );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator.acceptOnTruePaths( visitor );
	}

	public IntervalIterator iterator() {
		try {
			return intervalIterator();
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	public double weight() {
		return documentIterator.weight();
	}

	public DocumentIterator weight( final double weight ) {
		return documentIterator.weight( weight );
	}
}
