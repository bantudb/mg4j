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
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.Long2ReferenceAVLTreeMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/** An iterator over documents (pointers) and their intervals, realized in a very trivial way as a map from document pointers to
 *  a map from indices to interval sets. The latter have two special values, {@link #TRUE} and {@link #FALSE}.
 *  One can create an object of this class and then add at any time a new interval for a specific index and a specific document pointer using
 *  {@link #addIntervalForDocument(long, Index, Interval)}, {@link #addFalseIteratorDocument(long, Index)}, or {@link #addTrueIteratorDocument(long, Index)}.
 *  The maps from indices to interval sets have {@link #FALSE} as default value.
 *  When done, by calling {@link #start(boolean)} one gets a correct document iterator. There is no need to
 *  be sure that intervals are not contained in one another: non-minimal intervals will be automatically
 *  discarded. 
 */

public class MockDocumentIterator implements DocumentIterator {
	
	@SuppressWarnings("serial")
	protected static class IntervalSet extends ObjectAVLTreeSet<Interval> {
		
		private static Comparator<Interval> INTERVAL_COMPARATOR = new Comparator<Interval>() {
			public int compare( Interval o1, Interval o2 ) {
				return o1.left != o2.left? o1.left - o2.left : o1.right - o2.right;
			}
		};
		
		public IntervalSet() {
			super( INTERVAL_COMPARATOR );
		}
	}

	protected static IntervalSet TRUE = new IntervalSet() {
		private static final long serialVersionUID = 1L;
		public String toString() {
			return "TRUE";
		}
	};
	
	static {
		TRUE.add( Intervals.EMPTY_INTERVAL );
	}

	protected static IntervalSet FALSE = new IntervalSet() {
		private static final long serialVersionUID = 1L;
		public String toString() {
			return "FALSE";
		}
	};
	
	protected Long2ReferenceAVLTreeMap<Reference2ReferenceArrayMap<Index, IntervalSet>> elements = new Long2ReferenceAVLTreeMap<Reference2ReferenceArrayMap<Index,IntervalSet>>();
	private double weight;
	/** This is the iterator on document pointers */
	private ObjectBidirectionalIterator<it.unimi.dsi.fastutil.longs.Long2ReferenceMap.Entry<Reference2ReferenceArrayMap<Index, IntervalSet>>> fastIterator;
	/** This is the next entry to be returned by {@link #nextDocument()} */
	private it.unimi.dsi.fastutil.longs.Long2ReferenceMap.Entry<Reference2ReferenceArrayMap<Index, IntervalSet>> nextDocument;
	/** This is the map from indices to intervals relative to the last document returned by {@link #nextDocument} */
	private Reference2ReferenceArrayMap<Index, IntervalIterator> lastDocumentIntervals;
	ReferenceSet<Index> indices = new ReferenceArraySet<Index>();

	protected long lastValueReturned;

	public MockDocumentIterator() {}
	
	/** Makes a complete mock copy of the given iterator
	 * 
	 * @param documentIterator the iterator to be copied
	 */
	public MockDocumentIterator( DocumentIterator documentIterator ) {
		try {
			indices.addAll( documentIterator.indices() );
			long documentPointer;
			while ( ( documentPointer = documentIterator.nextDocument() ) != END_OF_LIST ) {
				for ( Index index: indices ) {
					if ( !index.hasPositions ) {
						addTrueIteratorDocument( documentPointer, index );
						continue;
					}
					IntervalIterator intervalIterator = documentIterator.intervalIterator( index );
					if ( intervalIterator == IntervalIterators.TRUE ) {
						addTrueIteratorDocument( documentPointer, index );
						continue;
					}
					if ( intervalIterator == IntervalIterators.FALSE ) {
						addFalseIteratorDocument( documentPointer, index );
						continue;
					}
					for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; )  
						addIntervalForDocument( documentPointer, index, interval );
				}
			}
			documentIterator.dispose();
			start( false );
		} catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}
	
	/** A very simple implementation of {@link IntervalIterator}, that exposes a simple {@link Iterator} as an
	 *  {@link IntervalIterator}.
	 */
	public class SimpleIntervalIterator implements IntervalIterator {
		private Iterator<Interval> underlying;
		
		public SimpleIntervalIterator( final Iterator<Interval> underlying ) {
			this.underlying = underlying;
		}
		
		@Override
		public int extent() {
			return -1;
		}

		@Override
		public void intervalTerms( LongSet terms ) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Interval nextInterval() throws IOException {
			return underlying.hasNext() ? underlying.next() : null;
		}

		@Override
		public IntervalIterator reset() throws IOException {
			return this;
		}
	}


	private Reference2ReferenceArrayMap<Index, IntervalSet> getIndex2IntervalMap( final long documentPointer ) {
		Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = elements.get( documentPointer );
		if ( index2IntervalMap == null ) {
			elements.put( documentPointer, index2IntervalMap = new Reference2ReferenceArrayMap<Index,IntervalSet>());
			index2IntervalMap.defaultReturnValue( FALSE );
		}
		return index2IntervalMap;
	}	
	
	/** Adds a new interval for a given pair (document pointer, index). This overrides a previous assignment of {@link IntervalIterators#TRUE}
	 *  to the same pair. On the other hand, if {@link IntervalIterators#FALSE} was assigned to the same pair, nothing happens.
	 * @param documentPointer the document pointer.
	 * @param index the index.
	 * @param interval the interval to add.
	 * @throws IllegalStateException if something was already assigned to the given pair.
	 */
	public void addIntervalForDocument( final long documentPointer, final Index index, final Interval interval ) {
		Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = getIndex2IntervalMap( documentPointer );

		final IntervalSet intervals; 
		if ( ! index2IntervalMap.containsKey( index  ) ) index2IntervalMap.put( index, intervals = new IntervalSet() );
		else {
			intervals = index2IntervalMap.get( index );
			if ( intervals == TRUE || intervals == FALSE ) throw new IllegalStateException();
		}
		intervals.add( interval );
	}

	/** Adds a new interval for a given pair (document pointer, index). This overrides a previous assignment of {@link IntervalIterators#TRUE}
	 *  to the same pair. On the other hand, if {@link IntervalIterators#FALSE} was assigned to the same pair, nothing happens.
	 * @param documentPointer the document pointer.
	 * @param index the index.
	 * @param intervalSet the intervals to add.
	 * @throws IllegalStateException if something was already assigned to the given pair.
	 */
	public void addIntervalsForDocument( final long documentPointer, final Index index, final IntervalSet intervalSet ) {
		Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = getIndex2IntervalMap( documentPointer );

		final IntervalSet intervals; 
		if ( ! index2IntervalMap.containsKey( index  ) ) index2IntervalMap.put( index, intervals = new IntervalSet() );
		else {
			intervals = index2IntervalMap.get( index );
			if ( intervals == TRUE || intervals == FALSE ) throw new IllegalStateException();
		}
		for( Interval interval: intervalSet ) intervals.add( interval );
	}

	/** Assigns a {@link IntervalIterators#TRUE} to a given pair (document pointer, index).
	 * 
	 * @param documentPointer the document pointer.
	 * @param index the index.
	 * @throws IllegalStateException if something was already assigned to the given pair.
	 */
	public void addTrueIteratorDocument( final long documentPointer, final Index index ) {
		Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = getIndex2IntervalMap( documentPointer );
		if ( index2IntervalMap.containsKey( index ) && index2IntervalMap.get( index ) != TRUE ) throw new IllegalStateException( "There is already a value for " + index + ":" + index2IntervalMap.get( index ) );
		index2IntervalMap.put( index, TRUE );
	}

	/** Assigns a {@link IntervalIterators#FALSE} to a given pair (document pointer, index),
	 *  provided that it was not assigned anything before. 
	 * 
	 * @param documentPointer the document pointer.
	 * @param index the index.
	 */
	public void addFalseIteratorDocument( final long documentPointer, final Index index ) {
		Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = getIndex2IntervalMap( documentPointer );
		if ( index2IntervalMap.containsKey( index ) && index2IntervalMap.get( index ) != FALSE ) throw new IllegalStateException( "There is already a value for " + index + ":" + index2IntervalMap.get( index ) );
		index2IntervalMap.put( index, FALSE );
	}

	/** Cleans up the {@link #elements} field, eliminating non-minimal intervals from every set, possibly eliminating indices mapping to
	 *  empty sets of intervals etc.
	 * @param eliminateFalse 
	 */
	private void cleanUp( final boolean eliminateFalse ) {
		Long2ReferenceAVLTreeMap<Reference2ReferenceArrayMap<Index, IntervalSet>> newElements = elements;
		for ( long documentPointer: elements.keySet() ) {
			Reference2ReferenceArrayMap<Index, IntervalSet> index2Intervals = new Reference2ReferenceArrayMap<Index, IntervalSet>();
			index2Intervals.defaultReturnValue( FALSE );
			for ( Index index: elements.get( documentPointer ).keySet() ) {
				IntervalSet result = new IntervalSet();
				IntervalSet intervalSet = elements.get( documentPointer ).get( index );
				if ( intervalSet == TRUE || intervalSet == FALSE ) result = intervalSet;
				else {
					assert ! intervalSet.isEmpty();
					ObjectArrayList<Interval> intervalList = new ObjectArrayList<Interval>( intervalSet );
					IntervalSet intervals = elements.get( documentPointer ).get( index );
					for ( Interval interval: intervals ) {
						int i;
						for ( i = 0; i < intervalList.size(); i++ ) {
							Interval other = intervalList.get( i );
							if ( interval.contains( other ) && !interval.equals( other ) ) {
								break;
							}
						}
						if ( i == intervalList.size() ) 
							result.add( interval );
					}
					assert ! result.isEmpty();
				}
				// No check on the size: if the list is empty now, it was empty before, and it should be put like that in the map
				index2Intervals.put( index, result );
			}
			assert ( index2Intervals.size() > 0 ); 
			newElements.put( documentPointer, index2Intervals );
		}
		elements = newElements;
		
		if ( eliminateFalse ) {
			// If we end up with documents all whose index iterators are FALSE, we remove them.
			for ( LongIterator document = elements.keySet().iterator(); document.hasNext(); ) {
				final long d = document.nextLong();
				boolean allFalse = true;
				for ( IntervalSet intervalSet: elements.get( d ).values() ) if ( intervalSet != FALSE ) {
					allFalse = false;
					break;
				}
				if ( allFalse ) document.remove(); 
			}
		}
	}

	protected void start( boolean eliminateFalse ) {
		cleanUp( eliminateFalse );
		fastIterator = elements.long2ReferenceEntrySet().iterator();
		nextDocument = fastIterator.hasNext()? fastIterator.next() : null;
		lastDocumentIntervals = new Reference2ReferenceArrayMap<Index, IntervalIterator>();
		lastDocumentIntervals.defaultReturnValue( IntervalIterators.FALSE );
		lastValueReturned = -1;
	}

	@Override
	public void dispose() throws IOException {
	}

	@Override
	public long document() {
		return lastValueReturned;
	}

	@Override
	public ReferenceSet<Index> indices() {
		return indices;
	}

	@Override
	public IntervalIterator intervalIterator() throws IOException {
		if ( lastDocumentIntervals.size() != 1 ) throw new IllegalStateException();
		Index[] indices = new Index[ 1 ];
		lastDocumentIntervals.keySet().toArray( indices );
		return intervalIterator( indices[ 0 ] );
	}
	
	@Override
	public IntervalIterator intervalIterator( Index index ) throws IOException {
		return lastDocumentIntervals.get( index );
	}

	@Override
	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		return lastDocumentIntervals;
	}

	@Override
	public long nextDocument() throws IOException {
		if ( nextDocument == null ) return lastValueReturned = END_OF_LIST;

		long nextDocumentPointer = nextDocument.getLongKey();
		Reference2ReferenceArrayMap<Index, IntervalSet> value = nextDocument.getValue();
		lastDocumentIntervals.clear();

		for ( Index index: value.keySet() ) {
			IntervalSet intervalSet = value.get( index );
			if ( intervalSet == TRUE ) lastDocumentIntervals.put( index, IntervalIterators.TRUE );
			else if ( intervalSet == FALSE ) lastDocumentIntervals.put( index, IntervalIterators.FALSE );
			else {
				assert ! intervalSet.isEmpty();
				lastDocumentIntervals.put( index, new SimpleIntervalIterator( intervalSet.iterator() ) );
			}
		}
		nextDocument = fastIterator.hasNext()? fastIterator.next() : null;
		return lastValueReturned = nextDocumentPointer;
	}

	public boolean mayHaveNext() {
		return nextDocument != null;
	}
	
	@Override
	public long skipTo( long n ) throws IOException {
		if ( lastValueReturned >= n ) return lastValueReturned;
		while( nextDocument() < n );
		return lastValueReturned;
	}

	@Override
	public double weight() {
		return weight;
	}

	@Override
	public DocumentIterator weight( double weight ) {
		this.weight = weight;
		return this;
	}

	@Override
	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		throw new UnsupportedOperationException();
	}
}
