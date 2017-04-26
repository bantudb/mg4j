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
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

public class MultiTermIndexIterator extends MockDocumentIterator implements IndexIterator {

	public static IndexIterator getInstance( final Index index, IndexIterator... indexIterator  ) throws IOException {
		if ( indexIterator.length == 0 ) return index.getEmptyIndexIterator();
		if ( indexIterator.length == 1 ) return indexIterator[ 0 ];
		return new MultiTermIndexIterator( indexIterator );
	}

	public static IndexIterator getInstance( IndexIterator... indexIterator  ) throws IOException {
		if ( indexIterator.length == 0 ) throw new IllegalArgumentException();
		return new MultiTermIndexIterator( indexIterator );
	}

	private boolean hasCounts;
	private Long2IntMap documentPointerToCount = new Long2IntOpenHashMap();
	private Long2LongOpenHashMap documentPointerToFrequency = new Long2LongOpenHashMap();
	private String term;
	private int id;
	{
		documentPointerToCount.defaultReturnValue( 0 );
		documentPointerToFrequency.defaultReturnValue( 0 );
	}

	/** Creates a new document iterator that computes the OR of the given array of iterators.
	 * @param indexIterators the iterators to be joined.
	 * @throws IOException 
	 */
	protected MultiTermIndexIterator( final IndexIterator... indexIterators ) throws IOException {
		try{
			hasCounts = true;
			
			
			for ( IndexIterator indexIterator: indexIterators ) {
				indices.addAll( indexIterator.indices() );
				if ( ! indexIterator.index().hasCounts ) hasCounts = false;
			}

			for ( IndexIterator indexIterator: indexIterators ) {
				long documentPointer;
				while ( ( documentPointer = indexIterator.nextDocument() ) != END_OF_LIST ) {
					Reference2ReferenceMap<Index, IntervalIterator> intervalIterators = indexIterator.intervalIterators();
					for ( Index index: intervalIterators.keySet() ) { 
						IntervalIterator intervalIterator = intervalIterators.get( index );
						Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = elements.get( documentPointer );
						
						if ( intervalIterator == IntervalIterators.FALSE ) continue;
						if ( intervalIterator == IntervalIterators.TRUE ) {
							if ( index2IntervalMap == null || ! index2IntervalMap.containsKey( index ) ) 
								addTrueIteratorDocument( documentPointer, index );
							continue;
						}

						// Cleanup TRUE if we find intervals.
						if ( index2IntervalMap != null && index2IntervalMap.get( index ) == TRUE ) index2IntervalMap.remove( index );
						for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) {
							addIntervalForDocument( documentPointer, index, interval );
						}
						if ( hasCounts ) documentPointerToCount.put( documentPointer, documentPointerToCount.get( documentPointer ) + indexIterator.count() );
						documentPointerToFrequency.put( documentPointer, Math.max( documentPointerToFrequency.get( documentPointer ), indexIterator.frequency() ) );
					}
				}
			}
			for ( DocumentIterator documentIterator: indexIterators ) documentIterator.dispose();
			start( true );
		} catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	@Override
	public int count() throws IOException {
		if ( !hasCounts ) throw new IllegalStateException( "Some of the underlying iterators do not have counts" );
		return documentPointerToCount.get( lastValueReturned );
	}

	@Override
	public long frequency() throws IOException {
		return documentPointerToFrequency.get( lastValueReturned );
	}

	@Override
	public IndexIterator id( final int id ) {
		this.id = id;
		return this;
	}

	@Override
	public int id() {
		return id;
	}

	@Override
	public Index index() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Payload payload() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String term() {
		return term;
	}

	@Override
	public IndexIterator term( final CharSequence term ) {
		this.term = term.toString();
		return this;
	}

	@Override
	public long termNumber() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public IndexIterator weight( double weight ) {
		super.weight( weight );
		return this;
	}

	@Override
	public int nextPosition() throws IOException {
		throw new UnsupportedOperationException();
	}
}
