package it.unimi.di.big.mg4j.index;

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

import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.AbstractCompositeDocumentIterator;
import it.unimi.di.big.mg4j.search.AbstractUnionDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.di.big.mg4j.search.OrDocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.objects.ObjectHeapIndirectPriorityQueue;

import java.io.IOException;

/** A virtual {@linkplain IndexIterator index iterator} that merges several component index iterators.
*
* <P>This class adds to {@link it.unimi.di.big.mg4j.search.AbstractUnionDocumentIterator}
* an interval iterator generating the OR of the intervals returned for each of the documents involved.
* The main difference with an {@link OrDocumentIterator} built on the same array of component iterators
* is that this class implements {@link IndexIterator} and hence provides a {@link #count()} (the sum
* of counts of those component iterators positioned on the current document), {@link #frequency()} and {@link #nextPosition()}. The
* frequency is by default the maximum frequency of a component iterator, but it can be set 
* at {@link MultiTermIndexIterator#getInstance(int, Index, IndexIterator[]) construction time}.
* 
* <p>The main <i>raison d'&ecirc;tre</i> of this class is support for query expansion: a blind application
* of {@link OrDocumentIterator} to an array of index iterators would mislead {@linkplain Scorer scorers} such as {@link BM25Scorer}
* because low-frequency terms (e.g., <i>hapax legomena</i>) would be responsible for most of the score.
* 
* <p>Note that {@linkplain DocumentIteratorVisitor} has a {@linkplain DocumentIteratorVisitor#visit(IndexIterator) visit method for generic index iterator}
* and a {@linkplain DocumentIteratorVisitor#visit(MultiTermIndexIterator) visit method for instances of this class}.
* This approach provides additional flexibility&mdash;a scorer, for instance, might treat an instance of
* this class as a standard {@link IndexIterator}, or it might choose to {@linkplain #front(IndexIterator[]) query which terms actually appear}
* and do something more sophisticated (for instance, using {@linkplain DocumentIterator#weight() weights}).
*/

public class MultiTermIndexIterator extends AbstractUnionDocumentIterator implements IndexIterator {
	@SuppressWarnings("unused")
	private static final boolean ASSERTS = false;
	
	/** Whether all underlying index iterators have counts. */
	private final boolean hasCounts; 
	/** Whether all underlying index iterators have positions. */
	private final boolean hasPositions;
	/** A heap-based indirect priority queue used to keep track of the currently scanned positions. */
	private final IntHeapSemiIndirectPriorityQueue positionQueue;
	/** Value to be used for term frequency, or {@link Integer#MIN_VALUE} to use the max; in any case, this attribute is used to cache
	 *  frequency after the first call to {@link #frequency()}. */
	private long frequency;
	/** The term of this iterator. */
	protected String term;
	/** The id of this iterator. */
	protected int id;
	/** The count of the last returned document. */
	private int count = -1;
	/** Whether the queue has been invalidated. */
	private boolean queueInvalid;
	/** The reference array for {@link #positionQueue}. */
	private int[] currPos;
	
	/** Returns an index iterator that merges the given array of iterators.
	 *  This method requires that at least one iterator is provided. The frequency is computed as a max,
	 *  and {@link #index()} will return the result of the same method on the first iterator.
	 * 
	 * @param indexIterator the iterators to be joined (at least one).
	 * @return a merged index iterator. 
	 * @throws IllegalArgumentException if no iterators were provided.
	 */
	public static IndexIterator getInstance( final IndexIterator... indexIterator  ) {
		return getInstance( Integer.MIN_VALUE, indexIterator );
	}

	/** Returns an index iterator that merges the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently. The frequency is computed as a max, and
	 * {@link #index()} will return <code>index</code>.
	 * 
	 * @param index the index that wil be returned by {@link #index()}.
	 * @param indexIterator the iterators to be joined.
	 * @return a merged index iterator. 
	 */
	public static IndexIterator getInstance( final Index index, final IndexIterator... indexIterator  ) {
		return getInstance( Integer.MIN_VALUE, index, indexIterator );
	}

	/** Returns an index iterator that merges the given array of iterators.
	 *  This method requires that at least one iterator is provided.
	 * 
	 * @param defaultFrequency the default term frequency (or {@link Integer#MIN_VALUE} for the max).
	 * @param indexIterator the iterators to be joined (at least one).
	 * @return a merged index iterator. 
	 * @throws IllegalArgumentException if no iterators were provided, or they run on different indices.
	 */
	public static IndexIterator getInstance( final int defaultFrequency, final IndexIterator... indexIterator  ) {
		if ( indexIterator.length == 0 ) throw new IllegalArgumentException();
		return getInstance( defaultFrequency, indexIterator[ 0 ].index(), indexIterator );
	}

	/** Returns an index iterator that merges the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently. 
	 * 
	 * @param defaultFrequency the default term frequency (or {@link Integer#MIN_VALUE} for the max).
	 * @param index the index that wil be returned by {@link #index()}.
	 * @param indexIterator the iterators to be joined.
	 * @return a merged index iterator. 
	 * @throws IllegalArgumentException if there is some iterator on an index different from <code>index</code>.
	 */
	public static IndexIterator getInstance( final int defaultFrequency, final Index index, final IndexIterator... indexIterator  ) {
		if ( indexIterator.length == 0 ) return index.getEmptyIndexIterator();
		if ( indexIterator.length == 1 ) return indexIterator[ 0 ];
		return new MultiTermIndexIterator( defaultFrequency, indexIterator );
	}

	
	/** Creates a new document iterator that merges the given array of iterators. 
	 * 
	 * @param defaultFrequency the default term frequency (or {@link Integer#MIN_VALUE} for the max).
  	 * @param indexIterator the iterators to be joined.
	 */
	protected MultiTermIndexIterator( final int defaultFrequency, final IndexIterator... indexIterator ) {
		super( indexIterator );
		if ( soleIndex == null ) throw new IllegalArgumentException();
		this.frequency = defaultFrequency;
		boolean havePositions = true, haveCounts = true;
		for( IndexIterator i: indexIterator ) {
			if ( ! i.index().hasCounts ) haveCounts = false;
			if ( ! i.index().hasPositions ) havePositions = false;
				
		}

		hasCounts = haveCounts;
		hasPositions = havePositions;
		positionQueue = hasPositions ? new IntHeapSemiIndirectPriorityQueue( currPos = new int[ n ] ) : null;
	}

	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object unused ) {
		return new IndexIntervalIterator( this );
	}

	@Override
	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		// We invalidate count before calling the superclass method.
		queueInvalid = true;
		count = 0;
		return super.skipTo( n );
	}
	
	public long nextDocument() throws IOException {
		// We invalidate count before calling the superclass method.
		queueInvalid = true;
		count = 0;
		return super.nextDocument();
	}
	
	/** The count is the sum of counts of those component iterators positioned on the current document.
	 * 
	 *  @return the sum of counts.
	 */
	public int count() throws IOException {
		ensureOnADocument();
		if ( ! hasCounts ) throw new IllegalStateException( "Some of the underlying iterators do not have counts" );
		if ( count == 0 ) {
			int count = 0;
			for ( int i = computeFront(); i-- != 0; ) count += indexIterator[ front[ i ] ].count();
			this.count = count;
		}
		return count;
	}

	/** Fills the given array with the index iterators composing the current front.
	 * 
	 * <p>This method is essentially a safe exposure of the {@linkplain ObjectHeapIndirectPriorityQueue#front(int[]) front of the queue}
	 * merging the component {@linkplain IndexIterator index iterators}.
	 * After a call to {@link #nextDocument()}, you can use this method to know
	 * which terms actually appear in the current document. You can use the public
	 * field {@link AbstractCompositeDocumentIterator#n} to size the argument
	 * array appropriately.
	 * 
	 * @param indexIterator an array, at least large as the number of component index iterators,
	 * that will be partially filled with the index iterators corresponding to terms appearing in the current document.
	 * @return the number of iterators written into <code>indexIterator</code>. 
	 */
	public int front( final IndexIterator[] indexIterator ) {
		final int s = computeFront();
		for( int i = s; i-- != 0; ) indexIterator[ i ] = this.indexIterator[ front[ i ] ];
		return s;
	}
	
	/** The frequency is either the default frequency set at construction time, or the maximum frequency of the component iterators. 
	 * 
	 * @return the frequency.
	 */
	public long frequency() throws IOException {
		if ( frequency != Integer.MIN_VALUE ) return frequency;
		long frequency = Integer.MIN_VALUE;
		for ( int i = n; i-- != 0; ) frequency = Math.max( frequency, indexIterator[ i ].frequency() );
		return this.frequency = frequency; // caching it!
	}

	public IndexIterator term( final CharSequence term ) {
		this.term = term == null ? null : term.toString();
		return this;
	}

	public String term() { 
		return term;
	}

	public long termNumber() {
		// TODO: this is not particularly sensible
		return indexIterator[ 0 ].termNumber();
	}
	
	public IndexIterator id( final int id ) {
		this.id = id;
		return this;
	}
	
	public int id() {
		return id;
	}

	public Index index() {
		return soleIndex;
	}

	public Payload payload() {
		throw new UnsupportedOperationException();
	}

	@Override
	public IndexIterator weight( final double weight ) {
		super.weight( weight );
		return this;
	}
	
	@Override
	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return visitor.visit( this );
	}

	@Override
	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return visitor.visit( this );
	}
	
	public <T> T acceptDeep( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return super.accept( visitor );
	}

	public <T> T acceptDeepOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return super.accept( visitor );
	}

	@Override
	public int nextPosition() throws IOException {
		if ( queueInvalid ) {
			if ( ! hasPositions ) throw new UnsupportedOperationException( "Index " + soleIndex + " does not contain positions" );
			positionQueue.clear();

			for ( int i = computeFront(), k; i-- != 0; ) {
				k = front[ i ];
				currPos[ k ] = indexIterator[ k ].nextPosition();
				positionQueue.enqueue( k );
			}

			queueInvalid = false;
		}

		if ( positionQueue.isEmpty() ) return END_OF_POSITIONS;
		final int first = positionQueue.first();
		final int result = currPos[ first ];

		if ( ( currPos[ first ] = indexIterator[ first ].nextPosition() ) == END_OF_POSITIONS ) positionQueue.dequeue();
		else positionQueue.changed();
		
		return result;
	}

	@Override
	public IntervalIterator intervalIterator() {
		return soleIntervalIterator;
	}
	
	public IntervalIterator intervalIterator( final Index index ) {
		return index == soleIndex ? soleIntervalIterator : IntervalIterators.FALSE;
	}
}
