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
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.AbstractIndirectPriorityQueue;
import it.unimi.dsi.fastutil.IndirectPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntIndirectPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntSemiIndirectHeaps;
import it.unimi.dsi.fastutil.longs.LongSemiIndirectHeaps;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/** A document iterator computing the union of the documents returned by a number of document iterators.
 * 
 * <p>This class provides a mechanism that makes accessible the {@linkplain #front set of component
 * document iterators} that are {@linkplain #computeFront() positioned on the current document}.
 */

public abstract class AbstractUnionDocumentIterator extends AbstractCompositeDocumentIterator {

	protected final static class IntHeapSemiIndirectPriorityQueue extends AbstractIndirectPriorityQueue<Integer> implements IntIndirectPriorityQueue {
		private static final boolean ASSERTS = false;
		/** The reference array. */
		protected final int refArray[];
		/** The semi-indirect heap. */
		protected final int heap[];
		/** The number of elements in this queue. */
		protected int size;

		public IntHeapSemiIndirectPriorityQueue( final int[] refArray ) {
			this.refArray = refArray;
			this.heap = new int[ refArray.length ];
		}

		@Override
		public void enqueue( int x ) {
			heap[ size++ ] = x;
			IntSemiIndirectHeaps.upHeap( refArray, heap, size, size - 1, null );
		}

		@Override
		public int dequeue() {
			if ( ASSERTS ) assert size != 0;
			final int result = heap[ 0 ];
			heap[ 0 ] = heap[ --size ];
			if ( size != 0 ) IntSemiIndirectHeaps.downHeap( refArray, heap, size, 0, null );
			return result;
		}

		@Override
		public int first() {
			if ( ASSERTS ) assert size != 0;
			return heap[ 0 ];
		}

		@Override
		public void changed() {
			if ( ASSERTS ) assert size != 0;
			// This method is inlined as it is responsible for almost all the heap work done.
			final int[] heap = this.heap;
			final int[] refArray = this.refArray;

			int i = 0;
			final int e = heap[ 0 ];
			final int E = refArray[ e ];
			int child;

			while ( ( child = 2 * i + 1 ) < size ) {
				if ( child + 1 < size && refArray[ heap[ child + 1 ] ] < refArray[ heap[ child ] ] ) child++;
				if ( E <= refArray[ heap[ child ] ] ) break;
				heap[ i ] = heap[ child ];
				i = child;
			}
			heap[ i ] = e;
		}

		@Override
		public void allChanged() {
			IntSemiIndirectHeaps.makeHeap( refArray, heap, size, null );
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public void clear() {
			size = 0;
		}

		@Override
		public IntComparator comparator() {
			return null;
		}

		@Override
		public int front( final int[] a ) {
			return IntSemiIndirectHeaps.front( refArray, heap, size, a );
		}
	}

	protected final static class LongHeapSemiIndirectPriorityQueue extends AbstractIndirectPriorityQueue<Integer> implements IntIndirectPriorityQueue {
		private static final boolean ASSERTS = false;
		/** The reference array. */
		protected final long refArray[];
		/** The semi-indirect heap. */
		protected final int heap[];
		/** The number of elements in this queue. */
		protected int size;

		public LongHeapSemiIndirectPriorityQueue( final long[] refArray ) {
			this.refArray = refArray;
			this.heap = new int[ refArray.length ];
		}

		@Override
		public void enqueue( int x ) {
			heap[ size++ ] = x;
			LongSemiIndirectHeaps.upHeap( refArray, heap, size, size - 1, null );
		}

		@Override
		public int dequeue() {
			if ( ASSERTS ) assert size != 0;
			final int result = heap[ 0 ];
			heap[ 0 ] = heap[ --size ];
			if ( size != 0 ) LongSemiIndirectHeaps.downHeap( refArray, heap, size, 0, null );
			return result;
		}

		@Override
		public int first() {
			if ( ASSERTS ) assert size != 0;
			return heap[ 0 ];
		}

		@Override
		public void changed() {
			if ( ASSERTS ) assert size != 0;
			// This method is inlined as it is responsible for almost all the heap work done.
			final int[] heap = this.heap;
			final long[] refArray = this.refArray;

			int i = 0;
			final int e = heap[ 0 ];
			final long E = refArray[ e ];
			int child;

			while ( ( child = 2 * i + 1 ) < size ) {
				if ( child + 1 < size && refArray[ heap[ child + 1 ] ] < refArray[ heap[ child ] ] ) child++;
				if ( E <= refArray[ heap[ child ] ] ) break;
				heap[ i ] = heap[ child ];
				i = child;
			}
			heap[ i ] = e;
		}

		@Override
		public void allChanged() {
			LongSemiIndirectHeaps.makeHeap( refArray, heap, size, null );
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public void clear() {
			size = 0;
		}

		@Override
		public IntComparator comparator() {
			return null;
		}

		@Override
		public int front( final int[] a ) {
			return LongSemiIndirectHeaps.front( refArray, heap, size, a );
		}
	}
	
	/** A heap-based semi-indirect priority queue used to keep track of the currently scanned integers. */
	protected final LongHeapSemiIndirectPriorityQueue queue;
	/** The {@link IndirectPriorityQueue#front(int[])} of {@link #queue}, if {@link #frontSize} is not 0. */
	public final int[] front;
	/** The reference array used for the queue. */
	protected final long[] refArray;
	/** The number of valid entries in {@link #front}, or 0 if the front has not been computed for the current document. */
	protected int frontSize;

	protected AbstractUnionDocumentIterator( final DocumentIterator... documentIterator ) {
		super( documentIterator );
		this.refArray = new long[ n ];
		Arrays.fill( refArray, -1 );

		queue = new LongHeapSemiIndirectPriorityQueue( refArray );
		// Only add to the queue reasonably nonempty iterators...
		for ( int i = n; i-- != 0; ) if ( documentIterator[ i ].mayHaveNext() ) queue.enqueue( i );		
		
		// If the queue is empty, we enqueue the first iterator just to get END_OF_LIST in nextDocument().
		if ( queue.isEmpty() ) queue.enqueue( 0 );
		front = new int[ queue.size() ];
	}


	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;

		currentIterators.clear(); 
		frontSize = 0; // Invalidate front

		for( int i = refArray.length; i-- != 0; )
			refArray[ i ] = documentIterator[ i ].skipTo( n );
		queue.allChanged();
		
		return curr = refArray[ queue.first() ];
	}

	
	public long nextDocument() throws IOException {
		assert curr != END_OF_LIST;
		final LongHeapSemiIndirectPriorityQueue queue = this.queue;
		int first = queue.first();
		final long c = refArray[ first ];
		// On the first call, the queue should not be advanced.
		currentIterators.clear(); 
		frontSize = 0; // Invalidate front

		// Advance all elements equal to c
		do {
			refArray[ first ] = documentIterator[ first ].nextDocument();
			queue.changed();
		} while( refArray[ first = queue.first() ] == c );

		return curr = refArray[ first ];
	}
	
	/** Forces computation of the current front, returning the number of indices it contains.
	 * 
	 * <p>After a call to this method, 
	 * the first elements of {@link #front} contain
	 * the indices of the {@linkplain AbstractCompositeDocumentIterator#documentIterator component document iterators}
	 * that are positioned on the current document. If the front has already been
	 * computed for the current document, this method has no side effects.
	 * 
	 * @return the size of the current front (the number of valid entries in {@link #front}).
	 */
	
	public int computeFront() {
		// TODO: it would be better to simply return queue.front()
		if ( frontSize == 0 ) frontSize = queue.front( front );
		return frontSize;
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		final Iterator<Index> i = indices.iterator();
		while( i.hasNext() ) intervalIterator( i.next() );
		return unmodifiableCurrentIterators;
	}


	/** Invokes {@link #acceptOnTruePaths(DocumentIteratorVisitor)} only on component
	 * iterators positioned on the current document.
	 * 
	 * @param visitor a visitor.
	 * @return true if the visit should continue.
	 * @throws IOException 
	 */
	
	@Override
	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final int s  = computeFront();
		final T[] a = visitor.newArray( s );
		if ( a == null ) {
			for( int i = 0; i < s; i++ ) if ( documentIterator[ front[ i ] ].acceptOnTruePaths( visitor ) == null ) return null;
		}
		else {
			for( int i = 0; i < s; i++ ) if ( ( a[ i ] = documentIterator[ front[ i ] ].acceptOnTruePaths( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}
}
