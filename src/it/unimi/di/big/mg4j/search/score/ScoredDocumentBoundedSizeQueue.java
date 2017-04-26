package it.unimi.di.big.mg4j.search.score;

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
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

/** A queue of scored documents with fixed maximum capacity.
 * 
 * <P>Instances of this class contain a queue in which it possible to {@linkplain #enqueue(long, double, Object) enqueue a document with given score}.
 * The capacity of the queue is fixed at creation time: once the queue is filled, new elements are enqueued
 * by dequeueing those in the queue or discarded, depending on their score; the return
 * value of {@link #enqueue(long, double, Object)} can be used to check whether the argument
 * has been actually enqueued or not. In particular, using the 
 * {@linkplain #ScoredDocumentBoundedSizeQueue(int) standard constructor} will
 * give a queue that remembers just the documents with highest score. As a commodity, document can be
 * {@linkplain #enqueue(long, double, Object) enqueued together with additional information} that can
 * be retrieved later.
 * 
 * <P>The {@linkplain #ScoredDocumentBoundedSizeQueue(int) standard constructor} orders document
 * by score first, and then by document index. This trick
 * guarantees that the queue is stable (because the order is an actual order, not a preorder), and makes it
 * possible to consistently retrieve documents from the <var>k</var>-th to the (<var>k</var>+<var>j</var>)-th
 * using a queue of capacity <var>k</var>+<var>j</var> from which <var>j</var> elements are extracted.
 * 
 * <p><strong>Warning</strong>: documents are dequeued in <em>{@linkplain #dequeue() score order}</em>, which means
 * documents with smaller score <em>are dequeued first</em>.
 * 
 * <P>The queue returns its results as instances of {@link DocumentScoreInfo}.
 */

public class ScoredDocumentBoundedSizeQueue<T> {
	private static final boolean ASSERTS = false;

	/** The underlying queue. */
	private final ObjectHeapPriorityQueue<DocumentScoreInfo<T>> queue;
	/** The maximum number of documents to be ranked. */
	private int maxSize;
	
	/** Creates a new empty bounded-size queue with a given capacity and natural order as comparator.
	 *
	 * <P>Documents with equal scores will be compared using their document index. 
	 *
	 * @param capacity the initial capacity of this queue.
	 */
	public ScoredDocumentBoundedSizeQueue( final int capacity ) {
		maxSize = capacity;
		queue = new ObjectHeapPriorityQueue<DocumentScoreInfo<T>>( capacity, DocumentScoreInfo.SCORE_DOCUMENT_COMPARATOR );
	}

	/** Checks whether a document with given score would be enqueued.
	 * 
	 * <p>If this methods returns true, an immediately following call to
	 * {@link #enqueue(long, double, Object)} with the same score would
	 * case the document to be enqueued.
	 * 
	 * @param d the document to test.
	 * @param s its score.
	 * @return true if the document would have been enqueued by {@link #enqueue(long, double, Object)}.
	 */

	public boolean wouldEnqueue( final int d, final double s ) {
		if ( queue.size() < maxSize ) return true;
		final DocumentScoreInfo<?> dsi = queue.first();
		// Note that we are replicating here the logic of DOCUMENT_SCORE_COMPARATOR.
		return s > dsi.score || s == dsi.score && d > dsi.document; 
	}


	/** Enqueues a document with given score and info.
	 * 
	 * @param document the document to enqueue.
	 * @param s its score.
	 * @param i additional information about this document.
	 * @return true if the document has been actually enqueued.
	 */

	public boolean enqueue( final long document, final double s, final T i ) {
		if ( maxSize == 0 ) return false;
		if ( queue.size() < maxSize ) {
			queue.enqueue( new DocumentScoreInfo<T>( document, s, i ) );
			return true;
		}
		else {
			final DocumentScoreInfo<T> dsi = queue.first();
			if ( ASSERTS ) assert document > dsi.document;
			if ( s > dsi.score ) { 
				dsi.document = document;
				dsi.score = s;
				dsi.info = i;
				queue.changed();
				return true;
			}
			return false;
		}
	}

	
	/** Enqueues a document with given score.
	 * 
	 * @param document the document to enqueue.
	 * @param s its score.
	 * @return true if the document has been actually enqueued.
	 */

	public boolean enqueue( final long document, final double s ) {
		return enqueue( document, s, null );
	}

	public boolean isEmpty() {
		return queue.isEmpty();
	}

	public int size() {
		return queue.size();
	}

	/** Dequeues a document from the queue, returning an instance of {@link DocumentScoreInfo}.
	 * 
	 * <p>Documents are dequeued in inverse score order.
	 * 
	 * @return the next {@link DocumentScoreInfo}.
	 */

	public DocumentScoreInfo<T> dequeue() {
		return queue.dequeue();
	}
}
