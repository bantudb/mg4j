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
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;


/** An abstract iterator on documents, generating the intersection of the documents returned by 
 * a number of document iterators.
 * 
 * <P>The important invariant is that <em>only</em> after a call to {@link #nextDocument()}, a call
 * to {@link #intervalIterator(Index)} will return an interval iterator over the document
 * just returned, and that for at least one index in {@link #indices()} the iterator will not be empty
 * or {@link it.unimi.di.big.mg4j.search.IntervalIterators#TRUE TRUE}.
 * 
 * <h2>The intersection algorithm</h2>
 * 
 * <p>Since MG4J 1.1, this class implements a new intersection algorithm that should be significantly
 * faster than the previous one. The main idea is that of letting sparser iterator interact as much
 * as possible to obtain a candidate common document, and then trying to align the others. At construction
 * time, the component iterators are sorted so that index iterators are separated, and sorted by frequency.
 * Then, each time we have to align the iterators we align them greedily starting from the index
 * iterators, in frequency order. This has the effect of skipping very quickly (and usually by
 * large jumps, which are handled nicely by indices with skips),
 * as the main interaction happens between low-frequency index iterators.
 * 
 * <p>Moreover, this class treats in a special way
 *  {@linkplain PayloadPredicateDocumentIterator index iterators coming from payload-based indices}. Such
 * iterators are checked at the end of the alignment process, 
 * after all standard index iterators (and general document iterators)
 * are aligned. At that point, the special method {@link PayloadPredicateDocumentIterator#skipUnconditionallyTo(long)}
 * is used to position unconditionally such iterators and check whether the payload predicate is satisfied.
 * If this doesn't happen, the current candidate (obtained by alignment of standard iterators) is increased and the
 * whole process is restarted. This procedure guarantees that we will never search exhaustively in a 
 * payload-based index a document record satisfying the predicate (unless, of course, we have a query
 * containing just {@link PayloadPredicateDocumentIterator}s), which is very efficient if the payload-based
 * index uses skipping.
 */

public abstract class AbstractIntersectionDocumentIterator extends AbstractCompositeDocumentIterator {
	private final static boolean DEBUG = false;
	@SuppressWarnings("unused")
	private final static boolean ASSERTS = false;

	/** The provided document iterators, suitably sorted. */
	protected final DocumentIterator[] sortedIterator;
	/** The last element of {@link #sortedIterator}, which is usually the rarest term in the query. */
	protected final DocumentIterator lastIterator;
	/** The prefix of {@link #sortedIterator} made of {@link PayloadPredicateDocumentIterator}s. */
	private final PayloadPredicateDocumentIterator[] payloadPredicateDocumentIterator;
	/** The number of documents in the collection. */
	private long numberOfDocuments;
	/** Iterators in {@link #sortedIterator} up to this position (exclusive) are instances of {@link PayloadPredicateDocumentIterator}. */
	private final int predicateStart;

	/** Creates a new intersection iterator using a given array of iterators and a given index.
	 *  @param index an index that will be passed to {@link AbstractCompositeDocumentIterator#AbstractCompositeDocumentIterator(Index, Object, DocumentIterator...)}.
	 *  @param arg an argument that will be passed to {@link #getIntervalIterator(Index, int, boolean, Object)}.
	 *  @param documentIterator the iterators to be intersected (at least one).
	 */
	protected AbstractIntersectionDocumentIterator( final Index index, final Object arg, final DocumentIterator[] documentIterator ) {
		super( index, arg, documentIterator ); 
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		sortedIterator = documentIterator.clone(); // We need a copy to reorder iterators
		numberOfDocuments = indices().iterator().next().numberOfDocuments; // They must all be the same.
		
		// We now reorder iterators putting in the back the index iterators of smallest frequency and moving to front payload-predicate iterators.
		Arrays.sort( sortedIterator, new Comparator<DocumentIterator>() {
			public int compare( final DocumentIterator d0, final DocumentIterator d1 ) {
				final PayloadPredicateDocumentIterator p0 = d0 instanceof PayloadPredicateDocumentIterator ? (PayloadPredicateDocumentIterator)d0 : null;
				final PayloadPredicateDocumentIterator p1 = d1 instanceof PayloadPredicateDocumentIterator ? (PayloadPredicateDocumentIterator)d1 : null;
				
				if ( p0 != null && p1 != null ) return 0;
				if ( p0 != null ) return -1;
				if ( p1 != null ) return 1;
				
				final IndexIterator i0 = d0 instanceof IndexIterator ? (IndexIterator)d0 : null;
				final IndexIterator i1 = d1 instanceof IndexIterator ? (IndexIterator)d1 : null;
				if ( i0 == null && i1 == null ) return 0;
				if ( ( i0 != null ) != ( i1 != null ) ) return ( i0 != null ) ? 1 : -1;
				try {
					return (int)( ( i1.frequency() - i0.frequency() ) >> Integer.SIZE );
				}
				catch ( IOException e ) {
					throw new RuntimeException( e );
				}
			}
		}
		);

		lastIterator = sortedIterator[ n - 1 ];
		int i;
		for( i = n; i-- != 0; ) if ( sortedIterator[ i ] instanceof PayloadPredicateDocumentIterator ) break;
		predicateStart = i + 1;
		payloadPredicateDocumentIterator = new PayloadPredicateDocumentIterator[ predicateStart ];
		for( i = predicateStart; i-- != 0; ) payloadPredicateDocumentIterator[ i ] = (PayloadPredicateDocumentIterator)sortedIterator[ i ];
		
		if ( DEBUG ) System.err.println( "Sorted iterators: " + Arrays.toString( sortedIterator ) );
		
		/* If any document iterator is surely empty, we set curr to END_OF_LIST,
		 * so that mayHaveNext() can return false immediately.
		 * Note that the first align() will return END_OF_LIST anyway, so there
		 * is no need to test for curr being already END_OF_LIST. 
		 * Note also that the difference between documentIterator and this.documentIterator
		 * is immaterial here. */

		for ( i = n; i-- != 0; ) 
			if ( ! documentIterator[ i ].mayHaveNext() ) {
				// If any of the iterators is empty, we're over.
				curr = END_OF_LIST;
				return;
			}
	}

	/** Creates a new intersection iterator using a given array of iterators.
	 *  @param arg an argument that will be passed to {@link #getIntervalIterator(Index, int, boolean, Object)}.
	 *  @param documentIterator the iterators to be intersected (at least one).
	 */
	protected AbstractIntersectionDocumentIterator( final Object arg, final DocumentIterator... documentIterator ) {
		this( null, arg, documentIterator );
	}
	
	/** Advances all iterators to the first common document pointer after the one specified.
	 * {@link #lastIterator} is assumed to be positioned on the specified document.
	 * 
	 * <P>After a call to this method, all component iterators are positioned
	 * on the returned document.
	 * 
	 * @return the document on which all iterators are aligned, or {@link DocumentIterator#END_OF_LIST}. 
	 */
	protected final long align( long to ) throws IOException {
		if ( DEBUG ) System.err.println( this + ".align() [curr = " + curr + ", candidate = " + to + "]" );
		
		final int predicateStart = this.predicateStart;
		
		main: for(;;) {
			for( int i = n; i-- != predicateStart; ) {
				final long res = sortedIterator[ i ].skipTo( to );
				if ( res != to ) {
					to = res;
					continue main;
				}
			}
			
			for( int i = predicateStart; i-- != 0 ; ) {
				final long res = payloadPredicateDocumentIterator[ i ].skipUnconditionallyTo( to );
				if ( res != to ) {
					if ( res < 0 ) {
						if ( ++to == numberOfDocuments ) to = END_OF_LIST;
					}
					else to = res;
					continue main;
				}
			}
			
			return to;
		}
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		final Iterator<Index> i = indices.iterator();
		while( i.hasNext() ) intervalIterator( i.next() );
		return unmodifiableCurrentIterators;
	}
}
