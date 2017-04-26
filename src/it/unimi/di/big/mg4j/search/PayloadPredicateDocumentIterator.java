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
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;

import org.apache.commons.collections.Predicate;


/** A document iterator that filters an {@link IndexIterator}, returning just
 * documents whose payload satisfies a given predicate.
 * The interval iterators are computed by delegation to the underlying {@link IndexIterator}.
 *
 * <p>Besides the classic {@link #skipTo(long)} method, this class provides a {@link #skipUnconditionallyTo(long)}
 * method that skips to a given document <em>even if the document does not match the predicate</em>. This
 * feature is fundamental to implement an efficient list intersection algorithm, as {@link #skipTo(long)} is
 * very expensive when the argument does not satisfy the predicate (as the next valid document must be searched
 * for exhaustively).
 * 
 * @author Sebastiano Vigna
 * @since 0.9
 */

public class PayloadPredicateDocumentIterator extends AbstractDocumentIterator implements DocumentIterator {
	/** The underlying iterator. */
	private final IndexIterator indexIterator;
	/** The predicate to filter payloads. */
	private final Predicate payloadPredicate;

	/** Creates a new payload-predicate document iterator over a given index iterator.
	 * @param indexIterator an index iterator.
	 * @param payloadPredicate a predicate on payloads that will be used to filter the documents returned by <code>indexIterator</code>.
	 */
	protected PayloadPredicateDocumentIterator( final IndexIterator indexIterator, final Predicate payloadPredicate ) {
		this.indexIterator = indexIterator;
		this.payloadPredicate = payloadPredicate;
	}

	/** Returns a new payload-predicate document iterator over a given index iterator.
	 * @param indexIterator an index iterator.
	 * @param payloadPredicate a predicate on payloads that will be used to filter the documents returned by <code>indexIterator</code>.
	 */
	public static PayloadPredicateDocumentIterator getInstance( final IndexIterator indexIterator, final Predicate payloadPredicate ) {
		return new PayloadPredicateDocumentIterator( indexIterator, payloadPredicate );
	}

	public ReferenceSet<Index> indices() {
		return indexIterator.indices();
	}

	public long skipTo( final long n ) throws IOException {
		if ( curr >= n ) return curr;
		if ( ( curr = indexIterator.skipTo( n ) ) != END_OF_LIST && ! payloadPredicate.evaluate( indexIterator.payload() ) ) nextDocument();
		return curr;
	}

	/** Skips to the given document, even if the document does not satisfy the predicate of this document iterator.
	 * 
	 * @param candidate a document pointer.
	 * @return assuming that <code>p</code> is the first document pointer larger than or equal to <code>n</code>,
	 * <code>p</code> if document <code>p</code> satisfies the predicate, <code>-p-1</code> otherwise; 
	 * if there is no document
	 * pointer larger than or equal to <code>n</code>, {@link DocumentIterator#END_OF_LIST}.
	 * @throws IOException 
	 * @see #skipTo(long)
	 */
	
	public long skipUnconditionallyTo( final long candidate ) throws IOException {
		if ( curr < candidate ) curr = indexIterator.skipTo( candidate );
		if ( curr == END_OF_LIST ) return END_OF_LIST;
		return payloadPredicate.evaluate( indexIterator.payload() ) ? curr : -curr - 1;
	}
	
	public long nextDocument() throws IOException {
		long d;
		while( ( d = indexIterator.nextDocument() ) != END_OF_LIST && ! payloadPredicate.evaluate( indexIterator.payload() ) );
		return curr = d;
	}
	
	public boolean mayHaveNext() {
		return indexIterator.mayHaveNext();
	}

	public void dispose() throws IOException {
		indexIterator.dispose();
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( indexIterator.accept( visitor ) == null ) return null;
		}
		else {			
			if ( ( a[ 0 ] = indexIterator.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( indexIterator.acceptOnTruePaths( visitor ) == null ) return null;
		}
		else {			
			if ( ( a[ 0 ] = indexIterator.acceptOnTruePaths( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public String toString() {
	   return getClass().getSimpleName() + "(" + indexIterator + ")" + payloadPredicate;
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		return indexIterator.intervalIterators();
	}

	public IntervalIterator intervalIterator() throws IOException {
		return indexIterator.intervalIterator();
	}

	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		return indexIterator.intervalIterator( index );
	}
}
