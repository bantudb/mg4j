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
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.util.Iterator;



/** A document iterator that filters another document iterator, returning just intervals (and containing
 * documents) whose length does not exceed a given threshold.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.9
 */

public class LowPassDocumentIterator extends AbstractIntervalDocumentIterator {

	private final static boolean DEBUG = false;
	@SuppressWarnings("unused")
	private final static boolean ASSERTS = false;

	/** The underlying iterator. */
	protected final DocumentIterator documentIterator;
	/** The iterator threshold. */
	protected final int threshold; 

	/** Creates a new low-pass document iterator over a given iterator.
	 * @param documentIterator the iterator to be filtered.
	 * @param threshold the filter threshold.
	 */
	protected LowPassDocumentIterator( final DocumentIterator documentIterator, final int threshold ) {
		super( 1, indices( null, documentIterator ), allIndexIterators( documentIterator ), null );
		this.documentIterator = documentIterator;
		this.threshold = threshold;
	}

	/** Returns a low-pass document iterator over a given iterator.
	 * @param it the iterator to be filtered.
	 * @param threshold the filter threshold.
	 */
	public static LowPassDocumentIterator getInstance( final DocumentIterator it, final int threshold ) {
		return new LowPassDocumentIterator( it, threshold );
	}

	@Override
	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object arg ) {
		return new LowPassIntervalIterator( index );
	}

	private boolean noIntervals() throws IOException {
		/* The policy here is that a low-pass is valid is at least one of the underlying
		 * interval iterators, once filtered, would return at least one interval. Note
		 * that TRUE iterators are not actually filtered, so they always 
		 * return true on a call to hasNext(). */
		
		if ( soleIndex != null ) return intervalIterator() == IntervalIterators.FALSE;
		
		for( Index index: indices() ) if ( intervalIterator( index ) != IntervalIterators.FALSE ) return false;
		return true;
	}

	@Override
	public long nextDocument() throws IOException {
		do currentIterators.clear(); while( ( curr = documentIterator.nextDocument() ) != END_OF_LIST && noIntervals() );
		return curr;
	}
	
	@Override
	public boolean mayHaveNext() {
		return documentIterator.mayHaveNext();
	}

	@Override
	public long skipTo( final long n ) throws IOException {
		if ( DEBUG ) System.err.println( this + ".skipTo(" + n + "); last = " + curr );
		if ( curr >= n ) return curr;

		currentIterators.clear();
		// We first try to get a candidate document.
		if ( ( curr = documentIterator.skipTo( n ) ) != END_OF_LIST && noIntervals() ) nextDocument();
		if ( DEBUG ) System.err.println( this + ".skipTo(" + n + ") => " + curr );
		return curr;
	}

	@Override
	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		final Iterator<Index> i = indices().iterator();
		while( i.hasNext() ) intervalIterator( i.next() );
		return unmodifiableCurrentIterators;
	}

	@Override
	public IntervalIterator intervalIterator() throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator()" );
		
		IntervalIterator intervalIterator;

		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( soleIndex ) ) != null ) return intervalIterator;

		intervalIterator = documentIterator.intervalIterator();

		/* If the underlying iterator is TRUE or FALSE, then our contribution to the result is not relevant,
		 * and we just pass this information upwards. E.g., consider the query (A OR title:B)~2 with
		 * a document containing A but not B in its title. When evaluating the query for the title index,
		 * the subquery before the low-pass operator evaluates to TRUE, meaning that its truth is independent
		 * of the title field. This fact is not changed by the low-pass operator. */
		
		if ( intervalIterator != IntervalIterators.TRUE && intervalIterator != IntervalIterators.FALSE ) intervalIterator = soleIntervalIterator.reset();
		
		currentIterators.put( soleIndex, intervalIterator );	
		return intervalIterator;
	}


	@Override
	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );
		if ( ! documentIterator.indices().contains( index ) ) return IntervalIterators.FALSE;

		IntervalIterator intervalIterator;

		// If the iterator has been created and it's ready, we just return it.		
		if ( ( intervalIterator = currentIterators.get( index ) ) != null ) return intervalIterator;

		intervalIterator = documentIterator.intervalIterator( index );
			
		/* If the underlying iterator is TRUE or FALSE, then our contribution to the result is not relevant,
		 * and we just pass this information upwards. E.g., consider the query (A OR title:B)~2 with
		 * a document containing A but not B in its title. When evaluating the query for the title index,
		 * the subquery before the low-pass operator evaluates to TRUE, meaning that its truth is independent
		 * of the title field. This fact is not changed by the low-pass operator. */
		
		if ( intervalIterator != IntervalIterators.TRUE && intervalIterator != IntervalIterators.FALSE ) intervalIterator = intervalIterators.get( index ).reset();

		currentIterators.put( index, intervalIterator );	
		return intervalIterator;
	}

	@Override
	public void dispose() throws IOException {
		documentIterator.dispose();
	}
	
	@Override
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( documentIterator.accept( visitor ) == null ) return null;
		}
		else {
			if ( ( a[ 0 ] = documentIterator.accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	@Override
	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( 1 );
		if ( a == null ) {
			if ( documentIterator.acceptOnTruePaths( visitor ) == null ) return null;			
		}
		else {
			if ( ( a[ 0 ] = documentIterator.acceptOnTruePaths( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}
	
	public String toString() {
	   return this.getClass().getSimpleName() + "(" + documentIterator + ", " + threshold + ")";
	}
	
	/** An interval iterator returning just the interval shorter than {@link #threshold}. */
	
	protected class LowPassIntervalIterator implements IntervalIterator {
		/** The index of this iterator. */
		final Index index;
		/** The underlying interval iterator. */
		private IntervalIterator intervalIterator;
		/** The first result, cached. */
		private Interval first;
		
		protected LowPassIntervalIterator( final Index index ) {
			this.index = index;
		}

		@Override
		public IntervalIterator reset( ) throws IOException {
			intervalIterator = documentIterator.intervalIterator( index );
			first = null;
			return ( first = nextInterval() ) == null ? IntervalIterators.FALSE : this;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			// Just delegate to the filtered iterator
			intervalIterator.intervalTerms( terms );
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			if ( first != null ) {
				final Interval result = first;
				first = null;
				return result;
			}
			
			Interval result;
			while( ( result = intervalIterator.nextInterval() ) != null && result.length() > threshold );
			return result;
		}
		
		@Override
		public int extent() {
			return Math.min( intervalIterator.extent(), threshold );
		}
		
		@Override
		public String toString() {
		   return getClass().getSimpleName() + "(" + intervalIterator + ", " + threshold + ")";
		}
	}
}
