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
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.util.Iterator;



/** A (temporary) document iterator that interpret an index iterator as an annotation and unpacks
 * the position list into an interval list.
 * 
 * @author Sebastiano Vigna
 * @since 5.1.0
 */

public class AnnotationDocumentIterator extends AbstractIntervalDocumentIterator {

	private final static boolean DEBUG = false;
	@SuppressWarnings("unused")
	private final static boolean ASSERTS = false;

	/** The underlying iterator. */
	protected final IndexIterator indexIterator;

	protected AnnotationDocumentIterator( final IndexIterator indexIterator ) {
		super( 1, indices( null, indexIterator ), allIndexIterators( indexIterator ), null );
		this.indexIterator = indexIterator;
	}

	public static AnnotationDocumentIterator getInstance( final IndexIterator it ) {
		return new AnnotationDocumentIterator( it );
	}

	@Override
	protected IntervalIterator getIntervalIterator( final Index index, final int n, final boolean allIndexIterators, final Object arg ) {
		return new AnnotationIntervalIterator( index );
	}

	@Override
	public long nextDocument() throws IOException {
		currentIterators.clear();
		return curr = indexIterator.nextDocument();
	}
	
	@Override
	public boolean mayHaveNext() {
		return indexIterator.mayHaveNext();
	}

	@Override
	public long skipTo( final long n ) throws IOException {
		currentIterators.clear();
		return curr = indexIterator.skipTo( n );
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
		currentIterators.put( soleIndex, intervalIterator = soleIntervalIterator.reset() );	
		return intervalIterator;
	}


	@Override
	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		ensureOnADocument();
		if ( DEBUG ) System.err.println( this + ".intervalIterator(" + index + ")" );
		if ( index != soleIndex ) return IntervalIterators.FALSE;
		return intervalIterator();
	}

	@Override
	public void dispose() throws IOException {
		indexIterator.dispose();
	}
	
	@Override
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

	@Override
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
	   return this.getClass().getSimpleName() + "(" + indexIterator + ")";
	}
	
	protected class AnnotationIntervalIterator implements IntervalIterator {
		/** The index of this iterator. */
		final Index index;
		/** The index of the next annotation minus one. */
		private int anno;
		
		protected AnnotationIntervalIterator( final Index index ) {
			this.index = index;
		}

		@Override
		public IntervalIterator reset( ) throws IOException {
			anno = -1;
			return this;
		}

		@Override
		public void intervalTerms( final LongSet terms ) {
			// Just delegate to the filtered iterator
			terms.add( indexIterator.termNumber() );
		}
		
		@Override
		public Interval nextInterval() throws IOException {
			final int firstPoint = indexIterator.nextPosition();
			if ( firstPoint == IndexIterator.END_OF_POSITIONS ) return null;
			++anno;
			return Interval.valueOf( firstPoint - anno, indexIterator.nextPosition() - anno - 1 );
		}
		
		@Override
		public int extent() {
			return 1; 
		}
		
		@Override
		public String toString() {
		   return getClass().getSimpleName() + "(" + indexIterator + ")";
		}
	}
}
