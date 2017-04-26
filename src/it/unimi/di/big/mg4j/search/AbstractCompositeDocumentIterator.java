package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** An abstract iterator on documents based on a list of component iterators.
 * 
 * <p>The {@linkplain #AbstractCompositeDocumentIterator(Index, Object, DocumentIterator...)} caches
 * into {@link #documentIterator} the component iterators, and sets up a number of protected
 * fields that can be useful to implementors. It also provide abstract member classes that make it
 * easier to implement interval iterators.
 * 
 * <p>Note that this class implements both {@link #accept(DocumentIteratorVisitor)}
 * and {@link #acceptOnTruePaths(DocumentIteratorVisitor)} with a series of recursive
 * calls on <em>all</em> component iterator. If you desire a different behaviour
 * for {@link #acceptOnTruePaths(DocumentIteratorVisitor)} (see, e.g.,
 * {@link it.unimi.di.big.mg4j.search.AbstractUnionDocumentIterator}, please override it. 
 */

public abstract class AbstractCompositeDocumentIterator extends AbstractIntervalDocumentIterator implements DocumentIterator {
	/** The number of component iterators. */
	public final int n;
	/** The component document iterators. */
	public final DocumentIterator[] documentIterator;
	/** A cached copy of {@link #documentIterator}, if all
	 * underlying iterators are {@linkplain IndexIterator index iterators}; <code>null</code>, otherwise. */
	public final IndexIterator[] indexIterator;
	/** If {@link #indexIterator} is not {@code null}, the number of index iterators without positions. */ 
	protected int indexIteratorsWithoutPositions;
	
	/** Creates a new composite document iterator using a given list of component document iterators and
	 * a specified index.
	 * 
	 *  @param index an index that will constitute the only index for which this iterator will return intervals, 
	 *  or <code>null</code> to require the computation of the set of indices as the union of the indices
	 *  of all component iterators.
	 *  @param arg an argument that will be passed to {@link #getIntervalIterator(Index, int, boolean, Object)}.
	 *  @param documentIterator the component iterators.
	 */
	protected AbstractCompositeDocumentIterator( final Index index, final Object arg, final DocumentIterator... documentIterator ) {
		super( documentIterator.length, indices( index, documentIterator ), allIndexIterators( documentIterator ),arg );
		this.documentIterator = documentIterator;
		this.n = documentIterator.length;

		int i = n;
		while( i-- != 0 ) if ( ! ( documentIterator[ i ] instanceof IndexIterator ) ) break;
		if ( i == -1 ) {
			indexIterator = new IndexIterator[ n ];
			System.arraycopy( documentIterator, 0, indexIterator, 0, n );
			
			int t = 0;
			for( int j = n; j-- != 0; ) if ( ! indexIterator[ j ].index().hasPositions ) t++;
			indexIteratorsWithoutPositions = t;
		}
		else {
			indexIterator = null;
			indexIteratorsWithoutPositions = 0;
		}
	}
	
	/** Creates a new composite document iterator using a given list of component document iterators.
	 * 
	 *  @param documentIterator the component iterators.
	 */
	protected AbstractCompositeDocumentIterator( final DocumentIterator... documentIterator ) {
		this( null, null, documentIterator );
	}
	
	public <T> T accept( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( n );
		if ( a == null ) {
			for( int i = 0; i < n; i++ ) if ( documentIterator[ i ] != null && documentIterator[ i ].accept( visitor ) == null ) return null;
		}
		else {
			for( int i = 0; i < n; i++ ) if ( documentIterator[ i ] != null && ( a[ i ] = documentIterator[ i ].accept( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}
	
	public <T> T acceptOnTruePaths( final DocumentIteratorVisitor<T> visitor ) throws IOException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] a = visitor.newArray( n ); 
		if ( a == null ) {
			for( int i = 0; i < n; i++ ) if ( documentIterator[ i ] != null && documentIterator[ i ].acceptOnTruePaths( visitor ) == null ) return null;
		}
		else {
			for( int i = 0; i < n; i++ ) if ( documentIterator[ i ] != null && ( a[ i ] = documentIterator[ i ].acceptOnTruePaths( visitor ) ) == null ) return null;
		}
		return visitor.visitPost( this, a );
	}

	public void dispose() throws IOException {
		for( DocumentIterator d: documentIterator ) d.dispose();
	}
	
	public String toString() {
		StringBuilder res = new StringBuilder();
		res.append( this.getClass().getSimpleName() ).append( "(" );
		for ( int i = 0; i < n; i++ ) res.append( i > 0 ? "," : "" ).append( documentIterator[ i ] );
		res.append( ")" );
		if ( weight != 1 ) res.append( '{' ).append( weight ).append( '}' );
		return res.toString();
	}
	
	/** An abstract interval iterator. Provide mainly storage for the {@linkplain #intervalIterator component interval iterators},
	 *  place for {@linkplain #curr the last interval returned by each iterator} and  {@link #toString()}. */
	
	protected abstract static class AbstractCompositeIntervalIterator implements IntervalIterator {
		/** The underlying iterators. */
		protected IntervalIterator[] intervalIterator;
		/** The last interval returned by each iterator. */	 
		protected Interval[] curr;

		public AbstractCompositeIntervalIterator( final int n ) {
			// We just set up some internal data, but we perform no initialisation.
			curr = new Interval[ n ];
			intervalIterator = new IntervalIterator[ n ];
		}

		public String toString() {
		   MutableString res = new MutableString();
		   res.append( this.getClass().getName() ).append( "(" ).delete( 0, res.lastIndexOf( '.' ) + 1 );
		   for ( int i = 0; i < intervalIterator.length; i++ ) res.append( i > 0 ? "," : "" ).append( intervalIterator[ i ] );
		   return res.append( ")" ).toString();
		}
	}

	/** An abstract {@link IndexIterator}-based interval iterator. The difference with {@link AbstractCompositeIntervalIterator}
	 * is that this class assumes that all document iterators are actually index iterators.
	 * The algorithms in this (very common) case can be significantly simplified, obtaining
	 * a large gain in performance. */
	
	protected abstract static class AbstractCompositeIndexIntervalIterator implements IntervalIterator {
		/** The last interval returned by each iterator. */	 
		protected int[] curr;

		public AbstractCompositeIndexIntervalIterator( final int n ) {
			// We just set up some internal data, but we perform no initialisation.
			curr = new int[ n ];
		}

		public String toString() {
		   MutableString res = new MutableString();
		   res.append( this.getClass().getName() ).append( "(" ).delete( 0, res.lastIndexOf( '.' ) + 1 );
		   //for ( int i = 0; i < position.length; i++ ) res.append( i > 0 ? "," : "" ).append( position[ i ] != null ? IntArrayList.wrap( xposition[ i ], count[ i ] ) : "{}" );
		   return res.append( ")" ).toString();
		}
	}

}