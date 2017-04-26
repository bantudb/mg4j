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
import it.unimi.di.big.mg4j.search.TrueDocumentIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** An iterator returning documents containing consecutive intervals (in query order) 
 * satisfying the underlying queries. 
 * 
 * <strong>Warning</strong>: this implementation will not work correctly if one or more of the last iterators
 * returns {@link IntervalIterators#TRUE}: in that case, the intervals returned will be artificially enlarged.
 */

public class ConsecutiveDocumentIterator extends MockDocumentIterator {
	
	/** Returns a document iterator that computes the consecutive AND of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>it</code> has zero length.
	 * @param documentIterator the iterators to be composed.
	 * @return a document iterator that computes the consecutive AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( final Index index, final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) return new MockDocumentIterator( TrueDocumentIterator.getInstance( index ) );
		if ( documentIterator.length == 1 ) return new MockDocumentIterator( documentIterator[ 0 ] );
		return new ConsecutiveDocumentIterator( documentIterator, null );
	}
	
	/** Returns a document iterator that computes the consecutive AND of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be composed (at least one).
	 * @return a document iterator that computes the consecutive AND of <code>documentIterator</code>. 
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( final DocumentIterator... documentIterator ) throws IOException {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		if ( documentIterator.length == 1 ) return new MockDocumentIterator( documentIterator[ 0 ] );
		return getInstance( null, documentIterator );
	}
	
	/** Returns a document iterator that computes the consecutive AND of the given nonzero-length array of iterators, adding
	 * gaps between intervals.
	 * 
	 * <p>A match will satisfy the condition
	 * that the left extreme of the first interval is larger than or equal to the
	 * first gap, the left extreme of the second interval is larger than 
	 * the right extreme of the first interval plus the second gap, and so on. This semantics
	 * makes it possible to perform phrasal searches &ldquo;with holes&rdquo;, typically
	 * because of stopwords that have not been indexed.
	 * 
	 * @param documentIterator the iterators to be composed (at least one).
	 * @param gap an array of gaps parallel to <code>documentIterator</code>, or <code>null</code> for no gaps. 
	 * @return a document iterator that computes the consecutive AND of <code>documentIterator</code> using the given gaps.  
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( final DocumentIterator documentIterator[], final int gap[] ) throws IOException {
		if ( gap != null && gap.length != documentIterator.length ) throw new IllegalArgumentException( "The number of gaps (" + gap.length + ") is not equal to the number of document iterators (" + documentIterator.length +")" );
		if ( documentIterator.length == 1 && ( gap == null || gap[ 0 ] == 0 ) ) return new MockDocumentIterator( documentIterator[ 0 ] );
		return new ConsecutiveDocumentIterator( documentIterator, gap );
	}
	
	protected ConsecutiveDocumentIterator( final DocumentIterator[] documentIterator, int[] gap ) throws IOException {
		int n = documentIterator.length;
		
		if ( gap == null ) gap = new int[ n ];
		
		for ( DocumentIterator it: documentIterator ) indices.addAll( it.indices() );

		if ( n == 1 ) {
			long documentPointer;
			while ( ( documentPointer = documentIterator[ 0 ].nextDocument() ) != END_OF_LIST ) {
				Reference2ReferenceMap<Index, IntervalIterator> intervalIterators = documentIterator[ 0 ].intervalIterators();
				for ( Index index: intervalIterators.keySet() ) {
					if ( intervalIterators.get( index ) == IntervalIterators.TRUE ) addTrueIteratorDocument( documentPointer, index );
					else if ( intervalIterators.get( index ) == IntervalIterators.FALSE ) addFalseIteratorDocument( documentPointer, index );
					else {
						IntervalIterator intervalIterator = intervalIterators.get( index );
						for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) {
							if ( interval.left >= gap[ 0 ] )
								addIntervalForDocument( documentPointer, index, Interval.valueOf( interval.left - gap[ 0 ], interval.right ) );
						}
					}
				}
			}
			documentIterator[ 0 ].dispose();
			start( true );
			return;
		}
		
		DocumentIterator[] remaining = new DocumentIterator[ n - 1 ];
		int[] remainingGap = new int[ n - 1 ];
		System.arraycopy( documentIterator, 1, remaining, 0, n - 1 );
		System.arraycopy( gap, 1, remainingGap, 0, n - 1 );
		MockDocumentIterator it1 = new MockDocumentIterator( documentIterator[ 0 ] );
		MockDocumentIterator it2 = getInstance( remaining, remainingGap );
		for ( long documentPointer: it1.elements.keySet() ) 
			if ( it2.elements.keySet().contains( documentPointer ) ) {
				Reference2ReferenceArrayMap<Index, IntervalSet> map1 = it1.elements.get( documentPointer );
				Reference2ReferenceArrayMap<Index, IntervalSet> map2 = it2.elements.get( documentPointer );

				for ( Index index: map1.keySet() ) {
					if ( map2.containsKey( index ) ) {
						IntervalSet set1 = map1.get( index );
						IntervalSet set2 = map2.get( index );
						//System.out.println( documentPointer + " -> " + set1 );
						//System.out.println( documentPointer + " -> " + set2 );
						if ( set1 == TRUE )
							if ( set2 == TRUE )  // TRUE and TRUE
								addTrueIteratorDocument( documentPointer, index );
							else
								if ( set2 == FALSE ) // TRUE and FALSE
									addFalseIteratorDocument( documentPointer, index );
								else // TRUE and something 
									for ( Interval interval: set2 ) {
										if ( interval.left >= gap[ 0 ] )
											addIntervalForDocument( documentPointer, index, Interval.valueOf( interval.left - gap[ 0 ], interval.right ) );
									}
						else 
							if ( set2 == TRUE ) 
								if ( set1 == FALSE ) // FALSE and TRUE
									addFalseIteratorDocument( documentPointer, index );
								else // something and TRUE
									for ( Interval interval: set1 ) {
										if ( interval.left >= gap[ 0 ] )
											addIntervalForDocument( documentPointer, index, Interval.valueOf( interval.left - gap[ 0 ], interval.right ) );
									}
							else 
								if ( set1 == FALSE ) // FALSE and something
									addFalseIteratorDocument( documentPointer, index );
								else // something and something
									for ( Interval interval1: set1 )
										for ( Interval interval2: set2 ) {
											//if ( documentPointer == 0 ) System.out.println( "\t*** " + interval1 + " " + interval2 + " " + gap[ gap.length - 1  ] );
											if ( interval1.left >= gap[ 0 ] && interval2.left == interval1.right + 1 ) { 
												addIntervalForDocument( documentPointer, index, Interval.valueOf( interval1.left - gap[ 0 ], interval2.right ) );
												//if ( documentPointer == 0 ) System.out.println( documentPointer + " For " + interval1 + " and " + interval2 +" adding " +  Interval.valueOf( Math.min( interval1.left, interval2.left ) - gap[ 0 ], Math.max( interval1.right, interval2.right ) ) + ", where gaps=" + Arrays.toString( gap )  );
											}
										}
					}
				}
			}
		it1.dispose();
		it2.dispose();
		for ( DocumentIterator it: documentIterator ) it.dispose();
		start( true );
	}
}
