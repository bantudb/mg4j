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
import it.unimi.di.big.mg4j.search.FalseDocumentIterator;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** An iterator on documents that returns the AND of a number of document iterators. */

public class AndDocumentIterator extends MockDocumentIterator {

	/** Returns a document iterator that computes the AND of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>it</code> has zero length.
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( final Index index, DocumentIterator... documentIterator  ) throws IOException {
		if ( documentIterator.length == 0 ) return new MockDocumentIterator( FalseDocumentIterator.getInstance( index ) );
		if ( documentIterator.length == 1 ) return new MockDocumentIterator( documentIterator[ 0 ] );
		return new AndDocumentIterator( documentIterator );
	}

	/** Returns a document iterator that computes the AND of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the AND of <code>it</code>. 
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( DocumentIterator... documentIterator  ) throws IOException {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		if ( documentIterator.length == 1 ) return new MockDocumentIterator( documentIterator[ 0 ] );
		return new AndDocumentIterator( documentIterator );
	}

	/** Creates a new document iterator that computes the AND of the given array of iterators.
	 *  @param documentIterators the iterators to be joined.
	 * @throws IOException 
	 */
	protected AndDocumentIterator( final DocumentIterator... documentIterators ) throws IOException {
		int n = documentIterators.length;
		
		for ( DocumentIterator documentIterator: documentIterators ) indices.addAll( documentIterator.indices() );
		
		DocumentIterator[] remaining = new DocumentIterator[ n - 1 ];
		System.arraycopy( documentIterators, 1, remaining, 0, n - 1 );
		MockDocumentIterator it1 = new MockDocumentIterator( documentIterators[ 0 ] );
		MockDocumentIterator it2 = getInstance( remaining );
		for ( long documentPointer: it1.elements.keySet() ) { 
			if ( it2.elements.keySet().contains( documentPointer ) ) {
				 Reference2ReferenceArrayMap<Index, IntervalSet> map1 = it1.elements.get( documentPointer );
				 Reference2ReferenceArrayMap<Index, IntervalSet> map2 = it2.elements.get( documentPointer );
				 for ( Index index: indices ) {
					 IntervalSet set1 = map1.get( index );
					 IntervalSet set2 = map2.get( index );
					 
					 if ( set1 == TRUE )
						 if ( set2 == TRUE )  // TRUE and TRUE
							 addTrueIteratorDocument( documentPointer, index );
						 else
							 if ( set2 == FALSE ) // TRUE and FALSE
								 addTrueIteratorDocument( documentPointer, index );
							 else // TRUE and something
								 addIntervalsForDocument( documentPointer, index, set2 );
					 else 
						 if ( set2 == TRUE ) 
							 if ( set1 == FALSE ) // FALSE and TRUE
								 addTrueIteratorDocument( documentPointer, index );
							 else // something and TRUE
								 addIntervalsForDocument( documentPointer, index, set1 );
						 else 
							 if ( set1 == FALSE && set2 == FALSE ) // FALSE and FALSE
								 addFalseIteratorDocument( documentPointer, index );
							 else if ( set1 == FALSE ) // FALSE and something
								 addIntervalsForDocument( documentPointer, index, set2 );
							 else if ( set2 == FALSE ) // something and FALSE
								 addIntervalsForDocument( documentPointer, index, set1 );
							 else // something and something
								 for ( Interval interval1: set1 )
									 for ( Interval interval2: set2 )
										 addIntervalForDocument( documentPointer, index, Interval.valueOf( Math.min( interval1.left, interval2.left ), Math.max( interval1.right, interval2.right ) ) );
				 }
			}
		}
		it1.dispose();
		it2.dispose();
		for ( DocumentIterator documentIterator: documentIterators ) documentIterator.dispose();

		start( true );
	}
}
