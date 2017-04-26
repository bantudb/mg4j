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
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** An iterator on documents that returns the OR of a number of document iterators. */

public class OrDocumentIterator extends MockDocumentIterator {

	/** Returns a document iterator that computes the OR of the given array of iterators.
	 * 
	 * <P>Note that the special case of the empty and of the singleton arrays
	 * are handled efficiently.
	 * 
 	 * @param index the default index; relevant only if <code>it</code> has zero length.
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the OR of <code>it</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( final Index index, DocumentIterator... documentIterator  ) throws IOException {
		if ( documentIterator.length == 0 ) return FalseDocumentIterator.getInstance( index );
		if ( documentIterator.length == 1 ) return documentIterator[ 0 ];
		return new OrDocumentIterator( documentIterator );
	}

	/** Returns a document iterator that computes the OR of the given nonzero-length array of iterators.
	 * 
	 * <P>Note that the special case of the singleton array is handled efficiently.
	 * 
	 * @param documentIterator the iterators to be joined.
	 * @return a document iterator that computes the OR of <code>it</code>. 
	 * @throws IOException 
	 */
	public static DocumentIterator getInstance( DocumentIterator... documentIterator  ) throws IOException {
		if ( documentIterator.length == 0 ) throw new IllegalArgumentException( "The provided array of document iterators is empty." );
		return new OrDocumentIterator( documentIterator );
	}

	/** Creates a new document iterator that computes the OR of the given array of iterators.
	 * @param documentIterators the iterators to be joined.
	 * @throws IOException 
	 */
	protected OrDocumentIterator( final DocumentIterator... documentIterators ) throws IOException {
		try{
			
			for ( DocumentIterator documentIterator: documentIterators ) indices.addAll( documentIterator.indices() );

			for ( DocumentIterator documentIterator: documentIterators ) {
				long documentPointer;
				while ( ( documentPointer = documentIterator.nextDocument() ) != END_OF_LIST ) {
					for ( Index index: indices ) {
						if ( !index.hasPositions ) {
							addTrueIteratorDocument( documentPointer, index );
							continue;
						}
						IntervalIterator intervalIterator = documentIterator.intervalIterator( index );
						Reference2ReferenceArrayMap<Index, IntervalSet> index2IntervalMap = elements.get( documentPointer );
						
						if ( intervalIterator == IntervalIterators.FALSE ) continue;
						if ( intervalIterator == IntervalIterators.TRUE ) {
							if ( index2IntervalMap == null || ! index2IntervalMap.containsKey( index ) ) 
								addTrueIteratorDocument( documentPointer, index );
							continue;
						}

						// Cleanup TRUE if we find intervals.
						if ( index2IntervalMap != null && index2IntervalMap.get( index ) == TRUE ) index2IntervalMap.remove( index );
						for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) {	
							addIntervalForDocument( documentPointer, index, interval );
						}
					}
				}
			}
			for ( DocumentIterator documentIterator: documentIterators ) documentIterator.dispose();
			start( true );
		} catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

}
