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
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;



public class LowPassDocumentIterator extends MockDocumentIterator {


	private final DocumentIterator documentIterator;
	private final int threshold;

	/** Creates a new low-pass document iterator over a given iterator.
	 *
	 * @param documentIterator the iterator to be filtered.
	 * @param threshold the filter threshold.
	 */
	protected LowPassDocumentIterator( final DocumentIterator documentIterator, final int threshold ) {
		this.documentIterator = documentIterator;
		this.threshold = threshold;
		
		indices.addAll( documentIterator.indices() );
		
		try{
			long documentPointer;
			while ( ( documentPointer = documentIterator.nextDocument() ) != END_OF_LIST ) {
				Reference2ReferenceMap<Index, IntervalIterator> intervalIterators = documentIterator.intervalIterators();
				for ( Index index: intervalIterators.keySet() ) { 
					IntervalIterator intervalIterator = intervalIterators.get( index );
					if ( intervalIterator == IntervalIterators.TRUE ) {
						addTrueIteratorDocument( documentPointer, index );
						continue;
					}
					if ( intervalIterator == IntervalIterators.FALSE ) {
						addFalseIteratorDocument( documentPointer, index );
						continue;
					}
					for( Interval interval; ( interval = intervalIterator.nextInterval() ) != null; ) {
						if ( interval.length() <= threshold )
							addIntervalForDocument( documentPointer, index, interval );
					}
				}
			}
			documentIterator.dispose();
			start( true );
		} catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	/** Returns a low-pass document iterator over a given iterator.
	 * @param it the iterator to be filtered.
	 * @param threshold the filter threshold.
	 */
	public static LowPassDocumentIterator getInstance( final DocumentIterator it, final int threshold ) {
		return new LowPassDocumentIterator( it, threshold );
	}

	public String toString() {
	   return this.getClass().getSimpleName() + "(" + documentIterator + ", " + threshold + ")";
	}
	
}
