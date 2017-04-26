package it.unimi.di.big.mg4j.mock.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 20132-2016 Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.objects.Reference2ReferenceArrayMap;
import it.unimi.dsi.util.Interval;

import java.io.IOException;


public class InclusionDocumentIterator extends MockDocumentIterator {

	public static MockDocumentIterator getInstance( final DocumentIterator firstIterator, final DocumentIterator secondIterator, final int leftMargin, final int rightMargin ) throws IOException {
		return new InclusionDocumentIterator( firstIterator, secondIterator, leftMargin, rightMargin );
	}

	static int call = -1;
	protected InclusionDocumentIterator( final DocumentIterator firstIterator, final DocumentIterator secondIterator, final int leftMargin, final int rightMargin ) throws IOException {
		indices.addAll( firstIterator.indices() );
		MockDocumentIterator it1 = new MockDocumentIterator( firstIterator );
		MockDocumentIterator it2 = new MockDocumentIterator( secondIterator );

		for ( long documentPointer: it1.elements.keySet() ) {
			Reference2ReferenceArrayMap<Index, IntervalSet> map1 = it1.elements.get( documentPointer );
			if ( it2.elements.keySet().contains( documentPointer ) ) {
				Reference2ReferenceArrayMap<Index, IntervalSet> map2 = it2.elements.get( documentPointer );
				assert map1.size() == 1;
				assert map2.size() == 1;
				final Index index1 = map1.keySet().iterator().next();
				final Index index2 = map2.keySet().iterator().next();
				IntervalSet set1 = map1.get( index1 );
				IntervalSet set2 = map2.get( index2 );

				if ( set1 == FALSE || set1 == FALSE ) {  
					addFalseIteratorDocument( documentPointer, index1 );
					continue;
				}
				if ( set1 == TRUE ) {  
					addTrueIteratorDocument( documentPointer, index1 );
					continue;
				}
				if ( set2 == TRUE ) {
					addFalseIteratorDocument( documentPointer, index1 );
					continue;
				}
				for ( Interval interval1: set1 ) {
					boolean good = false;
					for ( Interval interval2: set2 ) 
						if ( Interval.valueOf( interval2.left - leftMargin, interval2.right + rightMargin ).contains( interval1 ) ) {
							good = true;
							break;
						}
					if ( good ) addIntervalForDocument( documentPointer, index1, interval1 );
				}
			}
		}
		it1.dispose();
		it2.dispose();
		firstIterator.dispose();
		secondIterator.dispose();
		start( true );
	}
}
