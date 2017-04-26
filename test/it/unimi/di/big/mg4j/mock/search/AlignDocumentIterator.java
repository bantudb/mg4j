package it.unimi.di.big.mg4j.mock.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2008-2016 Sebastiano Vigna 
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


/** A document iterator that aligns the results of a number of document iterators over
 * different indices.
 */

public class AlignDocumentIterator extends MockDocumentIterator {
	/** Returns a document iterator that aligns the first iterator to the second.
	 * 
	 * @param aligneeIterator the iterator to be aligned.
	 * @param alignerIterator the iterator used to align <code>aligneeIterator</code>.
	 * 
	 * @return a document iterator that computes the alignment of <code>aligneeIterator</code> on <code>alignerIterator</code>. 
	 * @throws IOException 
	 * @throws IOException 
	 */
	public static MockDocumentIterator getInstance( final DocumentIterator aligneeIterator, final DocumentIterator alignerIterator ) throws IOException {
		return new AlignDocumentIterator( aligneeIterator, alignerIterator );
	}

	protected AlignDocumentIterator( final DocumentIterator aligneeIterator, final DocumentIterator alignerIterator ) throws IOException {
		if ( aligneeIterator.indices().size() != 1 || alignerIterator.indices().size() != 1 ) throw new IllegalArgumentException( "You can align single-index iterators only" );
		
		indices.addAll( aligneeIterator.indices() );
		indices.addAll( alignerIterator.indices() );
		
		MockDocumentIterator it1 = new MockDocumentIterator( aligneeIterator );
		MockDocumentIterator it2 = new MockDocumentIterator( alignerIterator );
		for ( long documentPointer: it1.elements.keySet() ) 
			if ( it2.elements.keySet().contains( documentPointer ) ) {
				Reference2ReferenceArrayMap<Index, IntervalSet> map1 = it1.elements.get( documentPointer );
				Reference2ReferenceArrayMap<Index, IntervalSet> map2 = it2.elements.get( documentPointer );
				assert map1.size() == 1;
				assert map2.size() == 1;
				final Index index1 = map1.keySet().iterator().next();
				final Index index2 = map2.keySet().iterator().next();
				IntervalSet set1 = map1.get( index1 );
				IntervalSet set2 = map2.get( index2 );
				if ( set1 == TRUE && set2 == TRUE ) {  
					addTrueIteratorDocument( documentPointer, index1 );
					continue;
				}
				if ( set1 != TRUE && set2 != TRUE ) {
					for ( Interval interval: set1 ) 
						if ( set2.contains( interval ) )
							addIntervalForDocument( documentPointer, index1, interval );
				}
			}
		it1.dispose();
		it2.dispose();
		aligneeIterator.dispose();
		alignerIterator.dispose();
		start( true );
	}
}
