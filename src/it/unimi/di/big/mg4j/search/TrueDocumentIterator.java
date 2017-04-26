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
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;

/** A single-index document iterator returning all documents with interval iterator {@link IntervalIterators#TRUE}. */

public class TrueDocumentIterator extends AbstractDocumentIterator {
	/** The only index. */
	private final Index soleIndex;
	/** A singleton set containing {@link #soleIndex}. */
	private final ReferenceSet<Index> indices;
	/** A singleton map associating {@link #soleIndex} with {@link IntervalIterators#TRUE}. */
	private Reference2ReferenceMap<Index, IntervalIterator> intervalIterators;
	
	protected TrueDocumentIterator( final Index index ) {
		indices = ReferenceSets.singleton( soleIndex = index );
		intervalIterators = Reference2ReferenceMaps.singleton( soleIndex, IntervalIterators.TRUE );
	}

	/** Creates a true document iterator with given index.
	 * 
	 * @param index the index of this iterator.
	 * @return a true document iterator with given index.
	 */
	public static TrueDocumentIterator getInstance( final Index index ) {
		return new TrueDocumentIterator( index );
	}
	
	public IntervalIterator intervalIterator() {
		return IntervalIterators.TRUE;
	}

	public IntervalIterator intervalIterator( Index index ) {
		return index == soleIndex ? IntervalIterators.TRUE : IntervalIterators.FALSE;
	}

	public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() {
		return intervalIterators;
	}

	public ReferenceSet<Index> indices() {
		return indices;
	}

	public long nextDocument() {
		if ( curr < soleIndex.numberOfDocuments - 1 ) return ++curr;
		return curr = END_OF_LIST;
	}

	public boolean mayHaveNext() {
		return curr < soleIndex.numberOfDocuments - 1;
	}
	
	public long skipTo( final long n ) {
		if ( n <= curr ) return curr;
		return curr = n >= soleIndex.numberOfDocuments ? END_OF_LIST : n;
	}

	public <T> T accept( DocumentIteratorVisitor<T> visitor ) {
		if ( !visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) {
		if ( !visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public void dispose() {}
}
