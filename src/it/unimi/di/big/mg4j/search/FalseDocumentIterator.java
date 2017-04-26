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
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;

/** An empty document iterator. */

public class FalseDocumentIterator extends AbstractDocumentIterator {
	final ReferenceSet<Index> indices;
	
	protected FalseDocumentIterator( final Index index ) {
		indices = ReferenceSets.singleton( index );
		curr = END_OF_LIST;
	}

	/** Creates a false document iterator with given index.
	 * 
	 * @param index the index of this iterator.
	 * @return a false document iterator with given index.
	 */
	public static FalseDocumentIterator getInstance( final Index index ) {
		return new FalseDocumentIterator( index );
	}
	
	public IntervalIterator intervalIterator() {
		throw new IllegalStateException();
	}

	public IntervalIterator intervalIterator( Index index ) {
		throw new IllegalStateException();
	}

	public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() {
		throw new IllegalStateException();
	}

	public ReferenceSet<Index> indices() {
		return indices;
	}
	
	@Override
	public long nextDocument() {
		return END_OF_LIST;
	}

	public long skipTo( long n ) {
		return END_OF_LIST;
	}

	public <T> T accept( DocumentIteratorVisitor<T> visitor ) {
		if ( !visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) {
		throw new IllegalStateException();
	}

	public void dispose() {}
}
