package it.unimi.di.big.mg4j.search.visitor;

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


import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.MultiTermIndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrays;

import java.io.IOException;
import java.util.ArrayList;

/** A visitor collecting terms that satisfy a query for the current document.
 * 
 * <p><em>True terms</em> are terms reachable by {@linkplain DocumentIterator#acceptOnTruePaths(DocumentIteratorVisitor) true paths}.
 * This visitor collects true terms are exposes them in the public {@link #trueTerms} variable, in the
 * order in which they appear in a visit of the iterator. 
 */


public class TrueTermsCollectionVisitor extends AbstractDocumentIteratorVisitor {
	/** The list of true terms collected in the last visit. */
	public final ArrayList<String> trueTerms;
	/** Temporary storage for the result of {@link MultiTermIndexIterator#front(IndexIterator[])}. */
	private IndexIterator[] indexIterator;

	/** Creates a new visitor collecting true terms. */

	public TrueTermsCollectionVisitor( ) {
		trueTerms = new ArrayList<String>();
		indexIterator = new IndexIterator[ 16 ];
	}	

	@Override
	public AbstractDocumentIteratorVisitor prepare() {
		trueTerms.clear();
		return this;
	}

	public Boolean visit( final IndexIterator indexIterator ) throws IOException {
		trueTerms.add( indexIterator.term() );
		return Boolean.TRUE;
	}

	public Boolean visit( final MultiTermIndexIterator multiTermIndexIterator ) throws IOException {
		indexIterator = ObjectArrays.grow( indexIterator, multiTermIndexIterator.n );
		final int k = multiTermIndexIterator.front( indexIterator );
		for( int i = 0; i < k; i++ ) trueTerms.add( indexIterator[ i ].term() );
		return Boolean.TRUE;
	}

	public String toString() {
		return getClass().getName() + trueTerms;
	}
}
