package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2004-2016 Paolo Boldi and Sebastiano Vigna
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.AbstractCompositeDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMaps;

import java.io.IOException;

/** An abstract implementation of {@link it.unimi.di.big.mg4j.search.score.Scorer}.
 * It provides internal caching of the underlying
 * document iterator during {@linkplain #wrap(DocumentIterator) wrapping},
 * and a {@link #nextDocument()} method that delegates
 * to the underlying document iterator (implementing subclasses
 * that do not alter this behaviour should implement {@link it.unimi.di.big.mg4j.search.score.DelegatingScorer}). 
 * The {@link #setWeights(Reference2DoubleMap)}
 * method simply returns false.
 * 
 */
public abstract class AbstractScorer implements Scorer {
	/** The current document iterator. */
	protected DocumentIterator documentIterator;
	/** In case the current iterator is just made of one or more {@linkplain IndexIterator index iterators}.
	 * This field can be used by implementing subclasses to perform optimized evaluations that do not
	 * rely on {@linkplain CounterCollectionVisitor visitors}). */
	protected IndexIterator[] indexIterator;
			
	/** Wraps the given document iterator.
	 * 
	 * <p>This method {@linkplain #documentIterator records internally the provided iterator}.
	 * 
	 * @param documentIterator the document iterator that will be used in subsequent calls to
	 * {@link #score()} and {@link #score(Index)}. 
	 */
	
	public void wrap( final DocumentIterator documentIterator ) throws IOException {
		this.documentIterator = documentIterator;
		if ( documentIterator instanceof IndexIterator ) indexIterator = new IndexIterator[] { (IndexIterator)documentIterator };
		else if ( documentIterator instanceof AbstractCompositeDocumentIterator )
			indexIterator = ((AbstractCompositeDocumentIterator)documentIterator).indexIterator;
		else indexIterator = null;
	}

	/** Returns false. */
	
	public boolean setWeights( final Reference2DoubleMap<Index> index2Weight ) {
		return false;
	}
	
	/** Returns an empty map. */
	@SuppressWarnings("unchecked")
	public Reference2DoubleMap<Index>  getWeights() {
		return Reference2DoubleMaps.EMPTY_MAP;
	}
	
	public long nextDocument() throws IOException {
		return documentIterator.nextDocument();
	}
}
