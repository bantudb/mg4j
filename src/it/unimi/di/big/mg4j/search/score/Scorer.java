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
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.lang.FlyweightPrototype;

import java.io.IOException;

/** A wrapper for a {@link DocumentIterator} returning scored document pointers. 
 * 
 * <p>Typically, a scorer may have one or more constructors, 
 * but all scorers should provide a constructor that takes only strings as arguments to
 * make the instantiation from command-line or similar interfaces easier.
 * 
 * <p>To be (re)used, a scorer must first {@linkplain #wrap(DocumentIterator) wrap} an
 * underlying {@link it.unimi.di.big.mg4j.search.DocumentIterator}. This
 * phase usually involves some preprocessing around properties of the document iterator to
 * be scored. After wrapping, calls to {@link #nextDocument()} and {@link #score()} (or possibly
 * {@link #score(Index)}) will return the next document pointer and
 * its score. Note that these methods are not usually idempotent, as they modify the state of the underlying iterator
 * (e.g., they consume intervals).
 * 
 * <p>Scores returned by a scorer might depend on some {@linkplain #setWeights(Reference2DoubleMap) weights}
 * associated with each index.
 * 
 * <p>Optionally, a scorer might be a {@link it.unimi.di.big.mg4j.search.score.DelegatingScorer}.
 * 
 * <p><strong>Warning</strong>: implementations of this interface are not required
 * to be thread-safe, but they provide {@linkplain it.unimi.dsi.lang.FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return an object implementing this interface.
 */
public interface Scorer extends FlyweightPrototype<Scorer> {

	/** Returns a score for the current document of the last document iterator
	 * given to {@link #wrap(DocumentIterator)}.
	 * 
	 * @return the score.
	 */
	public double score() throws IOException;

	/** Returns a score for the current document of the last document iterator
	 * given to {@link #wrap(DocumentIterator)}, but
	 * considering only a given index (optional operation).
	 * 
	 * @param index the only index to be considered.
	 * @return the score.
	 */
	public double score( Index index ) throws IOException;
	
	/** Sets the weight map for this scorer (if applicable). 
	 * 
	 * <p>The given map will be copied internally and can be used by 
	 * the caller without affecting the scorer behaviour. Implementing classes
	 * should rescale the weights so that they have sum equal to one.
	 * 
	 * <p>Indices <em>not</em> appearing in the map will have weight equal to 0.
	 * 
	 * @param index2Weight a map from indices to weights.
	 * @return true if this scorer supports weights.
	 */
	public boolean setWeights( Reference2DoubleMap<Index> index2Weight );
	
	/** Gets the weight map for this scorer (if applicable). 
	 * 
	 * <p>Returns a copy of the weight map of this scorer.
	 * 
	 * @return a copy of the weight map of this scorer.
	 */
	public Reference2DoubleMap<Index> getWeights();
	
	/** Wraps a document iterator and prepares the internal state of this scorer to work with it. 
	 * 
	 * <p>Subsequent calls to {@link #score()} and {@link #score(Index)} will use
	 * <code>d</code> to compute the score.
	 * 
	 * @param documentIterator the document iterator that will be used in subsequent calls to
	 * {@link #score()} and {@link #score(Index)}. 
	 */
	public void wrap( DocumentIterator documentIterator ) throws IOException;

	/** Whether this scorer uses intervals.
	 * 
	 * <p>This method is essential when {@linkplain AbstractAggregator aggregating scorers},
	 * because if several scores need intervals, a {@link it.unimi.di.big.mg4j.search.CachingDocumentIterator}
	 * will be necessary.
	 * 
	 * @return true if this scorer uses intervals.
	 */
	public boolean usesIntervals();

	/** Returns the next document provided by this scorer, or -1 if no more documents are available.
	 * 
	 * @return the next document, or -1 if no more documents are available.
	 */
	public long nextDocument() throws IOException;
	
	public Scorer copy();
}
