package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna
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
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;

/** Compute scores that do not depend on intervals, but that
 *  just assign a fixed score to each document starting from 1; scores are read
 *  from a file whose name is passed to the constructor.
 * 
 * <p>This scorer assumes that scores are nonnegative and that 
 * documents are ordered in decreasing
 * score order: that is, that if <var>i</var> &lt; <var>j</var> then
 * the score of <var>i</var> is greater than or equal to the score of <var>j</var>.
 * This allows to normalise the score (the document with the highest score has
 * exactly score 1) without additional costs. This scorer will throw an
 * {@link java.lang.IllegalStateException} if this assumption is violated.
 * 
 * <p><strong>Warning:</strong> this scorer assumes that there are no more than {@link Integer#MAX_VALUE} documents.
 */
public class DecreasingDocumentRankScorer extends AbstractScorer implements DelegatingScorer {
	/** The array of scores. */
	private double[] score;
	/** The first score returned after a call to {@link #wrap(DocumentIterator)}. */
	private double first;
	/** The latest score returned. */
	private double latest;
	
	/** Builds a document scorer by first reading the ranks from a file.
	 *  Ranks are saved as doubles (the first double is the rank of document 0
	 *  and so on).
	 * 
	 *  @param filename the name of the rank file.
	 */
	public DecreasingDocumentRankScorer( final String filename ) throws IOException {
		this( BinIO.loadDoubles( filename ) );
	}

	/** Builds a document scorer with given scores.
	 * 
	 *  @param score the scores (they are not copied, so the caller is supposed
	 *   not to change their values).
	 */
	public DecreasingDocumentRankScorer( final double[] score ) {
		this.score = score;
	}
	
	public DecreasingDocumentRankScorer copy() {
		return new DecreasingDocumentRankScorer( score );
	}
	
	public double score() {
		final long current = documentIterator.document();
		if ( current > Integer.MAX_VALUE ) throw new IndexOutOfBoundsException();
		if ( first == Double.MAX_VALUE ) {
			first = score[ (int)current ];
			if ( first == 0 ) first = 1; // All scores are 0.
		}
		else if ( score[ (int)current ] > latest ) throw new IllegalStateException();
		return ( latest = score[ (int)current ] ) / first;
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}

	public void wrap( final DocumentIterator documentIterator ) throws IOException {
		super.wrap( documentIterator );
		first = Double.MAX_VALUE;
	}

	public String toString() {
		return "DecreasingDocumentRank";
	}

	public boolean usesIntervals() {
		return false;
	}
}
