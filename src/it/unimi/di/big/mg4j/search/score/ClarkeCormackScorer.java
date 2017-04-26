package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2004-2016 Paolo Boldi
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
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.dsi.util.Interval;

import java.io.IOException;

/** Computes the Clarke&ndash;Cormack score of all interval iterators of a document.
 *  This score function is defined in Charles L.A. Clarke and Gordon V. Cormack, &ldquo;Shortest-Substring
 *  Retrieval and Ranking&rdquo;, <i>ACM Transactions on Information Systems</i>, 18(1):44&minus;78, 2000, 
 *  at page 65.
 * 
 *  <p>The score for each index depends on two parameters: an integer <var>h</var> and a double &alpha;.
 *  The score is obtained summing up a certain score assigned to all intervals in the interval iterator
 *  under examination. The score assigned to an interval is 1 if the interval
 *  has length smaller than <var>h</var>; otherwise, it is obtained by dividing <var>h</var> by
 *  the interval length, and raising the result to the power of &alpha;.
 * 
 *  <p>Note that the score assigned to each interval is between 0 and 1 (highest scores corresponding
 *  to best intervals). The score assigned to an interval iterator is thus bounded from above by the
 *  number of intervals; an alternative version allows one to have normalized scores (in this case, the resulting 
 *  value is an average instead of a sum). A scorer with similar relative ranks, but inherently (almost) normalised
 *  is provided by {@link it.unimi.di.big.mg4j.search.score.VignaScorer}. 
 * 
 *  <p>Typically, one sets <var>h</var>=16 (or a bit larger) and &alpha;=1 (or a bit smaller),
 *  but the authors say that the method is rather stable w.r.t. changes in the values of parameters.
 *  
 */
public class ClarkeCormackScorer extends AbstractWeightedScorer implements DelegatingScorer {
	/** The default value for <var>h</var>. */
	public static final int DEFAULT_H = 16;
	/** The parameter h. */
	public final int h;
	/** The parameter alpha. */
	public final double alpha;
	/** Whether the result should be normalized (i.e., between 0 and 1). */
	public final boolean normalize;

	/** Creates a Clarke&ndash;Cormack scorer.
	 * 
	 * @param h the parameter <var>h</var>.
	 * @param alpha the parameter &alpha;.
	 * @param normalize whether the result should be normalized.
	 */
	public ClarkeCormackScorer( final int h, final double alpha, final boolean normalize ) {
		this.h = h;
		this.alpha = alpha;
		this.normalize = normalize;
	}
	
	/** Creates a Clarke&ndash;Cormack scorer.
	 * 
	 * @param h the parameter <var>h</var>.
	 * @param alpha the parameter &alpha;.
	 * @param normalize whether the result should be normalized.
	 */
	public ClarkeCormackScorer( final String h, final String alpha, final String normalize ) {
		this( Integer.parseInt( h ), Double.parseDouble( alpha ), Boolean.parseBoolean( normalize ) );
	}
	
	/** Default constructor, assigning the default values (<var>h</var>={@link #DEFAULT_H}, &alpha;=1) to the
	 *  parameters; the resulting scorer is normalized.
	 */
	public ClarkeCormackScorer() {
		this( DEFAULT_H, 1, true );
	}

	public synchronized ClarkeCormackScorer copy() {
		final ClarkeCormackScorer scorer = new ClarkeCormackScorer( h, alpha, normalize );
		scorer.setWeights( index2Weight );
		return scorer;
	}
	
	public double score( final Index index ) throws IOException {
		final IntervalIterator it = documentIterator.intervalIterator( index );
		if ( it == IntervalIterators.TRUE || it == IntervalIterators.FALSE ) return 0;
		double result = 0;
		int lt, count = 0;
		Interval interval;
		while ( ( interval = it.nextInterval() ) != null ) {
			count++;
			lt = interval.length();
			if ( lt < h ) result += 1;
			else result += Math.pow( h / (double) lt, alpha );
		}
		return normalize? result / count : result;
	}

	public String toString() {
		return "Clarke-Cormack(" + h + ", " + alpha + ", " + normalize + ')';
	}

	/** Returns true.
	 * @return true.
	 */
	public boolean usesIntervals() {
		return true;
	}
}
