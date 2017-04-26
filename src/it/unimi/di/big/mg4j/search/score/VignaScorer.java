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
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
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

/** Computes the Vigna score of all interval iterators of a document.
 * 
 * <p>This scorer progressively moves score from a residual (initialised to 1)
 * to the current score (initialised to 0). For each interval, we move a fraction of the residual
 * equal to the ratio of the {@linkplain it.unimi.di.big.mg4j.search.IntervalIterator#extent() extent} 
 * over the interval length, minimised with 1 and divided by 2. For instance,
 * on a two-term query meeting intervals of length 2 will increase the score from 0 to 1/2, 3/4 and so on.
 * On the other hand, larger intervals take away less from the residual.
 * 
 * <p>When the score exceeds .99, the computation is interrupted. In this way, we exploit
 * the laziness of the algorithms for minimal-interval 
 * semantics implemented in {@link it.unimi.di.big.mg4j.search}, greatly improving performance for
 * extremely frequent terms, with no perceivable effect on the score itself. 
 */
public class VignaScorer extends AbstractWeightedScorer implements DelegatingScorer {
	
	public double score( final Index index ) throws IOException {
		final IntervalIterator it = documentIterator.intervalIterator( index );
		if ( it == IntervalIterators.TRUE || it == IntervalIterators.FALSE ) return 0;
		double score = 0, residual = 1, t;
		int extent = it.extent(), length;
		Interval interval;
		while ( ( interval = it.nextInterval() ) != null ) {
			length = interval.length();
			t = residual * Math.min( (double)extent / length, 1 ) / 2;
			residual -= t;
			score += t;
			if ( score > .99 ) return 1;
		}
		return score;
	}

	public String toString() {
		return "Vigna()";
	}
	
	public synchronized VignaScorer copy() {
		final VignaScorer scorer = new VignaScorer();
		scorer.setWeights( index2Weight );
		return scorer;
	}

	/** Returns true.
	 * @return true.
	 */
	public boolean usesIntervals() {
		return true;
	}
}
