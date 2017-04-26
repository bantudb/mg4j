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

import static it.unimi.di.big.mg4j.search.DocumentIterator.END_OF_LIST;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.CachingDocumentIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMaps;

import java.io.IOException;

/** A {@link Scorer} that aggregates a number of underlying {@link it.unimi.di.big.mg4j.search.score.DelegatingScorer delegating scorers}, providing equalisation if required.
 * 
 * <p>An aggregator combines the results of several scorers following some policy (see, e.g.,
 * {@link it.unimi.di.big.mg4j.search.score.LinearAggregator}). In doing so, often the aggregator
 * needs to explore the first scores returned by each scorer, and tune some internal parameters. This
 * procedure, <em>equalisation</em>, is supported by this class: if {@link #equalize(int)} is provided with a
 * positive number of samples, they will be fetched from the underlying document iterator, scored, and
 * passed to the implementing subclass so that equalisation information can be properly set up.
 * 
 * <p>Additionally, this class ensures that if several scorers need access to intervals, 
 * the document iterator to be scored is decorated with a {@link it.unimi.di.big.mg4j.search.CachingDocumentIterator},
 * so that several scorer can access intervals.
 * 
 * <p>Since this class uses the same document iterator for <em>all</em> aggregated scorers, they
 * must be necessarily {@linkplain it.unimi.di.big.mg4j.search.score.DelegatingScorer delegating scorers}.
 * 
 * <p>Implementing subclasses must provide the following methods: 
 * <ul>
 * <li>{@link #setupEqualizationFactors()}, which is called in case equalisation is required and
 * must examine {@link #actualSamples} elements from {@link #sampleScore} (each element is a tuple
 * of scores, one for each scorer) and use that information to set the equalisation factors (if {@link #samples}
 * is zero, default values must be applied);
 * <li>{@link #score(double[])}, which must compute the equalised aggregated score using
 * the given array of scores (each to be thought as a score coming from the respective scorer).
 * </ul>
 * 
 * <p>Additionally, implementing subclasses must remember to call {@link #equalize(int)}
 * when generating a {@linkplain it.unimi.dsi.lang.FlyweightPrototype#copy() flyweight copy},
 * so that the state of the aggregator is reproduced correctly.
 */
public abstract class AbstractAggregator implements Scorer {
	
	/** The current document iterator. */
	protected DocumentIterator documentIterator;
	/** The number of underlying scorers. */
	protected final int n;
	/** The underlying scorers. */
	protected final Scorer[] scorer;
	/** The current score. */
	protected final double[] currScore;
	/** Whether we need caching the intervals. */
	protected final boolean needsCaching;

	/** Cached sample of document pointers. */
	protected long[] sampleDocument;
	/** Cached sample of document scores. */
	protected double[][] sampleScore;
	/** The number of samples for equalisation (0 means no equalisation). */
	protected int samples;
	/** The next sample to be returned, if smaller than {@link #actualSamples}. */
	protected int currSample;
	/** The actual number of samples obtained (might be less than {@link #samples} if we exhausted the document iterator). */
	protected int actualSamples;
	/** The index of a scorer with weights, if any, or -1. It is set at the
	 * first call to {@link #setWeights(Reference2DoubleMap)} */
	private int scorerWithWeights;
	
	/** Creates an aggregator.
	 * 
	 * @param scorer the scorers.
	 */
	public AbstractAggregator( final Scorer[] scorer ) {
		this.n = scorer.length;
		this.scorer = scorer;
		this.currScore = new double[ n ];
		int needsIntervals = 0;
		for( int i = scorer.length; i-- != 0; ) {
			if ( ! ( scorer[ i ] instanceof DelegatingScorer ) ) throw new IllegalArgumentException( "An aggregator needs delegating scorers" );
			if ( scorer[ i ].usesIntervals() ) needsIntervals++;
		}
		needsCaching = needsIntervals > 1;
		actualSamples = -1;
		scorerWithWeights = -1;
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}
	
	public double score() throws IOException {
		// If we are still walking through the sample, return a score from there
		if ( currSample <= actualSamples ) return score( sampleScore[ currSample - 1 ] );
		// Otherwise, create new score array and pass it to the implementing subclass.
		final double[] currScore = this.currScore;
		for( int i = n; i-- != 0; ) currScore[ i ] = scorer[ i ].score(); 
		return score( currScore );
	}
		
	/** Set the number of samples for equalisation.
	 *  
	 * @param samples the number of samples to be used to equalise scores; a value
	 * of zero disables equalisation.
	 * 
	 * @see AbstractAggregator
	 */
	
	public synchronized void equalize( int samples ) {
		this.samples = samples;
		if ( samples == 0 ) {
			sampleDocument = null;
			sampleScore = null;
			actualSamples = -1;
		}
		else {
			sampleDocument = new long[ samples ];
			sampleScore = new double[ samples ][ n ];
		}
	}
	
	/** Delegates to the underlying scorers.
	 * 
	 * @return true if at least one underlying scorer supports weights.
	 */
	public synchronized boolean setWeights( final Reference2DoubleMap<Index> index2weight ) {
		scorerWithWeights = -1;
		for( int i = n; i-- != 0; ) if ( scorer[ i ].setWeights( index2weight ) && scorerWithWeights == -1 ) scorerWithWeights = i;
		return scorerWithWeights != -1;
	}
	
	/** Delegates to the underlying scorers.
	 * 
	 * @return true if at least one underlying scorer supports weights.
	 */
	@SuppressWarnings("unchecked")
	public synchronized Reference2DoubleMap<Index> getWeights() {
		return scorerWithWeights == -1 ? Reference2DoubleMaps.EMPTY_MAP : scorer[ scorerWithWeights ].getWeights();
	}
	
	/** Delegates to the underlying scorers.
	 * 
	 * @return true if at least one underlying scorer uses intervals.
	 */
	public boolean usesIntervals() {
		for( int i = n; i-- != 0; ) if ( scorer[ i ].usesIntervals() ) return true;
		return false;
	}

	/** Delegates to the underlying scorers, possibly wrapping the argument in a
	 * {@link CachingDocumentIterator}; then, if {@link #samples} is nonzero computes
	 * that many document scores and invokes {@link #setupEqualizationFactors()}.
	 */
	public void wrap( DocumentIterator documentIterator ) throws IOException {
		if ( needsCaching ) documentIterator = new CachingDocumentIterator( documentIterator );
		for( int i = n; i-- != 0; ) scorer[ i ].wrap( documentIterator );
		if ( samples > 0 ) {
			// Let us prepare a sample.
			int i;
			for( i = 0; i < samples && ( sampleDocument[ i ] = documentIterator.nextDocument() ) != END_OF_LIST; i++ ) {
				;
				for( int j = n; j-- != 0; ) sampleScore[ i ][ j ] = scorer[ j ].score();
			}
			// If we exhausted the iterator, we keep END_OF_LIST in the sample array (as we cannot call nextDocument() again).
			actualSamples = i == samples ? samples : i + 1;
			currSample = 0;
		}
		// This must be *always* called--in the worst case, it will just set all factors to 1.
		setupEqualizationFactors();

		this.documentIterator = documentIterator;
	}
	
	/** Computes an aggregated score using the given array of basic scores.
	 * The array is parallel to {@link #scorer}.
	 *  
	 * @param score an array of scores.
	 * @return the aggregated scorer.
	 */
	protected abstract double score( double score[] );
	
	/** Sets up the equalisation factors.
	 * 
	 * <p>Implementations should look into {@link #sampleScore} and set up the
	 * equalisation logic. Note that this method is responsible for setting
	 * up appropriate equalisation factors <em>even if no equalisation is required</em> 
	 * (e.g., setting all factors to 1 ).
	 */
	protected abstract void setupEqualizationFactors();

	public long nextDocument() throws IOException {
		if ( currSample < actualSamples ) return sampleDocument[ currSample++ ];
		return documentIterator.nextDocument();
	}
}
