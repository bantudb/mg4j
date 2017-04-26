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

import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.lang.FlyweightPrototypes;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An aggregator that computes a linear combination of the component scorers.
 * 
 * <p>This class requires, beside the usually array of scorers, a parallel array
 * of weights (not to be confused with 
 * {@link it.unimi.di.big.mg4j.search.score.Scorer#setWeights(Reference2DoubleMap) index weights}).
 * The score from each scorer will be multiplied by the respective weight, and the
 * overal score will be the sum of these values. Equalisation, if necessary, if performed
 * by maximising over the sample scores and dividing all scores by the resulting value.
 */
public class LinearAggregator extends AbstractAggregator {
	private final static boolean DEBUG = false;
	private final static Logger LOGGER = LoggerFactory.getLogger( LinearAggregator.class );
	
	/** The weights of each scorer. */
	protected final double[] weight;
	/** The equalisation factors for each scorer (all set to one if no equalisation is required). */
	protected final double[] equalisationFactor;
	
	/** Creates a linear aggregator.
	 * 
	 * @param scorer the array of scorers.
	 * @param weight the array of weights.
	 */
	public LinearAggregator( final Scorer[] scorer, final double[] weight ) {
		super( scorer );
		if ( scorer.length != weight.length ) throw new IllegalArgumentException();
		this.weight = weight.clone();
		this.equalisationFactor = new double[ n ];
	}

	public synchronized LinearAggregator copy() {
		final LinearAggregator linearCombinationScorer = new LinearAggregator( FlyweightPrototypes.copy( scorer ), weight.clone() );
		linearCombinationScorer.equalize( samples );
		return linearCombinationScorer;
	}
	
	protected double score( final double score[] ) {
		double total = 0;
		for ( int i = n; i-- != 0; ) total += weight[ i ] * score[ i ] / equalisationFactor[ i ];
		if ( DEBUG ) LOGGER.debug( "Scoring " + Arrays.toString( score ) + ": " + total + " (weight: " + Arrays.toString( weight ) + "; equalisation factors: " + Arrays.toString( equalisationFactor ) + ")" );
		return total;
	}


	protected void setupEqualizationFactors() {
		if ( samples == 0 ) Arrays.fill( equalisationFactor, 1 );
		else {
			final double sampleScore[][] = this.sampleScore; 
			for( int i = n; i-- != 0; ) {
				double m = 0;
				for( int j = actualSamples; j-- != 0; ) if ( m < sampleScore[ j ][ i ] ) m = sampleScore[ j ][ i ];
				equalisationFactor[ i ] = m == 0 ? 1 : m;
			}
		}
		
		LOGGER.debug( "Equalisation factors: " + Arrays.toString( equalisationFactor ) );
	}
	
	public String toString() {
		final StringBuilder s = new StringBuilder();
		s.append( this.getClass().getName() ).append( '(' );
		for( int i = 0; i < scorer.length; i++ ) {
			if ( i != 0 ) s.append( "; " );
			s.append( scorer[ i ].toString() ).append( ":" ).append( weight[ i ] );
		}
		
		return  s.append( ')' ).toString();
	}
	
		
}
