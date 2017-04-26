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
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;

import java.io.IOException;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An abstract subsclass of {@link it.unimi.di.big.mg4j.search.score.AbstractScorer}
 * providing internal storage and copy of the weight map and a default implementation of {@link #score()}.
 * 
 * <p><strong>Warning:</strong> implementing subclasses <strong>must</strong> implement
 * {@link it.unimi.di.big.mg4j.search.score.Scorer#copy()} so that the state of the
 * weight map is replicated, too.
 */
public abstract class AbstractWeightedScorer extends AbstractScorer {
	private static final Logger LOGGER = LoggerFactory.getLogger( AbstractWeightedScorer.class );
	
	/** A map associating a weight with each index. */
	protected Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>( 1, Hash.VERY_FAST_LOAD_FACTOR );
	/** An enumeration of the indices used by the current document iterator; set up by {@link #wrap(DocumentIterator)}. */
	private Index[] index;
	/** An array, parallel to {@link #index}, containing the weight of each index. */
	private double[] indexNumber2Weight;
	{
		// We leave it empty, but with default value 1, so all indices are peers.
		index2Weight.defaultReturnValue( 1 );
	}

	/** Copies the argument internally, rescaling weights so they sum up to one.
	 * 
	 * @param index2Weight the new map from indices to weights.
	 * @return true.
	 */
	public synchronized boolean setWeights( final Reference2DoubleMap<Index> index2Weight ) {
		this.index2Weight.clear();
		this.index2Weight.defaultReturnValue( 0 ); // Since we're setting up values, we must assume missing indices have weight 0.
		
		double weightSum = 0;
		for( DoubleIterator i = index2Weight.values().iterator(); i.hasNext(); ) weightSum += i.nextDouble();
		if ( weightSum == 0 ) weightSum = 1; // No positive weights.
		for( ObjectIterator<Entry<Index, Double>> i = index2Weight.entrySet().iterator(); i.hasNext(); ) {
			Reference2DoubleMap.Entry<Index> e = (Reference2DoubleMap.Entry<Index>)i.next();
			this.index2Weight.put( e.getKey(), e.getDoubleValue() / weightSum );
		}

		this.index2Weight.trim();
		LOGGER.debug( "New weight map for " + this + ": " + this.index2Weight );
		return true;
	}
	
	public synchronized final Reference2DoubleMap<Index> getWeights() {
		return new Reference2DoubleOpenHashMap<Index>( index2Weight, Hash.VERY_FAST_LOAD_FACTOR );
	}	

	/** Computes a score by calling {@link #score(Index)} for
	 * each index in the current document iterator, and adding the weighted results.
	 * 
	 * @return the combined weighted score.
	 */
	@Override
	public double score() throws IOException {
		final double indexNumber2Weight[] = this.indexNumber2Weight;
		final Index index[] = this.index;
		double result = 0;
		for( int i = index.length; i-- != 0; ) result += indexNumber2Weight[ i ] * score( index[ i ] );
		return result;	
	}

	@Override
	public void wrap( final DocumentIterator documentIterator ) throws IOException {
		super.wrap( documentIterator );
		index = documentIterator.indices().toArray( new Index[ documentIterator.indices().size() ] );
		indexNumber2Weight = new double[ index.length ];
		for( int i = index.length; i-- != 0; ) indexNumber2Weight[ i ] = index2Weight.getDouble( index[ i ] );
	}
}
