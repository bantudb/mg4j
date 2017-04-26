package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna
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
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor;
import it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor;
import it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor;
import it.unimi.dsi.fastutil.ints.IntBigList;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A scorer that implements the TF/IDF ranking formula.
 * 
 * <p>There are a number
 * of incarnations with small variations of the formula itself. Here, the weight
 * assigned to a term which appears in <var>f</var> documents out of a collection of <var>N</var> documents
 * w.r.t. to a document of length <var>l</var> in which the term appears <var>c</var> times is
 * <div style="text-align: center">
 * log(<var>N</var> / <var>f</var>) <var>c</var> / <var>l</var>,
 * </div>
 *  
 * <p>This class uses a {@link it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor}
 * and related classes to take into consideration only terms that are actually involved
 * in the current document. 
 * 
 * @author Sebastiano Vigna
 */
public class TfIdfScorer extends AbstractWeightedScorer implements DelegatingScorer {
	private static final Logger LOGGER = LoggerFactory.getLogger( TfIdfScorer.class );
	private static final boolean DEBUG = false;

	/** The counter collection visitor used to estimate counts. */
	private final CounterCollectionVisitor counterCollectionVisitor;
	/** The counter setup visitor used to estimate counts. */
	private final CounterSetupVisitor setupVisitor;
	/** The term collection visitor used to estimate counts. */
	private final TermCollectionVisitor termVisitor;

	/** An array (parallel to {@link #currIndex}) that caches size lists. */
	private IntBigList sizes[];
	/** An array (parallel to {@link #currIndex}) used by {@link #score()} to cache the current document sizes. */
	private int[] size;
	/** An array indexed by offsets that caches the inverse document-frequency part of the formula, multiplied by the index weight. */
	private double[] weightedIdfPart;
	
	public TfIdfScorer() {
		termVisitor = new TermCollectionVisitor();
		setupVisitor = new CounterSetupVisitor( termVisitor );
		counterCollectionVisitor = new CounterCollectionVisitor( setupVisitor );
	}

	public synchronized TfIdfScorer copy() {
		final TfIdfScorer scorer = new TfIdfScorer();
		scorer.setWeights( index2Weight );
		return scorer;
	}

	public double score() throws IOException {
		setupVisitor.clear();
		documentIterator.acceptOnTruePaths( counterCollectionVisitor );
		
		final long document = documentIterator.document();
		final int[] count = setupVisitor.count;
		final int[] indexNumber = setupVisitor.indexNumber;
		final double[] weightedIdfPart = this.weightedIdfPart;
		final int[] size = this.size;
		
		for( int i = size.length; i-- != 0; ) size[ i ] = sizes[ i ].getInt( document );

		int k;
		double score = 0;
		for ( int i = count.length; i-- != 0; ) {
			k = indexNumber[ i ];
			if ( count[ i ] != 0 ) score += (double)count[ i ] / size[ k ] * weightedIdfPart[ i ];
		}
		return score;
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}


	public void wrap( DocumentIterator d ) throws IOException {
		super.wrap( d );

		/* Note that we use the index array provided by the weight function, *not* by the visitor or by the iterator.
		 * If the function has an empty domain, this call is equivalent to prepare(). */
		termVisitor.prepare( index2Weight.keySet() );
		
		d.accept( termVisitor );

		if ( DEBUG ) LOGGER.debug( "Term Visitor found " + termVisitor.numberOfPairs() + " leaves" );

		// Note that we use the index array provided by the visitor, *not* by the iterator.
		final Index[] index = termVisitor.indices();

		if ( DEBUG ) LOGGER.debug( "Indices: " + Arrays.toString( index ) );

		// Some caching of frequently-used values
		sizes = new IntBigList[ index.length ];
		for( int i = index.length; i-- != 0; )
			if ( ( sizes[ i ] = index[ i ].sizes ) == null ) throw new IllegalStateException( "A BM25 scorer requires document sizes" );
			
		setupVisitor.prepare();
		d.accept( setupVisitor );

		final long[] frequency = setupVisitor.frequency;
		final int[] indexNumber = setupVisitor.indexNumber;
		
		// We do all logs here, and multiply by the weight
		weightedIdfPart = new double[ frequency.length ];
		for( int i = weightedIdfPart.length; i-- != 0; ) {
			weightedIdfPart[ i ] = Math.log( index[ indexNumber[ i ] ].numberOfDocuments / (double)frequency[ i ] ) * index2Weight.getDouble( index[ indexNumber[ i ] ] );
		}
		size = new int[ index.length ];
	}
	
	public boolean usesIntervals() {
		return false;
	}
}
