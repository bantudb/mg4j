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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A trivial scorer that computes the score by adding the counts 
 * (the number of occurrences within the current document) of each term 
 * multiplied by the weight of the relative index. 
 * Mainly useful for debugging and testing purposes.
 * 
 * <p>This class uses a {@link it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor}
 * and related classes to take into consideration only terms that are actually involved
 * in the current document. 
 *  
 * @author Mauro Mereu
 * @author Sebastiano Vigna
 */
public class CountScorer extends AbstractWeightedScorer implements DelegatingScorer {
	static Logger LOGGER = LoggerFactory.getLogger( CountScorer.class );
	
	/** The counter collection visitor used to estimate counts. */
	private final CounterCollectionVisitor counterCollectionVisitor;
	/** The counter setup visitor used to estimate counts. */
	private final CounterSetupVisitor counterSetupVisitor;
	/** The term collection visitor used to estimate counts. */
	private final TermCollectionVisitor termCollectionVisitor;
	/** An array parallel to {@link TermCollectionVisitor#indices()} containing the current corresponding values in {@link #index2Weight};
	 *  it is set up by {@link #wrap(DocumentIterator)}. */
	protected double indexNumber2Weight[];
	
	public CountScorer() {
		termCollectionVisitor = new TermCollectionVisitor();
		counterSetupVisitor = new CounterSetupVisitor( termCollectionVisitor );
		counterCollectionVisitor = new CounterCollectionVisitor( counterSetupVisitor );
	}

	public double score() throws IOException {
		counterSetupVisitor.clear();
		documentIterator.acceptOnTruePaths( counterCollectionVisitor );

		double score = 0;
		final int[] count = counterSetupVisitor.count;
		final int[] indexNumber = counterSetupVisitor.indexNumber;
		for ( int i = count.length; i-- != 0;  ) score += count[ i ] * indexNumber2Weight[ indexNumber[ i ] ];
		return score;
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}
	
	public void wrap( DocumentIterator d ) throws IOException {
		super.wrap( d );

		/* Note that we use the index array provided by the weight function, *not* by the visitor or by the iterator.
		 * If the function has an empty domain, this call is equivalent to prepare(). */
		termCollectionVisitor.prepare( index2Weight.keySet() );
		
		d.accept( termCollectionVisitor ); 
		final Index[] index = termCollectionVisitor.indices();
		indexNumber2Weight = new double[ index.length ];
		for( int i = index.length; i-- != 0; ) indexNumber2Weight[ i ] = index2Weight.getDouble( index[ i ] );
		counterSetupVisitor.prepare();
		d.accept( counterSetupVisitor ); 
	}
	
	public synchronized CountScorer copy() {		
		final CountScorer scorer = new CountScorer();
		scorer.setWeights( index2Weight );
		return scorer;
	}

	public boolean usesIntervals() {
		return false;
	}
}
