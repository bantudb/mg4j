package it.unimi.di.big.mg4j.search.visitor;

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

import it.unimi.di.big.mg4j.index.IndexIterator;

import java.io.IOException;
import java.util.Arrays;

/** A visitor using the information collected by a 
 * {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor}
 * to set up term frequencies and counters.
 * 
 * <p>Term {@linkplain #frequency frequencies} and {@linkplain #count counts} are stored 
 * in publicly accessible parallel arrays of integers indexed by <em>offsets</em>,
 * as defined by a {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor} provided at construction time.
 * Additionally, the {@linkplain #indexNumber index number} (a position into the array returned by 
 * {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor#indices()}) and the
 * {@linkplain #offset2Term term} for each offset are available.
 * 
 * <p>When instances of this class perform a visit, they prepare the arrays and
 * fill those contaning {@linkplain #frequency frequencies} and {@linkplain #indexNumber index numbers}.
 * It is up to an instance of {@link it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor}
 * (which requires an instance of this class in its constructor) to fill 
 * the {@linkplain #count counts} with data related to
 * the current document.
 * 
 * <p>For a more complete picture, see {@link it.unimi.di.big.mg4j.search.visitor.CounterCollectionVisitor}.
 */

public class CounterSetupVisitor extends AbstractDocumentIteratorVisitor {
	/** For each offset, the corresponding index as a position in {@link TermCollectionVisitor#indices()}. */
	public int[] indexNumber;
	/** For each offset, the corresponding term. */
	public String[] offset2Term;
	/** For each offset, its count. */
	public int[] count;
	/** For each offset, its frequency. */
	public long[] frequency;
	/** For each offset, its term id. */
	public int[] offset2TermId;
	/** For each term id, the corresponding term. */
	public String[] termId2Term;
	/** The underlying term-collection visitor. */
	private final TermCollectionVisitor termCollectionVisitor;
	
	/** Creates a new counter-setup visitor based on a given term-collection visitor.
	 * 
	 * @param termCollectionVisitor a term-collection visitor.
	 */
	
	public CounterSetupVisitor( TermCollectionVisitor termCollectionVisitor ) {
		this.termCollectionVisitor = termCollectionVisitor;
		prepare();
	}
	
	/** Prepares the internal state of this visitor using data from the associated
	 * {@link TermCollectionVisitor}.
	 * 
	 * <p>Note that because of this dependency, it is essential that you
	 * first prepare and visit with the associated {@link TermCollectionVisitor}, 
	 * and then prepare and visit with this visitor.
	 */
	
	public CounterSetupVisitor prepare() {
		final int numberOfPairs = termCollectionVisitor.numberOfPairs();
		count = new int[ numberOfPairs ];
		frequency = new long[ numberOfPairs ];
		indexNumber = new int[ numberOfPairs ];
		offset2Term = new String[ numberOfPairs ];
		offset2TermId = new int[ numberOfPairs ];
		termId2Term = new String[ termCollectionVisitor.term2Id().size() ];
		for( String term: termCollectionVisitor.term2Id().keySet() ) termId2Term[ termCollectionVisitor.term2Id().getInt( term ) ] = term;
		return this;
	}
	
	public Boolean visit( final IndexIterator indexIterator ) throws IOException {
		final int id = indexIterator.id(); // offset into all arrays; if -1, indexIterator has been skipped by the TermCollectionVisitor.
		if ( id != -1 ) {
			// We fill the frequency and index entries
			this.frequency[ id ] = indexIterator.frequency();
			this.indexNumber[ id ] = termCollectionVisitor.indexMap().getInt( indexIterator.index() );
			this.offset2Term[ id ] = indexIterator.term();
			this.offset2TermId[ id ] = termCollectionVisitor.term2Id().getInt( indexIterator.term() );
		}
		return Boolean.TRUE; 
	}

	/** Updates the {@link #count} using the provided index iterator.
	 * 
	 * <p>This method is usually called back by a {@link CounterCollectionVisitor} built upon
	 * this counter-setup visitor. It simply retrieves the index iterator
	 * {@linkplain IndexIterator#id() id} and use it as an index into
	 * {@link #count} to store {@link IndexIterator#count()}.
	 * 
	 * @param indexIterator an index iterator.
	 * @throws IOException 
	 */
	
	public void update( final IndexIterator indexIterator ) throws IOException {		
		count[ indexIterator.id() ] = indexIterator.count();
	}
	
	/** Zeroes all counters, but not frequencies. */
	public void clear() {
		Arrays.fill( count, 0 );
	}
	
	public String toString() {
		return "[" + Arrays.toString( frequency ) + ", " + Arrays.toString( count ) +"]";
	}
}
