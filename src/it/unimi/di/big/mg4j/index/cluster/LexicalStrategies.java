package it.unimi.di.big.mg4j.index.cluster;

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
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.dsi.big.util.StringMap;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Static utility methods for lexical strategies.
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public class LexicalStrategies {
	private final static Logger LOGGER = LoggerFactory.getLogger( LexicalStrategies.class );
	
	protected LexicalStrategies() {}

	/** Creates an {@linkplain ContiguousLexicalStrategy contiguous lexical strategy} in which
	 * all local indices have approximately the same number of documents.
	 * @param numberOfLocalIndices the number of local indices.
	 * @param index the global index to be partitioned.
	 * @return a {@link ContiguousLexicalStrategy} that will partition in <code>index</code> in
	 * <code>numberOfLocalIndices</code> local indices of approximately equal size.  
	 */
	public static ContiguousLexicalStrategy uniform( final int numberOfLocalIndices, final Index index ) throws IOException {		
		final int[] cutPoint = new int[ numberOfLocalIndices + 1 ];
		final CharSequence[] cutPointTerm = new CharSequence[ numberOfLocalIndices + 1 ]; 
		final long terms = index.numberOfTerms;

		LOGGER.info( "Computing a contiguous lexical strategy to partition " + index + " into " + numberOfLocalIndices + " parts..." );

		/* If we have positions, we partition following the number of occurrences; otherwise,
		 * we partition following the number of document pointers. */
		long total = index.hasPositions ? index.numberOfOccurrences : index.numberOfPostings; 
		
		final StringMap<? extends CharSequence> termMap = index.termMap;
		if ( termMap == null ) throw new IllegalStateException( "Index " + index + " has no term map" );
		
		cutPointTerm[ 0 ] = termMap.list().get( 0 );
		cutPointTerm[ numberOfLocalIndices ] = "\uFFFF";
		
		int k, blockSizeDivisor = numberOfLocalIndices;
		
		do {
			long frequency;
			int left = 0; // The left extreme of the current block
			long count = 0; // Number of documents/occurrences in the current block

			final IndexReader indexReader = index.getReader();
			long blockSize = total / blockSizeDivisor++; // The approximate size of a block
			IndexIterator indexIterator;
			
			for ( int i = k = 0; i < terms; i++ ) {
				indexIterator = indexReader.nextIterator();
				frequency = indexIterator.frequency();
				if ( ! index.hasPositions ) count += frequency; 
				for ( long j = frequency; j-- != 0; ) {
					indexIterator.nextDocument();
					if ( index.hasPositions ) count += indexIterator.count();
				}
				
				if ( i == terms - 1 ) i++; // To fool the next
				if ( count >= blockSize && k < numberOfLocalIndices - 1 || i == terms ) {
					LOGGER.info( "New term interval [" + left + ".." + i + "] (\"" + termMap.list().get( left ) + "\" -> " + ( i == terms ? "" : "\"" + termMap.list().get( i ) + "\"" ) + ")" );
					cutPoint[ ++k ] = i;
					if ( i != terms ) cutPointTerm[ k ] = termMap.list().get( i );
					left = i;
					count = 0;
				}
			}
			indexReader.close();
			// In case we did not generate enough blocks, we try again with a smaller block size.
		} while( k < numberOfLocalIndices );
		
		return new ContiguousLexicalStrategy( cutPoint, cutPointTerm );
	}
}
