package it.unimi.di.big.mg4j.index.cluster;

import it.unimi.dsi.util.Properties;

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

/** Static utility methods for documental strategies.
 * 
 * @author Alessandro Arabito
 * @author Sebastiano Vigna
 */

public class DocumentalStrategies {
	
	protected DocumentalStrategies() {}
	
	/** Creates an equally spaced {@linkplain ContiguousDocumentalStrategy contiguous documental strategy}.
	 * 
	 * @param numberOfLocalIndices the number of local indices.
	 * @param numberOfDocuments the global number of documents.
	 * @return a {@link ContiguousDocumentalStrategy} that will partition in <code>index</code> in
	 * <code>numberOfLocalIndices</code> local indices of approximately equal size.
	 */
	public static ContiguousDocumentalStrategy uniform( final int numberOfLocalIndices, final long numberOfDocuments ) {
		if ( numberOfLocalIndices > numberOfDocuments )
			throw new IllegalArgumentException( "The number of local indices (" + numberOfLocalIndices + ") is larger than the number of documents (" + numberOfDocuments + ")" );
		final long intervalSize = numberOfDocuments / numberOfLocalIndices;
		final long[] cutPoint = new long[ numberOfLocalIndices + 1 ];
		cutPoint[ numberOfLocalIndices ] = numberOfDocuments;
		for ( int i = 1; i < numberOfLocalIndices; i++ )
			cutPoint[ i ] = intervalSize * i;
		return new ContiguousDocumentalStrategy( cutPoint );
	}

	/** Creates an interleaved {@linkplain DocumentalPartitioningStrategy partitioning strategy}.
	 * 
	 * @param numberOfLocalIndices the number of local indices.
	 * @param numberOfDocuments the global number of documents.
	 * @return a strategy that will partition in <code>index</code> in
	 * <code>numberOfLocalIndices</code> local indices of approximately equal size by
	 * picking one every <code>numberOfLocalIndices</code> documents in a round-robin fashion.
	 */
	public static DocumentalPartitioningStrategy interleaved( final int numberOfLocalIndices, final int numberOfDocuments ) {
		return new DocumentalPartitioningStrategy() {
			private static final long serialVersionUID = 1L;

			@Override
			public Properties[] properties() {
				return null;
			}
			
			@Override
			public int numberOfLocalIndices() {
				return numberOfLocalIndices;
			}
			
			@Override
			public long numberOfDocuments( final int localIndex ) {
				return numberOfDocuments / numberOfLocalIndices + ( localIndex < numberOfDocuments % numberOfLocalIndices ? 1 : 0 );
			}
			
			@Override
			public long localPointer( final long globalPointer ) {
				return globalPointer / numberOfLocalIndices;
			}
			
			@Override
			public int localIndex( final long globalPointer ) {
				return (int)( globalPointer % numberOfLocalIndices );
			}
		};
	}
}
