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


import it.unimi.di.big.mg4j.index.IndexIterator;

import java.io.IOException;

/** An index reader for a {@linkplain LexicalCluster lexical cluster}.
 * It diverts a call for the documents of a given term or prefix to the suitable local index (or set of indices).
 *
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public class LexicalClusterIndexReader extends AbstractIndexClusterIndexReader {
	/** The index this reader refers to. */
	private final LexicalCluster index;
	
	public LexicalClusterIndexReader( final LexicalCluster index, final int bufferSize ) throws IOException {
		super( index, bufferSize );
		this.index = index;
	}

	public IndexIterator documents( final long term ) throws IOException {
		if ( index.partitioningStrategy != null ) 
			return indexReader[ index.partitioningStrategy.localIndex( term ) ].documents( index.partitioningStrategy.localNumber( term ) );
		else throw new UnsupportedOperationException();
	}

	public IndexIterator documents( final CharSequence term ) throws IOException {
		final int localIndex = index.strategy.localIndex( term );
		if ( localIndex == -1 ) return index.getEmptyIndexIterator( term );
		return indexReader[ localIndex ].documents( term );
	}
}
