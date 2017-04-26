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

import it.unimi.di.big.mg4j.index.AbstractIndexReader;
import it.unimi.di.big.mg4j.index.IndexReader;

import java.io.IOException;

/** An abstract implementation of an {@link IndexReader} for an {@link IndexCluster}. It
 * just keeps track of one reader per local index in {@link #indexReader}. 
 * It is up to the implementing subclasses to use the readers appropriately. 
 *  
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */
public abstract class AbstractIndexClusterIndexReader extends AbstractIndexReader {
	/** One reader per local index. */
	protected final IndexReader[] indexReader;

	public AbstractIndexClusterIndexReader( final IndexCluster index, final int bufferSize ) throws IOException {
		indexReader = new IndexReader[ index.localIndex.length ];
		for ( int i = 0; i < index.localIndex.length; i++ )
			indexReader[ i ] = index.localIndex[ i ].getReader( bufferSize );
	}
	
	public void close() throws IOException {
		super.close();
		for ( int i = 0; i < indexReader.length; i++ ) indexReader[ i ].close();
	}
}
