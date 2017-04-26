package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Sebastiano Vigna 
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

import it.unimi.dsi.io.SafelyCloseable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An abstract, {@linkplain SafelyCloseable safely closeable} implementation of an index reader.
 */

public abstract class AbstractIndexReader implements IndexReader {
	private static final Logger LOGGER = LoggerFactory.getLogger( AbstractIndexReader.class );

	/** Whether this reader has been closed. */
	protected boolean closed;

	public void close() throws IOException {
		closed = true;
	}
	
	protected void finalize() throws Throwable {
		try {
			if ( ! closed ) {
				LOGGER.warn( "This " + this.getClass().getName() + " [" + toString() + "] should have been closed." );
				close();
			}
		}
		finally {
			super.finalize();
		}
	}
	
	/** Throws an {@link UnsupportedOperationException}. */
	
	public IndexIterator nextIterator() throws IOException {
		throw new UnsupportedOperationException();
	}
}
