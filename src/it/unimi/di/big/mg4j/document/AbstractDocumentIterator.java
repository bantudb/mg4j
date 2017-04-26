package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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


/** An abstract, {@link it.unimi.dsi.io.SafelyCloseable safely closeable} 
 * implementation of a document iterator. */

public abstract class AbstractDocumentIterator  implements DocumentIterator, SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger( AbstractDocumentIterator.class );

	/** Whether this document iterator has been already closed. */
	private boolean closed;
	
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
	
	public void close() throws IOException {
		closed = true;
	}
}
