package it.unimi.di.big.mg4j.document;

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

import it.unimi.dsi.io.SafelyCloseable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An abstract, {@link it.unimi.dsi.io.SafelyCloseable safely closeable} implementation of a document. 
 * 
 * <p>This implementation provides also a {@link #toString()} method that just returns the document 
 * {@linkplain it.unimi.di.big.mg4j.document.Document#title() title}.
 * 
 * <p>Note that even if your {@link it.unimi.di.big.mg4j.document.Document} implementation does not allocate
 * any document-specific resource, it is nonetheless a good idea to inherit from this class, as tracking
 * missing calls to {@link java.io.Closeable#close() close()} will be easier to detect.
 * 
 * <p><strong>Warning</strong>: if you do not close an instance of this class, a warning will be logged
 * at some point during garbage collection. The warning will use the document title: you must be
 * sure to never leave the document in a state in which calling {@link #title()} is dangerous
 * (i.e., a document coming from a streaming source which retains a handle to the source for lazy parsing),
 * or bad things will happen.
 */
public abstract class AbstractDocument implements Document, SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger( AbstractDocument.class );
	/** Whether this document has been already closed. */
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

	public String toString() {
		return title().toString();
	}
}
