package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.SafelyCloseable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An abstract, {@link it.unimi.dsi.io.SafelyCloseable safely closeable} implementation of a document sequence. 
 * 
 * <p>Note that even if your {@link DocumentSequence} implementation does not allocate
 * any specific resource, it is nonetheless a good idea to inherit from this class, as tracking
 * missing calls to {@link java.io.Closeable#close() close()} will be easier to detect.
 */
public abstract class AbstractDocumentSequence implements DocumentSequence, SafelyCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger( AbstractDocumentSequence.class );
	
	/** Loads a sequence.
	 * 
	 *  <p>This method first deserialises the object specified by the given filename, and
	 *  then invokes {@link DocumentSequence#filename(CharSequence)} to ensure proper relativisation
	 *  of serialised filenames.
	 *  
	 * @param filename a filename that will be used to load a serialised sequence.
	 */
	public static DocumentSequence load( CharSequence filename ) throws IOException, ClassNotFoundException, IllegalArgumentException, SecurityException {
		DocumentSequence sequence = (DocumentSequence)BinIO.loadObject( filename.toString() );
		sequence.filename( filename );
		return sequence;
	}

	/** Does nothing. */
	public void filename( final CharSequence filename ) throws IOException {		
	}
	
	/** Whether this document sequence has already been closed. */
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
