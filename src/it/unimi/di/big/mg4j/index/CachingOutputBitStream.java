package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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

import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.io.FileNotFoundException;


/** A special output bit stream with an additional
 * method {@link #buffer()} that returns the internal buffer
 * if the internal buffer contains all that has been written since
 * the last call to {@link OutputBitStream#position(long) position(0)}.
 * 
 * <p>This bit stream is used every time that it is necessary to cache quickly a bit stream.
 * By sizing the buffer size appropriately, most of the times data written to the stream
 * will be readable directly from the buffer. Remember to call {@link OutputBitStream#align()}
 * before retrieving the buffer, or some bits might be still floating in the bit buffer.
 */

public final class CachingOutputBitStream extends OutputBitStream {

	public CachingOutputBitStream( final File file, final int bufferSize ) throws FileNotFoundException {
		super( file, bufferSize );
	}

	public CachingOutputBitStream( final String filename, final int bufferSize ) throws FileNotFoundException {
		super( filename, bufferSize );
	}

	/** Return the internal buffer, if it contains all data.
	 * 
	 * <p>Note that this method should always be called after an {@link OutputBitStream#align()},
	 * or some bits might be still floating in the bit buffer.
	 * 
	 * @return the internal buffer, if it contains the cached content written since the last call to
	 * {@link OutputBitStream#position(long) position(0)}, or <code>null</code>.
	 */
	
	public byte[] buffer() {
		return ( fileChannel != null || repositionableStream != null ) && position == 0 || wrapping ? buffer : null;
	}
}
