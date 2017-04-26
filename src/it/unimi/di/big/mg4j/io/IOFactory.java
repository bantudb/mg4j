package it.unimi.di.big.mg4j.io;

/*		 
* MG4J: Managing Gigabytes for Java (big)
*
* Copyright (C) 2012-2016 Sebastiano Vigna 
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
*  along with this program; if not, write to the Free Software
*  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*
*/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** A factory for streams.
 *
 * <P>Instances of this factory are used to make (at least partially)
 * index construction and access independent of the file system implementation.
 * For instance, {@link #FILESYSTEM_FACTORY} provides methods that are equivalent
 * to standard Java file handling, whereas {@link HadoopFileSystemIOFactory} makes it
 * possible to use a Hadoop file system like HDFS to build indices.
 * 
 * <p><strong>Warning</strong>: I/O factories were introduced in 5.1,
 * and they are still somewhat experimental. Some parts of the code of MG4J might
 * still open directly files using {@link java.io}'s methods instead of
 * factory methods. Please report issues and inconsistent behaviour.
 *  
 * @author Sebastiano Vigna
 * @since 5.1
 */

public interface IOFactory {
	public InputStream getInputStream( String name ) throws IOException;
	public OutputStream getOutputStream( String name ) throws IOException;
	public WritableByteChannel getWritableByteChannel( String name ) throws IOException;
	public ReadableByteChannel getReadableByteChannel( String name ) throws IOException;
	public boolean exists( String name ) throws IOException;
	public boolean delete( String name ) throws IOException;
	public void createNewFile( String name ) throws IOException;
	public long length( String name ) throws IOException;
	
	public static IOFactory FILESYSTEM_FACTORY = new IOFactory() {
		@Override
		public OutputStream getOutputStream( final String name ) throws IOException {
			return new FileOutputStream( name );
		}

		@Override
		public InputStream getInputStream( final String name ) throws IOException {
			return new FileInputStream( name );
		}

		@Override
		public boolean delete( final String name ) {
			return new File( name ).delete();
		}

		@Override
		public boolean exists( final String name ) {
			return new File( name ).exists();
		}

		@Override
		public void createNewFile( final String name ) throws IOException {
			new File( name ).createNewFile();
		}

		@SuppressWarnings("resource")
		@Override
		public WritableByteChannel getWritableByteChannel( String name ) throws IOException {
			return new FileOutputStream( name ).getChannel();
		}

		@SuppressWarnings("resource")
		@Override
		public ReadableByteChannel getReadableByteChannel( String name ) throws IOException {
			return new FileInputStream( name ).getChannel();
		}

		@Override
		public long length( String name ) throws IOException {
			return new File( name ).length();
		}
	};
}
