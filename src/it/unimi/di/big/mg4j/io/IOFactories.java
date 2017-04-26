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
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Properties;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.ConfigurationException;

/** Static methods that do useful things with {@linkplain IOFactory I/O factories}.
 * 
 * @author Sebastiano Vigna
 * @since 5.1
 */

public class IOFactories {
	private IOFactories() {}

	// HORRIBLE kluges to work around bug #6478546, borrowed from fastutil
	private final static int MAX_IO_LENGTH = 1024 * 1024;

	private static int read( final InputStream is, final byte a[], final int offset, final int length ) throws IOException {
		if ( length == 0 ) return 0;
		int read = 0, result;
		do {
			result = is.read( a, offset + read, Math.min( length - read, MAX_IO_LENGTH ) );
			if ( result < 0 ) return read;
			read += result;
		} while( read < length );
		return read;
	}

	public static byte[] loadBytes( final IOFactory ioFactory, final String filename ) throws IOException {
		final long length = ioFactory.length( filename );
		if ( length >= Integer.MAX_VALUE ) throw new IllegalArgumentException( "File is too long (" + length + " bytes)." );
		final byte[] array = new byte[ (int)length ];
		final InputStream is = ioFactory.getInputStream( filename );
		if ( read( is, array, 0, (int)length ) < length ) throw new EOFException();
		is.close();
		return array;
	}

	public static Object loadObject( final IOFactory ioFactory, final String filename ) throws IOException, ClassNotFoundException {
		final ObjectInputStream ois = new ObjectInputStream( new FastBufferedInputStream( ioFactory.getInputStream( filename ) ) );
		final Object result = ois.readObject();
		ois.close();
		return result;
	}
	
	public static void storeObject( final IOFactory ioFactory, final Object object, final String filename ) throws IOException {
		final ObjectOutputStream oos = new ObjectOutputStream( new FastBufferedOutputStream( ioFactory.getOutputStream( filename ) ) );
		oos.writeObject( object );
		oos.close();
	}
	
	public static Properties loadProperties( final IOFactory ioFactory, final String filename ) throws ConfigurationException, IOException {
		final Properties properties = new Properties();
		final InputStream propertiesInputStream = ioFactory.getInputStream( filename );
		properties.load(propertiesInputStream );
		propertiesInputStream.close();
		return properties;
	}
	

	public static final class FileLinesIterable implements Iterable<MutableString> {
		private final IOFactory ioFactory;
		private String filename;
		private String encoding;

		private FileLinesIterable( final IOFactory ioFactory, final String filename, final String encoding ) {
			this.ioFactory = ioFactory;
			this.filename = filename;
			this.encoding = encoding;
		}
		
		public final static class FileLinesIterator extends AbstractObjectIterator<MutableString> implements SafelyCloseable {
			private FastBufferedReader fbr;
			private MutableString s = new MutableString(), next;
			private boolean toAdvance = true;

			public FileLinesIterator( final IOFactory ioFactory, final String filename, final String encoding ) {
				try {
					fbr = new FastBufferedReader( new InputStreamReader( ioFactory.getInputStream( filename ), encoding ) );
				} catch (IOException e) {
					throw new RuntimeException( e );
				}
			}

			public boolean hasNext() {
				if ( toAdvance ) {
					try {
						next = fbr.readLine( s );
						if ( next == null ) close();
					} catch (IOException e) {
						throw new RuntimeException( e );
					}
					toAdvance = false;
				}

				return next != null;
			}

			public MutableString next() {
				if ( ! hasNext() ) throw new NoSuchElementException();
				toAdvance = true;
				return s;
			}

			public synchronized void close() {
				if ( fbr == null ) return;
				try {
					fbr.close();
				}
				catch ( IOException e ) {
					throw new RuntimeException( e );
				}
				finally {
					fbr = null;
				}
			}

			protected synchronized void finalize() throws Throwable {
				try {
					close();
				}
				finally {
					super.finalize();
				}
			}
		}
		
		public Iterator<MutableString> iterator() {
			return new FileLinesIterator( ioFactory, filename, encoding );
		}
	}

	public static Iterable<MutableString> fileLinesCollection( final IOFactory ioFactory, final String filename, final String encoding ) {
		return new FileLinesIterable( ioFactory, filename, encoding );
	}
}
