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

import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.dsi.lang.ObjectParser;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/** An {@linkplain IOFactory I/O factory} using a {@linkplain FileSystem Hadoop file system}. 
 * 
 * <p>The factory
 * provides some constructors ({@link #HadoopFileSystemIOFactory()} and
 * {@link #HadoopFileSystemIOFactory(String)}) that are suitable for use with
 * an {@link ObjectParser}. For instance, the object specification
 * <samp>it.unimi.di.mg4j.io.HadoopFileSystemIOFactory(hdfs://127.0.0.1:9000/)</samp>
 * could be used with {@link Scan} or {@link IndexBuilder} to make them use a local HDFS
 * file system. 
 * 
 * <p>Note that if you use the constructors based on the default configuration
 * {@linkplain Configuration you must set up your configuration files suitably}.
 * If you use an unconfigured Hadoop file system, you will get
 * a Hadoop {@link LocalFileSystem}.  
 */

public class HadoopFileSystemIOFactory implements IOFactory {
	/** The Hadoop file system used by this factory. */
	private final FileSystem fileSystem;

	/** Creates a factory using a file system with a {@linkplain Configuration default configuration}. */ 
	public HadoopFileSystemIOFactory() throws IOException {
		this.fileSystem = FileSystem.get( new Configuration() );
	}

	/** Creates a factory using a file system specified by a given {@link URI} with a {@linkplain Configuration default configuration}. 
	 * 
	 * @param uri a URI that will be passed to {@link FileSystem#get(URI, Configuration)}. 
	 */ 
	public HadoopFileSystemIOFactory( final URI uri ) throws IOException {
		this.fileSystem = FileSystem.get( uri, new Configuration() );
	}

	/** Creates a factory using a file system specified by a given {@link URI} with a {@linkplain Configuration default configuration}
	 * 
	 * <p>This constructor is essentially identical to {@link #HadoopFileSystemIOFactory(URI)}, but
	 * it can be used with an {@link ObjectParser}.
	 * 
	 * @param uri a URI, specified as a string, that will be passed to {@link FileSystem#get(URI, Configuration)}. 
	 */ 
	public HadoopFileSystemIOFactory( final String uri ) throws IOException {
		this.fileSystem = FileSystem.get( URI.create( uri ), new Configuration() );
	}

	/** Creates a factory using a given Hadoop file system. 
	 * 
	 * @param fileSystem a Hadoop file system. 
	 */ 
	public HadoopFileSystemIOFactory( final FileSystem fileSystem ) {
		this.fileSystem = fileSystem;
	}

	@Override
	public FSDataInputStream getInputStream( final String name ) throws IOException {
		return fileSystem.open( new Path( name ) );
	}

	@Override
	public FSDataOutputStream getOutputStream( final String name ) throws IOException {
		return fileSystem.create( new Path( name ) );
	}

	@Override
	public WritableByteChannel getWritableByteChannel( final String name ) throws IOException {
		return Channels.newChannel( getOutputStream( name ) );
	}

	@Override
	public ReadableByteChannel getReadableByteChannel( final String name ) throws IOException {
		return Channels.newChannel( getInputStream( name ) );
	}

	@Override
	public boolean exists( final String name ) throws IOException {
		return fileSystem.exists( new Path( name ) );
	}

	@Override
	public boolean delete( final String name ) throws IOException {
		return fileSystem.delete( new Path( name ), false );
	}

	@Override
	public void createNewFile( final String name ) throws IOException {
		fileSystem.create( new Path( name ) ).close();
	}

	@Override
	public long length( final String name ) throws IOException {
		return fileSystem.getFileStatus( new Path( name ) ).getLen();
	}
}
