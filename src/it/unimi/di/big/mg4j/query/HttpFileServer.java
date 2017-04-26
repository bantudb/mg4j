package it.unimi.di.big.mg4j.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import javax.activation.MimetypesFileTypeMap;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ResourceHandler;


/** A minimal, singleton server serving the whole filesystem.
 * 
 * <p>For security reasons, some browsers (notably Firefox at the time of this writing)
 * do not serve <samp>file:</samp> content from a non-<samp>file:</samp> page. The solution
 * is a minimal server that has the root of the filesystem as document root.
 * 
 * <p>To instantiate the server, you just need a variable of type {@link HttpFileServer}.
 * just retrieve and cache the result of {@link #getServer()} (to
 * avoid class garbage collection). A port is randomly assigned to the server; it can be retrieved with
 * {@link #getPort()}. In case you need a specific port, the system property
 * <samp>it.unimi.di.big.mg4j.query.HttpFileServer.port</samp> can be used to this purpose.
 * 
 * <p>It may happen that the server is not yet started when you call {@link #getPort()}, in which case
 * an {@link java.lang.IllegalStateException} will be thrown.
 */

public class HttpFileServer {
	/** The only server instance. */
	private final static Server SERVER;
	/** The only instance of this class. */
	private final static HttpFileServer INSTANCE = new HttpFileServer();
	/** The dynamically assigned port, or 0 if the port has not been detected yet. */
	private static int port;
	/** An instance of {@link MimetypesFileTypeMap} used to recognise MIME types. */
	@SuppressWarnings("unused")
	private final static MimetypesFileTypeMap MIME_TYPES_FILE_TYPE_MAP = new MimetypesFileTypeMap();
	
	static {
		// This allows symlinks.
		System.setProperty( "org.mortbay.util.FileResource.checkAliases", Boolean.FALSE.toString() );
	}
	
	/** Returns the only instance of an HTTP file server.
	 * 
	 * <p>Note that the instance should be cached by the application, to avoid garbage collection.
	 * 
	 * @return the only instance of an HTTP file server
	 */
	public static HttpFileServer getServer() {
		return INSTANCE;
	}
	
	/** Returns the port assigned to the server.
	 * 
	 * @return the port assigned to the server.
	 */
	
	public static int getPort() {
		if ( port == 0 ) {
			if ( ! SERVER.isStarted() ) throw new IllegalStateException( "The server is not started yet" );
			port = SERVER.getConnectors()[ 0 ].getLocalPort();
		}
		return port;
	}
	
	private HttpFileServer() {}
	
	static {
		// Create the server
		SERVER = new Server();

		// Create a port listener
		final SocketConnector connector = new SocketConnector();
		connector.setPort( Integer.parseInt( System.getProperty( HttpFileServer.class.getName() + ".port", "0" ) ) );
		SERVER.addConnector( connector );

		// Create a context 
		final ContextHandler context = new ContextHandler();
		context.setContextPath( "/" );
		context.setResourceBase( "/" );
		
		SERVER.addHandler( context );

		// Add a resource handler
		ResourceHandler resourceHandler = new ResourceHandler();
		//resourceHandler.setAllowedMethods( new String[] { "GET" } );
		
		context.addHandler( resourceHandler );

		// Start the http server
		new Thread() {
			public void run() {
				try {
					SERVER.start();
				}
				catch ( Exception e ) {
					throw new RuntimeException();
				}
			}
		}.start();
	}
}
