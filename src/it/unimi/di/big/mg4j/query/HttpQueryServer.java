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
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.dsi.fastutil.BigList;

import javax.servlet.http.HttpServlet;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A very basic HTTP server answering queries.
 * 
 * <p>The main method of this class starts a very basic HTTP server answering queries.
 * If a matching document collection is provided, the server will also display some
 * intervals satisfying the queries.
 * 
 * <p>Queries are served by the {@link it.unimi.di.big.mg4j.query.QueryServlet}; by
 * default, the servlet listen on port 4242, and the servlet is deployed under
 * the path <samp>/Query</samp>. A servlet displaying single documents from the collection
 * is deployed under the path <samp>/Item</samp>. The server and the servlet are fully multithreaded.
 * 
 * <p>If you want to start this server from the command line, you must use the
 * main method of {@link it.unimi.di.big.mg4j.query.Query}, providing the suitable option. 
 * Changes to the {@link it.unimi.di.big.mg4j.query.QueryEngine} made through the text interface will
 * be reflected in the web interface, making it possible to experiment with different
 * settings.
 * 
 */

public class HttpQueryServer {
	private static final Logger LOGGER = LoggerFactory.getLogger( HttpQueryServer.class );
	/** The underlying Jetty server. Access to this field is useful to tune or stop the server. */
	public final Server server;
	
	/** Sets the given extended properties so that velocity finds its files either
	 * by classpath, or by absolute filename, or by relative filename. 
	 * 
	 * @param p the extended properties of the servlet, obtained <i>via</i>
	 * <samp>super.loadConfiguration()</samp>.
	 * 
	 * @return <code>p</code> the additional items setting a liberal scheme for resource loading.
	 */
	
	public static ExtendedProperties setLiberalResourceLoading( final ExtendedProperties p ) {
		// TODO: This is ugly. If anybody can find how to set the ServletConfig from Jetty, just let me know.
		if ( ! p.containsKey( "resource.loader" ) ) {
			p.setProperty( "resource.loader", "class, current, absolute" );
			p.setProperty( "class.resource.loader.class", ClasspathResourceLoader.class.getName() );
			p.setProperty( "current.resource.loader.class", FileResourceLoader.class.getName() );
			p.setProperty( "current.resource.loader.path", System.getProperty( "user.dir" ) );
			p.setProperty( "absolute.resource.loader.class", FileResourceLoader.class.getName() );
			p.setProperty( "absolute.resource.loader.path", "" );
			p.setProperty( "input.encoding", "utf-8" );
			p.setProperty( "output.encoding", "utf-8" );
			p.setProperty( "default.contentType", "text/html; charset=UTF-8" );
		}
		
		return p;
	}
	
	/** Creates a new HTTP query server.
	 * 
	 * @param queryEngine the query engine that will be used (actually, {@linkplain QueryEngine#copy() copied}) by the
	 * servlets run by this query server.
	 * @param collection the document collection (related to the indices contained in <code>queryEngine</code>) that
	 * will be used to display documents.
	 * @param itemClass a class implementing an {@link javax.servlet.http.HttpServlet} and responsible
	 * for displaying documents (see, e.g., {@link GenericItem}.
	 * @param itemMimeType the default MIME type of a displayed item.
	 * @param port the port exposing the server.
	 * @param titleList an optional list of titles for all documents, or <code>null</code>.
	 */
	public HttpQueryServer( final QueryEngine queryEngine, final DocumentCollection collection, final Class<? extends HttpServlet> itemClass, final String itemMimeType, final int port, final BigList<? extends CharSequence> titleList ) throws Exception {

		LOGGER.debug( "itemClass: " + itemClass );
		LOGGER.debug( "itemMimeType: " + itemMimeType );
		LOGGER.debug( "queryEngine: " + queryEngine );
		LOGGER.debug( "port: " + port );

		// Create the server
		server = new Server();

		// Create a port listener
		SocketConnector connector = new SocketConnector();
		connector.setPort( port );
		server.addConnector( connector );

		// Create a context 
		ContextHandler contextHandler = new ContextHandler();
		contextHandler.setContextPath( "" );
		server.addHandler( contextHandler );

		// Create a servlet container
		ServletHandler servlets = new ServletHandler();
		contextHandler.addHandler( servlets );

		contextHandler.setAttribute( "queryEngine", queryEngine );
		contextHandler.setAttribute( "collection", collection );
		contextHandler.setAttribute( "titleList", titleList );
		contextHandler.setAttribute( "action", "/Query" );
		// TODO: very rudimentary: we should get the template from somewhere else instead...
		contextHandler.setAttribute( "template", System.getProperty( "it.unimi.di.big.mg4j.query.QueryServlet.template" ) );

		// Maps the main servlet onto the container.
		servlets.addServletWithMapping( QueryServlet.class, "/Query" );
		servlets.addServletWithMapping( HelpPage.class, "/Help" );
		
		/* If an item servlet was specified, we link it to /Item. Otherwise,
		 * we inform the query servlet that it should generate direct URIs. */

		if ( itemClass != null ) {
			servlets.addServletWithMapping( itemClass, "/Item" );
			if ( itemClass == FileSystemItem.class ) contextHandler.setAttribute( "derelativise", Boolean.TRUE );
		}
		else contextHandler.setAttribute( "uri", Boolean.TRUE );

		contextHandler.setAttribute( "mimeType", itemMimeType );

		// Start the http server
		server.start();
	}
}
