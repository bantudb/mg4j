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

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** An item serving a file from the file system.
 * 
 * <P>This kind of {@link it.unimi.di.big.mg4j.query.QueryServlet} item will load into the browser
 * the file specified by the parameter <code>uri</code>. Its only purpose is to work around security
 * problems that forbid in some browsers (e.g., Firefox) to link a file in 
 * the file system from a page retrieved from an HTTP server.
 * 
 * <P>When this class is loaded, it creates the singleton {@link it.unimi.di.big.mg4j.query.HttpFileServer}
 * and caches it to avoid class garbage collection.
 */


public class FileSystemItem extends HttpServlet {
	private static final long serialVersionUID = 1L;
	/** The singleton file server. */
	private final static HttpFileServer SERVER = HttpFileServer.getServer();

	public void init() {
		// This avoids class garbage collection.
		getServletContext().setAttribute( "HttpFileServer", SERVER );
	}
	
	protected void doGet( final HttpServletRequest request, final HttpServletResponse response ) throws IOException {

		String uri = request.getParameter( "uri" );
		if ( uri != null && uri.indexOf( "file:" ) == 0 ) {
			uri = uri.substring( "file:".length() );
			response.setStatus( HttpServletResponse.SC_MOVED_PERMANENTLY );
			response.addHeader( "location", "http://localhost:" + HttpFileServer.getPort() + uri );
			return;
		}
		
		response.sendError( HttpServletResponse.SC_FORBIDDEN, "This servlet requires a parameter uri containing a URI of type file:" );
	}

	protected void doPost( final HttpServletRequest request, final HttpServletResponse response ) throws IOException {
		doGet( request, response );
	}
}
