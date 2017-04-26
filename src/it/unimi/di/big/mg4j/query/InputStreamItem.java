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

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An item serving a raw input stream from the document collection.
 * 
 * <P>This kind of {@link it.unimi.di.big.mg4j.query.QueryServlet} item will load into the browser
 * the stream returned by the document collection for the given document. Using the system property
 * <samp>it.unimi.di.big.mg4j.query.InputStreamItem.skip</samp>, you can set a number of sub-input streams
 * that will be skipped by calling {@link java.io.InputStream#reset()} (see {@link it.unimi.di.big.mg4j.document}).
 */

public class InputStreamItem extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger( InputStreamItem.class );
	private static final int skip = Integer.getInteger( InputStreamItem.class.getName() + ".skip", 0 ).intValue();
	
	protected void doGet( final HttpServletRequest request, final HttpServletResponse response ) throws IOException {
		try {
			if ( request.getParameter( "m" ) != null && request.getParameter( "doc" ) != null ) {
				DocumentCollection collection = (DocumentCollection)getServletContext().getAttribute( "collection" );
				if ( collection == null ) LOGGER.error( "The servlet context does not contain a document collection." );
				response.setContentType( request.getParameter( "m" ) );
				response.setCharacterEncoding( "UTF-8" );
				InputStream rawContent = collection.stream( Long.parseLong( request.getParameter( "doc" ) ) );
				for( int i = skip; i-- != 0; ) rawContent.reset();
				IOUtils.copy( rawContent, response.getOutputStream() );
			}
		} catch( RuntimeException e ) {
			e.printStackTrace();
			LOGGER.error( e.toString() );
			throw e;
		}
	}

	protected void doPost( final HttpServletRequest request, final HttpServletResponse response ) throws IOException {
		doGet( request, response );
	}
}
