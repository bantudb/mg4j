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
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentFactory.FieldType;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;
import org.apache.velocity.tools.view.servlet.VelocityViewServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An generic item, displaying all document fields.
 * 
 * <P>This kind of {@link it.unimi.di.big.mg4j.query.QueryServlet} item will display each field
 * of a document inside a <samp>FIELDSET</samp> element. It is mainly useful for debugging purposes.
 */

public class GenericItem extends VelocityViewServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger( GenericItem.class );

	@Override
	protected ExtendedProperties loadConfiguration( final ServletConfig config ) throws FileNotFoundException, IOException {
		return HttpQueryServer.setLiberalResourceLoading( super.loadConfiguration( config ) );
	}

	public Template handleRequest( final HttpServletRequest request, final HttpServletResponse response, final Context context ) throws Exception {
		if ( request.getParameter( "doc" ) != null ) {
			DocumentCollection collection = (DocumentCollection)getServletContext().getAttribute( "collection" );
			response.setContentType( request.getParameter( "m" ) );
			response.setCharacterEncoding( "UTF-8" );
			final Document document = collection.document( Long.parseLong( request.getParameter( "doc" ) ) );
			final DocumentFactory factory = collection.factory();
			final ObjectArrayList<String> fields = new ObjectArrayList<String>();
			final int numberOfFields = factory.numberOfFields();
			
			LOGGER.debug( "ParsingFactory declares " + numberOfFields + " fields"  );
			
			for( int field = 0; field < numberOfFields; field++ ) {
				if ( factory.fieldType( field ) != FieldType.TEXT ) fields.add( StringEscapeUtils.escapeHtml( document.content( field ).toString() ) );
				else fields.add( StringEscapeUtils.escapeHtml( IOUtils.toString( (Reader)document.content( field ) ) ).replaceAll( "\n", "<br>\n" ) );
			}
			context.put( "title", document.title() );
			context.put( "fields", fields );
			context.put( "factory", factory );
			return getTemplate( "it/unimi/dsi/mg4j/query/generic.velocity" );
		}
		
		return null;
	}
}
