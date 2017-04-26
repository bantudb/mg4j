package it.unimi.di.big.mg4j.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.tools.view.servlet.VelocityViewServlet;

/** The help page. */

public class HelpPage extends VelocityViewServlet {
	private static final long serialVersionUID = 1L;
	
	@Override
	protected ExtendedProperties loadConfiguration( final ServletConfig config ) throws FileNotFoundException, IOException {
		return HttpQueryServer.setLiberalResourceLoading( super.loadConfiguration( config ) );
	}
	
	public Template handleRequest( final HttpServletRequest request, final HttpServletResponse response, final Context context ) throws ResourceNotFoundException, ParseErrorException, Exception {
		response.setCharacterEncoding( "UTF-8" );
		return getTemplate( "it/unimi/dsi/mg4j/query/help.velocity" );
	}
}
