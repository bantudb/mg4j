package it.unimi.di.big.mg4j.query.parser;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.query.nodes.Query;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.MutableString;

/** A parser transforming query strings in composite {@link it.unimi.di.big.mg4j.query.nodes.Query}
 * objects.
 *
 * <p>Besides the obvious {@link #parse(String)}/{@link #parse(MutableString)} methods, a query parser must provide an 
 * {@link #escape(String)}/{@link #escape(MutableString)} method that can be used to turn arbitrary strings into text tokens.
 * 
 * @author Sebastiano Vigna
 */

public interface QueryParser extends FlyweightPrototype<QueryParser> {
	/** Turns the given query string into a composite {@link it.unimi.di.big.mg4j.query.nodes.Query} object. 
	 * 
	 * @param query a string representing a query.
	 * @return the corresponding composite object.
	 * */
	public Query parse( String query ) throws QueryParserException;	

	/** Turns the given query mutable string into a composite {@link it.unimi.di.big.mg4j.query.nodes.Query} object. 
	 * 
	 * @param query a mutable string representing a query.
	 * @return the corresponding composite object.
	 * */
	public Query parse( MutableString query ) throws QueryParserException;	

	/** Escapes the provided string, making it into a text token. 
	 * 
	 * @param token a wannabe text token (maybe containing special characters, but no character below code 32). 
	 * @return an escaped representation of <code>token</code> that will be interpreted
	 * as a text token by this parser.
	 */
	public String escape( String token );

	/** Escapes the provided mutable string, making it into a text token. 
	 * 
	 * @param token a wannabe text token (maybe containing special characters, but no character below code 32). 
	 * @return <code>token</code>, escaped so that it will be interpreted
	 * as a text token by this parser.
	 */
	public MutableString escape( MutableString token );
}
