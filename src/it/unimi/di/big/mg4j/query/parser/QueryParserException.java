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

/** A parse exception.
 * 
 * Different compiler-compilers and different parsers will
 * generate 
 * during {@link it.unimi.di.big.mg4j.query.parser.QueryParser#parse(String)}
 * different exceptions, 
 * which should be wrapped into an instance of this class for uniform treatment.
 * 
 */

public class QueryParserException extends Exception {

	private static final long serialVersionUID = 1L;

	public QueryParserException( Throwable cause ) {
		super( cause );
	}

	public QueryParserException( String msg ) {
		super( msg );
	}
}
