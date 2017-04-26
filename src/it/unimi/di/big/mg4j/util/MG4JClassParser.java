package it.unimi.di.big.mg4j.util;

/*		 
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

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.ClassStringParser;

/** A small wrapper around JSAP's standard {@link ClassStringParser}. It
 * tries to prefix <samp>it.unimi.di.big.mg4j.<var>package</var>.</samp> to the provided
 * class name, making the specification of graph classes on the command line much easier.
 */

public class MG4JClassParser extends ClassStringParser {

	private final static MG4JClassParser INSTANCE = new MG4JClassParser();
	
	public final static String[] PACKAGE = { "it.unimi.di.big.mg4j.compression", "it.unimi.di.big.mg4j.document", "it.unimi.di.big.mg4j.index", "it.unimi.di.big.mg4j.index.snowball", "it.unimi.di.big.mg4j.index.cluster", "it.unimi.di.big.mg4j.index.payload", "it.unimi.di.big.mg4j.index.remote", "it.unimi.di.big.mg4j.io", "it.unimi.di.big.mg4j.query", "it.unimi.di.big.mg4j.query.parser", "it.unimi.di.big.mg4j.search", "it.unimi.di.big.mg4j.search.score", "it.unimi.di.big.mg4j.tool", "it.unimi.di.big.mg4j.util" };
	
	@SuppressWarnings("deprecation")
	protected MG4JClassParser() {}
	
	public static ClassStringParser getParser() {
		return INSTANCE; 
	}
	
	/** Parses the given class name, but as a first try prepends <samp>it.unimi.di.big.mg4j.<var>package</var>.</samp>.
	 *  
	 * @param className the name of a class, possibly without package specification.
	 */
	@Override
	public Object parse( String className ) throws ParseException {
		for( String p: PACKAGE ) {
			try {
				return super.parse( p + "." + className );
			}
			catch( Exception notFound ) {}
		}
		return super.parse( className );
	}
}
