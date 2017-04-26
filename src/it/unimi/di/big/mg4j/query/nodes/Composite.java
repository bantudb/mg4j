package it.unimi.di.big.mg4j.query.nodes;

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

import it.unimi.dsi.lang.MutableString;

/** A abstract composite node containing an array of component queries.
 * 
 * @author Sebastiano Vigna
 */

public abstract class Composite implements Query {
	private static final long serialVersionUID = 1L;

	/** The component queries. Although public, this field should not be changed after creation. */
	public final Query query[];
	
	public Composite( final Query... query ) {
		this.query = query;
	}
	
	/** Returns a copy of the vector of the component queries (the queries themselves are not copied). 
	 * 
	 * @return a copy of the vector of the component queries.
	 */
	public Query[] components() {
		return query.clone();
	}

	/** Returns a string representation of this node, given a start string, and end string and a separator.
	 * Instantiating subclasses can easily write their {@link Object#toString()}
	 * methods by supplying these three strings and calling this method.
	 * 
	 * @param start the string to be used at the start of the string representation.
	 * @param end the string to be used at the end of the string representation.
	 * @param sep the separator between component queries.
	 * @return a string representation for this composite query node.
	 */
	
	protected String toString( final String start, final String end, final String sep ) {
		final MutableString s = new MutableString();
		s.append( start );
		for( int i = 0; i < query.length; i++ ) { 
			if ( i != 0 ) s.append( sep );
			s.append( query[ i ] );
		}
		s.append( end );
		return s.toString();
	}
}
