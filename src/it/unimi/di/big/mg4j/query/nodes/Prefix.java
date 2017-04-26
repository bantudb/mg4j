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

/** A node representing a set of terms defined by a common prefix.
 * 
 * @author Sebastiano Vigna
 */

public class Prefix implements Query {
	private static final long serialVersionUID = 1L;

	/** The common prefix of the set of terms represented by this node. */
	public final CharSequence prefix;
	
	public Prefix( final CharSequence prefix ) {
		this.prefix = prefix;
	}
	
	public String toString() {
		return new MutableString().append( prefix ).append( '*' ).toString();
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		return visitor.visit( this );
	}

	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Prefix) ) return false;
		return prefix.equals( ((Prefix)o).prefix );
	}
	
	public int hashCode() {
		return prefix.hashCode() ^ getClass().hashCode();
	}

}
