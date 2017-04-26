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

/** A node representing a low-pass filtering of the only underlying query.
 *  
 * @author Sebastiano Vigna
 */

public class LowPass implements Query {
	private static final long serialVersionUID = 1L;

	/** The only underlying node. */
	public final Query query;
	/** The threshold above which intervals are eliminated. */
	public final int k;

	public LowPass( final Query query, final int k ) {
		this.query = query;
		this.k = k;
	}
	
	public String toString() {
		return new MutableString().append( '(' ).append( query ).append( ")~" ).append( k ).toString();
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T result = query.accept( visitor );
		if ( result == null ) return null;
		return visitor.visitPost( this, result );
	}
	
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof LowPass ) ) return false;
		final LowPass l = (LowPass)o;
		return query.equals( l.query ) && k == l.k;
	}
	
	public int hashCode() {
		return query.hashCode() ^ k;
	}

}
