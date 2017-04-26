package it.unimi.di.big.mg4j.query.nodes;

import it.unimi.di.big.mg4j.search.IntervalIterators;

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


/** A node representing truth (i.e., all documents are returned with interval iterator {@link IntervalIterators#TRUE}).
 * 
 * @author Sebastiano Vigna
 */

public class True implements Query {
	private static final long serialVersionUID = 1L;

	public String toString() {
		return "#TRUE";
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		return visitor.visit( this );
	}
	
	public boolean equals( final Object o ) {
		return o instanceof True;
	}
	
	public int hashCode() {
		return 23;
	}
}
