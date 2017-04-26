package it.unimi.di.big.mg4j.query.nodes;

import java.util.Arrays;

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

/** A node representing the logical and of the underlying queries.
 * 
 * @author Sebastiano Vigna
 */

public class And extends Composite {
	private static final long serialVersionUID = 1L;

	public And( final Query... query ) {
		super( query );
	}
	
	public String toString() {
		return super.toString( "AND(", ")", ", " );
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] result = visitor.newArray( query.length );
		for( int i = 0; i < query.length; i++ ) if ( ( result[ i ] = query[ i ].accept( visitor ) ) == null ) return null;
		return visitor.visitPost( this, result );
	}
	
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof And ) ) return false;
		return Arrays.equals( query, ((And)o).query );
	}
	
	public int hashCode() {
		return Arrays.hashCode( query ) ^ getClass().hashCode();
	}
}
