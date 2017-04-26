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

import it.unimi.di.big.mg4j.search.ConsecutiveDocumentIterator;

import java.util.Arrays;

/** A node representing the consecutive composition of the underlying queries.
 * 
 * @author Sebastiano Vigna
 */

public class Consecutive extends Composite {
	private static final long serialVersionUID = 1L;

	/** The gap array for this consecutive composition, or <code>null</code> for no gaps (see {@link ConsecutiveDocumentIterator}). 
	 * The array can be long as {@link Composite#query}, or have an additional element representing a final gap: in this
	 * case, the index against which the query is resolved must provide document sizes.*/
	public final int[] gap;
	
	public Consecutive( final Query... query ) {
		this( query, null );
	}

	public Consecutive( final Query[] query, final int[] gap ) {
		super( query );
		if ( gap != null && gap.length != query.length && gap.length != query.length + 1 ) throw new IllegalArgumentException();
		this.gap = gap;
	}

	public String toString() {
		return super.toString( "\"", "\"", " " ) + ( gap == null ? "" : Arrays.toString( gap ) );
	}
	
	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] result = visitor.newArray( query.length );
		for( int i = 0; i < query.length; i++ ) if ( ( result[ i ] = query[ i ].accept( visitor ) ) == null ) return null;
		return visitor.visitPost( this, result );
	}

	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Consecutive ) ) return false;
		return Arrays.equals( query, ((Consecutive)o).query );
	}
	
	public int hashCode() {
		return Arrays.hashCode( query ) ^ getClass().hashCode();
	}
}
