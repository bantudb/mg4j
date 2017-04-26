package it.unimi.di.big.mg4j.query.nodes;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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

/** A node representing a difference of two queries.
 *  
 * @author Sebastiano Vigna
 */

public class Difference implements Query {
	private static final long serialVersionUID = 1L;

	/** The minuend query. */
	public final Query minuend;
	/** The subtrahend query. */
	public final Query subtrahend;
	/** A margin that will be added to the left of each interval of the subtrahend. */
	public final int leftMargin;
	/** A margin that will be added to the right of each interval of the subtrahend. */
	public final int rightMargin;

	public Difference( final Query minuend, final Query subtrahend ) {
		this( minuend, subtrahend, 0, 0 );
	}
	
	public Difference( final Query minuend, final Query subtrahend, final int leftMargin, final int rightMargin ) {
		this.minuend = minuend;
		this.subtrahend = subtrahend;
		this.leftMargin = leftMargin;
		this.rightMargin = rightMargin;
	}

	public String toString() {
		return new MutableString().append( '(' ).append( minuend ).append( leftMargin == 0 && rightMargin == 0 ? " - " : " - [[" + leftMargin + ":" + rightMargin + "]] " ).append( subtrahend ).append( ')' ).toString();
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] result = visitor.newArray( 2 );
		if ( ( result[ 0 ] = minuend.accept( visitor ) ) == null ) return null;
		if ( ( result[ 1 ] = subtrahend.accept( visitor ) ) == null ) return null;
		return visitor.visitPost( this, result );
	}

	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Difference ) ) return false;
		final Difference d = (Difference)o;
		return minuend.equals( d.minuend ) && subtrahend.equals( d.subtrahend ) && leftMargin == d.leftMargin && rightMargin == d.rightMargin;
	}
	
	public int hashCode() {
		return minuend.hashCode() ^ ( subtrahend.hashCode() * 23 ) ^ leftMargin ^ ( rightMargin * 23 );
	}
}
