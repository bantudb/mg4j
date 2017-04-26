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

/** A node representing a range query on a payload-only index.
 * 
 * @author Sebastiano Vigna
 */

public class Range implements Query {
	private static final long serialVersionUID = 1L;

	/** The string representation of the left extreme of the range, or <code>null</code> for no left extreme. */
	public final CharSequence left;
	/** The string representation of the right extreme of the range, or <code>null</code> for no right extreme. */
	public final CharSequence right;
	

	public Range( final CharSequence left, final CharSequence right ) {
		this.left = left;
		this.right = right;
	}
	
	public String toString() {
		return "[" + ( left == null ? "-\u221e" : left.toString() ) + " .. " + ( right == null ? "\u221e" : right.toString() ) + "]";
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		return visitor.visit( this );
	}

	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Range ) ) return false;
		final Range r = ((Range)o);
		return left.equals( r.left ) && right.equals( r.right );
	}
	
	public int hashCode() {
		return left.hashCode() ^ ( right.hashCode() * 23 ) ^ getClass().hashCode();
	}
}
