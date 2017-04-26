package it.unimi.di.big.mg4j.query.nodes;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2008-2016 Sebastiano Vigna 
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

/** A node representing the alignment of the two iterators.
 * 
 * @author Sebastiano Vigna
 */

public class Align implements Query {
	private static final long serialVersionUID = 1L;

	/** The query to be aligned. */
	public final Query alignee;
	/** The aligner query. */
	public final Query aligner;

	public Align( final Query alignee, final Query aligner ) {
		this.alignee = alignee;
		this.aligner = aligner;
	}
	
	public String toString() {
		return "ALIGN(" + alignee + ", " + aligner + ")";
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] result = visitor.newArray( 2 );
		if ( ( result[ 0 ] = alignee.accept( visitor ) ) == null ) return null;
		if ( ( result[ 1 ] = aligner.accept( visitor ) ) == null ) return null;
		return visitor.visitPost( this, result );
	}
	
	public boolean equals( final Object o ) {
		if ( !( o instanceof Align ) ) return false;
		Align align = (Align)o;
		return align.alignee.equals( alignee ) && align.aligner.equals( aligner );
	}
	
	public int hashCode() {
		return alignee.hashCode() ^ ( aligner.hashCode() * 23 );
	}
}
