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

/** A node representing a single term.
 * 
 * @author Sebastiano Vigna
 */

public class Term implements Query {
	private static final long serialVersionUID = 1L;

	/** The term represented by this node, or <code>null</code> if the term is defined by its number. */
	public final CharSequence term;
	/** The number of the term represented by this node, or -1 if the term is defined literally. */
	public final int termNumber;
	

	public Term( final CharSequence term ) {
		this.term = term;
		this.termNumber = -1;
	}
	
	public Term( final int termNumber ) {
		this.term = null;
		this.termNumber = termNumber;
	}
	
	public String toString() {
		return term != null ? term.toString() : "#" + Integer.toString( termNumber );
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		return visitor.visit( this );
	}
	
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Term) ) return false;
		final Term t = ((Term)o);
		if ( ( term != null ) != ( t.term != null ) ) return false;
		return term != null && term.toString().equals( ((Term)o).term.toString() ) || term == null && termNumber == ((Term)o).termNumber;
	}
	
	public int hashCode() {
		return ( term == null ? termNumber : term.hashCode() ) ^ getClass().hashCode();
	}
}
