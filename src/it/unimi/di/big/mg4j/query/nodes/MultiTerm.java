package it.unimi.di.big.mg4j.query.nodes;

import it.unimi.di.big.mg4j.index.MultiTermIndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.MutableString;

import java.util.Arrays;

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

/** A node representing a virtual term obtained by merging the occurrences of the given (possibly weighted) terms.
 * 
 * <p>This node is mainly useful when performing query expansion. The {@link QueryBuilderVisitor}
 * used to generate {@linkplain DocumentIterator document iterators} can decide which
 * policy to use for setting the frequency and the name of the virtual term.
 * 
 * @author Sebastiano Vigna
 * @see MultiTermIndexIterator
 */

public class MultiTerm extends Composite {
	private static final long serialVersionUID = 1L;
	
	/** Creates a new multi-term node.
	 * 
	 * @param query a vector of nodes representing distinct terms; they must be either instances
	 * of {@link Term}, or instances of {@link Weight} containing instances of {@link Term}.
	 * @throws IllegalArgumentException if some term appears twice in <code>query</code>, or if
	 * the specification is not followed.
	 */
	
	public MultiTerm( final Query... query ) {
		super( query );
		final ObjectOpenHashSet<MutableString> s = new ObjectOpenHashSet<MutableString>( query.length );
		for( Query q : query ) {
			if ( ! ( q instanceof Term ) && ! ( ( q instanceof Weight ) && ( ((Weight)q).query instanceof Term ) ) ) throw new IllegalArgumentException();
			s.add( new MutableString( q instanceof Term ? ((Term)q).term : ((Term)((Weight)q).query ).term ) );
		}
		if ( s.size() != query.length ) throw new IllegalArgumentException( "Multiterm nodes require distinct terms" );
	}

	public String toString() {
		return super.toString( "MULTITERM(", ")", ", " );
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T[] result = visitor.newArray( query.length );
		for( int i = 0; i < query.length; i++ ) if ( ( result[ i ] = query[ i ].accept( visitor ) ) == null ) return null;
		return visitor.visitPost( this, result );
	}

	public boolean equals( final Object o ) {
		if ( ! ( o instanceof MultiTerm ) ) return false;
		return Arrays.equals( query, ((MultiTerm)o).query );
	}
	
	public int hashCode() {
		return Arrays.hashCode( query ) ^ getClass().hashCode();
	}
}
