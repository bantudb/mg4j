package it.unimi.di.big.mg4j.query.nodes;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2009-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.search.RemappingDocumentIterator;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

import java.util.Map;


/** A node representing an index remapping.
 * 
 * <P>A remapping takes results of the underlying {@link Query} for an <em>internal</em> index 
 * (which must be one of the indices for which the query provides results) and exhibits them
 * as results of an <em>external</em> index.
 * 
 * The map provided at construction time under the form of two parallel arrays
 * should remap internal indices to external indices.
 * 
 * @author Sebastiano Vigna
 * @see RemappingDocumentIterator
 */

public class Remap implements Query {
	private static final long serialVersionUID = 1L;

	/** The only underlying node. */
	public final Query query;
	/** The remapping from internal to external indices. */
	public final Object2ObjectLinkedOpenHashMap<String, String> indexRemapping;
	/** The remapping from external to internal indices. */
	public final Object2ObjectLinkedOpenHashMap<String, String> indexInverseRemapping;

	private static Object2ObjectLinkedOpenHashMap<String, String> invert( Object2ObjectMap<String, String> m ) {
		final Object2ObjectLinkedOpenHashMap<String, String> result = new Object2ObjectLinkedOpenHashMap<String, String>();
		for( Map.Entry<String, String> e : m.entrySet() ) result.put( e.getValue(), e.getKey() );
		if ( m.size() != result.size() ) throw new IllegalArgumentException( "Index remapping " + m + " is not a bijection" );
		return result;
	}
	
	/** Creates a new index remapping query node given explicit lists external and internal indices. 
	 * @param query the underlying query.
	 * @param internalIndex the array of internal index names. 
	 * @param externalIndex the array of external index names, parallel to <code>internalIndex</code>.
	 */
	public Remap( final Query query, final CharSequence[] internalIndex, final CharSequence[] externalIndex ) {
		indexRemapping = new Object2ObjectLinkedOpenHashMap<String, String>();
		for( int i = 0; i < externalIndex.length; i++ ) indexRemapping.put( internalIndex[ i ].toString(), externalIndex[ i ].toString() );
		indexInverseRemapping = invert( indexRemapping );
		this.query = query;
	}

	/** Creates a new index remapping query node given an index remapping.
	 * @param query the underlying query.
	 * @param indexRemapping a map from internal to external indices, which will be copied internally.
	 */
	public Remap( final Query query, final Object2ObjectMap<String, String> indexRemapping ) {
		this.indexRemapping = new Object2ObjectLinkedOpenHashMap<String, String>( indexRemapping );
		indexInverseRemapping = invert( indexRemapping );
		this.query = query;
	}

	public String toString() {
		return query.toString() + "{{ " + indexRemapping.toString() + " }}";
	}

	public <T> T accept( final QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException {
		if ( ! visitor.visitPre( this ) ) return null;
		final T result = query.accept( visitor );
		if ( result == null ) return null;
		return visitor.visitPost( this, result );
	}
	
	public boolean equals( final Object o ) {
		if ( ! ( o instanceof Remap) ) return false;
		return indexRemapping.equals( ((Remap)o).indexRemapping );
	
	}
	
	public int hashCode() {
		return indexRemapping.hashCode() ^ getClass().hashCode();
	}
}
