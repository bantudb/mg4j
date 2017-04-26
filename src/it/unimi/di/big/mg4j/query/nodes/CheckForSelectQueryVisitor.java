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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSets;

import java.util.Set;


/** A {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor} that
 * returns {@link Boolean#FALSE} only if the visited query contains a {@link Select} node
 * that does not lie in the aligner of an {@link Align} query (as in that case
 * the index is not part of the answer).
 * 
 * @author Sebastiano Vigna
 */

public class CheckForSelectQueryVisitor implements QueryBuilderVisitor<Set<String>> {
	private final String defaultIndex;
	private ObjectArrayList<String> currentIndex = new ObjectArrayList<String>();
	public String errorMessage;

	
	public CheckForSelectQueryVisitor( final String defaultIndex ) {
		this.defaultIndex = defaultIndex;
	}

	public QueryBuilderVisitor<Set<String>> prepare() { 
		currentIndex.clear();
		currentIndex.push( defaultIndex );
		return this; 
	}
	
	@SuppressWarnings("unchecked")
	public Set<String>[] newArray( int len ) { return new Set[ len ]; }
	
	public QueryBuilderVisitor<Set<String>> copy() { return new CheckForSelectQueryVisitor( defaultIndex ); }
	
	private static Set<String> union( Set<String>[] a ) {
		ObjectOpenHashSet<String> s = new ObjectOpenHashSet<String>();
		for( Set<String> t: a ) s.addAll( t );
		return s;
	}
	
	private Set<String> checkUnion( Set<String>[] a, String operator ) {
		ObjectOpenHashSet<String> s = new ObjectOpenHashSet<String>();
		for( Set<String> t: a ) s.addAll( t );
		if ( s.size() == 1 ) return s;
		errorMessage = "The " + operator + " operator requires subqueries on the same index";
		return null;
	}
	
	public boolean visitPre( And node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Consecutive node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( LowPass node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Annotation node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Not node ) throws QueryBuilderVisitorException  { return true; }
	public boolean visitPre( Or node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( OrderedAnd node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Align node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( MultiTerm node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Select node ) throws QueryBuilderVisitorException { 
		currentIndex.push( node.index.toString() );
		return true; 
	}
	public boolean visitPre( Remap node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Weight node ) throws QueryBuilderVisitorException  { return true; }
	public boolean visitPre( Difference node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Inclusion node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Containment node ) throws QueryBuilderVisitorException { return true; }

	public Set<String> visitPost( And node, Set<String>[] v ) throws QueryBuilderVisitorException { return union( v ); }
	public Set<String> visitPost( Consecutive node, Set<String>[] v ) throws QueryBuilderVisitorException { return checkUnion( v, "phrase" ); }
	public Set<String> visitPost( LowPass node, Set<String> v ) throws QueryBuilderVisitorException { return v; }
	public Set<String> visitPost( Annotation node, Set<String> v ) throws QueryBuilderVisitorException { return v; }
	public Set<String> visitPost( Not node, Set<String> v ) throws QueryBuilderVisitorException  { return v; }
	public Set<String> visitPost( Or node, Set<String>[] v ) throws QueryBuilderVisitorException { return union( v ); }
	public Set<String> visitPost( OrderedAnd node, Set<String>[] v ) throws QueryBuilderVisitorException { return checkUnion( v, "ordered AND" ); }
	public Set<String> visitPost( Align node, Set<String>[] v ) throws QueryBuilderVisitorException { 
		for( Set<String> t: v ) if ( t.size() != 1 ) {
			errorMessage = "The alignment operator requires single-index subqueries";
			return null;
		}
		return v[ 0 ];
	}
	public Set<String> visitPost( MultiTerm node, Set<String>[] v ) throws QueryBuilderVisitorException { return checkUnion( v, "expansion" ); } 
	public Set<String> visitPost( Select node, Set<String> v ) {
		currentIndex.pop();
		return v;
	}
	public Set<String> visitPost( Remap node, Set<String> v ) throws QueryBuilderVisitorException { 
		final ObjectOpenHashSet<String> result = new ObjectOpenHashSet<String>();
		for( String index: v ) {
			final String remappedIndex = node.indexRemapping.get( index ); 
			result.add( remappedIndex == null ? index : remappedIndex );
		}
		return result; 
	}
	public Set<String> visitPost( Weight node, Set<String> v ) throws QueryBuilderVisitorException { return v; }

	public Set<String> visitPost( Difference node, Set<String>[] v ) throws QueryBuilderVisitorException { return union( v ); }
	public Set<String> visitPost( Inclusion node, Set<String>[] v ) throws QueryBuilderVisitorException { return union( v ); }
	public Set<String> visitPost( Containment node, Set<String>[] v ) throws QueryBuilderVisitorException { return union( v ); }
	public Set<String> visit( Term node ) throws QueryBuilderVisitorException { return ObjectSets.singleton( currentIndex.top() ); }
	public Set<String> visit( Prefix node ) throws QueryBuilderVisitorException { return ObjectSets.singleton( currentIndex.top() ); }
	public Set<String> visit( Range node ) throws QueryBuilderVisitorException { return ObjectSets.singleton( currentIndex.top() ); }

	public Set<String> visit( True node ) throws QueryBuilderVisitorException { return ObjectSets.singleton( currentIndex.top() ); }
	public Set<String> visit( False node ) throws QueryBuilderVisitorException { return ObjectSets.singleton( currentIndex.top() ); }
}
