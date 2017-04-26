package it.unimi.di.big.mg4j.query.nodes;

import it.unimi.dsi.lang.FlyweightPrototype;

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


/** A visitor for a composite query. 
 * 
 * <p>Instances of this class are <em>builder visitors</em>, that is, 
 * visitors that during their visit build a complex object (of course, it is possible not building an
 * object at all). 
 *  
 * <p>Implementations of this interface must be reusable. The user must
 * invoke {@link #prepare()} before a visit so that the internal state
 * of the visitor can be suitably set up.
 * 
 * <p>The most typical usage of this class is that of building
 * {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator document iterators}
 * following the structure of the query. By writing different builder visitors
 * for different families of document iterators, parsing and
 * document-iterator construction are completely decoupled.
 * 
 * <p>Note that this visitor interface and that defined in {@link it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor}
 * are based on different principles: see the comments therein.
 *
 * @author Sebastiano Vigna
 */

public interface QueryBuilderVisitor<T> extends FlyweightPrototype<QueryBuilderVisitor<T>> {
	/** Prepares the internal state of this visitor for a(nother) visit. 
	 * 
	 * <p>By specification, it must be safe to call this method any number of times.
	 * 
	 * @return this visitor.
	 */
	QueryBuilderVisitor<T> prepare();
	
	/** Builds an array of given length of type <code>T</code>.
	 * 
	 * <p>Because of erasure, generic classes in Java cannot allocate arrays
	 * of generic types. This impossibility can be a problem if for some reason
	 * the <code>visitPost()</code> methods expect an <em>actual</em> array of 
	 * type <code>T</code>. This method must provide an array of given length
	 * that is an acceptable input for all <code>visitPost()</code> methods.
	 * 
	 * <p>Note that by declaring an implementing class of this interface
	 * that has a sole constructor accepting an argument of type <code>Class&lt;T></code>,
	 * you will force the user to provide the class of the generic type, opening
	 * the way for the reflective methods in {@link java.lang.reflect.Array}.
	 * 
	 * @param len the required array length. 
	 * @return an array of type <code>T</code> of length <code>len</code>.
	 */
	
	T[] newArray( int len );
	
	/** Visits an {@link And} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( And node ) throws QueryBuilderVisitorException;

	/** Visits an {@link And} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( And node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Consecutive} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Consecutive node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Consecutive} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Consecutive node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link LowPass} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( LowPass node ) throws QueryBuilderVisitorException;

	/** Visits a {@link LowPass} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( LowPass node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Annotation} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Annotation node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Annotation} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Annotation node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Not} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Not node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Not} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Not node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits an {@link Or} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Or node ) throws QueryBuilderVisitorException;

	/** Visits an {@link Or} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Or node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits an {@link OrderedAnd} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( OrderedAnd node ) throws QueryBuilderVisitorException;

	/** Visits an {@link OrderedAnd} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( OrderedAnd node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits an {@link Align} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Align node ) throws QueryBuilderVisitorException;

	/** Visits an {@link Align} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Align node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Difference} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Difference node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Difference} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Difference node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits an {@link Inclusion} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Inclusion node ) throws QueryBuilderVisitorException;

	/** Visits an {@link Inclusion} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Inclusion node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits an {@link Containment} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Containment node ) throws QueryBuilderVisitorException;

	/** Visits an {@link Containment} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Containment node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link MultiTerm} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( MultiTerm node ) throws QueryBuilderVisitorException;

	/** Visits a {@link MultiTerm} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( MultiTerm node, T[] subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Select} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Select node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Select} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Select node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Remap} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Remap node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Remap} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Remap node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Weight} node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param node the node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( Weight node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Weight} node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param node the internal node to be visited.
	 * @param subNodeResult the of result returned by the sole subnode.
	 * @return an appropriate return value (usually, the object built using <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( Weight node, T subNodeResult ) throws QueryBuilderVisitorException;

	/** Visits a {@link Term}.
	 * 
	 * @param node the leaf to be visited.
	 * @return true if the visit should continue.
	 */
	T visit( Term node ) throws QueryBuilderVisitorException;

	/** Visits a {@link Prefix}.
	 * 
	 * @param node the leaf to be visited.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNodeResult</code>) if the visit should continue, or <code>null</code>.
	 */
	T visit( Prefix node ) throws QueryBuilderVisitorException;
	
	/** Visits a {@link Range}.
	 * 
	 * @param node the leaf to be visited.
	 * @return true if the visit should continue.
	 */
	T visit( Range node ) throws QueryBuilderVisitorException;

	/** Visits {@link True}.
	 * 
	 * @param node the leaf to be visited.
	 * @return true if the visit should continue.
	 */
	T visit( True node ) throws QueryBuilderVisitorException;

	/** Visits {@link False}.
	 * 
	 * @param node the leaf to be visited.
	 * @return true if the visit should continue.
	 */
	T visit( False node ) throws QueryBuilderVisitorException;

	public QueryBuilderVisitor<T> copy();
}
