package it.unimi.di.big.mg4j.search.visitor;

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

import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.MultiTermIndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.FalseDocumentIterator;
import it.unimi.di.big.mg4j.search.TrueDocumentIterator;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;

import java.io.IOException;

/** A visitor for the tree defined by a {@link DocumentIterator}.
 *
 * <p>Implementations of this interface must be reusable. The user must
 * invoke {@link #prepare()} before a visit so that the internal state
 * of the visitor can be suitably set up.
 * 
 * <p>Document-iterator visitors can optionally return values. In this case, the {@link #newArray(int)} method
 * must be properly implemented. Otherwise, it can safely return <code>null</code>: all
 * implementations of {@link DocumentIterator#accept(DocumentIteratorVisitor)} and
 * {@link DocumentIterator#acceptOnTruePaths(DocumentIteratorVisitor)} must be prepared
 * for this to happen. (It would be of course possible to pass around empty arrays, but
 * visitors on document iterators usually have performance issues.)
 * 
 * <p>If a document-iterator visitor does not return values, it is suggested that it is
 * parameterised on {@link Boolean} and that it returns {@link Boolean#TRUE} to denote that a visit should not end.
 * 
 * <p>For maximum flexibility, there is just {@linkplain #visit(IndexIterator) one type of visit method}
 * for leaves, but <em>two</em> visits methods for internal nodes,
 * which should be used for {@linkplain #visitPre(DocumentIterator) preorder}
 * and {@linkplain #visitPost(DocumentIterator, Object[]) postorder} visits, respectively.
 *
 * <p>The visit methods for leaves are actually <em>two</em>, as they depend on the visited
 * leaf (more precisely, whether it is an {@link IndexIterator} or a {@link MultiTermIndexIterator}. The
 * reason for this choice is that in some cases the term-like nature of a {@link MultiTermIndexIterator}
 * makes it easier to write scorers that don't know about expansions (e.g., {@link BM25Scorer}. On the
 * other hand, some scorers might want to delve into a {@link MultiTermIndexIterator} and discover using
 * {@link MultiTermIndexIterator#front(IndexIterator[])} which terms where actually found. The
 * {@link #visit(MultiTermIndexIterator)} makes this goal easily reachable.
 *
 * <p>Note that this visitor interface and that defined in {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor}
 * are based on different principles: in the latter case, the action of the visitor will likely
 * be different for each type of internal node, so we have specific visit methods for each type of such nodes. 
 * In our case, the visit will most likely behave differently just for internal nodes and leaves, so we
 * prefer a simpler interface that also let us implement more easily visitor acceptance methods
 * ({@link it.unimi.di.big.mg4j.search.DocumentIterator#accept(DocumentIteratorVisitor)}
 * and {@link it.unimi.di.big.mg4j.search.DocumentIterator#acceptOnTruePaths(DocumentIteratorVisitor)}).
 *
 * @author Sebastiano Vigna
 */ 

public interface DocumentIteratorVisitor<T> {

	/** Prepares the internal state of this visitor for a(nother) visit. 
	 * 
	 * <p>By specification, it must be safe to call this method any number of times.
	 * @return this visitor.
	 */
	DocumentIteratorVisitor<T> prepare();

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
	 * @return an array of type <code>T</code> of length <code>len</code>, or <code>null</code> if
	 * this document iterator visitor does not return values.
	 */
	
	T[] newArray( int len );
	
	/** Visits an internal node <em>before</em> recursing into the corresponding subtree.
	 * 
	 * @param documentIterator the internal node to be visited.
	 * @return true if the visit should continue.
	 */
	boolean visitPre( DocumentIterator documentIterator );

	/** Visits an internal node <em>after</em> recursing into the corresponding subtree.
	 * 
	 * @param documentIterator the internal node to be visited.
	 * @param subNodeResult the array of results returned by subnodes.
	 * @return an appropriate return value (usually, the object built using the elements in <code>subNode</code>) if the visit should continue, or <code>null</code>.
	 */
	T visitPost( DocumentIterator documentIterator, T[] subNodeResult );

	/** Visits an {@link IndexIterator} leaf.
	 * 
	 * @param indexIterator the leaf to be visited.
	 * @return an appropriate return value if the visit should continue, or <code>null</code>.
	 */
	T visit( IndexIterator indexIterator ) throws IOException;

	/** Visits a {@link MultiTermIndexIterator} leaf.
	 * 
	 * @param multiTermIndexIterator the leaf to be visited.
	 * @return an appropriate return value if the visit should continue, or <code>null</code>.
	 */
	T visit( MultiTermIndexIterator multiTermIndexIterator ) throws IOException;

	/** Visits a {@link TrueDocumentIterator} leaf.
	 * 
	 * @param trueDocumentIterator the leaf to be visited.
	 * @return an appropriate return value if the visit should continue, or <code>null</code>.
	 */
	T visit( TrueDocumentIterator trueDocumentIterator ) throws IOException;

	/** Visits a {@link FalseDocumentIterator} leaf.
	 * 
	 * @param falseDocumentIterator the leaf to be visited.
	 * @return an appropriate return value if the visit should continue, or <code>null</code>.
	 */
	T visit( FalseDocumentIterator falseDocumentIterator ) throws IOException;

}
