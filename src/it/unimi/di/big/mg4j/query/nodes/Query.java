package it.unimi.di.big.mg4j.query.nodes;

import java.io.Serializable;

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

/** A node of a composite representing a query.
 * 
 * <p>A query is abstractly represented by a composite made of implementations
 * of this interface. The syntax can be different from parser to
 * parser, but the {@linkplain it.unimi.di.big.mg4j.query.parser.QueryParser#parse(String) 
 * result of the parsing process} is an instance of this class.
 * 
 * <p>Queries support <em>building visits</em>: invoking {@link #accept(QueryBuilderVisitor)} on
 * a suitable {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor} will return an object
 * that has been built by the visitor. 
 * 
 * <p>To this purpose, the implementation of 
 * {@link it.unimi.di.big.mg4j.query.nodes.Query#accept(QueryBuilderVisitor)}
 * on internal nodes must gather in an array (or in an element) 
 * the results returned by the recursive calls to 
 * {@link it.unimi.di.big.mg4j.query.nodes.Query#accept(QueryBuilderVisitor)}
 * on subnodes and pass the array (or the element) to the suitable <code>visitPost()</code>
 * method of {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor}.
 * 
 * <p>Since allocating a generic array is impossible, every visitor must provide an explicit
 * {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor#newArray(int)} 
 * method which returns an array of the correct type.
 *
 * @see Query
 * @author Sebastiano Vigna
 */

public interface Query extends Serializable {
	
	/** Accepts a visitor.
	 * 
	 * @param visitor the visitor.
	 * @return the result of the visit, or <code>null</code> if the visit should stop.
	 * @see Query
	 */
	public <T> T accept( QueryBuilderVisitor<T> visitor ) throws QueryBuilderVisitorException;
}
