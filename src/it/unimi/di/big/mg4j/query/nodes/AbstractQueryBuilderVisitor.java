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


/** A {@link it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitor} that
 * returns true on all <code>visitPre()</code> methods and does nothing
 * on {@link #prepare()}.
 * 
 * @author Sebastiano Vigna
 */

public abstract class AbstractQueryBuilderVisitor<T> implements QueryBuilderVisitor<T> {
	/** No-op. */
	public QueryBuilderVisitor<T> prepare() { return this; }

	public boolean visitPre( And node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Consecutive node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( LowPass node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Annotation node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Not node ) throws QueryBuilderVisitorException  { return true; }
	public boolean visitPre( Or node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( OrderedAnd node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Align node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( MultiTerm node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Select node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Remap node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Weight node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Difference node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Inclusion node ) throws QueryBuilderVisitorException { return true; }
	public boolean visitPre( Containment node ) throws QueryBuilderVisitorException { return true; }
}
