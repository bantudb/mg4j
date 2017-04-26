package it.unimi.di.big.mg4j.index;

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

import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;

import java.io.IOException;

/** A very basic abstract implementation of an index iterator,
 * providing an obvious implementation of {@link IndexIterator#term()}, {@link IndexIterator#id()}, {@link DocumentIterator#weight()}
 * and of the {@linkplain #accept(DocumentIteratorVisitor) visiting methods}.
 * 
 */

public abstract class AbstractIndexIterator implements IndexIterator {
	/** The term associated with this index iterator. */
	protected String term;
	/** The identifier associated with this index iterator. */
	protected int id;
	/** The weight associated with this index iterator. */
	protected double weight = 1;	

	public String term() { 
		return term;
	}

	public IndexIterator term( final CharSequence term ) {
		this.term = term == null ? null : term.toString();
		return this;
	}

	public int id() {
		return id;
	}
	
	public IndexIterator id( final int id ) {
		this.id = id;
		return this;
	}
	
	public double weight() {
		return weight;
	}
	
	public IndexIterator weight( final double weight ) {
		this.weight = weight;
		return this;
	}

	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		// TODO: there used to be a visitPost(); check that this works
		return visitor.visit( this );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return visitor.visit( this );
	}
}
