package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna
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

import it.unimi.di.big.mg4j.index.Index;

/** A scorer assigning a constant score (0 by default) to all documents. */
public class ConstantScorer extends AbstractScorer implements DelegatingScorer {
	
	public final double score;
	
	public ConstantScorer( double score ) {
		this.score = score;
	}

	public ConstantScorer( String score ) {
		this( Double.parseDouble( score) );
	}
	
	public ConstantScorer() {
		this( 0 );
	}
	
	public double score( final Index index ) {
		return score;
	}

	public double score() {
		return 0;
	}
	
	public ConstantScorer copy() {
		return this;
	}

	public boolean usesIntervals() {
		return false;
	}

	public String toString() {
		return this.getClass().getName() + "(" + score + ")";
	}
}
