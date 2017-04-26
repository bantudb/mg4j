package it.unimi.di.big.mg4j.query;

import java.util.Formatter;

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
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

/** An instance of this class is used to pack the results gathered by {@link it.unimi.di.big.mg4j.query.QueryServlet}
 * in such a way that they are easily accessible from the Velocity Template Language.
 * 
 * @author Sebastiano Vigna
 * @since 0.9.2
 */

public class ResultItem extends BrowseItem {
	/** The document index of this result. */
	public final long doc;
	/** The score of this result. */
	public final double score;
	
	public ResultItem( final long doc, final double score ) {
		this.doc = doc;
		this.score = score;
	}
		
	public long doc() {
		return doc;
	}

	public final double score() {
		return score;
	}
	
	@SuppressWarnings("resource")
	public final String score( int digits ) {
		return new Formatter( new StringBuilder() ).format( "%." + digits + "f", Double.valueOf( score ) ).out().toString();
	}

	public String toString() {
		return "[doc: " + doc + " title: " + title + " score: " + score + "]";
	}
	
}
