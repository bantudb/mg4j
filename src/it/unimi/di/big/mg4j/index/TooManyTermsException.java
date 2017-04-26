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

/** Thrown to indicate that a prefix query generated too many terms.
 *
 * @author Sebastiano Vigna
 */
public class TooManyTermsException extends Exception {
	private static final long serialVersionUID = 0L;

	public final long numberOfTerms;
	
	public TooManyTermsException( final long numberOfTerms ) {
		this.numberOfTerms = numberOfTerms;
	}
	
	public String toString() {
		return "Too many terms for a prefix: " + Long.toString( numberOfTerms );
	}
}
