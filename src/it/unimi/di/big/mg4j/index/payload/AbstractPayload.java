package it.unimi.di.big.mg4j.index.payload;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Paolo Boldi and Sebastiano Vigna 
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

import org.apache.commons.collections.Predicate;

/** An abstract payload.
 * 
 * <p>The main responsibility of this class is that of implementing {@link #rangeFilter(Payload, Payload)}
 * using the {@link Comparable} methods.
 */


public abstract class AbstractPayload implements Payload {
	private static final long serialVersionUID = 1L;

	protected final class ComparatorPayloadPredicate implements Predicate {
		private final Payload left;
		private final Payload right;

		protected ComparatorPayloadPredicate( final Payload left, final Payload right ) {
			this.left = left == null ? null : left.copy();
			this.right = right == null ? null : right.copy();
		}

		public boolean evaluate( final Object payload ) {
			return ( left == null || left.compareTo( (Payload)payload ) <= 0 ) && ( right == null || right.compareTo( (Payload)payload ) >= 0 ); 
		}
		
		public String toString() {
			return "[" + ( left != null ? left : "\u221e" ) + ".." + ( right != null ? right : "-\u221e" ) + "]";
		}
	}
	
	public ComparatorPayloadPredicate rangeFilter( Payload left, Payload right ) {
		return new ComparatorPayloadPredicate( left, right );
	}
}
