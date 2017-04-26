package it.unimi.di.big.mg4j.search.score;

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
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

/** A marker interface for those scorers that delegate all  
 * {@link it.unimi.dsi.fastutil.ints.IntIterator}'s method to the
 * underlying {@link it.unimi.di.big.mg4j.search.DocumentIterator DocumentIterator}.
 * 
 * <p>An {@linkplain it.unimi.di.big.mg4j.search.score.AbstractAggregator aggregator}
 * can only aggregate scorers of this kind.  
 */

public interface DelegatingScorer extends Scorer {
}
