package it.unimi.di.big.mg4j.index.cluster;

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

/** A way to associate a term with a local index out of a given set.
 * 
 * <p>A {@link LexicalCluster} needs a way to
 * retrieve, given a term, the corresponding {@linkplain #localIndex(CharSequence) local index}.
 * 
 * @see it.unimi.di.big.mg4j.index.cluster.LexicalPartitioningStrategy
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */


public interface LexicalClusteringStrategy extends ClusteringStrategy {

	/** Returns the index to which a given term is be mapped by this strategy.
	 * 
	 * @param term a term.
	 * @return the corresponding local index, or -1 if no index contains the term.
	 */
	int localIndex( CharSequence term );

	/** Returns the global term number given a local index and a local term number (optional operation).
	 * 
	 * <p>This operation is not, in general, necessary for a {@link LexicalCluster}
	 * to work, as no action on a local index returns local numbers. It is defined here
	 * mainly for completeness and for debugging purposes (in case it is implemented). 
	 * 
	 * @param localIndex the local index.
	 * @param localNumber the local term number.
	 * @return the global term number.
	 */
	long globalNumber( final int localIndex, final long localNumber );
}
