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

/** A way to associate a term number with a local index out of a given set and a local term number in the local index.
 * 
 * <p>When partitioning lexically an index (i.e., termwise), we need a way to associate
 * to each term {@linkplain #localIndex(long) a local index} (the index
 * that will contain the postings of that term) and {@linkplain #localNumber(long) a local
 * term number} (the number of the term in the local index).
 * 
 * <p>Usually, a lexical partitioning strategy has a matching 
 * {@link it.unimi.di.big.mg4j.index.cluster.LexicalClusteringStrategy} whose methods
 * satisfy the following equations:
 * <pre style="margin: 1em 0; text-align:center">
 * globalNumber(localIndex(t), localNumber(t)) = t
 * &lt;localIndex(globalNumber(i, l)), localNumber(globalNumber(i, l))> = &lt;i, l>
 * </pre> 
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */


public interface LexicalPartitioningStrategy extends PartitioningStrategy {
	/** Returns the index to which a given term number is mapped by this strategy.
	 * 
	 * @param globalNumber the term global number.
	 * @return the corresponding local index, or -1 if the term should be removed from the partitioned index.
	 */
	int localIndex( long globalNumber );

	/** Returns the local term number given a global term number.
	 * @param globalNumber a global term number.
	 * @return the corresponding local term number, or -1 if the term should be removed from the partitioned index.
	 */
	long localNumber( final long globalNumber );
}
