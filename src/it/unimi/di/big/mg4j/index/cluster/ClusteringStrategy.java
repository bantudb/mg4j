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

import java.io.Serializable;

/** A common ancestor interface for all clustering strategies.
 * 
 * <p>Clustering strategies are dual to 
 * {@linkplain it.unimi.di.big.mg4j.index.cluster.PartitioningStrategy partitioning strategies}.
 * After partitioning an index, you can see the set of its parts as local indices
 * of a cluster. Sometimes a partitioning strategy is also a clustering strategy
 * (see, e.g., {@link ContiguousDocumentalStrategy}
 * and {@link ContiguousLexicalStrategy}), but
 * sometimes a strategy serves just 
 * one purpose (see, e.g., {@link ChainedLexicalClusteringStrategy}).
 * 
 * <p>Each local index is defined by an integer starting from 0 up to
 * {@link #numberOfLocalIndices()} &minus; 1. 
 * 
 * @author Sebastiano Vigna
 */


public interface ClusteringStrategy extends Serializable {
	
	/** Returns the number of local indices handled by this strategy. 
	 * @return the number of local indices handled by this strategy.
	 */
	int numberOfLocalIndices();
}
