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

import it.unimi.dsi.util.Properties;

import java.io.Serializable;

/** A common ancestor interface for all partitioning strategies.
 * 
 * <p>When partitioning an index, there are a few pieces of data that
 * must be provided independently of the particular strategy chosen. This
 * interface embodies methods to access them.
 * 
 * <p>Each local index is defined by an integer starting from 0 up to
 * {@link #numberOfLocalIndices()} &minus; 1. Each local index is also associated with a 
 * set of {@link #properties()} that is usually merged with the property file
 * of the local index.
 * 
 * @see it.unimi.di.big.mg4j.index.cluster.ClusteringStrategy
 * @author Sebastiano Vigna
 */


public interface PartitioningStrategy extends Serializable {
	
	/** Returns the number of local indices created by this strategy. 
	 * @return the number of local indices created by this strategy.
	 */
	int numberOfLocalIndices();

	/** Returns an array of properties, one for each local index, that specify additional information about local indices.
	 * 
	 * @return an array of properties, one for each local index; any element
	 * can be <code>null</code> (in that case, the set of properties is assumed to be empty). 
	 */
	Properties[] properties();
}
