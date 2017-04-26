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

/** A way to associate (quite bidirectionally) local and global document pointers.
 * 
 * <p>A {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster} needs a way to turn
 * {@linkplain #globalPointer(int, long) local document pointer into global pointers}, 
 * but also {@linkplain #localPointer(long) global pointers into local pointers} (for skipping).
 * 
 * @see it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public interface DocumentalClusteringStrategy extends ClusteringStrategy {

	/** Returns the global document pointer given a local index and a local document pointer. 
	 * 
	 * @param localIndex the local index.
	 * @param localPointer the local document pointer in <code>localIndex</code>.
	 * @return the global document pointer.
	 */
	long globalPointer( int localIndex, long localPointer );

	/** Returns the local document pointer corresponding to a global document pointer.
	 * 
	 * @param globalPointer a global document pointer.
	 * @return the corresponding local document pointer.
	 */
	long localPointer( long globalPointer );

	/** Returns the number of documents that will be assigned to the given local index. 
	 * 
	 * @param localIndex the local index.
	 * @return the number of documents that will be assigned to <code>localIndex</code>.
	 */
	long numberOfDocuments( int localIndex );
}
