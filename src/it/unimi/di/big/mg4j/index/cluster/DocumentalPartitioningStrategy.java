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

/** A way to associate a document with a local index out of a given set and a local document number in the local index.
 * 
 * <p>When partitioning documentally an index, we need a way to associate
 * to each global document pointer {@linkplain #localIndex(long) a local index} (the index
 * that will contain the postings of that document) and {@linkplain #localPointer(long) a local
 * document pointer} (the document pointer actually represented in the local index).
 * 
 * <p>Usually, a documental partitioning strategy has a matching
 * {@link it.unimi.di.big.mg4j.index.cluster.DocumentalClusteringStrategy}
 * whose methods satisfy the following equations:
 * <pre style="margin: 1em 0; text-align:center">
 * globalPointer(localIndex(d), localPointer(d)) = d
 * &lt;localIndex(globalPointer(i, l)), localPointer(globalPointer(i, l))> = &lt;i, l>
 * </pre> 
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public interface DocumentalPartitioningStrategy extends PartitioningStrategy {

	/** Returns the index to which a given global document pointer is be mapped by this strategy.
	 * 
	 * @param globalPointer a global document pointer.
	 * @return the corresponding local index.
	 */
	int localIndex( final long globalPointer );

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
