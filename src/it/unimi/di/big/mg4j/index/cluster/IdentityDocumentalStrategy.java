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

/** A documental strategy that maps identically local to global pointers and viceversa.
 * 
 * <p>When clustering <i>a posteriori</i> a set of previously built renumbered indices 
 * using a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster}, 
 * local document pointers are actually global document pointers. 
 * In this case, an instance of this class should be used as a strategy
 * for the cluster.
 * 
 * @author Sebastiano Vigna
 */

public class IdentityDocumentalStrategy implements DocumentalClusteringStrategy, Serializable {

	private static final long serialVersionUID = 0L;

	/** The number of local indices. */
	private final int numberOfLocalIndices;
	/** The number of documents. */
	private final long numberOfDocuments;

	/** Creates a new identity documental clustering strategy.
	 * 
	 * @param numberOfLocalIndices the number of local indices.
	 * @param numberOfDocuments the number of documents.
	 */
	
	public IdentityDocumentalStrategy( final int numberOfLocalIndices, final long numberOfDocuments ) {
		this.numberOfLocalIndices = numberOfLocalIndices;
		this.numberOfDocuments = numberOfDocuments;
	}	
	
	public int numberOfLocalIndices() {
		return numberOfLocalIndices;
	}

	public long localPointer( final long globalPointer ) {
		return globalPointer;
	}

	public long globalPointer( final int localIndex, final long localPointer ) {
		return localPointer;		
	}

	public long numberOfDocuments( final int localIndex ) {
		return numberOfDocuments;
	}
	
	public String toString() {
		return this.getClass().getName() + "[local indices: " + numberOfLocalIndices + " documents: " + numberOfDocuments + "]";
	}
}
