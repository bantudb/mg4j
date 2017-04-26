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
import java.util.Arrays;

/** A documental partitioning and clustering strategy that partitions an index into contiguous segments.
 * 
 * <p>To partition an index in contiguous parts, you must provide an array
 * of <em>cutpoints</em>, which define each part. More precisely, given
 * cutpoints <var>c</var><sub>0</sub>,<var>c</var><sub>2</sub>,&hellip;,<var>c</var><sub><var>k</var></sub>,
 * the global index will be partitioned into <var>k</var> local indices containing the documents
 * from <var>c</var><sub>0</sub> (included) to <var>c</var><sub>1</sub> (excluded), from
 * <var>c</var><sub>1</sub> (included) to <var>c</var><sub>2</sub> and so on. Note that
 * necessarily <var>c</var><sub>0</sub>=0 and <var>c</var><sub><var>k</var></sub>=<var>N</var>,
 * where <var>N</var> is the number of globally indexed documents.
 *
 * <p>The {@link #properties()} method provides two properties, <samp>pointerfrom</samp> and <samp>pointerto</samp>,
 * that contain the first (included) and last (excluded) pointer in the local index.
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */

public class ContiguousDocumentalStrategy implements DocumentalPartitioningStrategy, DocumentalClusteringStrategy,Serializable{

	private static final long serialVersionUID = 0L;

	/** The cutpoints.*/
	private final long[] cutPoint;
	/** The (cached) number of segments. */
	private final int k;

	/** Creates a new contiguous strategy with the given cutpoints.
	 * 
	 * <P>Note that {@link DocumentalStrategies} has ready-made factory
	 * methods for the common cases.
	 * 
	 * @param cutPoint an array of cutpoints (see the class description}.
	 */
	
	public ContiguousDocumentalStrategy( final long... cutPoint ) {
		if ( cutPoint.length == 0 ) throw new IllegalArgumentException( "Empty cutpoint array" );
		if ( cutPoint[ 0 ] != 0 ) throw new IllegalArgumentException( "The first cutpoint must be 0" );
		this.cutPoint = cutPoint;
		this.k = cutPoint.length - 1;
	}	
	
	public int numberOfLocalIndices() {
		return k;
	}

	public int localIndex( final long globalPointer ) {
		if ( globalPointer >= cutPoint[ k ] ) throw new IndexOutOfBoundsException( Long.toString( globalPointer ) );
		for ( int i = k; i-- != 0; ) if ( globalPointer >= cutPoint[ i ] ) return i;
		throw new IndexOutOfBoundsException( Long.toString( globalPointer ) );
	}

	public long localPointer( final long globalPointer ) {
		return globalPointer - cutPoint[ localIndex( globalPointer ) ];
	}

	public long globalPointer( final int localIndex, final long localPointer ) {
		return localPointer + cutPoint[ localIndex ];		
	}

	public long numberOfDocuments( final int localIndex ) {
		return cutPoint[ localIndex + 1 ] - cutPoint[ localIndex ];
	}
	
	public Properties[] properties() {
		Properties[] properties = new Properties[ k ];
		for( int i = 0; i < k; i++ ) {
			properties[ i ] = new Properties();
			properties[ i ].addProperty( "pointerfrom", cutPoint[ i ] );	
			properties[ i ].addProperty( "pointerto", cutPoint[ i + 1 ] );	
		}
		return properties;
	}

	public String toString() {
		return Arrays.toString( cutPoint );
	}
}
