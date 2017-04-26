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

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Properties;

import java.util.Arrays;
import java.util.NoSuchElementException;


/** A lexical strategy that partitions terms into contiguous segments.
 * 
 * <p>To partition terms in contiguous segments, you must provide an array
 * of <em>cutpoints</em>, which define each segment of terms. More precisely, given
 * cutpoints <var>c</var><sub>0</sub>,<var>c</var><sub>2</sub>,&hellip;,<var>c</var><sub><var>k</var></sub>,
 * terms will be partitioned into <var>k</var> local indices containing the terms numbered
 * from <var>c</var><sub>0</sub> (included) to <var>c</var><sub>1</sub> (excluded), from
 * <var>c</var><sub>1</sub> (included) to <var>c</var><sub>2</sub> and so on. Note that
 * necessarily <var>c</var><sub>0</sub>=0 and <var>c</var><sub><var>k</var></sub>=<var>T</var>,
 * where <var>T</var> is the number of terms in the global index.
 *
 * <p>To make mapping of terms (as opposed to term numbers) possible, you must also provide
 * a parallel array of {@linkplain java.lang.CharSequence character sequences} that
 * contain the terms corresponding to the terms numbers
 * <var>c</var><sub>0</sub>,<var>c</var><sub>2</sub>,&hellip;,<var>c</var><sub><var>k</var></sub>.
 * The content of the last element of the array should be <code>null</code>. 
 *
 * <p>The {@link #properties()} method provides two properties, <samp>termfrom</samp> and <samp>termto</samp>,
 * that contain the first (included) and last (excluded) term in the local index, and two 
 * analogous properties <samp>termnumberfrom</samp> and <samp>termnumberto</samp>.
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 */


public class ContiguousLexicalStrategy implements LexicalPartitioningStrategy, LexicalClusteringStrategy {
	static final long serialVersionUID = 0;
	/** The cutpoints.*/
	private final int[] cutPoint;
	/** The cutpoint terms.*/
	private final MutableString[] cutPointTerm;
	/** The (cached) number of segments. */
	private final int k;

	
	/** Creates a new contiguous lexical strategy with the given cutpoints.
	 * @param cutPoint an array of cutpoints (see the class description}.
	 * @param cutPointTerm a parallel array of cutpoints terms; the last element must be <code>null</code>.
	 */
	public ContiguousLexicalStrategy( final int[] cutPoint, final CharSequence[] cutPointTerm ) {
		if ( cutPoint.length == 0 ) throw new IllegalArgumentException( "Empty cutpoint array" );
		if ( cutPoint.length != cutPointTerm.length ) throw new IllegalArgumentException( "The cutpoint array and the term cutpoint array have different lengths (" + cutPoint.length + ", " + cutPointTerm.length + ")" );
		if ( cutPoint[ 0 ] != 0 ) throw new IllegalArgumentException( "The first cutpoint must be 0" );
		this.cutPoint = cutPoint;
		// Defensive copy
		this.k = cutPoint.length - 1;
		this.cutPointTerm = new MutableString[ k + 1 ];
		for( int i = 0; i < k; i++ ) this.cutPointTerm[ i ] = new MutableString( cutPointTerm[ i ] );
		this.cutPointTerm[ k ] = new MutableString( "\uFFFF" );
	}

	public int numberOfLocalIndices() {
		return k;
	}

	public int localIndex( final long globalNumber ) {
		if ( globalNumber >= cutPoint[ k ] ) throw new IndexOutOfBoundsException( Long.toString( globalNumber ) );
		for ( int i = k; i-- != 0; ) if ( cutPoint[ i ] <= globalNumber ) return i;
		throw new IndexOutOfBoundsException( Long.toString( globalNumber ) );
	}

	public int localIndex( final CharSequence term ) {
		for ( int i = k; i-- != 0; ) if ( cutPointTerm[ i ].compareTo( term ) <= 0 ) return i;
		throw new NoSuchElementException( term.toString() );
	}

	public long globalNumber( final int localIndex, final long localNumber ) {
		return localNumber + cutPoint[ localIndex ];
	}

	public long localNumber( final long globalNumber ) {
		return globalNumber - cutPoint[ localIndex( globalNumber ) ];
	}

	public Properties[] properties() {
		Properties[] properties = new Properties[ k ];
		for( int i = 0; i < k; i++ ) {
			properties[ i ] = new Properties();
			properties[ i ].addProperty( "termfrom", cutPointTerm[ i ] );	
			properties[ i ].addProperty( "termto", cutPointTerm[ i + 1 ] );	
			properties[ i ].addProperty( "termnumberfrom", cutPoint[ i ] );	
			properties[ i ].addProperty( "termnumberto", cutPoint[ i + 1 ] );	
		}
		return properties;
	}

	public String toString() {
		return "{ " + Arrays.toString( cutPoint ) + ", " + Arrays.toString( cutPointTerm ) + " }";
	}
}
