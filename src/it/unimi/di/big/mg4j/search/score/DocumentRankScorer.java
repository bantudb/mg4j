package it.unimi.di.big.mg4j.search.score;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2004-2016 Paolo Boldi
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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.lang.ObjectParser;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/** Compute scores that do not depend on intervals, but that
 *  just assign a fixed score to each document; scores are read
 *  from a file whose name is passed to the constructor.
 */
public class DocumentRankScorer extends AbstractScorer implements DelegatingScorer {
	/** The big array of scores. */
	private double[][] score;
	
	/** Builds a document scorer by reading the ranks from a file.
	 *  Ranks are saved as doubles (the first double is the rank of document 0
	 *  and so on).
	 * 
	 * <p>This constructor can be used with an {@link ObjectParser}.
	 * 
	 * @param filename the name of the rank file.
	 */
	public DocumentRankScorer( final String filename ) throws IOException {
		this( BinIO.loadDoublesBig( filename ) );
	}

	/** Builds a document scorer by reading the ranks from a file of specified type.
	 * 
	 * <p>This constructor has the same arguments as {@link DocumentRankScorer#DocumentRankScorer(String, String, boolean)},
	 * but it can be used with an {@link ObjectParser}.
	 * 
	 *  @param filename the name of the rank file.
	 *  @param type one of <samp>int</samp>, <samp>long</samp>, <samp>float</samp> or <samp>double</samp>.
	 *  @param gzip a boolean specifying whether the file is gzip'd.
	 *  @see #DocumentRankScorer(String)
	 */
	public DocumentRankScorer( final String filename, final String type, String gzip ) throws IOException {
		this( filename, type, Boolean.parseBoolean( gzip ) );
	}

	/** Builds a document scorer by reading the ranks from a file of specified type.
	 * 
	 * <p>This constructor can be used with an {@link ObjectParser}.
	 * 
	 *  @param filename the name of the rank file.
	 *  @param type one of <samp>int</samp>, <samp>long</samp>, <samp>float</samp> or <samp>double</samp>.
	 *  @see #DocumentRankScorer(String)
	 */
	public DocumentRankScorer( final String filename, final String type ) throws IOException {
		this( filename, type, false );
	}

	/** Builds a document scorer by reading the ranks from a file of specified type.
	 * 
	 *  @param filename the name of the rank file.
	 *  @param type one of <samp>int</samp>, <samp>long</samp>, <samp>float</samp> or <samp>double</samp>.
	 *  @param gzip whether the file is gzip'd.
	 *  @see #DocumentRankScorer(String)
	 */
	@SuppressWarnings("resource")
	public DocumentRankScorer( final String filename, final String type, final boolean gzip ) throws IOException {
		final File file = new File( filename );
		final int size = type.equals( "int" ) || type.equals( "float" ) ? 4 : 8;
		final int n = (int)( file.length() / size );
		InputStream is = new FileInputStream( file );
		if ( gzip ) is = new GZIPInputStream( is );
		final DataInputStream dis = new DataInputStream( new FastBufferedInputStream( is ) );
		if ( gzip ) {
			final DoubleBigArrayBigList score = new DoubleBigArrayBigList( 2 * n ); // Very wild approximation
			try {
				if ( type.equals( "int" ) ) for(;;) score.add( dis.readInt() );
				else if ( type.equals( "long" ) ) for(;;) score.add( dis.readLong() );
				else if ( type.equals( "float" ) ) for(;;) score.add( dis.readFloat() );
				else if ( type.equals( "double" ) ) for(;;) score.add( dis.readDouble() );
				else throw new IllegalArgumentException( "Unknown type \"" + type + "\"" );
			}
			catch( EOFException e ) {}
			this.score = DoubleBigArrays.newBigArray( score.size64() );
			DoubleBigArrays.copy( score.elements(), 0, this.score, 0, score.size64() ); 
		}
		else {
			score = DoubleBigArrays.newBigArray( n );
			if ( type.equals( "int" ) ) for( int i = 0; i < n; i++ ) DoubleBigArrays.set( score, i, dis.readInt() );
			else if ( type.equals( "long" ) ) for( int i = 0; i < n; i++ ) DoubleBigArrays.set( score, i, dis.readLong() );
			else if ( type.equals( "float" ) ) for( int i = 0; i < n; i++ ) DoubleBigArrays.set( score, i, dis.readFloat() );
			else if ( type.equals( "double" ) ) for( int i = 0; i < n; i++ ) DoubleBigArrays.set( score, i, dis.readDouble() );
			else throw new IllegalArgumentException( "Unknown type \"" + type + "\"" );
		}
		dis.close();
	}

	/** Builds a document scorer with given scores.
	 * 
	 *  @param score the scores (they are not copied, so the caller is supposed
	 *   not to change their values).
	 */
	public DocumentRankScorer( final double[][] score ) {
		this.score = score;
	}
	
	public DocumentRankScorer copy() {
		return new DocumentRankScorer( score );
	}

	public double score() {
		return DoubleBigArrays.get( score, documentIterator.document() );
	}

	public double score( final Index index ) {
		throw new UnsupportedOperationException();
	}

	public String toString() {
		return "DocumentRank";
	}

	public boolean usesIntervals() {
		return false;
	}
}
