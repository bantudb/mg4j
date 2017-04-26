package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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


import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.dsi.Util;

import java.io.PrintStream;
import java.util.Map;

/** An abstract bitstream-based index writer, providing common variables and a basic {@link #printStats(PrintStream)} implementation.
 * 
 * <H2>Compression flags</H2>
 *
 * <P>Implementing subclasses need to know the compression method that they should use
 * to write frequencies, pointers, payloads, counts and positions (and whether to write any of them).
 * This information is passed to the {@linkplain #AbstractBitStreamIndexWriter(long, Map) constructor}
 * using a suitable <em>flag map</em> (see {@link CompressionFlags}).
 * 
 * @author Sebastiano Vigna
 * @since 1.2
 */

public abstract class AbstractBitStreamIndexWriter implements IndexWriter {

	/** The number of documents of the collection to be indexed. */
	protected final long numberOfDocuments;
	/** The flag map. */
	public Map<Component,Coding> flags;
	/** The coding for frequencies. */
	protected Coding frequencyCoding;
	/** The coding for pointers. */
	protected Coding pointerCoding;
	/** The coding for counts. */
	protected Coding countCoding;
	/** The coding for positions. */
	protected Coding positionCoding;
	/** Whether this index contains payloads. */
	protected final boolean hasPayloads;
	/** Whether this index contains counts. */
	protected final boolean hasCounts;
	/** Whether this index contains positions. */
	protected final boolean hasPositions;
	
	/** The number of indexed postings (pairs term/document). */
	protected long numberOfPostings;
	/** The number of indexed occurrences. */
	protected long numberOfOccurrences;
	/** The current term. */
	protected long currentTerm;
	/** The number of bits written for frequencies. */
	public long bitsForFrequencies;
	/** The number of bits written for document pointers. */
	public long bitsForPointers;
	/** The number of bits written for counts. */
	public long bitsForCounts;
	/** The number of bits written for payloads. */
	public long bitsForPayloads;
	/** The number of bits written for positions. */
	public long bitsForPositions;

	public AbstractBitStreamIndexWriter( final long numberOfDocuments, final Map<Component,Coding> flags ) {
		this.numberOfDocuments = numberOfDocuments;
		this.flags = flags;
		frequencyCoding = flags.get( Component.FREQUENCIES );
		pointerCoding = flags.get( Component.POINTERS );
		countCoding = flags.get( Component.COUNTS );
		positionCoding = flags.get( Component.POSITIONS );
		
		hasPayloads = flags.containsKey(  Component.PAYLOADS ); 
		hasCounts = countCoding != null;
		hasPositions = positionCoding != null;
	}

	public void printStats( PrintStream stats ) {
		stats.println( "Number of documents: " + Util.format( numberOfDocuments ) );
		stats.println( "Number of terms: " + Util.format( currentTerm + 1 ) );
		
		stats.println( "Frequencies: " + Util.format( bitsForFrequencies ) + " bits, " + Util.format( bitsForFrequencies / ( currentTerm + 1.0 ) ) + " bits/frequency." );
		stats.println( "Document pointers: " + Util.format( numberOfPostings ) + " (" + Util.format( bitsForPointers ) + " bits, " + Util.format( bitsForPointers / (double)numberOfPostings ) + " bits/pointer).");

		if ( hasCounts ) stats.println( "Counts: " + Util.format( numberOfPostings ) + " (" + Util.format( bitsForCounts ) + " bits, " + Util.format( bitsForCounts/ (double)numberOfPostings ) + " bits/count).");
		if ( hasPositions ) stats.println( "Occurrences: " + Util.format( numberOfOccurrences ) + " (" +  Util.format( bitsForPositions ) + " bits, " + Util.format( bitsForPositions / (double)numberOfOccurrences ) + " bits/occurrence).");
		if ( hasPayloads ) stats.println( "Payloads: " + Util.format( numberOfPostings )  + " (" + Util.format( bitsForPayloads ) + " bits, " + Util.format( bitsForPayloads / (double)numberOfPostings )  + " bits/payload)." );
		if ( hasPositions ) stats.println( "Total: " + Util.format( writtenBits() ) + " bits, " + Util.format( writtenBits() / (double)numberOfOccurrences ) + " bits/occurrence" );
		else stats.println( "Total: " + Util.format( writtenBits() ) + " bits, " + Util.format( writtenBits() / (double)numberOfPostings ) + " bits/posting" );
	}
}
