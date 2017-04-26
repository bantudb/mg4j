package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Paolo Boldi and Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.InterpolativeCoding;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.util.Map;

/** Writes a bitstream-based interleaved index.
 * 
 * <p>Indices written by this class are somewhat classical. Each inverted list contains the frequency, followed by gap-encoded pointers optionally
 * interleaved with counts and gap-encoded positions. The compression technique used for each component can be chosen using a {@linkplain CompressionFlags compression flag}.
 * 
 * <p>Interleaved indices of this kind are essentially unusable, as all information in each posting list must be entirely read (no skipping is possible). One
 * possible exception is disjunctive queries which use all the information in the index (e.g., with proximity scoring).
 * Another possible usage is to test the compression power of different codes, as essentially all classical compression 
 * techniques are available. But, most importantly, the {@link Scan} tool
 * generates interleaved indices as batches (albeit not using this class).
 * 
 * <P>These are the files that form an interleaved index:
 * 
 * <dl>
 * <dt><samp><var>basename</var>.properties</samp>
 * 
 * <dd>A Java {@linkplain Properties property file} containing {@linkplain it.unimi.di.big.mg4j.index.Index.PropertyKeys information about the index}.
 * 
 * <dt><samp><var>basename</var>.terms</samp>
 * 
 * <dd>For each indexed term, the corresponding literal string in UTF-8 encoding. More precisely,
 * the <var>i</var>-th line of the file (starting from 0) contains the literal string corresponding
 * to term index <var>i</var>.
 * 
 * <dt><samp><var>basename</var>.frequencies</samp>
 * 
 * <dd>For each term, the number of documents in which the term appears in &gamma; coding. More
 * precisely, <var>i</var>-th integer of the file (starting from 0) is the number of documents in
 * which the term of index <var>i</var> appears. This information appears also at the start
 * of each posting list in the index, but it is also stored in this file for convenience.
 * 
 * <dt><samp><var>basename</var>.sizes</samp> (not generated for payload-based indices)
 * 
 * <dd>For each indexed document, the corresponding size (=number of words) in &gamma; coding. More
 * precisely, <var>i</var>-th integer of the file (starting from 0) is the size in words of the
 * document of index <var>i</var>.
 * 
 * <dt><samp><var>basename</var>.index</samp>
 * 
 * <dd>The inverted index.
 * 
 * <dt><samp><var>basename</var>.offsets</samp>
 * 
 * <dd>For each term, the bit offset in <samp><var>basename</var>.index</samp> at which the
 * inverted lists start. More precisely, the first integer is the offset for term 0 in &gamma;
 * coding, and then the <var>i</var>-th integer is the difference between the <var>i</var>-th and
 * the <var>i</var>&minus;1-th offset in &gamma; coding. If <var>T</var> terms were indexed, this
 * file will contain <var>T</var>+1 integers, the last being the difference (in bits) between the
 * length of the entire inverted index and the offset of the last inverted list.
 * Thus, in practice, the file is formed by the number zero (the offset of the first list) followed by the length in bits of each inverted list.
 * 
 * <dt><samp><var>basename</var>.occurrencies</samp>
 * 
 * <dd>For each term, its <em>occurrency</em>, that is, the number of its occurrences throughout the whole document collection, in
 * &gamma; coding. More precisely, the <var>i</var>-th integer of the file (starting from 0) is the
 * occurrency of the term of index <var>i</var>.
 * 
 * <dt><samp><var>basename</var>.posnumbits</samp>
 * 
 * <dd>For each term, the number of bits spent to store positions in &gamma; code (used just for {@linkplain BitStreamHPIndexWriter quantum-optimisation
 * purposes}).
 * 
 * <dt><samp><var>basename</var>.sumsmaxpos</samp>
 * 
 * <dd>For each term, the sum of the maximum positions in which the term appears (necessary to build a {@link QuasiSuccinctIndex}) in &delta; code.
 * 
 * <dt><samp><var>basename</var>.stats</samp>
 * 
 * <dd>Miscellaneous detailed statistics about the index.
 *
 * </dl> 
 * 
 * @author Paolo Boldi 
 * @author Sebastiano Vigna 
 * @since 0.6
 */


public class BitStreamIndexWriter extends AbstractBitStreamIndexWriter {
	private static final boolean ASSERTS = false;
	
	/** This value of {@link #state} means that we should call {@link #newInvertedList()}.*/
	protected static final int BEFORE_INVERTED_LIST = 0;

	/** This value of {@link #state} means that we are positioned at the start of an inverted list,
	 * and we should call {@link #writeFrequency(long)}.*/
	protected static final int BEFORE_FREQUENCY = 1;

	/** This value of {@link #state} means that we are ready to call {@link #newDocumentRecord()}. */
	protected static final int BEFORE_DOCUMENT_RECORD = 2;

	/** This value of {@link #state} means that we just started a new document record, and we
	 * should call {@link #writeDocumentPointer(OutputBitStream, long)}. */
	protected static final int BEFORE_POINTER = 3;

	/** This value of {@link #state} can be assumed only in indices that contain payloads; it
	 * means that we are positioned just before the payload for the current document record. */
	protected static final int BEFORE_PAYLOAD = 4;

	/** This value of {@link #state} can be assumed only in indices that contain counts; it
	 * means that we are positioned just before the count for the current document record. */
	protected static final int BEFORE_COUNT = 5;

	/** This value of {@link #state} can be assumed only in indices that contain document positions; 
	 * it means that we are positioned just before the position list of the current document record. */
	protected static final int BEFORE_POSITIONS = 6;

	/** This is the first unused state. Subclasses may start from this value to define new states. */
	protected static final int FIRST_UNUSED_STATE = 7;

	/** The underlying {@link OutputBitStream}. */
	protected OutputBitStream obs;
	/** The offsets {@link OutputBitStream}. */
	private OutputBitStream offsets;
	/** The {@link OutputBitStream} for the number of bits for positions. */
	private OutputBitStream posNumBits;
	/** The output bitstream for frequencies (&gamma; coded). */ 
	private OutputBitStream frequencies;
	/** The output bitstream for occurrencies (&gamma; coded). */
	private OutputBitStream occurrencies;
	/** The output bitstream for the sum of maximum positions (&delta; coded). */
	private OutputBitStream sumsMaxPos;
	/** The current state of the writer. */
	protected int state;
	/** The number of document records that the current inverted list will contain. */
	protected long frequency;
	/** The number of document records already written for the current inverted list. */
	protected long writtenDocuments;
	/** The current document pointer. */
	protected long currentDocument;
	/** The last document pointer in the current list. */
	protected long lastDocument;
	/** The position (in bytes) where the last inverted list started. */
	protected long lastInvertedListPos;
	/** The number of bits spent for positions in this the current inverted list. */
	private long currPosNumBits;
	/** The maximum number of positions in a document record so far. */
	public int maxCount;
	/** The occurrency of the current term so far. */
	private long occurrency;
	/** The sum of maximum positions of the current term so far. */
	private long sumMaxPos;
	/** The parameter <code>b</code> for Golomb coding of pointers. */
	protected int b;
	/** The parameter <code>log2b</code> for Golomb coding of pointers; it is the most significant bit of {@link #b}. */
	protected int log2b;

	/** Creates a new index writer with the specified basename. The index will be written on a file (stemmed with <samp>.index</samp>).
	 *  If <code>writeOffsets</code>, also an offset file will be produced (stemmed with <samp>.offsets</samp>). 
	 *  When {@link #close()} will be called, the property file will also be produced (stemmed with <samp>.properties</samp>),
	 *  or enriched if it already exists.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename.
	 * @param numberOfDocuments the number of documents in the collection to be indexed.
	 * @param writeOffsets if <code>true</code>, the offset file will also be produced.
	 * @param flags a flag map setting the coding techniques to be used (see {@link CompressionFlags}).
	 */
	public BitStreamIndexWriter( final IOFactory ioFactory, final CharSequence basename, final long numberOfDocuments, final boolean writeOffsets, final Map<Component,Coding> flags ) throws IOException {
		super( numberOfDocuments, flags );
		this.obs = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.INDEX_EXTENSION ), false );
		this.posNumBits = writeOffsets && flags.get( Component.POSITIONS ) != null ? new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION ), false ) : null;
		this.offsets = writeOffsets ? new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.OFFSETS_EXTENSION ), false ) : null;
		this.frequencies = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.FREQUENCIES_EXTENSION ), false );
		this.occurrencies = hasCounts ? new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.OCCURRENCIES_EXTENSION ), false ) : null;
		this.sumsMaxPos = hasPositions ? new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION ), false ) : null;
		this.frequency = -1;
		this.currentTerm = -1;
		this.maxCount = 0;

		if ( ! hasCounts && hasPositions ) throw new IllegalArgumentException( "Index would have positions but no counts (this can't happen)" );
	}

	public long newInvertedList() throws IOException {
		if ( frequency >= 0 && frequency != writtenDocuments ) throw new IllegalStateException( "The number of document records (" + this.writtenDocuments + ") does not match the frequency (" + this.frequency + ")" );
		if ( state != BEFORE_INVERTED_LIST && state != BEFORE_DOCUMENT_RECORD ) throw new IllegalStateException( "Trying to start new inverted list in state " + state );

		if ( currentTerm != -1 ) {
			if ( hasCounts ) occurrencies.writeLongGamma( occurrency );
			if ( hasPositions ) sumsMaxPos.writeLongDelta( sumMaxPos );
			if ( posNumBits != null ) posNumBits.writeLongGamma( currPosNumBits );
			occurrency = 0;
			sumMaxPos = 0;
			currPosNumBits = 0;
		}
		// The position (in bits) where the new inverted list starts
		long pos = obs.writtenBits();
		// Reset variables
		writtenDocuments = 0;
		currentTerm++;
		currentDocument = -1;

		// If needed, write the offset
		if ( offsets != null ) offsets.writeLongGamma( pos - lastInvertedListPos );
		lastInvertedListPos = pos;

		state = BEFORE_FREQUENCY;
		return pos;
	}

	public void writeFrequency( final long frequency ) throws IOException {
		if ( state != BEFORE_FREQUENCY ) throw new IllegalStateException( "Trying to write frequency in state " + state );

		int bitCount;
		// Write the frequency
		switch( frequencyCoding ) {
		case SHIFTED_GAMMA:
			bitCount = obs.writeLongShiftedGamma( frequency - 1 ); // frequency cannot be 0
			break;
		case GAMMA:
			bitCount = obs.writeLongGamma( frequency - 1 ); // frequency cannot be 0
			break;
		case DELTA:
			bitCount = obs.writeLongDelta( frequency - 1 ); // frequency cannot be 0
			break;
		default:
			throw new IllegalStateException( "The required frequency coding (" + frequencyCoding + ") is not supported." );
		}

		frequencies.writeLongGamma( frequency );
		this.frequency = frequency;

		// We compute the modulus used for pointer Golomb coding 
		if ( pointerCoding == Coding.GOLOMB ) {
			b = BitStreamIndex.golombModulus( frequency, numberOfDocuments ); 
			log2b = Fast.mostSignificantBit( b );
		}

		state = BEFORE_DOCUMENT_RECORD;
		bitsForFrequencies += bitCount;
	}

	public OutputBitStream newDocumentRecord() throws IOException {
		if ( frequency == writtenDocuments ) throw new IllegalStateException( "Document record overflow (written " + this.frequency + " already)" );
		if ( state != BEFORE_DOCUMENT_RECORD ) throw new IllegalStateException( "Trying to start new document record in state " + state );

		writtenDocuments++;
		numberOfPostings++;
		lastDocument = currentDocument;
		state = BEFORE_POINTER;
		return obs;
	}

	public void writeDocumentPointer( final OutputBitStream out, final long pointer ) throws IOException {
		if ( state != BEFORE_POINTER ) throw new IllegalStateException( "Trying to write pointer in state " + state );

		currentDocument = pointer;
		long bitCount = 0;

		if ( frequency != numberOfDocuments ) { // We do not write pointers for everywhere occurring documents.
			switch( pointerCoding ) {
				case SHIFTED_GAMMA:
					bitCount = out.writeLongShiftedGamma( pointer - lastDocument - 1 );
					break;
				case UNARY:
					bitCount = out.writeLongUnary( pointer - lastDocument - 1 );
					break;
				case GAMMA:
					bitCount = out.writeLongGamma( pointer - lastDocument - 1 );
					break;
				case DELTA:
					bitCount = out.writeLongDelta( pointer - lastDocument - 1 );
					break;
				case GOLOMB:
					bitCount = out.writeLongGolomb( pointer - lastDocument - 1, b, log2b );
					break;
				default:
					throw new IllegalStateException( "The required pointer coding (" + pointerCoding + ") is not supported." );
			}
		}
		else if ( pointer - lastDocument != 1 ) throw new IllegalStateException( "Term " + currentTerm + " has frequency equal to the number of documents, but pointers are not consecutive integers" );

		state = hasPayloads ? BEFORE_PAYLOAD : hasCounts ? BEFORE_COUNT : BEFORE_DOCUMENT_RECORD;
		bitsForPointers += bitCount;
	}

	public void writePayload( final OutputBitStream out, final Payload payload ) throws IOException {
		if ( frequency < 0 ) throw new IllegalStateException( "Trying to write payload without calling newInvertedList" );
		if ( state != BEFORE_PAYLOAD ) throw new IllegalStateException( "Trying to write payload in state " + state );
		final int count = payload.write( out );
		bitsForPayloads += count;
		state = hasCounts ? BEFORE_COUNT : BEFORE_DOCUMENT_RECORD;
	}


	public void close() throws IOException {
		if ( state != BEFORE_DOCUMENT_RECORD && state != BEFORE_INVERTED_LIST ) throw new IllegalStateException( "Trying to close index in state " + state );
		if ( frequency >= 0 && frequency != writtenDocuments ) throw new IllegalStateException( "The number of document records (" + this.writtenDocuments + ") does not match the frequency (" + this.frequency + ")" );

		if ( writtenBits() != obs.writtenBits() ) 
			throw new IllegalStateException( "Written bits count mismatch: we say " + writtenBits() + ", the stream says " + obs.writtenBits() );

		if ( currentTerm != -1 ) {
			if ( hasCounts ) occurrencies.writeLongGamma( occurrency );
			if ( hasPositions ) sumsMaxPos.writeLongDelta( sumMaxPos );
		}

		if ( offsets != null ) {
			offsets.writeLongGamma( obs.writtenBits() - lastInvertedListPos );
			offsets.close();
		}
		
		if ( posNumBits != null ) {
			if ( currentTerm != -1 ) posNumBits.writeLongGamma( currPosNumBits );
			posNumBits.close();
		}

		frequencies.close();
		if ( hasCounts ) occurrencies.close();
		if ( hasPositions ) sumsMaxPos.close();
		obs.close();
	}
	

	public void writePositionCount( final OutputBitStream out, final int count ) throws IOException {
		if ( frequency < 0 ) throw new IllegalStateException( "Trying to write count without calling newInvertedList" );
		if ( state != BEFORE_COUNT ) throw new IllegalStateException( "Trying to write count in state " + state );
		final int bitCount;

		numberOfOccurrences += count;
		occurrency += count;
		
		switch( countCoding ) {
			case SHIFTED_GAMMA:
				bitCount = out.writeShiftedGamma( count - 1 );
				break;
			case GAMMA:
				bitCount = out.writeGamma( count - 1 );
				break;
			case UNARY:
				bitCount = out.writeUnary( count - 1 );
				break;
			case DELTA:
				bitCount = out.writeDelta( count - 1 );
				break;
			default:
				throw new IllegalStateException( "The required count coding (" + countCoding + ") is not supported." );
		}
		
		state = hasPositions ? BEFORE_POSITIONS : BEFORE_DOCUMENT_RECORD;
		bitsForCounts += bitCount;
	}

	public void writeDocumentPositions( final OutputBitStream out, final int[] position, final int offset, final int count, final int docSize ) throws IOException {
		if ( frequency < 0 ) throw new IllegalStateException( "Trying to write occurrences without calling newInvertedList" );
		if ( state != BEFORE_POSITIONS ) throw new IllegalStateException( "Trying to write positions in state " + state );

		if ( ASSERTS ) if ( docSize > 0 ) for( int i = 0; i< count; i++ ) assert position[ offset + i ] < docSize : "Position " + position[ offset + i ] + " for document " + currentDocument + " is too large; size is " + docSize;
		
		int i;
		int prev = -1;
		int bitCount = 0;
		final int end = offset + count;

		switch( positionCoding ) {
			case GAMMA:
				for( i = offset; i < end; i++ ) {
					bitCount += out.writeGamma( position[ i ] - prev - 1 );
					prev = position[ i ];
				}
				break;
			case DELTA:
				for( i = offset; i < end; i++ ) {
					bitCount += out.writeDelta( position[ i ] - prev - 1 );
					prev = position[ i ];
				}
				break;
			case SHIFTED_GAMMA:
				for( i = offset; i < end; i++ ) {
					bitCount += out.writeShiftedGamma( position[ i ] - prev - 1 );
					prev = position[ i ];
				}
				break;
			case GOLOMB:
				if ( count < 3 ) {
					for( i = 0; i < count; i++ ) bitCount += out.writeMinimalBinary( position[ i ], docSize );
					break;
				}

				// We compute b and log2b for positions
				final int positionB = BitStreamIndex.golombModulus( count, docSize );
				final int positionLog2b = Fast.mostSignificantBit( positionB );

				for( i = offset; i < end; i++ ) {
					bitCount += out.writeGolomb( position[ i ] - prev - 1, positionB, positionLog2b );
					prev = position[ i ];
				}
				break;
			case INTERPOLATIVE:
				bitCount = InterpolativeCoding.write( out, position, 0, count, 0, docSize - 1 );
				break;
			default:
				throw new IllegalStateException( "The required position coding (" + positionCoding + ") is not supported." );
		}

		state = BEFORE_DOCUMENT_RECORD;
		bitsForPositions += bitCount;
		currPosNumBits += bitCount;
		sumMaxPos += position[ offset + count - 1 ];
		if ( count > maxCount ) maxCount = count;
	}

	public long writtenBits() {
		return bitsForFrequencies + bitsForPointers + bitsForPayloads + bitsForCounts + bitsForPositions;
	}

	public Properties properties() {
		Properties result = new Properties();
		result.setProperty( Index.PropertyKeys.DOCUMENTS, numberOfDocuments );
		result.setProperty( Index.PropertyKeys.TERMS, currentTerm + 1 );
		result.setProperty( Index.PropertyKeys.POSTINGS, numberOfPostings );
		result.setProperty( Index.PropertyKeys.MAXCOUNT, maxCount );
		result.setProperty( Index.PropertyKeys.INDEXCLASS, FileIndex.class.getName() );
		// We save all flags, except for the PAYLOAD component, which is just used internally.
		for( Map.Entry<Component,Coding> e: flags.entrySet() )
			if ( e.getKey() != Component.PAYLOADS ) result.addProperty( Index.PropertyKeys.CODING, new MutableString().append( e.getKey() ).append( ':' ).append( e.getValue() ) );
		return result;
	}
}
