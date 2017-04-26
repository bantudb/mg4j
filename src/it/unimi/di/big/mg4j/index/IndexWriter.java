package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.PrintStream;


/** An interface for classes that generate indices.
 *  
 * <P>Implementations of this interface are used to write inverted lists in
 * sequential order, as follows:
 * <UL>
 *  <li>to create a new inverted list, you must call {@link #newInvertedList()};
 *  <li>then, you must specified the frequency using {@link #writeFrequency(long)}; 
 *  <li>the document records follow; before writing a new document record, you must call {@link #newDocumentRecord()}; 
 *   note that, all in all, the number of calls to {@link #newDocumentRecord()} must be equal to the frequency;
 *  <li>for each document record, you must supply the information needed for the index you are building 
 *  ({@linkplain #writeDocumentPointer(OutputBitStream, long) pointer}, 
 *  {@linkplain #writePayload(OutputBitStream, Payload) payload}, 
 *  {@linkplain #writePositionCount(OutputBitStream, int) count}, and 
 *  {@linkplain #writeDocumentPositions(OutputBitStream, int[], int, int, int) positions}, in this order).
 * </UL>
 * 
 * <p>{@link #newDocumentRecord()} returns an {@link OutputBitStream} that must be used to write the document-record data. 
 * Note that there is no guarantee that the returned {@link OutputBitStream} coincides with the 
 * underlying bit stream, or that is even <code>null</code>. Moreover, there is no guarantee as to when the bits will be actually 
 * written on the underlying stream, except that when starting a new inverted list, the previous 
 * inverted list, if any, will be written onto the underlying stream.
 * 
 * <p>Indices with special needs, such as {@linkplain VariableQuantumIndexWriter variable-quantum index writers} or
 * {@linkplain QuasiSuccinctIndexWriter quasi-succinct index writers} might require <i>ad hoc</i> methods
 * to start a new inverted list (e.g., {@link QuasiSuccinctIndexWriter#newInvertedList(long, long, long)}). If you
 * want to use these writers, your code must use <code>instanceof</code> and act accordingly. 
 *
 * @author Paolo Boldi 
 * @author Sebastiano Vigna 
 * @since 1.2
 */

public interface IndexWriter {

	/** Starts a new inverted list. The previous inverted list, if any, is actually written
	 *  to the underlying bit stream.
	 *  
	 *  @return the position (in bits) of the underlying bit stream where the new inverted
	 *  list starts.
	 *  @throws IllegalStateException if too few records were written for the previous inverted
	 *  list.
	 */
	long newInvertedList() throws IOException;

	/** Writes the frequency.
	 *  
	 *  @param frequency the (positive) number of document records that this inverted list will contain.
	 */
	void writeFrequency( final long frequency ) throws IOException;

	/** Starts a new document record. 
	 * 
	 * <P>This method must be called exactly exactly <var>f</var> times, where <var>f</var> is the frequency specified with 
	 * {@link #writeFrequency(long)}.
	 *
	 *  @return the output bit stream where the next document record data should be written, if necessary, or <code>null</code>,
	 *  if {@link #writeDocumentPointer(OutputBitStream, long)} ignores its first argument.
	 *  @throws IllegalStateException if too many records were written for the current inverted list,
	 *  or if there is no current inverted list.
	 */
	OutputBitStream newDocumentRecord() throws IOException;

	/** Writes a document pointer.
	 * 
	 * <P>This method must be called immediately after {@link #newDocumentRecord()}.
	 * 
	 * @param out the output bit stream where the pointer will be written.
	 * @param pointer the document pointer.
	 */
	void writeDocumentPointer( final OutputBitStream out, final long pointer ) throws IOException;

	/** Writes the payload for the current document.
	 * 
	 * <P>This method must be called immediately after {@link #writeDocumentPointer(OutputBitStream, long)}.
	 * 
	 * @param out the output bit stream where the payload will be written.
	 * @param payload the payload.
	 */
	void writePayload( final OutputBitStream out, final Payload payload ) throws IOException;

	/** Writes the count of the occurrences of the current term in the current document to the given {@link OutputBitStream}. 
	 *  @param out the output stream where the occurrences should be written.
	 *  @param count the count.
	 */
	void writePositionCount( final OutputBitStream out, final int count ) throws IOException;

	/** Writes the positions of the occurrences of the current term in the current document to the given {@link OutputBitStream}. 
	 *
	 *  @param out the output stream where the occurrences should be written.
	 *  @param position the position vector (a sequence of strictly increasing natural numbers).
	 *  @param offset the first valid entry in <code>position</code>.
	 *  @param count the number of valid entries in <code>position</code> starting from <code>offset</code>.
	 *  @param docSize the size of the current document (only for Golomb and interpolative coding; you can safely pass -1 otherwise).
	 *  @throws IllegalStateException if there is no current inverted list.
	 */
	void writeDocumentPositions( final OutputBitStream out, final int[] position, final int offset, final int count, final int docSize ) throws IOException;

	/** Returns the overall number of bits written onto the underlying stream(s).
	 *
	 *  @return the number of bits written, according to the variables keeping statistical records.
	 */
	long writtenBits();

	/** Returns properties of the index generated by this index writer.
	 * 
	 * <p>This method should only be called after {@link #close()}. 
	 * It returns a new {@linkplain Properties property object} 
	 * containing values for (whenever appropriate)
	 * {@link Index.PropertyKeys#DOCUMENTS}, {@link Index.PropertyKeys#TERMS},
	 * {@link Index.PropertyKeys#POSTINGS}, {@link Index.PropertyKeys#MAXCOUNT},
	 * {@link Index.PropertyKeys#INDEXCLASS}, {@link Index.PropertyKeys#CODING}, {@link Index.PropertyKeys#PAYLOADCLASS},
	 * {@link BitStreamIndex.PropertyKeys#SKIPQUANTUM}, and {@link BitStreamIndex.PropertyKeys#SKIPHEIGHT}. 
	 * 
	 * @return properties a new set of properties for the just created index.
	 */
	Properties properties();

	/** Closes this index writer, completing the index creation process and releasing all resources.
	 * 
	 * @throws IllegalStateException if too few records were written for the last inverted list.
	 */
	void close() throws IOException;
	
	/** Writes to the given print stream statistical information about the index just built.
	 * This method must be called after {@link #close()}.
	 *  
	 * @param stats a print stream where statistical information will be written.
	 */
	void printStats( final PrintStream stats );

}
