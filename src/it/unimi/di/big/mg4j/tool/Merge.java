package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexIterators;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndex;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.io.OutputBitStream;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAPException;

/** Merges several indices.
 * 
 * <P>This class merges indices by performing a simple ordered list merge. Documents
 * appearing in two indices will cause an error.
 * 
 * @author Sebastiano Vigna
 * @since 1.0
 * 
 */

public class Merge extends Combine {
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger( Merge.class );

	/** The reference array of the document queue. */
	protected long[] doc;
	/** The queue containing document pointers (for remapped indices). */
	protected LongHeapSemiIndirectPriorityQueue documentQueue;
	
	/** Merges several indices into one.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param outputBasename the basename of the combined index.
	 * @param inputBasename the basenames of the input indices.
	 * @param metadataOnly if true, we save only metadata (term list, frequencies, global counts).
	 * @param bufferSize the buffer size for index readers.
	 * @param writerFlags the flags for the index writer.
	 * @param indexType the type of the index to build.
	 * @param skips whether to insert skips in case <code>interleaved</code> is true.
	 * @param quantum the quantum of skipping structures; if negative, a percentage of space for variable-quantum indices (irrelevant if <code>skips</code> is false).
	 * @param height the height of skipping towers (irrelevant if <code>skips</code> is false).
	 * @param skipBufferOrCacheSize the size of the buffer used to hold temporarily inverted lists during the skipping structure construction, or the size of the bit cache used when
	 * building a {@linkplain QuasiSuccinctIndex quasi-succinct index}.
	 * @param logInterval how often we log.
	 */
	public Merge( 			
			final IOFactory ioFactory,
			final String outputBasename,
			final String[] inputBasename,
			final boolean metadataOnly,
			final int bufferSize,
			final Map<Component,Coding> writerFlags,
			final IndexType indexType,
			final boolean skips,
			final int quantum,
			final int height,
			final int skipBufferOrCacheSize,
			final long logInterval ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this( ioFactory, outputBasename, inputBasename, null, metadataOnly, bufferSize, writerFlags, indexType, skips, quantum, height, skipBufferOrCacheSize, logInterval );
	}

	/** Merges several indices into one.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param outputBasename the basename of the combined index.
	 * @param inputBasename the basenames of the input indices.
	 * @param delete a monotonically increasing list of integers representing documents that will be deleted from the output index, or <code>null</code>.
	 * @param metadataOnly if true, we save only metadata (term list, frequencies, global counts).
	 * @param bufferSize the buffer size for index readers.
	 * @param writerFlags the flags for the index writer.
	 * @param indexType the type of the index to build.
	 * @param skips whether to insert skips in case <code>interleaved</code> is true.
	 * @param quantum the quantum of skipping structures; if negative, a percentage of space for variable-quantum indices (irrelevant if <code>skips</code> is false).
	 * @param height the height of skipping towers (irrelevant if <code>skips</code> is false).
	 * @param skipBufferOrCacheSize the size of the buffer used to hold temporarily inverted lists during the skipping structure construction, or the size of the bit cache used when
	 * building a {@linkplain QuasiSuccinctIndex quasi-succinct index}.
	 * @param logInterval how often we log.
	 */
	public Merge( 
			final IOFactory ioFactory,
			final String outputBasename,
			final String[] inputBasename,
			final IntList delete,
			final boolean metadataOnly,
			final int bufferSize,
			final Map<Component,Coding> writerFlags,
			final IndexType indexType,
			final boolean skips,
			final int quantum,
			final int height,
			final int skipBufferOrCacheSize,
			final long logInterval ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		super( ioFactory, outputBasename, inputBasename, delete, metadataOnly, false, bufferSize, writerFlags, indexType, skips, quantum, height, skipBufferOrCacheSize, logInterval );

		doc = new long[ numIndices ];
		documentQueue = new LongHeapSemiIndirectPriorityQueue( doc, numIndices );
	}

	protected long combineNumberOfDocuments() {
		long n = 0;
		for( int i = 0; i < numIndices; i++ ) n = Math.max( n, index[ i ].numberOfDocuments );
		return n;
	}
	
	protected int combineSizes( final OutputBitStream sizesOutputBitStream ) throws IOException {
		int curSize, s, maxDocSize = 0;
		if ( needsSizes ) size = IntBigArrays.newBigArray( numberOfDocuments );
		
		final IntIterator[] sizes = new IntIterator[ numIndices ];
		for( int i = 0; i < numIndices; i++ ) sizes[ i ] = sizes( i );
		
		for( int d = 0; d < numberOfDocuments; d++ ) {
			curSize = 0;
			for( int i = 0; i < numIndices; i++ ) {
				if ( d < index[ i ].numberOfDocuments && ( s = sizes[ i ].nextInt() ) != 0 ) {
					if ( curSize != 0 ) throw new IllegalArgumentException( "Document " + d + " has nonzero length in two indices" );
					curSize = s;
				}
			}
			if ( needsSizes ) IntBigArrays.set( size, d, curSize );
			if ( curSize > maxDocSize ) maxDocSize = curSize;
			sizesOutputBitStream.writeGamma( curSize );
		}

		for( int i = 0; i < numIndices; i++ ) if ( sizes[ i ] instanceof Closeable ) ((Closeable)sizes[ i ]).close();
		return maxDocSize;
	}

	@Override
	protected long combine( final int numUsedIndices, final long occurrency ) throws IOException {
		// We gather the frequencies from the subindices and just add up. At the same time, we load the document queue.
		long totalFrequency = 0, totalSumMaxPos = 0;

		int currIndex, lastIndex = -1;
		for( int k = numUsedIndices; k-- != 0; ) {
			currIndex = usedIndex[ k ];
			totalFrequency += ( frequency[ currIndex ] = indexIterator[ currIndex ].frequency() );
			if ( haveSumsMaxPos ) totalSumMaxPos += sumsMaxPos[ currIndex ].readLongDelta();
			if ( ! metadataOnly ) {
				doc[ currIndex ] = indexIterator[ currIndex ].nextDocument();
				documentQueue.enqueue( currIndex );
			}
		}
		
		if ( ! metadataOnly ) {

			if ( quasiSuccinctIndexWriter != null ) quasiSuccinctIndexWriter.newInvertedList( totalFrequency, occurrency, totalSumMaxPos );
			else {
				if ( p != 0 ) variableQuantumIndexWriter.newInvertedList(totalFrequency, p, predictedSize, predictedLengthNumBits ); 
				else indexWriter.newInvertedList();
			}

			indexWriter.writeFrequency( totalFrequency );

			long currDoc = -1;
			int count; 
			OutputBitStream obs;
			Index i;
			IndexIterator ir;

			while( ! documentQueue.isEmpty() ) {
				// We extract the smallest document pointer, and enqueue it in the new index.
				if ( currDoc == doc[ currIndex = documentQueue.first() ] ) throw new IllegalStateException( "The indices to be merged contain document " + currDoc + " at least twice (once in index " + inputBasename[ lastIndex ] + " and once in index " + inputBasename[ currIndex ] + ")" );
				currDoc = doc[ currIndex ];

				obs = indexWriter.newDocumentRecord();
				indexWriter.writeDocumentPointer( obs, currDoc );
				i = index[ currIndex ];
				ir = indexIterator[ currIndex ];

				if ( i.hasPayloads ) indexWriter.writePayload( obs, ir.payload() );

				if ( i.hasCounts ) {
					count = ir.count();
					if ( hasCounts ) indexWriter.writePositionCount( obs, count );
					if ( hasPositions ) indexWriter.writeDocumentPositions( obs, positionArray = IndexIterators.positionArray( ir, positionArray ), 0, count, size != null ? IntBigArrays.get( size, currDoc ) : -1 );
				}

				// If we just wrote the last document pointer of this term in index j, we dequeue it.
				if ( --frequency[ currIndex ] == 0 ) documentQueue.dequeue();
				else {
					doc[ currIndex ] = ir.nextDocument();
					documentQueue.changed();
				}
				lastIndex = currIndex;
			}
		}
		
		return totalFrequency;
	}

	public static void main( String arg[] ) throws ConfigurationException, SecurityException, JSAPException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Combine.main( arg, Merge.class );
	}
}
