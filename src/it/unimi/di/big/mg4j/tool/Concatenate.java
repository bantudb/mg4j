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
import it.unimi.dsi.io.OutputBitStream;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;

import com.martiansoftware.jsap.JSAPException;

/** Concatenates several indices.
 * 
 * <p>This implementation of {@link it.unimi.di.big.mg4j.tool.Combine} concatenates
 * the involved indices: document 0 of the first index is document 0 of the 
 * final collection, but document 0 of the second index is numbered after
 * the number of documents in the first index, and so on. The resulting
 * index is exactly what you would obtain by concatenating the document
 * sequences at the origin of each index.
 * 
 * <p>Note that this class can be used also with a single index, making it possible to recompress easily
 * an index using different compression flags.
 * 
 * @author Sebastiano Vigna
 * @since 1.0
 * 
 */

final public class Concatenate extends Combine {
	
	/** Concatenates several indices into one.
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
	public Concatenate( 
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

	/** Concatenates several indices into one.
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
	public Concatenate( 
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
	}

	protected long combineNumberOfDocuments() {
		long n = 0;
		for( int i = 0; i < numIndices; i++ ) n += index[ i ].numberOfDocuments;
		return n;
	}
	
	
	protected int combineSizes( final OutputBitStream sizesOutputBitStream ) throws IOException {
		int maxDocSize = 0, currDoc = 0;
		if ( needsSizes ) size = IntBigArrays.newBigArray( numberOfDocuments );
		for( int i = 0; i < numIndices; i++ ) {
			final IntIterator sizes = sizes( i );
			int s = 0;
			long j = index[ i ].numberOfDocuments;
			while( j-- != 0 ) {
				maxDocSize = Math.max( maxDocSize, s = sizes.nextInt() );
				if ( needsSizes ) IntBigArrays.set( size, currDoc++, s );
				sizesOutputBitStream.writeGamma( s );
			}
			if ( sizes instanceof Closeable ) ((Closeable)sizes).close();
		}
		return maxDocSize;
	}

	@Override
	protected long combine( final int numUsedIndices, final long occurrency ) throws IOException {
		// We gather the frequencies and possibly sums of maximum position from the subindices and just add up.
		long totalFrequency = 0;
		long totalSumsMaxPos = 0;
		for( int k = numUsedIndices; k-- != 0; ) {
			final int currIndex = usedIndex[ k ];
			totalFrequency += ( frequency[ currIndex ] = indexIterator[ currIndex ].frequency() );
			if ( haveSumsMaxPos ) totalSumsMaxPos += sumsMaxPos[ currIndex ].readLongDelta();
		}

		if ( ! metadataOnly ) {
			int currIndex, count;
			long numPrevDocs = 0;
			long currDoc;
			OutputBitStream obs;
			Index i;
			IndexIterator ii;

			if ( quasiSuccinctIndexWriter != null ) quasiSuccinctIndexWriter.newInvertedList( totalFrequency, occurrency, totalSumsMaxPos );
			else {
				if ( p != 0 ) variableQuantumIndexWriter.newInvertedList( totalFrequency, p, predictedSize, predictedLengthNumBits ); 
				else indexWriter.newInvertedList();
			}
			 
			indexWriter.writeFrequency( totalFrequency );

			for( int k = currIndex = 0; k < numUsedIndices; k++ ) { // We can just concatenated posting lists.

				// We must update the number of previously seen documents, possibly adding those in skipped indices.
				while( currIndex < usedIndex[ k ] ) numPrevDocs += index[ currIndex++ ].numberOfDocuments;

				i = index[ currIndex ];
				ii = indexIterator[ currIndex ];

				for( long j = frequency[ currIndex ]; j-- != 0; ) {
					obs = indexWriter.newDocumentRecord();
					currDoc = ii.nextDocument() + numPrevDocs;
					indexWriter.writeDocumentPointer( obs, currDoc );

					if ( i.hasPayloads ) indexWriter.writePayload( obs, ii.payload() );

					if ( i.hasCounts ) {
						count = ii.count();
						if ( hasCounts ) indexWriter.writePositionCount( obs, count );
						if ( hasPositions ) indexWriter.writeDocumentPositions( obs, positionArray = IndexIterators.positionArray( ii, positionArray ), 0, count, size != null ? IntBigArrays.get( size, currDoc ) : -1 );
					}		
				}
			}
		}
		
		return totalFrequency;
	}
	
	public static void main( String arg[] ) throws ConfigurationException, SecurityException, JSAPException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		Combine.main( arg, Concatenate.class );
	}

}

