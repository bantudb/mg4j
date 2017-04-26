package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2004-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;

/** A file-based index.
 *
 * <P>An instance of this class stores additional index data for indices
 * based on files: for instance, basename, compression flags, etc.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.9
 */

public class FileIndex extends BitStreamIndex {
	private static final long serialVersionUID = 0;
	
	/** The I/O factory that will be used to open files. */
	private final IOFactory ioFactory;
	/** The basename of this index. All file names will be stemmed from the basename. It may
	 * be <code>null</code>. */
	public final String basename;
	/** The file containing the index. It may be <code>null</code>. */
	public final String indexFile;

	public FileIndex( final IOFactory ioFactory, final String basename, final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, final int bufferSize, 
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, frequencyCoding, pointerCoding, countCoding, positionCoding, quantum, height, bufferSize, termProcessor, field, properties, termMap, prefixMap, sizes, offsets );
		this.ioFactory = ioFactory;
		this.basename = basename;
		this.indexFile = basename + DiskBasedIndex.INDEX_EXTENSION;
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(" + basename + ")";
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return ioFactory.getInputStream( indexFile );
	}

	@Override
	public InputBitStream getInputBitStream( final int bufferSize ) throws IOException {
		return new InputBitStream( ioFactory.getInputStream( indexFile ), bufferSize );
	}
}
