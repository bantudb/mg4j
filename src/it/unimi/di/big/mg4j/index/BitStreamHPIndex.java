package it.unimi.di.big.mg4j.index;

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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A {@linkplain BitStreamIndexWriter high-performance bitstream-based} index.
 *
 * <P>Implementing subclasses must provide access to the index bitstream (as it
 * happens for a {@link BitStreamIndex}) but also to the positions stream, 
 * both at {@linkplain #getPositionsInputStream() byte} and {@linkplain #getPositionsInputBitStream(int) bit} level.
 * 
 * <h2>Wired implementations</h2>
 * 
 * <p>The standard readers associated with an instance of this class are of type {@link BitStreamHPIndexReader}.
 * Nonetheless, it is possible to generate automatically sources for wired classes that
 * work only for a particular set of codings and flags. The wired classes will be fetched
 * automagically by reflection, if available. Please read the section about performance in the MG4J manual.
 *
 * @author Sebastiano Vigna
 * @since 1.1
 */

public abstract class BitStreamHPIndex extends BitStreamIndex {
	private static final long serialVersionUID = 0;
	private static final Logger LOGGER = LoggerFactory.getLogger( BitStreamHPIndex.class );
	
	public BitStreamHPIndex( final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, final int bufferSize, final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, frequencyCoding, pointerCoding, countCoding, positionCoding, quantum, height, bufferSize, termProcessor, field, properties, termMap, prefixMap, sizes, offsets );
		if ( height < 0 ) throw new IllegalArgumentException( "Illegal height " + height );
		if ( quantum < 0 || ( quantum & -quantum ) != quantum ) throw new IllegalArgumentException( "Illegal quantum " + quantum );
	}
	
	@SuppressWarnings("unchecked")
	protected Constructor<? extends IndexReader> getConstructor() {
		Class<? extends IndexReader> readerClass = BitStreamHPIndexReader.class;
		String className = 	BitStreamHPIndexReader.class.getPackage().getName() + ".wired." 
		+ featureName( frequencyCoding ) + featureName( pointerCoding )
		+ ( hasPayloads ? "Payloads" : featureName( countCoding ) + featureName( positionCoding ) )
		+ BitStreamHPIndexReader.class.getSimpleName(); 

		try {
			readerClass = (Class<? extends IndexReader>)Class.forName( className );
			LOGGER.info( "Dynamically fetched reader class " + readerClass.getSimpleName() );
		}
		catch( Exception e ) {
			LOGGER.info( "Cannot fetch dynamically class " + className + "; falling back to generic (slower) class " + BitStreamHPIndexReader.class.getSimpleName() );
		}

		try {
			return readerClass.getConstructor( BitStreamHPIndex.class, InputBitStream.class, InputBitStream.class );
		}
		catch( Exception shouldntReallyHappen ) {
			throw new RuntimeException( "Cannot find suitable constructor in " + readerClass.getSimpleName() );
		}
	}

	
	/** Returns an input bit stream over the index.
	 * 
	 * @param bufferSize a suggested buffer size.
	 * @return an input bit stream over the index.
	 */
	public abstract InputBitStream getPositionsInputBitStream( final int bufferSize ) throws IOException;
	
	/** Returns an input stream over the index.
	 * 
	 * @return an input stream over the index.
	 */
	public abstract InputStream getPositionsInputStream() throws IOException;

	public IndexReader getReader( final int bufferSize ) throws IOException {
		try {
			return readerConstructor.newInstance( this, getInputBitStream( bufferSize == -1 ? this.bufferSize : bufferSize ), getPositionsInputBitStream( bufferSize == -1 ? this.bufferSize : bufferSize ) );
		}
		catch( IOException e ) {
			throw e;
		}
		catch( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		readerConstructor = getConstructor();
	}
}
