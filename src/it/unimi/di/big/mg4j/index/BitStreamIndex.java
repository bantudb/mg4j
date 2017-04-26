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
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A {@linkplain BitStreamIndexWriter bitstream-based} index. Instances of this class contains additional 
 * index data related to compression, such as the codes used for each part of the index.
 *
 * <P>Implementing subclasses must provide access to the index bitstream 
 * both at {@linkplain #getInputStream() byte} and {@linkplain #getInputBitStream(int) bit} level.
 * 
 * <A>A bitstream-based index usually exposes the {@linkplain #offsets offset list}.
 * 
 * <h2>Wired implementations</h2>
 * 
 * <p>The standard readers associated with an instance of this class are of type {@link BitStreamIndexReader}.
 * Nonetheless, it is possible to generate automatically sources for wired classes that
 * work only for a particular set of codings and flags. The wired classes will be fetched
 * automagically by reflection, if available. Please read the section about performance in the MG4J manual.
 *
 * @author Sebastiano Vigna
 * @since 1.1
 */

public abstract class BitStreamIndex extends Index {
	private static final long serialVersionUID = 0;
	private static final Logger LOGGER = LoggerFactory.getLogger( BitStreamIndex.class );
	private static final boolean ASSERTS = false;
	
	/** Symbolic names for additional properties of a {@link BitStreamIndex}. */
	public static enum PropertyKeys {
		/** The skip quantum. */
		SKIPQUANTUM,			
		/** The skip height. */
		SKIPHEIGHT,
		/** The size of the buffer used to read the bit stream. */
		BUFFERSIZE
	}

	/** The default height (fairly low, due to memory consumption). */
	public final static int DEFAULT_HEIGHT = 16;

	/** The default variable quantum (1% of index size). */
	public final static int DEFAULT_QUANTUM = -1;
	/** The default fixed quantum (each 64 postings). */
	public final static int DEFAULT_FIXED_QUANTUM = 64;
	/** The default buffer size. */
	public final static int DEFAULT_BUFFER_SIZE = 4 * 1024;
	
	/** The coding for frequencies. See {@link CompressionFlags}. */
	public final Coding frequencyCoding;
	/** The coding for pointers. See {@link CompressionFlags}. */
	public final Coding  pointerCoding;
	/** The coding for counts. See {@link CompressionFlags}. */
	public final Coding countCoding;
	/** The coding for positions. See {@link CompressionFlags}. */
	public final Coding positionCoding;
	/** The offset of each term, if offsets were loaded or specified at creation time, or <code>null</code>. */
	public final LongBigList offsets;
	/** The parameter <code>h</code> (the maximum height of a skip tower), or -1 if this index has no skips. */
	public final int height;
	/** The quantum, or -1 if this index has no skips, or 0 if this is a {@link BitStreamHPIndex} and quanta are variable. */
	public final int quantum;
	/** The size of the buffer used to read the bit stream. */
	public final int bufferSize;
	/** The constructor that will be used to create new index readers. */
	public transient Constructor<? extends IndexReader> readerConstructor;

	public BitStreamIndex( final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings, final long numberOfOccurrences, final int maxCount, 
			final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, final int bufferSize, 
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, countCoding != null, positionCoding != null, termProcessor, field, termMap, prefixMap, sizes, properties );
		this.frequencyCoding = frequencyCoding;
		this.pointerCoding = pointerCoding;
		this.countCoding = countCoding;
		this.positionCoding = positionCoding;
		this.offsets = offsets;
		this.quantum = quantum;
		this.height = height;
		this.bufferSize = bufferSize;

		// Note that we just check that quantum is -1, 0, or a power of two. Subclasses must perform additional checks.
		if ( quantum != -1 ) {
			if ( height < 0 ) throw new IllegalArgumentException( "Illegal height " + height );
			if ( quantum < 0 || ( quantum & -quantum ) != quantum ) throw new IllegalArgumentException( "Illegal quantum " + quantum );
		}

		readerConstructor = getConstructor();
	}

	@SuppressWarnings("unchecked")
	protected Constructor<? extends IndexReader> getConstructor() {
		Class<? extends IndexReader> readerClass = BitStreamIndexReader.class;
		String className = 	BitStreamIndexReader.class.getPackage().getName() + ".wired." 
			+ ( quantum != -1 ? "Skip" : "" )
			+ featureName( frequencyCoding ) + featureName( pointerCoding )
			+ ( hasPayloads ? "Payloads" : featureName( countCoding ) + featureName( positionCoding ) )
			+ BitStreamIndexReader.class.getSimpleName(); 

		try {
			readerClass = (Class<? extends IndexReader>)Class.forName( className );
 			LOGGER.info( "Dynamically fetched reader class " + readerClass.getSimpleName() );
		}
		catch( Exception e ) {
			LOGGER.info( "Cannot fetch dynamically class " + className + "; falling back to generic (slower) class " + BitStreamIndexReader.class.getSimpleName() );
		}

		
		try {
			return readerClass.getConstructor( BitStreamIndex.class, InputBitStream.class );
		}
		catch( Exception shouldntReallyHappen ) {
			throw new RuntimeException( "Cannot find suitable constructor in " + readerClass.getSimpleName() );
		}
	}
	
	protected static String featureName( final Coding coding ) {
		return StringUtils.capitalize( ( coding == null ? CompressionFlags.NONE : coding.toString() ).toLowerCase() );
	}
	
	/** Returns an input bit stream over the index.
	 * 
	 * @param bufferSize a suggested buffer size.
	 * @return an input bit stream over the index.
	 */
	public abstract InputBitStream getInputBitStream( final int bufferSize ) throws IOException;
	
	/** Returns an input stream over the index.
	 * 
	 * @return an input stream over the index.
	 */
	public abstract InputStream getInputStream() throws IOException;

	public IndexReader getReader( final int bufferSize ) throws IOException {
		try {
			return readerConstructor.newInstance( this, getInputBitStream( bufferSize == -1 ? this.bufferSize : bufferSize ) );
		}
		catch( IOException e ) {
			throw e;
		}
		catch( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	/** Fixed number of fractional binary digits used in fixed-point computation of Golomb moduli. */
	public final static int FIXED_POINT_BITS = 31;
	/** <code>1L << {@link #FIXED_POINT_BITS}</code>. */
	public final static long FIXED_POINT_MULTIPLIER = ( 1L << FIXED_POINT_BITS );

	private final static int[] GOLOMB_STEP = {
		(int)( .3819660112501052 * FIXED_POINT_MULTIPLIER ),
		(int)( .245122333753307  * FIXED_POINT_MULTIPLIER ),
		(int)( .1808274866038356 * FIXED_POINT_MULTIPLIER ),
		(int)( .1433251161454971 * FIXED_POINT_MULTIPLIER ),
		(int)( .1187285383664304 * FIXED_POINT_MULTIPLIER ),
		(int)( .1013462873713007 * FIXED_POINT_MULTIPLIER ),
		(int)( .0884076465179451 * FIXED_POINT_MULTIPLIER ),
		(int)( .0784006803660170 * FIXED_POINT_MULTIPLIER ),
		(int)( .0704298717679771 * FIXED_POINT_MULTIPLIER ),
		(int)( .0639308889222416 * FIXED_POINT_MULTIPLIER ),
		(int)( .0585303826783648 * FIXED_POINT_MULTIPLIER ),
		(int)( .0539714717143864 * FIXED_POINT_MULTIPLIER ),
		(int)( .0500716000363801 * FIXED_POINT_MULTIPLIER ),
		(int)( .0466974625983358 * FIXED_POINT_MULTIPLIER ),
		(int)( .0437494423620109 * FIXED_POINT_MULTIPLIER ),
	};
		
	private final static int GOLOMB_STEP_LENGTH = GOLOMB_STEP.length;
	private final static int GOLOMB_THRESHOLD = (int)( .0411515989924386 * FIXED_POINT_MULTIPLIER );
	private final static long GOLOMB_ADD =  (long)( ( -( 1 + Math.log( 2 ) ) / 2 ) * FIXED_POINT_MULTIPLIER ), GOLOMB_MULT = (int)( Math.log( 2 )* FIXED_POINT_MULTIPLIER );
		

	/** Computes the Golomb modulus for a given fraction using
	 * fixed-point arithmetic and a precomputed table for
	 * small values. This gives results that are
	 * extremely close to &lceil; log( 2 - <code>p</code>/<code>q</code> ) / log( 1 - <code>p</code>/<code>q</code> ) &rceil;,
	 * but the computation is orders of magnitude quicker.
	 * 
	 * @param p the numerator.
	 * @param q the denominator (larger than or equal to <code>p</code>).
	 * @return the Golomb modulus for <code>p</code>/<code>q</code>.
	 */
	public static int golombModulus( final long p, final long q ) {
		if ( ASSERTS ) assert p <= q;
		final int f = (int)( ( p * FIXED_POINT_MULTIPLIER ) / q );
		if ( f < GOLOMB_THRESHOLD ) return (int)( ( GOLOMB_ADD + ( GOLOMB_MULT * q ) / p + FIXED_POINT_MULTIPLIER - 1 ) >> FIXED_POINT_BITS ); 
		int i = GOLOMB_STEP_LENGTH;
		while( i-- != 0 ) if ( f < GOLOMB_STEP[ i ] ) return i + 2;
		return 1;
	}

	/** Fixed-point reprentation of the constant part of the formula for Gaussian Golomb codes. */
	private final static long GOLOMB_GAUSSIAN = (long)( ( 2 * Math.sqrt( 2 / Math.PI ) * Math.log( 2 ) ) * FIXED_POINT_MULTIPLIER );
	/** Fixed-point representation of the square root of 2<sup>i-1</sup>.*/
	private final static long[] SQRT_2_TO = new long[ 32 ];
	static {
		for( int i = SQRT_2_TO.length; i-- != 0; ) SQRT_2_TO[ i ] = (long)( ( Math.sqrt( ( 1L << i ) / 2.0 ) * FIXED_POINT_MULTIPLIER ) );
	}
	
	/** Computes the Gaussian Golomb modulus for a given standard deviation
	 * and shift using fixed-point arithmetic.
	 * 
	 * <p>The Golomb modulus for (positive and negative) 
	 * integers normally distributed with standard deviation &sigma; can be computed using
	 * the formula &lceil; 2 sqrt( 2 / &pi; ) ln(2) &sigma; &rceil;.
	 *
	 * <P>The resulting Golomb modulus is near to optimal for coding such
	 * integers after they have been passed through {@link Fast#int2nat(int)}. Note,
	 * however, that Golomb coding is <em>not</em> optimal for a normal distribution.
	 *
	 * <p>This function is used to compute the correct Golomb modulus for skip towers.
	 *
	 * @param quantumSigma the standard deviation of a quantum as returned by {@link #quantumSigma(long, long, long)}.
	 * @param shift a shift parameter.
	 * @return the Golomb modulus for the standard deviation obtained multiplying <code>quantumSigma</code> by
	 * the square root of 2<sup><code>shift</code>-1</sup>.
	 */
	public static int gaussianGolombModulus( final long quantumSigma, final int shift ) {
		return (int)( ( ( ( ( GOLOMB_GAUSSIAN >> FIXED_POINT_BITS / 2 ) * 
				( quantumSigma >> FIXED_POINT_BITS - FIXED_POINT_BITS / 2 ) ) >> FIXED_POINT_BITS / 2 ) *
				( SQRT_2_TO[ shift ] >> FIXED_POINT_BITS - FIXED_POINT_BITS / 2 ) ) + FIXED_POINT_MULTIPLIER - 1 >> FIXED_POINT_BITS );
	}

	/** Computes the standard deviation associated with a given quantum and document frequency.
	 * 
	 * @param frequency the document frequency.
	 * @param numberOfDocuments the overall number of documents.
	 * @param quantum the quantum.
	 * @return a long representing in fixed-point arithmetic the value <code>Math.sqrt( quantum * ( 1 - p ) ) / p</code>, where
	 * <code>p</code> is the relative frequency.
	 */
	public static long quantumSigma( final long frequency, final long numberOfDocuments, final long quantum ) {
		return (long)( ( ( Math.sqrt( quantum * ( 1 - (double)frequency/numberOfDocuments ) ) * numberOfDocuments ) / frequency ) * FIXED_POINT_MULTIPLIER ); 
	}
	
	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		readerConstructor = getConstructor();
	}

	public String toString() {
		return this.getClass().getSimpleName() + "[" + field + "]";
	}
}
