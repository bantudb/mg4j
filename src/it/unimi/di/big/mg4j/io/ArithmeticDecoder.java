package it.unimi.di.big.mg4j.io;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2002-2016 Sebastiano Vigna 
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

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/** An arithmetic decoder.
 *
 * <P>This class provides an arithmetic decoder. The input of the coder is an
 * {@link InputBitStream}.  You must first create a decoder specifying the
 * number of input symbols, then call {@link #decode(InputBitStream)} for all
 * symbols to be decoded, and finally {@link #flush(InputBitStream)}. This last
 * operation reads the bits flushed by {@link
 * ArithmeticCoder#flush(OutputBitStream)}. Thereafter, exactly {@link #BITS}
 * excess bits will be present in the current window of the decoder. You can
 * get them using {@link #getWindow()}.
 *
 * <P>The code is inspired by the arithmetic decoder by John Carpinelli,
 * Radford M. Neal, Wayne Salamonsen and Lang Stuiver, which is in turn based
 * on <em>Arithmetic Coding Revisited</em>, by Alistair Moffat, Radford M. Neal
 * and Ian H.&nbsp;Witten (Proc. IEEE Data Compression Conference, Snowbird,
 * Utah, March 1995).
 *
 * @see ArithmeticCoder
 * @author Sebastiano Vigna
 * @since 0.1
 */


final public class ArithmeticDecoder {
	/** Number of bits used by the decoder. */
	public final static int BITS = 63;

	/** Bit-level representation of 1/2. */
	private final static long HALF = 1L << ( BITS - 1 );

	/** Bit-level representation of 1/4. */
	private final static long QUARTER = 1L << ( BITS - 2 );

	/** Cumulative counts for all symbols. */
	private int count[];

	/** Total count. */
	private int total;

	/** Number of symbols. */
	private int n;

	/** Current width of the range. */
	private long range = HALF;

	/** Current bits being decoded. */
	private long buffer = -1;

	/** Current window on the bit stream. */
	private long window = 0;

	/** Creates a new decoder.
	 *
	 * @param n number of symbols used by the decoder. 
	 */

	public ArithmeticDecoder( final int n ) {
		if ( n < 1 )
			throw new IllegalArgumentException( "You cannot use " + n + " symbols." );
		this.n = n;
		count = new int[ n + 1 ];
		for ( int i = 0; i < n; i++ )
			incrementCount( i ); // Initially, everything is equiprobable.
		total = n;
	}


	/* The following methods implement a Fenwick tree. */

	private void incrementCount( int x ) {
		x++;

		while ( x <= n ) {
			count[ x ]++;
			x += x & -x; // By chance, this gives the right next index 8^).
		}
	}

	private int getCount( int x ) {
		int c = 0;

		while ( x != 0 ) {
			c += count[ x ];
			x = x & x - 1; // This cancels out the least nonzero bit.
		}

		return c;
	}


	/** Decodes a symbol.
	 *
	 * @param ibs the input stream.
	 * @return the next symbol encoded.
	 * @throws IOException if <code>ibs</code> does.
	 */

	public int decode( final InputBitStream ibs ) throws IOException {

		if ( buffer == -1 )
			window = buffer = ibs.readLong( BITS - 1 ); // The first output bit is always 0 and is not output.

		final long r = range / total;
		int x = (int)( buffer / r );
		if ( total - 1 < x )
			x = total - 1;
		for ( int i = 1; i <= n; i++ )
			if ( x < getCount( i ) ) {
				x = i - 1;
				break;
			}

		final int lowCount = getCount( x ), highCount = getCount( x + 1 );

		buffer -= r * lowCount;

		if ( x != n - 1 )
			range = r * ( highCount - lowCount );
		else
			range -= r * lowCount;

		incrementCount( x );
		total++;

		while ( range <= QUARTER ) {
			buffer <<= 1;
			range <<= 1;
			window <<= 1;
			if ( ibs.readBit() != 0 ) {
				buffer++;
				window++;
			}
		}

		return x;
	}


	/** Flushes (reads) the disambiguating bits.
	 *
	 * <P>This method must be called when all symbols have been decoded.  After
	 * the call, exactly {@link #BITS} excess bits will be present in the
	 * current window of the decoder. You can get them using {@link #getWindow()}; 
	 * usually you will then unget them to the bit stream.
	 *
	 * @param ibs the input stream.
	 * @throws IOException if <code>ibs</code> does.
	 */

	public void flush( final InputBitStream ibs ) throws IOException {
		int nbits, i;
		long roundup, bits, value, low;

		low = ( ( window & ( HALF - 1 ) ) + HALF ) - buffer;

		for ( nbits = 1; nbits <= BITS; nbits++ ) {
			roundup = ( 1L << ( BITS - nbits ) ) - 1;
			bits = ( low + roundup ) >>> ( BITS - nbits );
			value = bits << ( BITS - nbits );

			if ( low <= value && ( value + roundup <= low + ( range - 1 ) || value + roundup >= 0 && low + ( range - 1 ) < 0 ) // This handles overflows onto the most significant bit.
			)
				break;
		}

		for ( i = 1; i <= nbits; i++ ) {
			window <<= 1;
			window |= ibs.readBit();
		}
	}

	/** Returns the current bit stream window.
	 *
	 * @return the current bit stream window in the lower {@link #BITS} bits.
	 */

	public long getWindow() {
		return window & ( ( HALF << 1 ) - 1 );
	}
}
