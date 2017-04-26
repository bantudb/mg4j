package it.unimi.di.big.mg4j.io;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/** Static methods implementing interpolative coding.
 *
 * <P>Interpolative coding is a sophisticated compression technique that can be
 * applied to increasing sequences of integers. It is based on the idea that,
 * for instance, when compressing the sequence 2 5 6 we already know that
 * between 2 and 6 there are only 3 integers, so if we already know that the middle
 * integer is between 2 and 6 we can use a small index to denote 5 among 3, 4, and 5.
 *
 * <P>The main limitation of interpolative coding is that it needs to code and
 * decode the entire sequence in an array. This, however, makes it very
 * suitable to code the positions of the occurrences of a term in a document,
 * in particular in short documents.
 *
 * @author Sebastiano Vigna
 * @since 0.6
 */

final public class InterpolativeCoding {

	private InterpolativeCoding() {}

	/** Writes to a bit stream a increasing sequence of integers using interpolative coding.
	 *
	 * <P>Note that the length of the sequence and the arguments
	 * <code>lo</code> and <code>hi</code> <em>must</em> be known at decoding
	 * time.
	 *
	 * @param out the output bit stream.
	 * @param data the vector containing the integer sequence.
	 * @param offset the offset into <code>data</code> where the sequence starts.
	 * @param len the number of integers to code.
	 * @param lo a lower bound (must be smaller than or equal to the first integer in the sequence). 
	 * @param hi an upper bound (must be greater than or equal to the last integer in the sequence).
	 * @return the number of written bits.
	 */

    public static int write( final OutputBitStream out, final int[] data, final int offset, final int len, final int lo, final int hi ) throws IOException {
		final int h, m;
		int l;

		if ( len == 0 ) return 0;
		if ( len == 1 ) return out.writeMinimalBinary( data[offset] - lo, hi - lo + 1 );
		  
		h = len / 2;
		m = data[ offset + h ];
		  
		l = out.writeMinimalBinary( m - ( lo + h ), hi - len + h + 1 - ( lo + h ) + 1 );
		l += write( out, data, offset, h, lo, m - 1 );
		return l + write( out, data, offset + h + 1, len - h - 1, m + 1, hi );
    }


	/** Reads from a bit stream an increasing sequence of integers coded using interpolative coding.
	 *
	 * @param in the input bit stream.
	 * @param data the vector that will store the sequence; it may be
	 * <code>null</code>, in which case the integers are discarded.
	 * @param offset the offset into <code>data</code> where to store the result.
	 * @param len the number of integers to decode.
	 * @param lo a lower bound (the same as the one given to {@link #write(OutputBitStream,int[],int,int,int,int) write()}).
	 * @param hi an upper bound (the same as the one given to {@link #write(OutputBitStream,int[],int,int,int,int) write()}).
	 */

	public static void read( final InputBitStream in, final int[] data, final int offset, final int len, final int lo, final int hi ) throws IOException {
		final int h, m;

		if ( len == 0 ) return;
		if ( len == 1 ) {
			if ( data != null ) data[ offset ] = in.readMinimalBinary( hi - lo + 1 ) + lo;
			else in.readMinimalBinary( hi - lo + 1 );

			return;
		}

		h = len / 2;
		m = in.readMinimalBinary( hi - len + h + 1 - ( lo + h ) + 1 ) + lo + h;
		if ( data != null ) data[ offset + h ] = m;
		  
		read( in, data, offset, h, lo, m - 1 );
		read( in, data, offset + h + 1, len - h - 1, m + 1, hi );
	}

}
