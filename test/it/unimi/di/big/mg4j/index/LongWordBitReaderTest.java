package it.unimi.di.big.mg4j.index;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.LongWordBitReader;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.util.XorShift128PlusRandom;

import java.util.Arrays;

import org.junit.Test;

public class LongWordBitReaderTest {
	
	public static int writeNonZeroGamma( LongArrayBitVector bv, long value ) {
		if ( value <= 0 ) throw new IllegalArgumentException( "The argument " + value + " is not strictly positive." );
		final int msb = Fast.mostSignificantBit( value );
		final long unary = 1L << msb;
		bv.append( unary, msb + 1 );
		bv.append( value ^ unary, msb );
		return 2 * msb + 1;
	}
	
	public static int writeUnary( LongArrayBitVector bv, int value ) {
		if ( value < 0 ) throw new IllegalArgumentException( "The argument " + value + " is negative." );
		final int result = value + 1;
		while( value >= Long.SIZE ) {
			bv.append( 0, Long.SIZE );
			value -= Long.SIZE;
		}

		final long unary = 1L << value;
		bv.append( unary, value + 1 );
		return result;
	}
	
	public static int writeGamma( LongArrayBitVector bv, long value ) {
		if ( value < 0 ) throw new IllegalArgumentException( "The argument " + value + " is negative." );
		return writeNonZeroGamma( bv, value + 1 );
	}
	
	@Test
	public void test() {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );

		for( int length: new int[] { 0, 1, 10, 100, 1000, 10000, 100000, 1000000 } ) {
			final long[] value = new long[ length ];
			final int[] width = new int[ length ];
			final long[] pos = new long[ length + 1 ];

			for( int l = 0; l < Long.SIZE - 1; l++ ) {
				LongArrayBitVector bv = LongArrayBitVector.getInstance();
				Arrays.fill( pos, 0 );
				//System.err.println( "length: " + length + " l: " + l );
				for( int i = 0; i < value.length; i++ ) {
					switch( random.nextInt( 3 ) ) {
					case 0: 
						width[ i ] = -2; 
						value[ i ] = random.nextLong( 63 );
						pos[ i + 1 ] = pos[ i ] + writeUnary( bv, (int)value[ i ] );
						break; // Unary
					case 1: 
						width[ i ] = -1; 
						value[ i ] = random.nextLong( 64 );
						pos[ i + 1 ] = pos[ i ] + writeGamma( bv, value[ i ] );
						break; // Gamma
					case 2: 
						width[ i ] = l;
						value[ i ] = random.nextLong( 1L << l );
						bv.append( value[ i ], l );
						pos[ i + 1 ] = pos[ i ] + l;
					}
				}
				
				bv.append( 0, Long.SIZE );

				final LongBigArrayBigList list = LongBigArrayBigList.wrap( LongBigArrays.wrap( bv.bits() ) );
				LongWordBitReader lwbr = new LongWordBitReader( list, l );

				for( int i = 0; i < value.length; i++ ) {
					if ( width[ i ] == -2 ) assertEquals( value[ i ], lwbr.readUnary() );
					else if ( width[ i ] == -1 ) assertEquals( value[ i ], lwbr.readGamma() );
					else assertEquals( value[ i ], lwbr.extract() );
				}

				for( int i = value.length; i-- != 0; ) {
					lwbr.position( pos[ i ] );
					assertEquals( pos[ i ], lwbr.position() );

					if ( width[ i ] == -2 ) assertEquals( value[ i ], lwbr.readUnary() );
					else if ( width[ i ] == -1 ) assertEquals( value[ i ], lwbr.readGamma() );
					else assertEquals( value[ i ], lwbr.extract() );
				}

				for( int i = value.length; i-- != 0; ) if ( width[ i ] == l ) assertEquals( value[ i ], lwbr.extract( pos[ i ]) );
			}
		}
	}

	@Test
	public void testSelectTrim() {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );

		LongArrayBitVector bv = LongArrayBitVector.getInstance();
		final long[] value = new long[ 100000 ];
		final long[] pos = new long[ value.length + 1 ];

		for( int i = 0; i < value.length; i++ ) { 
			value[ i ] = random.nextLong( 100 );
			pos[ i + 1 ] = pos[ i ] + writeUnary( bv, (int)value[ i ] );
		}

		final LongBigArrayBigList list = LongBigArrayBigList.wrap( LongBigArrays.wrap( bv.bits() ) );
		LongWordBitReader lwbr = new LongWordBitReader( list, 0 );

		for( int i = 0; i < value.length; i++ ) 
			assertEquals( Integer.toString( i ), value[ i ], lwbr.readUnary() );

		lwbr = new LongWordBitReader( list, 0 );
	}
}
