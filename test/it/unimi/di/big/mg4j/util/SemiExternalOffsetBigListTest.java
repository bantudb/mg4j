package it.unimi.di.big.mg4j.util;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

import org.junit.Test;

/**
 * @author Fabien Campagne
 */
public class SemiExternalOffsetBigListTest {

	private static InputBitStream buildInputStream( LongList offsets ) throws IOException {
		byte[] array = new byte[ offsets.size() * 4 ];
		final OutputBitStream streamer = new OutputBitStream( array );
		long previous = 0;
		for ( int i = 0; i < offsets.size(); i++ ) {
			final long value = offsets.getLong( i );
			streamer.writeLongGamma( value - previous );
			previous = value;
		}
		int size = (int)( streamer.writtenBits() / 8 ) + ( ( streamer.writtenBits() % 8 ) == 0 ? 0 : 1 );
		byte[] smaller = new byte[ size ];
		System.arraycopy( array, 0, smaller, 0, size );
		streamer.close();
		return new InputBitStream( smaller );

	}

	@Test
    public void testSemiExternalOffsetBigListGammaCoding() throws IOException {

		long[] offsets = { 10, 300, 450, 650, 1000, 1290, 1699 };
		LongList listOffsets = new LongArrayList( offsets );

		SemiExternalOffsetBigList list = new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 1, listOffsets.size() );
		for ( int i = 0; i < offsets.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
		}

		list = new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 2, listOffsets.size() );
		for ( int i = 0; i < offsets.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
		}

		list = new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 4, listOffsets.size() );
		for ( int i = 0; i < offsets.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
		}

		list = new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 7, listOffsets.size() );
		for ( int i = 0; i < offsets.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
		}
		
		list = new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 8, listOffsets.size() );
		for ( int i = 0; i < offsets.length; ++i ) {
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
			assertEquals( ( "test failed for index: " + i ), offsets[ i ], list.getLong( i ) );
		}
    }

	@Test
    public void testEmptySemiExternalOffsetBigListGammaCoding() throws IOException {

		long[] offsets = {  };
		LongList listOffsets = new LongArrayList( offsets );

		new SemiExternalOffsetBigList( buildInputStream( listOffsets ), 1, listOffsets.size() );
		assertTrue( true );
    }

}

