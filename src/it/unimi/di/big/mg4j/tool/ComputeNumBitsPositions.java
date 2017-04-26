package it.unimi.di.big.mg4j.tool;

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

import it.unimi.di.big.mg4j.index.BitStreamHPIndex;
import it.unimi.di.big.mg4j.index.BitStreamHPIndexWriter;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;


/** Computes the number of bits used by a {@link BitStreamHPIndexWriter high-performance index} for positions.
 * 
 * <p>This tool is a temporary patch to avoid rebuilding indices that
 * do not possess such information.
 * 
 * @author Sebastiano Vigna
 * @since 2.2
 */

public class ComputeNumBitsPositions {
	
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, IllegalAccessException, InstantiationException, ConfigurationException, SecurityException, URISyntaxException {

		SimpleJSAP jsap = new SimpleJSAP( ComputeNumBitsPositions.class.getName(), "Scans and prints to standard output metadata of a collection. All line terminators in the metadata will be substituted with spaces.",
			new Parameter[] {
				new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the index." ),
		});

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		final String basename = jsapResult.getString( "basename" );

		// Just to check that the index is of the right type
		final BitStreamHPIndex index = (BitStreamHPIndex)Index.getInstance( basename, false, false );
		
		final InputBitStream ibs = new InputBitStream( basename + DiskBasedIndex.INDEX_EXTENSION );
		final InputBitStream offsets = new InputBitStream( basename + DiskBasedIndex.OFFSETS_EXTENSION );
		final OutputBitStream numBitsPos = new OutputBitStream( basename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION );
		long pos = 0, positionOffset = 0, o;
		for( int i = 0; i < index.numberOfTerms; i++ ) {
			ibs.position( pos += offsets.readLongGamma() );
			// Read offset into position file
			o = ibs.readLongDelta();

			if ( i > 0 ) numBitsPos.writeLongGamma( o - positionOffset );
			positionOffset = o;
		}

		// This is necessarily imprecise
		numBitsPos.writeLongGamma( new File( basename + DiskBasedIndex.POSITIONS_EXTENSION ).length() * 8 - positionOffset );

		numBitsPos.close();
		offsets.close();
		ibs.close();	
	}
}
