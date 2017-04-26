package it.unimi.di.big.mg4j.index.cluster;

/*		 
 * MG4J: Managing Gigabytes for Java
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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.commons.configuration.ConfigurationException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;


/** A lexical strategy that creates an index containing a subset of the terms.
 * 
 * @author Sebastiano Vigna
 */


public class FrequencyLexicalStrategy implements LexicalPartitioningStrategy {
	static final long serialVersionUID = 0;
	/** The local number of each term. */
	private final Long2LongOpenHashMap localNumber;
	
	/** Creates a new subset lexical strategy.
	 * @param subset the subset of terms.
	 */
	public FrequencyLexicalStrategy( final LongSet subset ) {
		final long[] t = subset.toLongArray();
		Arrays.sort( t );
		localNumber = new Long2LongOpenHashMap();
		localNumber.defaultReturnValue( -1 );
		for( int i = 0; i < t.length; i++ ) localNumber.put( t[ i ], i ); 
	}

	public int numberOfLocalIndices() {
		return 2;
	}

	public int localIndex( final long globalNumber ) {
		return localNumber.get( globalNumber ) == -1 ? 1 : 0;
	}

	public long localNumber( final long globalNumber ) {
		long n = localNumber.get( globalNumber );
		return n == -1 ? 0 : n;
	}

	@Override
	public Properties[] properties() {
		return null;
	}
	
	public static void main( final String[] arg ) throws JSAPException, IOException, ConfigurationException, SecurityException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		final SimpleJSAP jsap = new SimpleJSAP( FrequencyLexicalStrategy.class.getName(), "Builds a lexical partitioning strategy based on a frequency threshold.",
				new Parameter[] {
					new FlaggedOption( "threshold", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "threshold", "The frequency threshold." ),
					new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the index." ),
					new UnflaggedOption( "strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the strategy." )
		});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		final Index index = Index.getInstance( jsapResult.getString( "basename" ) );
		final long threshold = jsapResult.getLong( "threshold" );
		final LongOpenHashSet subset = new LongOpenHashSet();

		final InputBitStream frequencies = new InputBitStream( jsapResult.getString( "basename" ) );
		
		for( long t = 0; t < index.numberOfTerms; t++ )
			if ( frequencies.readLongGamma() >= threshold ) subset.add( t );
		frequencies.close();

		BinIO.storeObject( new FrequencyLexicalStrategy( subset ), jsapResult.getString( "strategy" ) );
	} 
}
