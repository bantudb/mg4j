package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Paolo Boldi 
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

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.util.SignedFunctionStringMap;
import it.unimi.dsi.util.BloomFilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A virtual-document resolver based on document URIs.
 *
 * <p>Instances of this class store in a {@link StringMap} instances
 * all URIs from a collection, and consider a virtual-document specification a (possibly relative) URI. The
 * virtual-document specification is resolved against the document URI, and then the perfect hash is used
 * to retrieve the corresponding document.
 * 
 * <p>This class provides a main method that helps in building serialised resolvers from URI lists.
 * In case of pathological document collections with duplicate URIs (most notably, the GOV2 collection
 * used for TREC evaluations), an option makes it possible to add random noise to duplicates, so that
 * minimal perfect hash construction does not go into an infinite loop. It is a rather crude solution, but it
 * is nonsensical to have duplicate URIs in the first place. Additional option include the kind of minimal perfect
 * hash function you want to use (e.g., out of {@link it.unimi.dsi.sux4j}) and the number of bits used to sign them.
 * 
 * <p><strong>Warning</strong>: up to version 5.2.1, this class was applying {@link URI#normalize()} in
 * {@link #context(Document)} and {@link #resolve(CharSequence)} methods. This does not happen any longer,
 * as it was breaking URLs such as <samp>http://en.wikipedia.org/wiki//dev/null</samp>.
 */

public class URLMPHVirtualDocumentResolver implements VirtualDocumentResolver {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger( URLMPHVirtualDocumentResolver.class );
	
	/** The term map used by this resolver to associated URI strings to numbers. */
	private final StringMap<? extends CharSequence> url2DocumentPointer;
	/** The cached URI of the last argument to {@link #context(Document)}. */
	private transient URI documentURI;

	public URLMPHVirtualDocumentResolver( final StringMap<? extends CharSequence> url2DocumentPointer ) {
		this.url2DocumentPointer = url2DocumentPointer;
	}

	@Override
	public void context( final Document document ) {
		documentURI = URI.create( document.uri().toString() );
	}

	@Override
	public long resolve( final CharSequence virtualDocumentSpec ) {
		try {
			URI virtualURI = URI.create( virtualDocumentSpec.toString() );
			if ( ! virtualURI.isAbsolute() ) {
				if ( documentURI == null ) return -1;
				virtualURI = documentURI.resolve( virtualURI );
			}

			return url2DocumentPointer.getLong( virtualURI.toString() );
		} catch ( Exception e ) {
			return -1;
		}
	}

	@Override
	public long numberOfDocuments() {
		return url2DocumentPointer.size64();
	}

	private static void makeUnique( final BloomFilter<Void> filter, final MutableString uri ) {
		while( ! filter.add( uri ) ) {
			LOGGER.debug( "Duplicate URI " + uri );
			uri.append( '/' ).append( RandomStringUtils.randomAlphanumeric( 32 ) );
		}
	}


	@SuppressWarnings("deprecation")
	public static void main( final String[] arg ) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP( URLMPHVirtualDocumentResolver.class.getName(), "Builds a URL document resolver from a sequence of URIs, extracted typically using ScanMetadata, using a suitable function. You can specify that the list is sorted, in which case it is possible to generate a resolver that occupies less space.",
				new Parameter[] {
					new Switch( "sorted", 's', "sorted", "URIs are sorted: use a monotone minimal perfect hash function." ),
					new Switch( "iso", 'i', "iso", "Use ISO-8859-1 coding internally (i.e., just use the lower eight bits of each character)." ),
					new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, "64Ki", JSAP.NOT_REQUIRED, 'b',  "buffer-size", "The size of the I/O buffer used to read terms." ),
					new FlaggedOption( "class", MG4JClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "class", "A class used to create the function from URIs to their ranks; defaults to it.unimi.dsi.sux4j.mph.MHWCFunction for non-sorted inputs, and to it.unimi.dsi.sux4j.mph.TwoStepsLcpMonotoneMinimalPerfectHashFunction for sorted inputs." ),
					new FlaggedOption( "width", JSAP.INTEGER_PARSER, Integer.toString( Long.SIZE ), JSAP.NOT_REQUIRED, 'w', "width", "The width, in bits, of the signatures used to sign the function from URIs to their rank." ),
					new FlaggedOption( "termFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "offline", "Read terms from this file (without loading them into core memory) instead of standard input." ),
					new FlaggedOption( "uniqueUris", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'U', "unique-uris", "Force URIs to be unique by adding random garbage at the end of duplicates; the argument is an upper bound for the number of URIs that will be read, and will be used to create a Bloom filter." ),
					new UnflaggedOption( "resolver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename for the resolver." )
		});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		final int bufferSize = jsapResult.getInt( "bufferSize" );
		final String resolverName = jsapResult.getString( "resolver" );
		//final Class<?> tableClass = jsapResult.getClass( "class" );
		final boolean iso = jsapResult.getBoolean( "iso" );
		String termFile = jsapResult.getString( "termFile" );
		
		BloomFilter<Void> filter = null;
		final boolean uniqueURIs = jsapResult.userSpecified( "uniqueUris" ); 
		if ( uniqueURIs ) filter = BloomFilter.create( jsapResult.getInt( "uniqueUris" ) );
		
		final Collection<? extends CharSequence> collection;
		if ( termFile == null ) {
			ArrayList<MutableString> termList = new ArrayList<MutableString>();
			final ProgressLogger pl = new ProgressLogger();
			pl.itemsName = "URIs";
			final LineIterator termIterator = new LineIterator( new FastBufferedReader( new InputStreamReader( System.in, "UTF-8" ), bufferSize ), pl );
			
			pl.start( "Reading URIs..." );
			MutableString uri;
			while( termIterator.hasNext() ) {
				uri = termIterator.next();
				if ( uniqueURIs ) makeUnique( filter, uri );
				termList.add( uri.copy() );
			}
			pl.done();
			
			collection = termList;
		}
		else {
			if ( uniqueURIs ) {
				// Create temporary file with unique URIs
				final ProgressLogger pl = new ProgressLogger();
				pl.itemsName = "URIs";
				pl.start( "Copying URIs..." );
				final LineIterator termIterator = new LineIterator( new FastBufferedReader( new InputStreamReader( new FileInputStream( termFile ) ), bufferSize ), pl );
				File temp = File.createTempFile( URLMPHVirtualDocumentResolver.class.getName(), ".uniqueuris" );
				temp.deleteOnExit();
				termFile = temp.toString();
				final FastBufferedOutputStream outputStream = new FastBufferedOutputStream( new FileOutputStream( termFile ), bufferSize );
				MutableString uri;
				while( termIterator.hasNext() ) {
					uri = termIterator.next();
					makeUnique( filter, uri );
					uri.writeUTF8( outputStream );
					outputStream.write( '\n' );
				}
				pl.done();
				outputStream.close();
			}
			collection = new FileLinesCollection( termFile, "UTF-8" );
		}
		LOGGER.debug( "Building function..." );
		final int width = jsapResult.getInt( "width" );
		if (jsapResult.getBoolean("sorted")) BinIO.storeObject(new URLMPHVirtualDocumentResolver(new SignedFunctionStringMap(new TwoStepsLcpMonotoneMinimalPerfectHashFunction.Builder<CharSequence>().keys(collection).transform(iso ? TransformationStrategies.prefixFreeIso() : TransformationStrategies.prefixFreeUtf16()).signed(width).build())), resolverName);
		else BinIO.storeObject(new URLMPHVirtualDocumentResolver(new SignedFunctionStringMap(new GOV3Function.Builder<CharSequence>().keys(collection).transform(iso ? TransformationStrategies.prefixFreeIso() : TransformationStrategies.prefixFreeUtf16()).signed(width).build())), resolverName);

		LOGGER.debug( " done." );
    }
}
