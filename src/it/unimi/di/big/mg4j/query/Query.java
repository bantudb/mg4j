package it.unimi.di.big.mg4j.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.nodes.QueryTransformer;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.di.big.mg4j.search.score.Scorer;
import it.unimi.di.big.mg4j.search.score.VignaScorer;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.sux4j.io.FileLinesBigList;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A command-line interpreter to query indices.
 * 
 * <p>This class can be used to start a {@linkplain it.unimi.di.big.mg4j.query.QueryEngine query engine}
 * from the command line. Optionally, it can
 * start a {@linkplain HttpQueryServer web server} that will serve the results in a 
 * search-engine-like environment. Changes
 * to the query engine made on the command line will reflect on subsequent queries (also on the
 * web server). The web server access is fully multithreaded.
 * 
 * <p>This class does not provide command-line history or editing: to get that effect,
 * we suggest to rely on some operating-system utility such as 
 * <a href="http://utopia.knoware.nl/~hlub/uck/rlwrap/"><samp>rlwrap</samp></a>.
 * 
 * <p><strong>Warning:</strong> This class is <strong>highly experimental</strong> (it is the 
 * place that we tweak to experiment every kind of new indexing/ranking method).
 */
public class Query {
	private static final Logger LOGGER = LoggerFactory.getLogger( Query.class );

	/** A formatter for TREC results. */
	private static final java.text.NumberFormat FORMATTER = new java.text.DecimalFormat( "0.0000000000" );
	
	public final static int MAX_STEMMING = 2048;

	public static enum Command {
		MODE,
		LIMIT,
		SELECT,
		SCORE,
		MPLEX,
		EXPAND,
		DIVERT,
		WEIGHT,
		EQUALIZE,
		QUIT
	}
	
	public static enum OutputType {
		/** Display just timings. */
		TIME,
		/** Display document pointers, but not intervals. */
		SHORT,
		/** Display document pointers and not intervals (requires an index with positions). */
		LONG,
		/** Display document pointers and snippets (requires an index with positions and a collection). */
		SNIPPET,
		/** Display results in TREC format. */
		TREC;
	}
	
	/** The maximum number of items output to the console. */
	private int maxOutput = 10; 
	/** Current topic number, for {@link OutputType#TREC} only. */
	private int trecTopicNumber;
	/** Current run tag, for {@link OutputType#TREC} only. */
	private String trecRunTag;
	/** The current display mode. */
	private OutputType displayMode = OutputType.SHORT;
	/** The current output stream, changeable with <samp>$divert</samp>. */
	private PrintStream output = System.out;
	/** The current query engine. */
	private final QueryEngine queryEngine;
		
	public Query( final QueryEngine queryEngine ) {
		this.queryEngine = queryEngine;
	}
	
	/** Parses a given array of index URIs/weights, loading the correspoding indices
	 * and writing the result of parsing in the given maps.
	 * 
	 * @param basenameWeight an array of index URIs of the form <samp><var>uri</var>[:<var>weight</var>]</samp>, specifying
	 * the URI of an index and the weight for the index (1, if missing).
	 * @param loadSizes forces size loading.
	 * @param documentCollection an optional document collection, or <code>null</code>.
	 * @param name2Index an empty, writable map that will be filled with pairs given by an index basename (or field name, if available) and an {@link Index}.
	 * @param index2Weight an empty, writable map that will be filled with a map from indices to respective weights.
	 */
	private static void loadIndicesFromSpec( final String[] basenameWeight, boolean loadSizes, final DocumentCollection documentCollection, final Object2ReferenceMap<String,Index> name2Index, final Reference2DoubleMap<Index> index2Weight ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		for ( int i = 0; i < basenameWeight.length; i++ ) {
			
			// We must be careful, as ":" is used by Windows to separate the device from the path.
			final int split = basenameWeight[ i ].lastIndexOf( ':' );
			double weight = 1;

			if ( split != -1 ) {
				try {
					weight = Double.parseDouble( basenameWeight[ i ].substring( split + 1 ) );
				}
				catch( NumberFormatException e ) {}
			}
				
			final Index index;

			if ( split == -1 || basenameWeight[ i ].startsWith("mg4j://") ) {
				index = Index.getInstance( basenameWeight[ i ], true, loadSizes );
				index2Weight.put( index, 1 );
			}
			else {
				index = Index.getInstance( basenameWeight[ i ].substring( 0, split ), true, loadSizes );
				index2Weight.put( index, weight );
			}
			if ( documentCollection != null && index.numberOfDocuments != documentCollection.size() ) LOGGER.warn( "Index " + index + " has " + index.numberOfDocuments + " documents, but the document collection has size " + documentCollection.size() );
			name2Index.put( index.field != null ? index.field : basenameWeight[ i ], index );
		}
	}

	/** Parses a specification of the form <samp>class(&lt;arg>,&hellip;)[:weight]</samp> and returns the weight
	 *  (1 if missing) as result, assigning the just created object in the given index of the given array.
	 *  The arguments are all considered as strings.
	 * 
	 * @param spec the specification.
	 * @param array the array where the object is going to be stored.
	 * @param index the offset within the array.
	 * @return the weight (1 if missing).
	 */
	@SuppressWarnings("unchecked")
	private static <S> double loadClassFromSpec( String spec, final S[] array, final int index ) throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException, ClassNotFoundException, NoSuchMethodException, IOException {
		int pos = spec.indexOf( ':' );
		Class<S> klass = (Class<S>)array.getClass().getComponentType();
		double weightSpec = 1;
		if ( pos >= 0 ) {
			try {
				weightSpec = Double.parseDouble ( spec.substring( pos + 1 ) );
			} catch ( NumberFormatException e ) {
				throw new IllegalArgumentException( "Malformed weight " + spec.substring( 0, pos ) );
			}
			spec = spec.substring( 0, pos );
		}
		array[ index ] = ObjectParser.fromSpec( spec, klass, new String[] { "it.unimi.di.big.mg4j.search.score", "it.unimi.di.big.mg4j.query.nodes" } );
		return weightSpec;
	}
	
	/** Interpret the given command, changing the static variables.
	 *  See the help printing code for possible commands.
	 * 
	 * @param line the command line.
	 * @return false iff we should exit after this command.
	 */
	public boolean interpretCommand( final String line ) {
		String[] part = line.substring( 1 ).split( "[ \t\n\r]+" );

		final Command command;
		int i;

		if ( part[ 0 ].length() == 0 ) {
			System.err.println( "$                                                       prints this help." );
			System.err.println( "$mode [time|short|long|snippet|trec <topicNo> <runTag>] chooses display mode." );
			System.err.println( "$select [<maxIntervals> <maxLength>] [all]              installs or removes an interval selector." );
			System.err.println( "$limit <max>                                            output at most <max> results per query." );
			System.err.println( "$divert [<filename>]                                    diverts output to <filename> or to stdout." );
			System.err.println( "$weight {index:weight}                                  set index weights (unspecified weights are set to 1)." );
			System.err.println( "$mplex [<on>|<off>]                                     set/unset multiplex mode." );
			System.err.println( "$equalize <sample>                                      equalize scores using the given sample size." );
			System.err.println( "$score {<scorerClass>(<arg>,...)[:<weight>]}            order documents according to <scorerClass>." );
			System.err.println( "$expand {<expanderClass>(<arg>,...)}                    expand terms and prefixes according to <expanderClass>." );
			System.err.println( "$quit                                                   quits." );
			return true;
		}

		try {
			command = Command.valueOf( part[ 0 ].toUpperCase() );
		}
		catch( IllegalArgumentException e ) {
			System.err.println( "Invalid command \"" + part[ 0 ] + "\"; type $ for help." );
			return true;
		}

		switch( command ) {
		case MODE:
			if ( part.length >= 2 ) {
				try {
					final OutputType tempMode = OutputType.valueOf( part[ 1 ].toUpperCase() );

					if ( tempMode != OutputType.TREC && part.length > 2 ) System.err.println( "Extra arguments." ); 
					else if ( tempMode == OutputType.TREC && part.length != 4 ) System.err.println( "Missing or extra arguments." ); 
					else {
						displayMode = tempMode;
						if ( displayMode == OutputType.TREC ) {
							trecTopicNumber = Integer.parseInt( part[ 2 ] );
							trecRunTag = part[ 3 ];
						}
					}
				}
				catch( IllegalArgumentException e ) {
					System.err.println( "Unknown mode: " + part[ 1 ] );
				}
			}
			else System.err.println( "Missing mode." );
			break;
			
		case LIMIT:
			int out = -1;
			if ( part.length == 2 ) {
				try {
					out = Integer.parseInt( part[ 1 ] );
				}
				catch( NumberFormatException e ) {}
				if ( out >= 0 ) maxOutput = out;
			}
			if ( out < 0 ) System.err.println( "Missing or incorrect limit." );
			break;

			
		case SELECT:
			int maxIntervals = -1, maxLength = -1;
			if ( part.length == 1 ) {
				queryEngine.intervalSelector = null;
				System.err.println( "Intervals have been disabled." );
			}
			else if ( part.length == 2 && "all".equals( part[ 1 ] ) ) {
				queryEngine.intervalSelector = new IntervalSelector(); // All intervals
				System.err.println( "Interval selection has been disabled (will compute all intervals)." );
			}
			else {
				if ( part.length == 3 ) {
					try {
						maxIntervals = Integer.parseInt( part[ 1 ] );
						maxLength = Integer.parseInt( part[ 2 ] );
						queryEngine.intervalSelector = new IntervalSelector( maxIntervals, maxLength );
					}
					catch( NumberFormatException e ) {}
				}
				if ( maxIntervals < 0 || maxLength < 0 ) System.err.println( "Missing or incorrect selector parameters." );
			}
			break;
			
		case SCORE:
			final Scorer[] scorer = new Scorer[ part.length - 1 ];
			final double[] weight = new double[ part.length - 1 ];
			for ( i = 1; i < part.length; i++ ) 
				try {
					weight[ i - 1 ] = loadClassFromSpec( part[ i ], scorer, i - 1 );
					if ( weight[ i - 1 ] < 0 ) throw new IllegalArgumentException( "Weights should be non-negative" );
				} catch ( Exception e ) {
					System.err.print( "Error while parsing specification: " );
					e.printStackTrace( System.err );
					break;
				}
			if ( i == part.length ) queryEngine.score( scorer, weight );
			break;
			
		case EXPAND:
			if ( part.length > 2  ) System.err.println( "Wrong argument(s) to command" );
			else if ( part.length == 1 ) {
				queryEngine.transformer( null );
			}
			else {
				QueryTransformer[] t = new QueryTransformer[ 1 ];
				try {
					loadClassFromSpec( part[ 1 ], t, 0 );
					queryEngine.transformer( t[ 0 ] );
				}
				catch ( Exception e ) {
					System.err.print( "Error while parsing specification: " );
					e.printStackTrace( System.err );
					break;
				}
			}
			break;
			
		case MPLEX:
			if ( part.length != 2 || ( part.length == 2 && !"on".equals( part[ 1 ] ) && !"off".equals( part[ 1 ] ) ) ) 
				System.err.println( "Wrong argument(s) to command" );
			else {
				if ( part.length > 1 ) queryEngine.multiplex = "on".equals( part[ 1 ] );
				System.err.println( "Multiplex: " + part[ 1 ] );
			}
			break;
			
		case DIVERT:
			if ( part.length > 2 ) System.err.println( "Wrong argument(s) to command" );
			else {
				if ( output != System.out )
					output.close();
				try {
					output = part.length == 1 ? System.out : new PrintStream( new FastBufferedOutputStream( new FileOutputStream( part[ 1 ] ) ) );
				}
				catch ( FileNotFoundException e ) {
					System.err.println( "Cannot create file " + part[ 1 ] );
					output = System.out;
				}
			}
			break;
			
		case WEIGHT:
			final Reference2DoubleOpenHashMap<Index> newIndex2Weight = new Reference2DoubleOpenHashMap<Index>();
			for ( i = 1; i < part.length; i++ ) {
				final int pos = part[ i ].indexOf( ':' );
				if ( pos < 0 )  {
					System.err.println( "Missing colon: " + part[ i ] );
					break;
				}
				else if ( ! queryEngine.indexMap.containsKey( part[ i ].substring( 0, pos ) ) )  {
					System.err.println( "Unknown index: " + part[ i ].substring( 0, pos ) );
					break;
				}
				try {
					double newWeight = Double.parseDouble( part[ i ].substring( pos + 1 ) );
					newIndex2Weight.put( queryEngine.indexMap.get( part[ i ].substring( 0, pos ) ), newWeight );
				} catch ( NumberFormatException e ) {
					System.err.println( "Wrong weight specification: " + part[ i ].substring( pos + 1 ) );
					break;
				}
			}
			if ( i == part.length ) {
				if ( i > 1 ) queryEngine.setWeights( newIndex2Weight );
				for( String key : queryEngine.indexMap.keySet() )
					System.err.print( key + ":" + newIndex2Weight.getDouble( queryEngine.indexMap.get( key ) ) + " " );
				System.err.println();
			}
			break;
			
		case EQUALIZE:
			try {
				if ( part.length != 2 ) throw new NumberFormatException( "Illegal number of arguments" );
				queryEngine.equalize( Integer.parseInt( part[ 1 ] ) );
				System.err.println( "Equalization sample set to " + Integer.parseInt( part[ 1 ] ) );
			} catch ( NumberFormatException e ) {
				System.err.println( e.getMessage() );
			}
			break;
		
		case QUIT:
			return false;
		}

		return true;
	}
	
	/** Scores the given document iterator and produces score output.
	 * 
	 * @param results an iterator returning instances of {@link DocumentScoreInfo}.
	 * @param documentCollection an optional document collection, or <code>null</code>.
	 * @param titleList an optional list of titles, or <code>null</code>.
	 * @param marker an optional text marker to mark snippets, or <code>null</code>.
	 * @return the number of documents scanned. 
	 */
	@SuppressWarnings("boxing")
	public int output( final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results, final DocumentCollection documentCollection, final BigList<? extends CharSequence> titleList, final Marker marker ) throws IOException {	 
		int i;
		DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> dsi;

		if ( displayMode == OutputType.TREC ) {
			if ( titleList == null ) throw new IllegalStateException( "You cannot use TREC mode without a title list" );
			for( i = 0; i < results.size(); i++ ) {
				dsi = results.get( i );
				output.println( trecTopicNumber + " Q0 " + titleList.get( dsi.document ) + " " + i + " " + FORMATTER.format( dsi.score ) + " " +  trecRunTag );
			}

			// Horrible patch for no-answer queries (a workaround for TREC necessity of at least one result per query)
			if ( results.size() == 0  ) output.println( trecTopicNumber + " Q0 GX000-00-0000000 1 0 " +  trecRunTag );
		}
		else {
			
			for ( i = 0; i < results.size(); i++ ) {
				dsi = results.get( i );
				final long document = dsi.document;
				
				output.print( "Document #" + document );
				output.printf( " [%.6f]", dsi.score );
				
				Document d = null; // Filled lazily
				
				// We try to print a title, preferring the supplied title list if present
				if ( titleList != null ) output.println( " " + titleList.get( document ) );
				else if ( documentCollection != null ) {
					d = documentCollection.document( document );
					output.println( " " + d.title().toString().trim() );
				}
				else output.println();
				
				if ( ( displayMode == OutputType.LONG || displayMode == OutputType.SNIPPET ) && dsi.info != null && queryEngine.intervalSelector != null ) {
					final Index[] sortedIndex = dsi.info.keySet().toArray( new Index[ 0 ] );
					if ( documentCollection != null ) Arrays.sort( sortedIndex, new Comparator<Index>() {
						public int compare( final Index i0, final Index i1 ) {
							return documentCollection.factory().fieldIndex( i0.field ) - documentCollection.factory().fieldIndex( i1.field );
						}} );
					for( Index index: sortedIndex ) 
						if ( index.hasPositions ) {
							SelectedInterval[] interval = dsi.info.get( index );
							if ( interval == SelectedInterval.TRUE_ARRAY ) output.println( index.field + ": TRUE" );
							else if ( interval == SelectedInterval.FALSE_ARRAY ) output.println( index.field + ": FALSE" );
							else if ( displayMode == OutputType.LONG || documentCollection == null ) output.println( index.field + ": " + Arrays.toString( interval ) );
							else { // SNIPPET_MODE
								final MarkingMutableString s = new MarkingMutableString( marker );
								s.startField( interval );
								// TODO: this must be in increasing field order
								if ( d == null ) d = documentCollection.document( document );
								int fieldIndex = documentCollection.factory().fieldIndex( index.field );
								if ( fieldIndex == -1 || documentCollection.factory().fieldType( fieldIndex ) != DocumentFactory.FieldType.TEXT ) continue;
								final Reader reader = (Reader)d.content( fieldIndex );
								s.appendAndMark( d.wordReader( fieldIndex ).setReader( reader ) );
								s.endField();
								output.println( index.field + ": " + s.toString() );
							}
						}
						else if ( index.hasPayloads && dsi.info.get( index ) == SelectedInterval.TRUE_ARRAY ) {
							if ( d == null ) d = documentCollection.document( document );
							int fieldIndex = documentCollection.factory().fieldIndex( index.field );
							if ( fieldIndex == -1 ) continue;
							output.println( d.content( fieldIndex ) );
						}
					output.println();
				}

				if ( d != null ) d.close();
			}
		}
		return i;
	}
	
	@SuppressWarnings("unchecked")
	public static void main( final String[] arg ) throws Exception {

		SimpleJSAP jsap = new SimpleJSAP( Query.class.getName(), "Loads indices relative to a collection, possibly loads the collection, and answers to queries.",
				new Parameter[] {
					new FlaggedOption( "collection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "collection", "The collection of documents indexed by the given indices." ),
					new FlaggedOption( "objectCollection", new ObjectParser( DocumentCollection.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-collection", "An object specification describing a document collection." ),
					new FlaggedOption( "titleList", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "title-list", "A serialized big list of titles (will override collection titles if specified)." ),
					new FlaggedOption( "titleFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "title-file", "A file of newline-separated, UTF-8 titles (will override collection titles if specified)." ),
					new FlaggedOption( "input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "input", "A file containing the input." ),
					new Switch( "noSizes", 'n', "no-sizes", "Disable loading document sizes (they are necessary for BM25 scoring)." ),
					new Switch( "http", 'h', "http", "Starts an HTTP query server." ),
					new Switch( "verbose", 'v', "verbose", "Print full exception stack traces." ),
					new FlaggedOption( "itemClass", MG4JClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'i', "item-class", "The class that will handle item display in the HTTP server." ),
					new FlaggedOption( "itemMimeType", JSAP.STRING_PARSER, "text/html", JSAP.NOT_REQUIRED, 'm', "item-mime-type", "A MIME type suggested to the class handling item display in the HTTP server." ),
					new FlaggedOption( "port", JSAP.INTEGER_PARSER, "4242", JSAP.NOT_REQUIRED, 'p', "port", "The port on localhost where the server will appear." ),
					new UnflaggedOption( "basenameWeight", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The indices that the servlet will use. Indices are specified using their basename, optionally followed by a colon and a double representing the weight used to score results from that index. Indices without a specified weight are weighted 1." )
			});

		final JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		final DocumentCollection documentCollection = (DocumentCollection)(jsapResult.userSpecified( "collection" ) ? AbstractDocumentSequence.load( jsapResult.getString( "collection" ) ) : 
			jsapResult.userSpecified( "objectCollection" ) ? jsapResult.getObject( "objectCollection" ): null );
		final BigList<? extends CharSequence> titleList = (BigList<? extends CharSequence>) ( 
				jsapResult.userSpecified( "titleList" ) ? BinIO.loadObject( jsapResult.getString( "titleList" ) ) :
					jsapResult.userSpecified( "titleFile" ) ? new FileLinesBigList( jsapResult.getString( "titleFile" ), "UTF-8" ) :
						null );		
		final String[] basenameWeight = jsapResult.getStringArray( "basenameWeight" );
		final Object2ReferenceLinkedOpenHashMap<String,Index> indexMap = new Object2ReferenceLinkedOpenHashMap<String,Index>( Hash.DEFAULT_INITIAL_SIZE, .5f );
		final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<Index>();
		final boolean verbose = jsapResult.getBoolean( "verbose" );
		final boolean loadSizes = ! jsapResult.getBoolean( "noSizes" );
		Query.loadIndicesFromSpec( basenameWeight, loadSizes, documentCollection, indexMap, index2Weight );

		final long numberOfDocuments = indexMap.values().iterator().next().numberOfDocuments;
		if ( titleList != null && titleList.size64() != numberOfDocuments ) 
			throw new IllegalArgumentException( "The number of titles (" + titleList.size64() + " and the number of documents (" + numberOfDocuments + ") do not match" );
		
		final Object2ObjectOpenHashMap<String,TermProcessor> termProcessors = new Object2ObjectOpenHashMap<String,TermProcessor>( indexMap.size() );
		for( String alias: indexMap.keySet() ) termProcessors.put( alias, indexMap.get( alias ).termProcessor );
		
		final SimpleParser simpleParser = new SimpleParser( indexMap.keySet(), indexMap.firstKey(), termProcessors );

		final Reference2ReferenceMap<Index, Object> index2Parser = new Reference2ReferenceOpenHashMap<Index, Object>();
		/*
		// Fetch parsers for payload-based fields.
		for( Index index: indexMap.values() ) if ( index.hasPayloads ) {
			if ( index.payload.getClass() == DatePayload.class ) index2Parser.put( index, DateFormat.getDateInstance( DateFormat.SHORT, Locale.UK ) );
		}
		*/
		
		final QueryEngine queryEngine = new QueryEngine( simpleParser, new DocumentIteratorBuilderVisitor( indexMap, index2Parser, indexMap.get( indexMap.firstKey() ), MAX_STEMMING ), indexMap );
		queryEngine.setWeights( index2Weight );
		queryEngine.score( new Scorer[] { new BM25Scorer(), new VignaScorer() }, new double[] { 1, 1 } );
		// We set up an interval selector only if there is a collection for snippeting
		queryEngine.intervalSelector = documentCollection != null ? new IntervalSelector( 4, 40 ) : new IntervalSelector();
		queryEngine.multiplex = true;
		queryEngine.equalize( 1000 );
		
		Query query = new Query( queryEngine );
		query.displayMode = OutputType.SNIPPET;
		
		String q;

		System.err.println( "Welcome to the MG4J query class (setup with $mode snippet, $score BM25Scorer VignaScorer, $mplex on, $equalize 1000, $select " + ( documentCollection != null ?  "4 40" : "all" ) + ")" );
		System.err.println( "Please type $ for help." );
		
		String prompt = indexMap.keySet().toString() + ">";
		int n;
		
		HttpQueryServer httpQueryServer = null;
		if ( jsapResult.getBoolean( "http" ) ) httpQueryServer = new HttpQueryServer( queryEngine, documentCollection, jsapResult.getClass( "itemClass" ), jsapResult.getString( "itemMimeType" ), jsapResult.getInt( "port" ), titleList );
		try {
			final BufferedReader br = new BufferedReader( new InputStreamReader( jsapResult.userSpecified( "input" ) ? new FileInputStream( jsapResult.getString( "input") ) : System.in ) );
			final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();
			
			for ( ;; ) {
				System.out.print( prompt );
				q = br.readLine();
				if ( q == null ) {
					System.err.println();
					break; // CTRL-D
				}
				if ( q.length() == 0 ) continue;
				if ( q.charAt( 0 ) == '$' ) {
					if ( ! query.interpretCommand( q ) ) break;
					continue;
				}
				
				long time = -System.nanoTime();
				
				try {
					n = queryEngine.process( q, 0, query.maxOutput, results );
				}
				catch( QueryParserException e ) {
					if ( verbose ) e.getCause().printStackTrace( System.err ); 
					else System.err.println( e.getCause() );
					continue;
				}
				catch( Exception e ) {
					if ( verbose ) e.printStackTrace( System.err ); 
					else System.err.println( e );
					continue;
				}
				
				time += System.nanoTime();
				query.output( results, documentCollection, titleList, TextMarker.TEXT_BOLDFACE );				
				System.err.println( results.size() + " results; " + n + " documents examined; " + time / 1000000. + " ms; " + Util.format( ( n * 1000000000.0 ) / time ) + " documents/s, " + Util.format( time / (double)n ) + " ns/document" );
			}
			
		}
		finally {
			if ( httpQueryServer != null ) httpQueryServer.server.stop();
			if ( query.output != System.out ) query.output.close();
		}
	}
}
