package it.unimi.di.big.mg4j.query;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.BigList;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.ExtendedProperties;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;
import org.apache.velocity.tools.view.servlet.VelocityViewServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A query servlet.
 * 
 * <p>This class provides a basic servlet for searching a collection.
 * It expects some data (a collection, an index map and a path) 
 * in the {@link javax.servlet.ServletContext} (see the code for {@link #init()}). It
 * can be used to search in a collection, but it is essentially a worked-out example.
 * 
 * <p>The three parameters are <samp>q</samp>, the query, <samp>m</samp>, the maximum
 * number of results to be displayed, and <samp>s</samp>, the first result to be displayed.
 * 
 * <p>Usually, the URI associated with each result is taken from the collection. Alternatively, each
 * result will point to the <samp>/Item</samp> path with some query arguments (<samp>doc</samp>, containing
 * the document pointer, <samp>uri</samp>, containing the original URI, and <samp>m</samp>, containing
 * an optional suggested MIME type). See, for instance, {@link it.unimi.di.big.mg4j.query.GenericItem} and {@link it.unimi.di.big.mg4j.query.InputStreamItem}.
 * 
 * <p>The Velocity template used by this servlet can be set using the initialisation parameter
 * <samp>template</samp> (or using a context attribute with the same name). If you're using
 * this servlet via {@link HttpQueryServer}, please read the documentation therein for
 * information about template resolution order.
 * 
 * <p>This servlet is thread safe. Each instance uses its own flyweight copies of the
 * {@linkplain it.unimi.di.big.mg4j.document.DocumentCollection collection} and
 * {@linkplain it.unimi.di.big.mg4j.query.QueryEngine query engine} to return the result (in particular, snippets). In a production
 * site it might be more sensible to pool and reuse such classes.
 * 
 * <p><strong>Warning</strong>: the {@link #loadConfiguration(ServletConfig)} method initialises
 * Velocity with some default parameters: in particular, template resolution is performed first on the classpath, then relatively to the current directory, and
 * finally using absolute pathnames. Watch out for template resolution issues.
 */
public class QueryServlet extends VelocityViewServlet {
	private static final long serialVersionUID = 1L;

	private final static Logger LOGGER = LoggerFactory.getLogger( QueryServlet.class );
	/** Standard maximum number of items to be displayed (may be altered with the <samp>m</samp> query parameter). */
	private final static int STD_MAX_NUM_ITEMS = 10;
	/** The default Velocity template used by this servlet; may be overriden in the context using an attribute named <samp>template</samp>. */
	protected final static String DEFAULT_TEMPLATE = "it/unimi/di/big/mg4j/query/query.velocity";
	/** The actual template used by this servlet (default: {@link #DEFAULT_TEMPLATE}). */
	protected String template;
	/** The query engine. */
	protected QueryEngine queryEngine;
	/** The document collection. */
	protected DocumentCollection documentCollection;
	/** An optional title list if the document collection is not present. */
	protected BigList<? extends CharSequence> titleList;
	/** A sorted map from index names to indices: the first entry is the default index. */
	protected Object2ReferenceMap<String,Index> indexMap;
	/** The indices of the fields specified in the index map, in increasing order (for document access).  */
	private Index[] sortedIndex;
	/** If not <code>null</code>, a MIME type suggested to the servlet. */
	private String urlEncodedMimeType;
	/** If true, the link associated with each item must be built using the document URI. */
	private boolean useUri;
	/** If true, URIs are files that should be derelativised. */
	private boolean derelativise;
	
	@Override
	protected ExtendedProperties loadConfiguration( final ServletConfig config ) throws FileNotFoundException, IOException {
		return HttpQueryServer.setLiberalResourceLoading( super.loadConfiguration( config ) );
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init() throws ServletException {
		super.init();
		ServletContext context = getServletContext();

		if ( ( template = (String)getServletContext().getAttribute( "template" ) ) == null &&
				( template = getInitParameter( "template" ) ) == null ) template = DEFAULT_TEMPLATE; 

		queryEngine = (QueryEngine)context.getAttribute( "queryEngine" );
		documentCollection = (DocumentCollection)context.getAttribute( "collection" );
		titleList = (BigList<? extends CharSequence>)context.getAttribute( "titleList" );
		indexMap = queryEngine.indexMap;
		try {
			urlEncodedMimeType = URLEncoder.encode( (String)context.getAttribute( "mimeType" ), "UTF-8" );
		}
		catch ( UnsupportedEncodingException cantHappen ) {
			throw new RuntimeException( cantHappen );
		}
		useUri = context.getAttribute( "uri" ) == Boolean.TRUE;
		derelativise = context.getAttribute( "derelativise" ) == Boolean.TRUE;

		if ( documentCollection != null ) {
			sortedIndex = new Index[ indexMap.size() ];
			indexMap.values().toArray( sortedIndex );
			Arrays.sort( sortedIndex, new Comparator<Index>() {
				public int compare( final Index x, final Index y ) {
					return documentCollection.factory().fieldIndex( x.field ) - documentCollection.factory().fieldIndex( y.field );
				}
			});
		}
	}
	
	public Template handleRequest( final HttpServletRequest request, final HttpServletResponse response, final Context context ) {
    
		try {
			response.setCharacterEncoding( "UTF-8" );
			
			// This string is URL-encoded, and with the wrong coding.
			//String query = request.getParameter( "q" ) != null ? new String( request.getParameter( "q" ).getBytes( "ISO-8859-1" ), "UTF-8" ) : null;
			String query = request.getParameter( "q" );
			context.put( "action", request.getContextPath() + request.getServletPath() );
			
			// Sanitise parameters.
			int start = 0, maxNumItems = STD_MAX_NUM_ITEMS;
			try { maxNumItems = Integer.parseInt( request.getParameter( "m" ) ); } catch( NumberFormatException dontCare ) {}
			try { start = Integer.parseInt( request.getParameter( "s" ) ); } catch( NumberFormatException dontCare ) {}
			
			if ( maxNumItems < 0 || maxNumItems > 1000 ) maxNumItems = STD_MAX_NUM_ITEMS;
			if ( start < 0 ) start = 0;
        	
			if ( query != null && query.length() != 0 ) {
				
				// This is used to display again the query in the input control.
				context.put( "q", StringEscapeUtils.escapeHtml( query ) );
				// This is used to put the query in URLs.
				context.put( "qUrl", URLEncoder.encode( query, "UTF-8" ) );
				context.put( "firstItem", new Integer( start ) );

				// First of all, we check that the query is correct

				long time = -System.currentTimeMillis();
				ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>> results = new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();

				int globNumItems;

				try {
					globNumItems = queryEngine.copy().process( query, start, maxNumItems, results );
				}
				catch( QueryBuilderVisitorException e ) {
					context.put( "errmsg", StringEscapeUtils.escapeHtml( e.getCause().toString() ) );
					return getTemplate( template );
				}
				catch( QueryParserException e ) {
					context.put( "errmsg", StringEscapeUtils.escapeHtml( e.getCause().toString() ) );
					return getTemplate( template );
				}
				catch( Exception e ) {
					context.put( "errmsg", StringEscapeUtils.escapeHtml( e.toString() ) );
					return getTemplate( template );
				}

				time += System.currentTimeMillis();

				ObjectArrayList<ResultItem> resultItems = new ObjectArrayList<ResultItem>();

				if ( ! results.isEmpty() ) {
					SelectedInterval[] selectedInterval = null;

					final DocumentCollection collection = documentCollection != null ? documentCollection.copy() : null;

					for( int i = 0; i < results.size(); i++ ) {
						DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>> dsi = results.get( i );
						LOGGER.debug( "Intervals for item " + i );
						final ResultItem resultItem = new ResultItem( dsi.document, dsi.score );
						resultItems.add( resultItem );

						if ( collection != null ) {
							final Document document = collection.document( dsi.document );
							// If both collection and title list are present, we override the collection title (cfr. Query)
							resultItem.title = StringEscapeUtils.escapeHtml( titleList != null ? titleList.get( resultItem.doc ).toString() : document.title().toString() );
							if ( useUri ) {
								if ( document.uri() != null ) resultItem.uri = StringEscapeUtils.escapeHtml( document.uri().toString() );
							}
							else {
								if ( document.uri() != null ) {
									String stringUri = document.uri().toString();
									// TODO: this is a quick patch to get the file server running with relative files
									final String documentUri = URLEncoder.encode( derelativise
									? new File( stringUri.startsWith( "file:" ) ? stringUri.substring( 5 ) : stringUri ).getAbsoluteFile().toURI().toASCIIString()
											: document.uri().toString(), "UTF-8" );
									resultItem.uri = StringEscapeUtils.escapeHtml( "./Item?doc=" + resultItem.doc + "&m=" + urlEncodedMimeType + "&uri=" + documentUri );
								}
								else resultItem.uri = StringEscapeUtils.escapeHtml( "./Item?doc=" + resultItem.doc + "&m=" + urlEncodedMimeType );
							}
							
							MarkingMutableString snippet = new MarkingMutableString( TextMarker.HTML_STRONG, MarkingMutableString.HTML_ESCAPE ); 
							
							for( int j = 0; j < sortedIndex.length; j++ ) {
								if ( ! sortedIndex[ j ].hasPositions || dsi.info == null ) continue;
								selectedInterval = dsi.info.get( sortedIndex[ j ] );
								if ( selectedInterval != null ) {
									final int field = documentCollection.factory().fieldIndex( sortedIndex[ j ].field );
									// If the field is not present (e.g., because of parallel indexing) or it is not text we skip
									if ( field == -1 || documentCollection.factory().fieldType( field ) != DocumentFactory.FieldType.TEXT ) continue;
									LOGGER.debug( "Found intervals for " + sortedIndex[ j ].field + " (" + field + ")" );
									final Reader content = (Reader)document.content( field );
									snippet.startField( selectedInterval ).appendAndMark( document.wordReader( field ).setReader( content ) ).endField();
								}
								if ( LOGGER.isDebugEnabled() ) LOGGER.debug( sortedIndex[ j ].field + ": " + ( selectedInterval == null ? null : Arrays.asList( selectedInterval ) ) ); 
								document.close();
							}
							
							resultItem.text = snippet; 
						}
						else {
							if ( titleList != null ) {
								// TODO: this is a bit radical
								resultItem.title = resultItem.uri = titleList.get( resultItem.doc );
							}
							else {
								resultItem.title = "Document #" +  resultItem.doc;
								resultItem.uri = new MutableString( "./Item?doc=" ).append( resultItem.doc ).append( "&m=" ).append( urlEncodedMimeType );
							}
							
							MutableString text = new MutableString();
							for( Iterator<Index> j = indexMap.values().iterator(); j.hasNext(); ) {
								final Index index = j.next();
								selectedInterval = dsi.info.get( index );
								if ( selectedInterval != null )
									text.append( "<p>" ).append( index.field ).append( ": " ).append( Arrays.asList( selectedInterval ) );
								LOGGER.debug( index.field + ": " + ( selectedInterval == null ? null : Arrays.asList( selectedInterval ) ) ); 
							}
							resultItem.text = text;
						}
					}
					
					if ( collection != null ) collection.close();
				}

				
				// Note that if we pass an array to the template we lose the possibility of measuring its length.
				context.put( "result", resultItems );
				/* Note that this number is just the number of relevant documents met while
				   trying to obtain the current results. Due to the short-circuit semantics of the
				   "and then" operator, it  might not reflect accurately the overall number of
				   results of the query. */
				context.put( "globNumItems", new Integer( globNumItems ) );
				context.put( "start", new Integer( start ) );
				context.put( "maxNumItems", new Integer( maxNumItems ) );
				context.put( "time", new Integer( (int)time ) );
				context.put( "speed", new Long( (int)( globNumItems * 1000L / ( time + 1 ) ) ) );
			}

			return getTemplate( template );
		}
		catch( Exception e ) { 
			e.printStackTrace( System.err );
			return null;
		}
	}
}
