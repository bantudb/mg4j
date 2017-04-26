package it.unimi.di.big.mg4j.document;

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

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.MultipleInputStream;
import it.unimi.dsi.io.NullInputStream;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.di.big.mg4j.document.DocumentCollection} corresponding to
 *  the result of a query in a relational database.
 * 
 * <p>An instance of this class is based on a query. The query should produce two
 * fixed columns: the first, named <samp>id</samp>, must be an increasing integer
 * which act as an identifier (i.e., as a key); the second, named <samp>title</samp>, must be a text field and will 
 * be used as a title. The remaining columns will be indexed, and the name of the corresponding
 * field will be the name of the column (use judiciously <samp>AS</samp>).
 *
 * <p>In complex queries, the specification <samp>id</samp> for the first column
 * could be ambiguous; in that case, you can provide an alternate (and hopefully more precise) specification.
 * 
 * <p>At construction time, the query is executed, obtaining a bijection between
 * the values of the identifier and document indices. The bijection is exposed by the
 * methods {@link #id2doc(int)} and {@link #doc2id(int)}. The class tolerates
 * additions to the database (and they will be skipped), but deletions will cause errors.
 * 
 * <P>This class provides a main method with a flexible syntax that serialises
 * a query into a document collection.
 */
public class JdbcDocumentCollection extends AbstractDocumentCollection implements Serializable {

	private static final long serialVersionUID = 1L;
	/** The map from database identifiers to documents. */
	final protected Int2IntMap id2doc;
	/** The map (as an array) from documents to database identifiers. */
	final protected int[] doc2id;
	/** The URI pointing at the database. */
	final protected String dbUri;
	/** Optionally, the driver. */
	transient Class<?> jdbcDriver;
	/** Optionally, the driver name. */
	final String jdbcDriverName;
	/** The factory to be used by this collection. */
	final protected DocumentFactory factory;
	/** The query generating the collection (without the <samp>SELECT</samp> keyword). */
	final protected String select;
	/** The spec for the id field; by default it is <samp>id</samp>, but in complex query it could be ambiguous. */
	final protected String idSpec;
	/** The <samp>WHERE</samp> part of the query generating 
	 * the collection (without the <samp>WHERE</samp> keyword), or <code>null</code>. */
	final protected String where;
	/** The currently open connection, if any. */
	protected transient Connection connection;
	
	/** Creates a document collection based on the result set of an SQL query using <samp>id</samp> as id specifier.
	 * 
	 * <p><strong>Beware.</strong> This class is not guaranteed to work if the database is
	 * deleted or modified after creation!
	 * 
	 * @param dbUri a JDBC URI pointing at the database.
	 * @param jdbcDriverName the name of a JDBC driver, or <code>null</code> if you do not want to load a driver.
	 * @param select the SQL query generating the collection (without the <samp>SELECT</samp> keyword), except for the <samp>WHERE</samp> part.
	 * @param where the <samp>WHERE</samp> part (without the <samp>WHERE</samp> keyword) of the SQL query generating the collection, or <code>null</code>.
	 * @param factory the factory that will be used to create documents.
	 */
	public JdbcDocumentCollection( final String dbUri, final String jdbcDriverName, final String select, final String where, final DocumentFactory factory ) throws SQLException, ClassNotFoundException {
		this( dbUri, jdbcDriverName, select, "id", where, factory );
	}

	/** Creates a document collection based on the result set of an SQL query.
	 * 
	 * <p><strong>Beware.</strong> This class is not guaranteed to work if the database is
	 * deleted or modified after creation!
	 * 
	 * @param dbUri a JDBC URI pointing at the database.
	 * @param jdbcDriverName the name of a JDBC driver, or <code>null</code> if you do not want to load a driver.
	 * @param select the SQL query generating the collection (without the <samp>SELECT</samp> keyword), except for the <samp>WHERE</samp> part.
	 * @param idSpec the complete SQL spec for the <samp>id</samp> (necessary for complex queries with multiple tables).
	 * @param where the <samp>WHERE</samp> part (without the <samp>WHERE</samp> keyword) of the SQL query generating the collection, or <code>null</code>.
	 * @param factory the factory that will be used to create documents.
	 */
	public JdbcDocumentCollection( final String dbUri, final String jdbcDriverName, final String select, final String idSpec, final String where, final DocumentFactory factory ) throws SQLException, ClassNotFoundException {
		this.dbUri = dbUri;
		this.jdbcDriverName = jdbcDriverName;
		this.select = select;
		this.idSpec = idSpec;
		this.where = where;
		this.factory = factory;
		
		initDriver();
		Connection connection = DriverManager.getConnection( dbUri );
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery( buildQuery( null ) );
		
		id2doc = new Int2IntOpenHashMap();
		id2doc.defaultReturnValue( -1 );
		final IntArrayList ids = new IntArrayList();
		int id;
		for( int i = 0; rs.next(); i++ ) {
			id = rs.getInt( 1 );
			ids.add( id );
			id2doc.put( i, id );
		}
		
		doc2id = ids.toIntArray();
		rs.close();
		s.close();
		connection.close();
	}

	protected void ensureConnection() throws SQLException {
		if ( connection == null ) connection = DriverManager.getConnection( dbUri ); 
	}
	
	public void close() throws IOException {
		super.close();
		if ( connection != null ) {
			try {
				connection.close();
				connection = null;
			}
			catch ( SQLException e ) {
				throw new IOException( e.toString() );
			}
		}
	}
	
	public JdbcDocumentCollection copy() {
		try {
			return new JdbcDocumentCollection( dbUri, jdbcDriverName, select, idSpec, where, factory.copy() );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}
	
	/** Creates a complete query using instance data and possibly an additional <samp>WHERE</samp> clause.
	 * 
	 * @param additionalWhere an additional condition for the <samp>WHERE</samp> clause.
	 * @return a complete query based on instance data and <code>additionalWhere</code>,
	 */
	
	private String buildQuery( final String additionalWhere ) {
		final MutableString query = new MutableString();
		query.append( "SELECT " ).append( select );
		if ( where == null && additionalWhere != null ) query.append( " WHERE (" ).append( additionalWhere ).append( ")" );
		if ( where != null && additionalWhere == null ) query.append( " WHERE (" ).append( where ).append( ")" );
		if ( where != null && additionalWhere != null ) query.append( " WHERE (" ).append( where ).append( ") AND (" ).append( additionalWhere ).append( ")" ); 
		query.append( " ORDER BY 1" );
		return query.toString();
	}
	
	private void initDriver() throws ClassNotFoundException {
		jdbcDriver = jdbcDriverName != null ? Class.forName( jdbcDriverName ) : null;
	}
	
	public DocumentFactory factory() {
		return factory;
	}
	
	public long size() {
		return doc2id.length;
	}

	public Document document( final long index ) throws IOException {
		final MutableString title = new MutableString();
		return factory.getDocument( stream( index, title ), metadata( index, title ) );
	}
	
	/** Returns the document associated with a given database identifier.
	 * 
	 * @param id a database identifier.
	 * @return the associated document.
	 */

	public int id2doc( final int id ) {
		return id2doc.get( id );
	}
	
	/** Returns the database identifier associated with a given document.
	 * 
	 * @param doc a document index.
	 * @return the associated database identifier.
	 */

	public int doc2id( final int doc ) {
		ensureDocumentIndex( doc );
		return doc2id[ doc ];
	}

	/** Creates metadata with the given title; if the title is not available, it is fetched from the database.
	 * 
	 * @param index a document index.
	 * @param title a suggested title, or <code>null</code>.
	 * @return the metadata for the document <code>index</code>.
	 */
	protected Reference2ObjectMap<Enum<?>,Object> metadata( final long index, CharSequence title ) {
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		if ( title == null ) {
			try {
				ensureConnection();
				final Statement s = connection.createStatement();
				final ResultSet rs = s.executeQuery( buildQuery( idSpec + "=" + doc2id[ (int)index ] ) );
				if ( ! rs.next() ) throw new IllegalStateException( "Id " + doc2id[ (int)index ] + " is no longer in the database" );
				title = rs.getString( 2 );
				rs.close();
				s.close();
			}
			catch ( SQLException e ) {
				throw new RuntimeException( e );
			}
		}
		metadata.put( MetadataKeys.TITLE, title );
		metadata.put( MetadataKeys.URI, Integer.toString( doc2id[ (int)index ] ) );
		return metadata;
	}

	public Reference2ObjectMap<Enum<?>,Object> metadata( final long index ) {
		ensureDocumentIndex( index );
		return metadata( index, null );
	}
		
	public InputStream stream( final long index ) throws IOException {
		return stream( index, null );
	}

	private InputStream getStreamFromResultSet( final ResultSet rs, final MutableString title ) throws SQLException {
		final InputStream[] a = new InputStream[ rs.getMetaData().getColumnCount() - 2 ]; // -2 for id and title
		for( int i = 0; i < a.length; i++ ) {
			a[ i ] = rs.getBinaryStream( i + 3 );
			if ( a[ i ] ==  null ) a[ i ] = NullInputStream.getInstance();
		}
		if ( title != null ) title.replace( rs.getString( 2 ) );
		return MultipleInputStream.getStream( a );
	}

	
	private InputStream stream( final long index, final MutableString title ) throws IOException {
		ensureDocumentIndex( index );
		try {
			ensureConnection();
			Statement s = connection.createStatement();
			
			// TODO: we might want at some point support >2^32 records.
			ResultSet rs = s.executeQuery( buildQuery( idSpec + "=" + doc2id[ (int)index ] ) );
			if ( ! rs.next() ) throw new IllegalStateException( "Id " + doc2id[ (int)index ] + " is no longer in the database" );
			return getStreamFromResultSet( rs, title );
		}
		catch ( SQLException e ) {
			throw new IOException( e.toString() );
		}
	}

	/** An iterator over the whole collection that performs a single DBMS transaction. */
	
	// ALERT: this is actually VERY inefficient, as metadata() makes a query.
	protected class JdbcDocumentIterator extends AbstractDocumentIterator {
		private final Statement s;
		private final ResultSet rs;
		private final MutableString title = new MutableString();
		private int index = 0;

		private JdbcDocumentIterator() throws SQLException {
			ensureConnection();
			s = connection.createStatement();
			rs = s.executeQuery( buildQuery( null ) );
		}

		public Document nextDocument() throws IOException {
			try {
				if ( ! rs.next() ) return null;
				while( rs.getInt( 1 ) < doc2id[ index ] ) rs.next();
				if ( rs.getInt( 1 ) > doc2id[ index ] ) throw new IllegalStateException( "Row with id " + doc2id[ index ] + " is missing" );
				return factory.getDocument( getStreamFromResultSet( rs, title ), metadata( index++, title ) );
			}
			catch ( SQLException e ) {
				throw new IOException( e.toString() );
			}
		}

		public void close() throws IOException {
			super.close();
			try {
				rs.close();
				s.close();
			}
			catch ( SQLException e ) {
				throw new IOException( e.toString() );
			}
		}
	}
	
	public DocumentIterator iterator() throws IOException {

		try {
			return new JdbcDocumentIterator();
		}
		catch ( SQLException e ) {
			throw new IOException( e.toString() );
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		initDriver();
	}
	
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException, SQLException, ClassNotFoundException, InstantiationException {

		SimpleJSAP jsap = new SimpleJSAP( JdbcDocumentCollection.class.getName(), "Saves a serialised document collection based on a set of database rows. The first column of the query is used as an integer id, and the second column for titles. Each remaining column is used to build a segmented input stream, which is passed to a ComposedDocumentFactory made of the specified factories.",
				new Parameter[] {
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "jdbcDriver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "driver", "The JDBC driver. You can omit it if it is already loaded." ),
					new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "One document factory for each indexed field." ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "fieldName", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "field-name", "One field name for each field in the composed document factory. If all specified factories have just one field, the name of the SQL column will be used as a default field name." ).setAllowMultipleDeclarations( true ),
					new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
					new UnflaggedOption( "dburi", JSAP.STRING_PARSER, JSAP.REQUIRED, "The JDBC URI defining the database." ),
					new UnflaggedOption( "select", JSAP.STRING_PARSER, JSAP.REQUIRED, "A SQL query generating the collection, except for the WHERE part." ),
					new FlaggedOption( "idSpec", JSAP.STRING_PARSER, "id", JSAP.NOT_REQUIRED, 'i', "id-spec", "An optional, more precise specification for the id field (the first column)." ),
					new FlaggedOption( "where", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'w', "where", "The the WHERE part (without the WHERE keyword) of the SQL query generating the collection." )				

				}
		);
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		// We run the query to get meta-information about the columns.
		@SuppressWarnings("unused") Class<?> jdbcDriver = Class.forName( jsapResult.getString( "jdbcDriver" ) );
		Connection connection = DriverManager.getConnection( jsapResult.getString( "dburi" ) );
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery( "SELECT " + jsapResult.getString( "select" ) );
		ResultSetMetaData metaData = rs.getMetaData();		
		String[] column = new String[ metaData.getColumnCount() - 2 ];
		for( int i = 3; i <= metaData.getColumnCount(); i++ ) column[ i - 3 ] = metaData.getColumnName( i );
		rs.close();
		s.close();
		connection.close();
		
		final DocumentFactory[] factory = new DocumentFactory[ column.length ];
		final Class<?>[] factoryClass = jsapResult.getClassArray( "factory" );
		final String[] property = jsapResult.getStringArray( "property" );
		for( int i = 0; i < factory.length; i++ ) { 
			factory[ i ] = PropertyBasedDocumentFactory.getInstance( factoryClass[ Math.min( i, factoryClass.length - 1 ) ], property );
			if ( factory[ i ].numberOfFields() > 1 && ! jsapResult.userSpecified( "fieldName" ) ) throw new IllegalArgumentException( "For factories with more than one field you must specify the name of each field of the composed factory" );
		}

		if ( jsapResult.userSpecified(  "fieldName" ) ) column = jsapResult.getStringArray( "fieldName" );
		
		BinIO.storeObject( new JdbcDocumentCollection( 
								jsapResult.getString( "dburi" ),
								jsapResult.getString( "jdbcDriver" ), 
								jsapResult.getString( "select" ),
								jsapResult.getString( "idSpec" ),
								jsapResult.getString( "where" ),
								CompositeDocumentFactory.getFactory( factory, column )
							), jsapResult.getString( "collection" ) );
	}
}
