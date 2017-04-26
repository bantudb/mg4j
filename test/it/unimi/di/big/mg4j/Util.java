package it.unimi.di.big.mg4j;

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

import java.io.File;

import org.junit.Assert;

/** A static container of utility methods for test cases. */
public final class Util {

    /** The property specifying the data directory. */
    public static final String DATA_DIR = " it.unimi.di.big.mg4j.data";

    /** Cannot be instantiated. */
    private Util() {}
    
    /** Returns a derelativised version of the given filename.
     * 
     * <P>The general data directory must be specified in the
     * property <samp>it.unimi.dsi.law.data</samp>. The directory
     * must contain a <samp>test</samp> subdirectory (in which
     * <code>name</code> will be searched for).
     * 
     * <P>If <code>name</code> starts with a slash, it will
     * be derelativised w.r.t. the directory <samp>test</samp>
     * in <samp>it.unimi.dsi.law.data</samp>. Otherwise, 
     * the package name (with dots replaced by directory separator)
     * will be used additionally.
     *
     * <P>This class will directly {@link org.junit.Assert#fail()} 
     * the current test if the property is not defined. It will
     * return <code>null</code> if <samp>it.unimi.dsi.law.data</samp>
     * is the empty string. Thus, you should start your tests as follows:
     * 
     * <pre>
     * String filename = Util.getTestFile( ... );
     * if ( filename == null ) return;
     * </pre>
     *  
     * @param self the object performing the derelativisation.
     * @param name a filename (to be found in the test data directory).
     * @return the derelativised filename, or <code>null</code>
     * if <samp>it.unimi.dsi.law.data</samp> is not set.
     */
    public static String getTestFile( final Object self, final String name ) {
        
        final String dataDirName = System.getProperty( DATA_DIR );
        
        if ( dataDirName == null ) Assert.fail( DATA_DIR + " is not defined" );
        else if ( dataDirName.length() == 0 ) return null;
        
        File testDir = new File( dataDirName, "test" );
        
        if ( name.charAt( 0 ) != '/' ) {
            final String[] piece = self.getClass().getName().split( "\\." );
        
            File actualDir = testDir;
            // Note that we skip "test".
            for( int i = 1; i < piece.length - 1; i++ ) actualDir = new File( actualDir, piece[ i ] );
         
            testDir = actualDir;
        }
        
        return new File( testDir, name ).toString();
        
    }
 }
