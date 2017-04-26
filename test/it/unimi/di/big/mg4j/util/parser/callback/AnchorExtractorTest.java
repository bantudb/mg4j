package it.unimi.di.big.mg4j.util.parser.callback;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2016 Sebastiano Vigna 
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


import org.junit.Assert;
import org.junit.Test;

import it.unimi.dsi.parser.BulletParser;

public class AnchorExtractorTest {
	@Test
	public void testExtractor() {
		BulletParser parser = new BulletParser();
		AnchorExtractor anchorExtractor = new AnchorExtractor(0, 100,  0);
		parser.setCallback(anchorExtractor);
		parser.parse("<html>pre <a href=''>anchor</a> post</html>".toCharArray());
		Assert.assertEquals("anchor", anchorExtractor.anchors.get(0).text().toString());

		anchorExtractor = new AnchorExtractor(10, 100,  10);
		parser.setCallback(anchorExtractor);
		parser.parse("<html> pre <a href='x'>anchor</a> post </html>".toCharArray());
		Assert.assertEquals(" pre anchor post ", anchorExtractor.anchors.get(0).text().toString());

		anchorExtractor = new AnchorExtractor(10, 100,  10, "OXOXO");
		parser.setCallback(anchorExtractor);
		parser.parse("<html> pre <a href='x'>anchor</a> post </html>".toCharArray());
		Assert.assertEquals(" pre OXOXO anchor OXOXO post ", anchorExtractor.anchors.get(0).text().toString());
	}
}
