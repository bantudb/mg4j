#!/usr/bin/ruby

puts("gcc -E -C -P -DGENERIC -DCLASSNAME=BitStreamIndexReader -c src/it/unimi/di/big/mg4j/index/BitStreamIndexReader.c >src/it/unimi/di/big/mg4j/index/BitStreamIndexReader.java")

for skips in [ "-DSKIPS", "" ] do
	for frequencies in [ "GAMMA", "SHIFTED_GAMMA", "DELTA" ] do
		for pointers in [ "GAMMA", "SHIFTED_GAMMA", "DELTA", "GOLOMB" ] do
			for counts in [ "NONE", "UNARY", "GAMMA", "SHIFTED_GAMMA", "DELTA" ] do
				for positions in [ "NONE", "GAMMA", "SHIFTED_GAMMA", "DELTA", "GOLOMB", "SKEWED_GOLOMB", "INTERPOLATIVE" ] do
					if positions == "NONE" || counts != "NONE"; then
						classname = ( skips != "" ? "Skip" : "" ) + frequencies.capitalize + pointers.capitalize + counts.capitalize + positions.capitalize + "BitStreamIndexReader"
						puts("gcc -E -C -P " + skips + " -Afrequencies=" + frequencies + " -Apointers=" + pointers + " -Acounts=" + counts + " -Apositions=" + positions + " -DCLASSNAME=" + classname + " -c src/it/unimi/di/big/mg4j/index/BitStreamIndexReader.c >src/it/unimi/di/big/mg4j/index/wired/" + classname + ".java")
					end
				end
			end
			classname = ( skips != "" ? "Skip" : "" ) + frequencies.capitalize + pointers.capitalize + "PayloadBitStreamIndexReader"
			puts("gcc -E -C -P " + skips + " -Afrequencies=" + frequencies + " -Apointers=" + pointers + " -Acounts=NONE -Apositions=NONE -DPAYLOADS -DCLASSNAME=" + classname + " -c src/it/unimi/di/big/mg4j/index/BitStreamIndexReader.c >src/it/unimi/di/big/mg4j/index/wired/" + classname + ".java")
		end
	end
end

puts("gcc -E -C -P -DGENERIC -DCLASSNAME=BitStreamHPIndexReader -c src/it/unimi/di/big/mg4j/index/BitStreamHPIndexReader.c >src/it/unimi/di/big/mg4j/index/BitStreamHPIndexReader.java")

for frequencies in [ "GAMMA", "SHIFTED_GAMMA", "DELTA" ] do
	for pointers in [ "GAMMA", "SHIFTED_GAMMA", "DELTA", "GOLOMB" ] do
		for counts in [ "UNARY", "GAMMA", "SHIFTED_GAMMA", "DELTA" ] do
			for positions in [ "GAMMA", "SHIFTED_GAMMA", "DELTA" ] do
				classname = ( skips != "" ? "Skip" : "" ) + frequencies.capitalize + pointers.capitalize + counts.capitalize + positions.capitalize + "BitStreamHPIndexReader"
				puts("gcc -E -C -P " + skips + " -Afrequencies=" + frequencies + " -Apointers=" + pointers + " -Acounts=" + counts + " -Apositions=" + positions + " -DCLASSNAME=" + classname + " -c src/it/unimi/di/big/mg4j/index/BitStreamHPIndexReader.c >src/it/unimi/di/big/mg4j/index/wired/" + classname + ".java")
			end
		end
	end
end
