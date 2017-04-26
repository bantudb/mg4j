#!/usr/bin/ruby

# Generates a binary decision tree for finding the MSB of a signed type with ARGV[0] bits.
# You may specify in ARGV[1] an additional number of tabs to indent.

def rec(from,to,max,tabs,lev)
	if from == to then 
		if from == 0 then 
			print("\t"*(tabs+lev) + "x < 0 ? " + max.to_s + " : -1 /* " + (lev+1).to_s + " */\n")
		else 
			print("\t"*(tabs+lev) + ( from - 1 ).to_s + " /* " + lev.to_s + " */\n")
		end
		return
	end
	middle = (from + to)/2
	print("\t"*(tabs+lev) + "( x < 1<<" + middle.to_s + " ?\n")
	rec( from, middle, max, tabs, lev+1 )
	print("\t"*(tabs+lev) +  ":\n" )
	rec( middle + 1, to, max, tabs, lev+1 )
	print("\t"*(tabs+lev) + ")\n")
end

rec(0, ARGV[0].to_i, ARGV[0].to_i, ARGV[1].to_i, 0)
