infile=File.open(ARGV[0],"r")
inotu=File.open(ARGV[1],"r")
outotu=File.open(ARGV[2],"w")

outotu.puts inotu.gets
outotu.puts inotu.gets 
idhash={}

infile.each{|line|
	line.strip!
	cols=line.split("\t")
	id=cols[0]
	if line !~ /None/
		taxo=cols[1]
		idhash[id]="Root;#{taxo}"
	else 
		idhash[id]="Root"
	end
}
inotu.each{|line|
	line.strip!
	cols=line.split("\t")
	id=cols[0]
	if idhash.key?(id)
		cols.pop
		cols << idhash[id]
		newline=cols.join("\t")
		outotu.puts newline
	end
}
inotu.close()
infile.close()
outotu.close()
