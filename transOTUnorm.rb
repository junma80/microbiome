#!/usr/bin/env ruby

inotu=File.open(ARGV[0],"r")

outotufile=ARGV[0].gsub(/.txt/,"-normalized.txt")
outotu=File.open(outotufile,"w")

header1=inotu.gets
header=inotu.gets.strip
headercols=header.split("\t")
headercols.delete_at(0)
headercols.pop
count=0
poshash={}
headercols.each{|ele|
  poshash[count]=ele
  count+=1
}
rdphash={}
taxohash=Hash.new{|h,k| h[k]=Hash.new(&h.default_proc)}
inotu.each{|line|
  line.strip!
  cols=line.split("\t")
  otuid=cols[0]
  cols.delete_at(0)
  taxo=cols.pop
  rdphash[otuid]=taxo
  for jj in 0...cols.size()
    sampleid=poshash[jj]
    taxohash[otuid][sampleid] = cols[jj].to_f
  end 
}
outotu.puts header1
outotu.puts header

sumhash={}
poshash.each{|k,v|
  sumhash[v]=0
}

taxohash.each{|taxo, samplehash|
    samplehash.each{|sample,count|
      sumhash[sample]+=count
    }
}
taxohash.sort.each{|taxo,samplehash|
     line="#{taxo}"
     poshash.sort.each{|k,v|
      # puts "#{k}\t#{v}\t#{samplehash[v]}\t#{sumhash[v]}"
       if (! sumhash[v].nil? ) and sumhash[v] != 0
         value=samplehash[v]*1000000/sumhash[v]
         line="#{line}\t#{value.to_i}"
       else 
         line="#{line}\t0"
       end 
     }
     line="#{line}\t#{rdphash[taxo]}"
     outotu.puts line
}


