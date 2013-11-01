#!/usr/bin/env ruby

inotu=File.open(ARGV[0],"r")
inmapping=File.open(ARGV[1],"r")
meta=ARGV[2]
mheader=inmapping.gets.strip!
mheadercols=mheader.split("\t")
metapos=mheadercols.index(meta)
metahash={}
inmapping.each{|line|
  line.strip!
  cols=line.split("\t")
  metahash[cols[0]]=cols[metapos]
}

outtaxofile=ARGV[0].gsub(/.txt/,"-LEfSe.txt")
outtaxo=File.open(outtaxofile,"w")

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
  if taxo !~ /Root/
    taxo="Root;#{taxo}"
  end 
  rdphash[otuid]=taxo
  taxocols=taxo.split(";")
  taxoid="Root"
  for ii in 1...taxocols.size()
    if ! taxocols[ii].nil? 
       taxoid="#{taxoid}|#{taxocols[ii]}"
       if ! taxohash.has_key?(taxoid)
          poshash.each{|k,v|
            taxohash[taxoid][v]=0
            taxohash[otuid][v]=0
          }
       end
    end
    for jj in 0...cols.size()
       sampleid=poshash[jj]
       taxohash[taxoid][sampleid] += cols[jj].to_f
    end 
  end
  for jj in 0...cols.size()
    sampleid=poshash[jj]
    taxohash[otuid][sampleid] = cols[jj].to_f
  end 
}

#outotu.puts header1
#outotu.puts header

sumhash={}
poshash.each{|k,v|
  sumhash[v]=0
}

taxohash.each{|taxo, samplehash|
 
  if taxo !~ /^Root/
    samplehash.each{|sample,count|
      sumhash[sample]+=count
    }
  end 
}
=begin

taxohash.sort.each{|taxo,samplehash|
  if taxo =~ /^Consensus/
     line="#{taxo}"
     poshash.sort.each{|k,v|
      # puts "#{k}\t#{v}\t#{samplehash[v]}\t#{sumhash[v]}"
       value=samplehash[v]*100000/sumhash[v]
       line="#{line}\t#{value.to_i}"
     }
     line="#{line}\t#{rdphash[taxo]}"
     outotu.puts line
  end
}
=end 

header.gsub!(/#OTU ID/,"id")
header.gsub!(/Consensus Lineage/,"")

metaline=meta
poshash.sort.each{|k,v|
  metaline="#{metaline}\t#{metahash[v]}"
}
outtaxo.puts metaline
outtaxo.puts header

taxohash.sort.each{|taxo,samplehash|
   if taxo != /^Root/
      modtaxo=rdphash[taxo]
      if ! modtaxo.nil?
        modtaxo.gsub!(/;/,"|")
        modtaxo.gsub!(/"/,"")
        taxo="#{modtaxo}-#{taxo}"
      end
   end 
   line="#{taxo}"
   poshash.sort.each{|k,v|
   # puts "#{k}\t#{v}\t#{samplehash[v]}\t#{sumhash[v]}"
   value=samplehash[v]/sumhash[v]
     line="#{line}\t#{value}"
   }
   outtaxo.puts line
}

