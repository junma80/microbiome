#!/usr/bin/env ruby

inputfolder=ARGV[0]

files=`find #{inputfolder} -name "*coords.txt"`.to_a
pcahash={}

files.each{|filepath|
   filepath.strip!
   line= `cat #{filepath} | grep "variation explained"`
   cols=line.split("\t")
   sum=cols[1].to_f+cols[2].to_f+cols[3].to_f
   basename=File.basename(filepath)
   basename.gsub!(/_coords.txt/,"")
   pcahash[basename]=sum
}

pcahash.sort{|a,b| b[1] <=> a[1]}.each{|elem|
  puts "#{elem[0]}\t#{elem[1]}"
}
