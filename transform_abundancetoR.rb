#!/usr/bin/env ruby
  
require 'fileutils'
require 'matrix.rb'
require "brl/util/util"


alldata=[]
matrixfile=File.open(ARGV[0], "r")
matrixfile.each_line{ |line|
  line.strip!
  cols=line.split(/\t/)
  alldata << cols
}
matrixfile.close()
matrixinput=Matrix.rows(alldata)
inputmatrix=matrixinput.t

outfile=File.open(ARGV[1],"w")
for ii in 0...inputmatrix.row_size
    line=""
    for jj in 0...inputmatrix.column_size
        line="#{line}\t#{inputmatrix.[](ii,jj)}"
    end
        outfile.puts line.strip!
end
outfile.close()


 
