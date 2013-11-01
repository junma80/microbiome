#!/usr/bin/env ruby
  
#require 'brl/microbiome/workbench/RandomForestUtils'
require '/home/junm/gaussCode/brlheadmicrobiome/RandomForestUtils'
require 'fileutils'
require 'matrix.rb'
require "brl/util/textFileUtil"
require "brl/util/util"


def processArguments()
    # We want to add all the prop_keys as potential command line options
      optsArray = [ ['--inputTable','-f', GetoptLong::REQUIRED_ARGUMENT],
                    ['--inputMeta','-a', GetoptLong::REQUIRED_ARGUMENT],
                    ['--outputFolder','-o', GetoptLong::REQUIRED_ARGUMENT],
                    ['--metaLabel','-m', GetoptLong::REQUIRED_ARGUMENT],
                  ]
      progOpts = GetoptLong.new(*optsArray)
      usage("USAGE ERROR: some required arguments are missing") unless(progOpts.getMissingOptions().empty?)
      optsHash = progOpts.to_hash
      return optsHash
end

 
def usage(msg='')
    unless(msg.empty?)
      puts "\n#{msg}\n"
    end
    puts "

  PROGRAM DESCRIPTION:
   Microbiome workbench run QIIME pipeline 
   
  COMMAND LINE ARGUMENTS:
    --inputTable                         | -f => inputfolder
    --inputMeta				 | -a => inputMeta
    --outputFolder                       | -o => outputfolder
    --metaLabel                          | -m => metalabel


 usage:
  run_RandomForest_matrix.rb -f inputtable -a inputMeta -o projecttest/ -m Body_Site 

";
   exit;
end

settinghash=processArguments()
matrixFile=File.expand_path(settinghash["--inputTable"])
r=File.open(matrixFile,"r")
metahash=Hash.new{|h,k| h[k]=Hash.new(&h.default_proc)}
inputFilePath=File.expand_path(settinghash["--inputMeta"])
inputFile=File.open(inputFilePath,"r")

line=inputFile.gets.chop!
#get meta data names
colnames=line.split("\t")
metanames=[]
for ii in 0...colnames.size
    #puts "meta #{colnames[ii]}"
    metanames.push(colnames[ii])
end

#read in information for each sample
inputFile.each_line{ |line|
  line.chop!
  cols=line.split("\t")
  sampleName=cols[0]
  metadata=[]
  for ii in 1...cols.size
    metahash[metanames[ii]][sampleName]=cols[ii]
  end
}

feature=settinghash["--metaLabel"]
#check featuers in setting file
#for this tool, only metalables are provided
outDir = File.expand_path(settinghash["--outputFolder"])
attrhash=metahash[feature]

#optional filtering step if we have really large input
FileUtils.mkdir_p outDir
transMatrixFile=matrixFile.gsub(/txt/,"trans")
rfobject=RandomForestUtils.new(matrixFile,attrhash,feature,outDir)
transRF=rfobject.unfilteredmatrix.t  
transRF=rfobject.addmeta(transRF)
rfobject.printmatrixTofile(transRF,transMatrixFile)  
#run Randome Forest
rfobject.machineLearning(transMatrixFile,0,feature)
#run Boruta
rfobject.borutaFeatureSelection(transMatrixFile,0)

 
