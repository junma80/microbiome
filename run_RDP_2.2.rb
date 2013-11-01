#!/usr/bin/env ruby
require "fileutils"

def usage()
  if ARGV.size != 2 || ARGV[0] =~ /--help/
    $stderr.puts "--USAGE----------------------------"
    $stderr.print "ruby run_RDP_2.2.rb <the location of input FASTA file>"
    $stderr.print " <output folder directory> \n"
    $stderr.puts "-----------------------------------"
    exit
  end
end


file =ARGV[0]

outFolder = File.expand_path(ARGV[1]) + "/"
FileUtils.mkdir_p "#{ARGV[1]}"

cmd = "/opt/jdk/1.6/bin/java -Xmx1g -jar /cluster.shared/local/bin/rdp_classifier-2.2.jar"

inFile = file.strip
fullyQfile = File.expand_path(inFile)
outFile = outFolder + File.basename(inFile) + ".ignore."

system("#{cmd} -q #{fullyQfile} -o #{outFile}")
