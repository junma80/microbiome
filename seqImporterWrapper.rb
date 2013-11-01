#!/usr/bin/env ruby
require 'json'
require 'fileutils'
require 'cgi'
require 'brl/util/util'
require 'brl/util/emailer'
require 'gsl'
require 'brl/genboree/rest/apiCaller'
require 'brl/genboree/helpers/expander'

include GSL
include BRL::Genboree::REST


class SeqImporterWrapper

   def initialize(optsHash)
      @input    = File.expand_path(optsHash['--jsonFile'])
      jsonObj = JSON.parse(File.read(@input))
      @input  = jsonObj["inputs"]
      @output = jsonObj["outputs"][0]
     
      @gbConfFile = jsonObj["context"]["gbConfFile"]
      @apiDBRCkey = jsonObj["context"]["apiDbrcKey"]
      @scratch = jsonObj["context"]["scratchDir"]
      @email = jsonObj["context"]["userEmail"]
      @user_first = jsonObj["context"]["userFirstName"]
      @user_last = jsonObj["context"]["userLastName"]
      @username = jsonObj["context"]["userLogin"]
      @toolTitle = jsonObj["context"]["toolTitle"]
      @gbAdminEmail = jsonObj["context"]["gbAdminEmail"]
      @jobID = jsonObj["context"]["jobId"]
      @userId = jsonObj["context"]["userId"]
      
      @sampleSetName = jsonObj["settings"]["sampleSetName"]
      @minAvgQuality = jsonObj["settings"]["minAvgQuality"].to_i  
      @minSeqCount = jsonObj["settings"]["minSeqCount"].to_i
      @minSeqLength = jsonObj["settings"]["minSeqLength"].to_i
      @blastDistalPrimer1 = jsonObj["settings"]["blastDistalPrimer"]
      if(@blastDistalPrimer1 == true)
        @blastDistalPrimer = 1
      else
        @blastDistalPrimer = 0
      end
      
      @cutAtEnd1 = jsonObj["settings"]["cutAtEnd"]
      if(@cutAtEnd1 == true)
        @cutAtEnd = 1
      else
        @cutAtEnd = 0
      end
      
      @removeNSequences1 = jsonObj["settings"]["removeNSequences"]
      if(@removeNSequences1 == true)
        @removeNSequences = 1
      else
        @removeNSequences = 0
      end
      
      @trimLowQualityRun1 = jsonObj["settings"]["trimLowQualityRun"]
      if(@trimLowQualityRun1 == true)
        @trimLowQualityRun = 1
      else
        @trimLowQualityRun = 0
      end
      
      @sampleSetNameOriginal = @sampleSetName
      @sampleSetName1 = CGI.escape(@sampleSetName)
      @sampleSetName = @sampleSetName1.gsub(/%[0-9a-f]{2,2}/i, "_")
      
      @grph = BRL::Genboree::REST::Helpers::GroupApiUriHelper.new(@gbConfFile)
      @dbhelper = BRL::Genboree::REST::Helpers::DatabaseApiUriHelper.new(@gbConfFile)
      @trackhelper = BRL::Genboree::REST::Helpers::TrackApiUriHelper.new(@gbConfFile)
      
      ##pulling out upload location specifications
      @output = @output.chomp('?')
      @dbOutput = @dbhelper.extractName(@output)
      @grpOutput = @grph.extractName(@output)
      uriOutput = URI.parse(@output)
      @hostOutput = uriOutput.host
      @pathOutput = uriOutput.path
         
      @uri = @grph.extractPureUri(@output)
      dbrc = BRL::DB::DBRC.new(nil, @apiDBRCkey)
      @pass = dbrc.password
      @user = dbrc.user
      @uri = URI.parse(@input[0])
      @host = @uri.host
       
      @fileNameBuffer = []
      @sampleNameBuffer = []
      @exitCode = ""
      @hashTable = Hash.new{|hh,kk| hh[kk] = []}
      @localFilelocation = []
      @jointInfo = Struct.new(:index, :value)
   end
   
  
   def work
      system("mkdir -p #{@scratch}")
      Dir.chdir(@scratch)
      @outputDir = "#{@scratch}/seqImporter"
      system("mkdir -p #{@outputDir}")
      saveFile = File.open("#{@outputDir}/#{@sampleSetName}.local.metadata","w+")
      saveFile2 = File.open("#{@outputDir}/#{@sampleSetName}.metadata","w+")
      #saveFile.puts "sampleID\tsampleName\tbarcode\tminseqLength\tminAveQual\tminseqCount\tproximal\tdistal\tregion\tflag1\tflag2\tflag\3flag4\fileLocation\tTreatment\tBody_Site\tAge\tETHNIC\tSeq_center"
      for i in 0...@input.size
        puts "downloading sample file #{@input[i]}"
        @db  = @dbhelper.extractName(@input[i])
        @grp = @grph.extractName(@input[i])
        @trk  = @trackhelper.extractName(@input[i])
        uri = URI.parse(@input[i])
        host = uri.host
        path = uri.path
        path = path.chomp('?')
        apicaller =ApiCaller.new(host,"",@user,@pass)
        path = "#{path}?format=tabbed"
        apicaller.setRsrcPath(path)
        httpResp = apicaller.get(){|chunck|
            lines = chunck.split(/\n/)
            @headers = lines[0].split(/\t/, Integer::MAX32)
            @columns = lines[1].split(/\t/, Integer::MAX32)
            @headers[0] = "sampleName"
            for ii in 0 ...@headers.size
              ji = @jointInfo.new(i,@columns[ii])
              #@hashTable[@headers[ii]].push(@columns[ii])
              @hashTable[@headers[ii]].push(ji)
            end
            
            @fileLocation = @hashTable["fileLocation"][i][1].to_s
            @sampleNameBuffer[i] = @columns[0]
            puts @fileLocation
        }                                 
         ##Downloading file from give location in matadata to server
                  @dbF  = @dbhelper.extractName(@fileLocation)
                  @grpF = @grph.extractName(@fileLocation)
                  puts @fileLocation
                  uriF = URI.parse(@fileLocation)
                  hostF = uriF.host
                  apicallerF =ApiCaller.new(hostF,"",@user,@pass)
                  pathF = uriF.path + "/data"
                  apicallerF.setRsrcPath(pathF)
                  filenNameF = File.basename(@fileLocation)
                  @fileNameBuffer[i] = filenNameF
                  saveFileF = File.open("#{@outputDir}/#{filenNameF}","w+")
                  @localFilelocation[i] = "#{@outputDir}/#{filenNameF}"
                  
                  @buff = '' 
                  httpRespF = apicallerF.get(){ |chunck1|
                    fullChunk = "#{@buff}#{chunck1}"           
                    @buff = ''
           
                    fullChunk.each_line{ |line|
                      if(line[-1].ord == 10)
                        saveFileF.write line
                      else
                        @buff += line
                      end
                      }
                    }
                   seqCenter = "WUGSC"
               saveFileF.close
             ##uncompressing sff file
             expanderObj = BRL::Genboree::Helpers::Expander.new("#{@outputDir}/#{filenNameF}")
             if(compressed = expanderObj.isCompressed?("#{@outputDir}/#{filenNameF}"))
                expanderObj.extract(desiredType = 'text')
             end  

      end
      vv = 0
      @hashTable.delete("biomaterialProvider")
      @hashTable.delete("biomaterialState")
      @hashTable.delete("biomaterialSource")
      @hashTable.delete("type")
      @hashTable.delete("state")
    
      saveFile.print "sampleID\tsampleName\tbarcode\tminseqLength\tminAveQual\tminseqCount\tproximal\tdistal\tregion\tflag1"
      saveFile.print "\tflag2\tflag3\tflag4\tfileLocation"
      saveFile2.print "sampleID\tsampleName\tbarcode\tminseqLength\tminAveQual\tminseqCount\tproximal\tdistal\tregion\tflag1"
      saveFile2.print "\tflag2\tflag3\tflag4\tfileLocation"
      
      @hashTable.each {|k,v| 
         unless(k == "sampleName" or k == "barcode" or k == "proximal" or k == "distal" or k == "region" or k == "fileLocation")
          saveFile.print "\t#{k}"
          saveFile2.print "\t#{k}"
         end
      }
     
     saveFile.print "\n"
     saveFile2.print "\n"
      
      foundValue = true
      for i in 0...@input.size
         saveFile.print "#{@hashTable["sampleName"][i].value}\t#{@hashTable["sampleName"][i].value}\t#{@hashTable["barcode"][i].value}\t"
         saveFile.print "#{@minSeqLength}\t#{@minAvgQuality}\t#{@minSeqCount}\t#{@hashTable["proximal"][i].value}\t#{@hashTable["distal"][i].value}\t"
         saveFile.print "#{@hashTable["region"][i].value}\t#{@cutAtEnd}\t#{@blastDistalPrimer}\t#{@removeNSequences}\t#{@trimLowQualityRun}\t"
         saveFile.print "#{@localFilelocation[i]}"
         
         saveFile2.print "#{@hashTable["sampleName"][i].value}\t#{@hashTable["sampleName"][i].value}\t#{@hashTable["barcode"][i].value}\t"
         saveFile2.print "#{@minSeqLength}\t#{@minAvgQuality}\t#{@minSeqCount}\t#{@hashTable["proximal"][i].value}\t#{@hashTable["distal"][i].value}\t"
         saveFile2.print "#{@hashTable["region"][i].value}\t#{@cutAtEnd}\t#{@blastDistalPrimer}\t#{@removeNSequences}\t#{@trimLowQualityRun}\t"
         saveFile2.print "#{@hashTable["fileLocation"][i].value}"
         
      @hashTable.each{ |k,v|
         
            for jj in 0...@hashTable[k].size
              unless(k == "sampleName" or k == "barcode" or k == "proximal" or k == "distal" or k == "region" or k == "fileLocation") 
               if(@hashTable[k][jj].index == i)
                 saveFile.print "\t#{@hashTable[k][jj].value}"
                 saveFile2.print "\t#{@hashTable[k][jj].value}"
                 foundValue = true
                 break;
               end
              end
            end
            if(foundValue == false)
              saveFile.print "\tnoValue"
              saveFile2.print "\tnoValue"
            end

      }
      saveFile.print "\n"
      saveFile2.print "\n"
       end      
      
      saveFile.close
      saveFile2.close
      sucess = true
      system(" run_readsfilter.rb #{@outputDir}/#{@sampleSetName}.local.metadata  #{@outputDir} >>#{@outputDir}/seqImporter.log 2>>#{@outputDir}/seqImporter.error.log")
      if(!$?.success?)
            @exitCode = $?.exitstatus
            raise " readsfilter script didn't run properly"
      end
      compression()
       if(!$?.success?)
          sucess = false
            @exitCode = $?.exitstatus
            raise " compression didn't run properly"
       end
       upload()
       if(!$?.success?)
          sucess = false
            @exitCode = $?.exitstatus
            raise "upload failed"
       end
       if( sucess == true)
          sendSEmail()
       end
       
      
   end
   
   ##tar of output directory
   def compression
     for i in 0...@localFilelocation.size
       File.delete(@localFilelocation[i])
     end
     Dir.chdir(@outputDir)
     #system("tar -zcf #{@sampleSetName1}.tar.gz * --exclude=*.log --exclude=*.sra --exclude=*.sff --exclude=*.local.metadata")    
     system("tar czf fasta.result.tar.gz `find . -name '*.fasta'`")
     system("tar czf filtered_fasta.result.tar.gz `find . -name '*.fa'`")
     system("tar czf stats.result.tar.gz `find . -name '*stat'`")
     system("tar czf fastq.result.tar.gz `find . -name '*.fq'`")
     Dir.chdir(@scratch)
     
   end
   
   ##uploading files on specified location
   def upload
     begin
          apicaller =ApiCaller.new(@hostOutput,"",@user,@pass)
          restPath = @pathOutput
          
          ##uplaoding fasta tarred files
            path = restPath +"/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/fasta.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/fasta.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded fasta.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
          ##uplaoding stats tarred files   
            path = restPath +"/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/stats.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/stats.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded stats.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
           
           ##Uploading filtered fasta files  
            path = restPath +"/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/filtered_fasta.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/filtered_fasta.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded filtered_fasta.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
           ##Uploading fastq files  
            path = restPath +"/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/fastq.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/fastq.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded fastq.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end  
            
          #uploading metadata file back
           path = restPath + "/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/sample.metadata/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@outputDir}/#{@sampleSetName}.metadata","r")
           apicaller.put(infile)
            if apicaller.succeeded?
               $stdout.puts "successfully uploaded metadata file "
            else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
             raise "#{apicaller.apiStatusObj['msg']}"
            end
            
          ##uploading json setting file
           path = restPath + "/file/MicrobiomeData/#{CGI.escape(@sampleSetNameOriginal)}/settings.json/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@scratch}/jobFile.json","r")
           apicaller.put(infile)
            if apicaller.succeeded?
               $stdout.puts "successfully uploaded jsonfile file "
            else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
             raise "#{apicaller.apiStatusObj['msg']}"
            end
            
     end
     
     end
     
     def sendSEmail()
      body =
"
Hello #{@user_first.capitalize} #{@user_last.capitalize}

Your #{@toolTitle} job is completed successfully.

Job Summary:
   JobID                  : #{@jobID}
   Analysis Name          : #{@sampleSetNameOriginal}
   
Settings:
   minAvgQuality           : #{@minAvgQuality}
   minSeqCount             : #{@minSeqCount}
   minSeqLength            : #{@minSeqLength}
   blastDistalPrimer       : #{@cutAtEnd1}
   cutAtEnd                : #{@blastDistalPrimer1}
   trimLowQualityRun       : #{@trimLowQualityRun1}
   removeNSequences        : #{@removeNSequences1}
   
      
Result File Location in the Genboree Workbench:
   Group : #{@grpOutput}
   DataBase : #{@dbOutput}
   Path to File:
      Files
      * MicrobiomeData
         * #{@sampleSetNameOriginal}
           

The Genboree Team"

      subject = "Genboree: Your #{@toolTitle} job is complete "
        if (!@email.nil?) then
             sendEmail(subject,body)
        end
   end
   
        def sendFailureEmail(errMsg)
         
          body =
"
Hello #{@user_first.capitalize} #{@user_last.capitalize}

Your #{@toolTitle} job is unsucessfull.

Job Summary:
  JobID : #{@jobID}
  Analysis Name : #{@sampleSetNameOriginal}
  
      Error Message : #{errMsg}
      Exit Status   : #{@exitCode}
Please Contact Genboree team with above information. 
        

The Genboree Team"

      subject = "Genboree: Your #{@toolTitle} job is unsuccessfull "
        
         if (!@email.nil?) then
             sendEmail(subject,body)
           end
     
   end
  
  ##Email
  def sendEmail(subjectTxt, bodyTxt)
    email = BRL::Util::Emailer.new()
    email.setHeaders("genboree_admin@genboree.org", @email, subjectTxt)
    email.setMailFrom('genboree_admin@genboree.org')
    email.addRecipient(@email)
    email.addRecipient("genboree_admin@genboree.org")
    email.setBody(bodyTxt)
    email.send()
  end
  
  
   def SeqImporterWrapper.usage(msg='')
          unless(msg.empty?)
            puts "\n#{msg}\n"
          end
          puts "
      
        PROGRAM DESCRIPTION:
           seqImporter wrapper for microbiome workbench
        COMMAND LINE ARGUMENTS:
          --file         | -j => Input json file
          --help         | -h => [Optional flag]. Print help info and exit.
      
       usage:
       
      ruby removeAdaptarsWrapper.rb -f jsonFile  
      
        ";
            exit;
        end # 
      
      # Process Arguements form the command line input
      def SeqImporterWrapper.processArguements()
        # We want to add all the prop_keys as potential command line options
          optsArray = [ ['--jsonFile' ,'-j', GetoptLong::REQUIRED_ARGUMENT],
                        ['--help'      ,'-h',GetoptLong::NO_ARGUMENT]
                      ]
          progOpts = GetoptLong.new(*optsArray)
          SeqImporterWrapper.usage("USAGE ERROR: some required arguments are missing") unless(progOpts.getMissingOptions().empty?)
          optsHash = progOpts.to_hash
        
          Coverage if(optsHash.empty? or optsHash.key?('--help'));
          return optsHash
      end 

end

begin
optsHash = SeqImporterWrapper.processArguements()
performQCUsingFindPeaks = SeqImporterWrapper.new(optsHash)
performQCUsingFindPeaks.work()
rescue => err
      $stderr.puts "Details: #{err.message}"
      $stderr.puts err.backtrace.join("\n")
     performQCUsingFindPeaks.sendFailureEmail(err.message)
end