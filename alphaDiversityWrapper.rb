#!/usr/bin/env ruby
require 'json'
require 'fileutils'
require 'cgi'
require 'brl/util/util'
require 'brl/util/emailer'
require 'gsl'
require 'brl/genboree/rest/apiCaller'


include GSL
include BRL::Genboree::REST


class AlphaDiversityWrapper
 
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
      
      @featurs = ""
      @featureLists = jsonObj["settings"]["featureList"]
      for i in 0...@featureLists.size
        @featurs << "#{@featureLists[i]},"
      end
      @featurs = @featurs.chomp(",")
      
      @jobName = jsonObj["settings"]["jobName"]
      @studyName = jsonObj["settings"]["studyName"]
      
      @renyiOffset = jsonObj["settings"]["renyiOffset"]
      @pngDensity = jsonObj["settings"]["pngDensity"]
      @legendPosition = jsonObj["settings"]["legendPosition"]
      @permutations = jsonObj["settings"]["permutations"]
      @renyiScale = jsonObj["settings"]["renyiScale"]
      @renyiScale = @renyiScale.split(',')
      @renyi = ""
      for i in 0...@renyiScale.size
        @renyi << "#{@renyiScale[i]},"
      end
      @renyi = @renyi.chomp(",")
      
      @richnessOffset2 = jsonObj["settings"]["richnessOffset2"]
      @rainbow = jsonObj["settings"]["rainbow"]
      @meta = jsonObj["settings"]["meta"]
      @richnessOffset = jsonObj["settings"]["richnessOffset"]
      @legendBoolChar = jsonObj["settings"]["legendBoolChar"]
      @height = jsonObj["settings"]["height"]
      @colors= jsonObj["settings"]["colors"]
      @colors = @colors.split(',')
      @color = ""
      for i in 0...@colors.size
        
        @color << "#{@colors[i]},"
      end
      @color = @color.chomp(",")
      @width = jsonObj["settings"]["width"]
      @legendMarkerSizeMod = jsonObj["settings"]["legendMarkerSizeMod"]
          
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
      @exitCode= ""   
      
      
      @fileNameBuffer = []
      @jobName1 = CGI.escape(@jobName)
      @jobName = @jobName1.gsub(/%[0-9a-f]{2,2}/i, "_")
      @studyName1 = CGI.escape(@studyName)
      @studyName = @studyName1.gsub(/%[0-9a-f]{2,2}/i, "_")
   end
   
  
   def work
      system("mkdir -p #{@scratch}")
      Dir.chdir(@scratch)
      @outputDir = "#{@scratch}/#{@jobName}"
      system("mkdir -p #{@outputDir}/otu_table")
      saveFileM = File.open("#{@outputDir}/otu_table/mapping.txt","w+")
      for i in 0...@input.size
         @input[i] = @input[i].chomp('?')
        saveFile = File.open("#{@outputDir}/otu_table/otu_table.txt","w+")
        puts "downloading otu tables from #{File.basename(@input[i])}"
        @db  = @dbhelper.extractName(@input[i])
        @grp = @grph.extractName(@input[i])
        @trk  = @trackhelper.extractName(@input[i])
        uri = URI.parse(@input[i])
        host = uri.host
        path = uri.path
        path = path.chomp('?')
        apicaller =ApiCaller.new(host,"",@user,@pass)
        path = path.gsub(/\/files\//,'/file/')
        pathR = "#{path}/otu.table/data?"
        apicaller.setRsrcPath(pathR)
        @buff = '' 
        httpResp = apicaller.get(){|chunck|
               saveFile.write chunck
         }                                          
        saveFile.close
        #downloading metadata file
        pathR = "#{path}/mapping.txt/data?"
        apicaller.setRsrcPath(pathR)
        track = 0
        httpResp = apicaller.get(){|chunck|
            lines = chunck.split(/\n/)
            track = lines.size
            if(i==0)
              saveFileM.print "#{lines[0]}\n" 
            end
            for ii in 1...lines.size
               saveFileM.print "#{lines[ii]}\n"
            end
         }
        saveFileM.close
      end
      if(track ==2)
        raise " Needs at least 2 samples for classification"
      end
      
      
      ##Calling qiime pipeline

      cmd = " module load R/2.11.1; run_alpha_diversity_pipeline_ARG.rb -f #{@outputDir}/otu_table -o #{@outputDir} -m '#{@featurs}' -c '#{@color}' -r '#{@renyi}' -p #{@permutations}"
      cmd <<" -n #{@legendPosition} -b #{@legendBoolChar} -k #{@legendMarkerSizeMod} -y #{@renyiOffset} -l #{@richnessOffset} -g #{@richnessOffset2} -h #{@height} "
      cmd <<" -w #{@width} -i #{@rainbow}  -j #{@pngDensity}  >#{@outputDir}/alpha.log 2>#{@outputDir}/alpha.error.log"
     
      $stdout.puts cmd
      sucess = true
      system(cmd)
       if(!$?.success?)
         sucess =false
            @exitCode = $?.exitstatus
            raise " alpha diversity script didn't run properly"
       end
       compression()
       if(!$?.success?)
         sucess =false  
            @exitCode = $?.exitstatus
            raise " compression didn't run properly"
       end
       upload()
       if(!$?.success?)
         sucess =false  
            @exitCode = $?.exitstatus
            raise "upload failed"
       end
       if(sucess == true)
         sendSEmail()
         $stdout.puts "sent"
       end
      
   end
   
    ##tar of output directory
   def compression
     Dir.chdir("#{@outputDir}/alphadiversity")
     system("tar czf raw.result.tar.gz * --exclude=*.pdf")
     system("tar czf richnessPlots.result.tar.gz `find 'richnessPlots' -name '*.pdf'`")
     system("tar czf rankAbundancePlots.result.tar.gz `find 'rankAbundancePlots' -name '*.pdf'`")
     system("tar czf renyiProfilePlots.result.tar.gz `find 'renyiProfilePlots' -name '*.pdf'`")
     Dir.chdir(@scratch)
     
   end
   
     ##uploading files on specified location
   def upload
     begin
          apicaller =ApiCaller.new(@hostOutput,"",@user,@pass)
          restPath = @pathOutput
          
           ##uplaoding otu table
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/raw.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/alphadiversity/raw.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded raw.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
          
          ##uplaoding otu table
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/richnessPlots.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/alphadiversity/richnessPlots.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded richnessPlots.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
             
             
              path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/rankAbundancePlots.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/alphadiversity/rankAbundancePlots.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded rankAbundancePlots.result.tar.gz "
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
             
             path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/renyiProfilePlots.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/alphadiversity/renyiProfilePlots.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded renyiProfilePlots.result.tar.gz "
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
             #uploading metadata file back
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/sample.mapping.txt/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@outputDir}/otu_table/mapping.txt","r")
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
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/AlphaDiversity/#{@jobName1}/settings.json/data"
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
    $stdout.puts "sending"
    body =
"
Hello #{@user_first.capitalize} #{@user_last.capitalize}

Your #{@toolTitle} job is completed successfully.

Job Summary:
   JobID                  : #{@jobID}
   Study Name             : #{CGI.unescape(@studyName)}
   Job Name               : #{CGI.unescape(@jobName)}
      
Result File Location in the Genboree Workbench:
   Group : #{@grpOutput}
   DataBase : #{@dbOutput}
   Path to File:
      Files
      * MicrobiomeData
         * #{CGI.unescape(@studyName)}
            *AlphaDiversity
               *#{CGI.unescape(@jobName1)}
               
               
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
  JobID                    : #{@jobID}
  Study Name               : #{CGI.unescape(@studyName)}
   Job Name                : #{CGI.unescape(@jobName)}
  
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
  
  
   def AlphaDiversityWrapper.usage(msg='')
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
        end #pwd 
      
      # Process Arguements form the command line input
      def AlphaDiversityWrapper.processArguements()
        # We want to add all the prop_keys as potential command line options
          optsArray = [ ['--jsonFile' ,'-j', GetoptLong::REQUIRED_ARGUMENT],
                        ['--help'      ,'-h',GetoptLong::NO_ARGUMENT]
                      ]
          progOpts = GetoptLong.new(*optsArray)
          AlphaDiversityWrapper.usage("USAGE ERROR: some required arguments are missing") unless(progOpts.getMissingOptions().empty?)
          optsHash = progOpts.to_hash
        
          Coverage if(optsHash.empty? or optsHash.key?('--help'));
          return optsHash
      end 

end
begin
optsHash = AlphaDiversityWrapper.processArguements()
performQCUsingFindPeaks = AlphaDiversityWrapper.new(optsHash)
performQCUsingFindPeaks.work()
 rescue => err
      $stderr.puts "Details: #{err.message}"
      $stderr.puts err.backtrace.join("\n")
     performQCUsingFindPeaks.sendFailureEmail(err.message)
end
#performQCUsingFindPeaks.compression()
#performQCUsingFindPeaks.upload()
