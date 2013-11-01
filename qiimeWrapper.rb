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


class QiimeWrapper

   def initialize(optsHash)
      @input    = File.expand_path(optsHash['--jsonFile'])
      jsonObj = JSON.parse(File.read(@input))
      @input  = jsonObj["inputs"]
      @outputArray = jsonObj["outputs"]
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
      
      @createTaxaSummaries = jsonObj["settings"]["createTaxaSummaries"]
      @runAlphaDiversityFlag = jsonObj["settings"]["runAlphaDiversityFlag"]
      @jobName = jsonObj["settings"]["jobName"]
      @studyName = jsonObj["settings"]["studyName"]
      @otuFastMethod = jsonObj["settings"]["otuFastMethod"]
      @runBetaDiversityFlag = jsonObj["settings"]["runBetaDiversityFlag"]
      @assignTaxonomyMethod = jsonObj["settings"]["assignTaxonomyMethod"]
      @createPhylogeneticTreeFlag = jsonObj["settings"]["createPhylogeneticTreeFlag"]
      @makeTreeMethod = jsonObj["settings"]["makeTreeMethod"]
      @runAlphaDiversityFlag = jsonObj["settings"]["runAlphaDiversityFlag"]
      @runLoopWithNormalizedDataFlag = jsonObj["settings"]["runLoopWithNormalizedDataFlag"]
      @alphaMetrics= jsonObj["settings"]["alphaMetrics"]
      @otuSlowMethod = jsonObj["settings"]["otuSlowMethod"]
      @alignmentMethod = jsonObj["settings"]["alignmentMethod"]
      @createOTUnetworkFlag = jsonObj["settings"]["createOTUnetworkFlag"]
      @createHeatmapFlag = jsonObj["settings"]["createHeatmapFlag"]
      
      @createOTUtableFlag= jsonObj["settings"]["createOTUtableFlag"]
      @assignTaxonomyMinConfidence = jsonObj["settings"]["assignTaxonomyMinConfidence"]
      @qiimeVersion = jsonObj["settings"]["qiimeVersion"]
      @alignSeqsMinLen = jsonObj["settings"]["alignSeqsMinLen"]
      @betaMetrics = jsonObj["settings"]["betaMetrics"]
      
      
     
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
      system("mkdir -p #{@outputDir}")
      saveFileM = File.open("#{@outputDir}/metadata.txt","w+")
      for i in 0...@input.size
         @input[i] = @input[i].chomp('?')
        saveFile = File.open("#{@outputDir}/#{File.basename(@input[i])}.tar.gz","w+")
        puts "downloading metadata and filtered files from #{File.basename(@input[i])}"
        @db  = @dbhelper.extractName(@input[i])
        @grp = @grph.extractName(@input[i])
        @trk  = @trackhelper.extractName(@input[i])
        uri = URI.parse(@input[i])
        host = uri.host
        path = uri.path
        path = path.chomp('?')
        apicaller =ApiCaller.new(host,"",@user,@pass)
        path = path.gsub(/\/files\//,'/file/')
        pathR = "#{path}/filtered_fasta.result.tar.gz/data?"
        apicaller.setRsrcPath(pathR)
        @buff = '' 
        httpResp = apicaller.get(){|chunck|
               saveFile.write chunck
         }                                          
      saveFile.close
      Dir.chdir(@outputDir)
      system("tar -zxf #{@outputDir}/#{File.basename(@input[i])}.tar.gz")
      Dir.chdir(@scratch)
      #downloading metadata file
        pathR = "#{path}/sample.metadata/data?"
        apicaller.setRsrcPath(pathR)
        @buff = ''
        httpResp = apicaller.get(){|chunck|
            lines = chunck.split(/\n/)
            if(i==0)
              saveFileM.print "#{lines[0]}\n" 
            end
            for ii in 1...lines.size
               saveFileM.print "#{lines[ii]}\n"
            end
         }
      end
      saveFileM.close
      
      
      ##Calling qiime pipeline
      
      cmd = "module load mbwDeps/v1 ; run_QIIME_ARG_pipeline.rb -u #{@outputDir}/metadata.txt -z #{@outputDir} -f #{@otuFastMethod} -s #{@otuSlowMethod} -b '#{@betaMetrics}' -a '#{@alphaMetrics}' "
      cmd << "-t #{@assignTaxonomyMethod} -c #{@assignTaxonomyMinConfidence} -l #{@alignSeqsMinLen} -r #{@runAlphaDiversityFlag} -p #{@runBetaDiversityFlag} "
      cmd << "-i #{@createPhylogeneticTreeFlag} -o #{@createOTUtableFlag} -m #{@createHeatmapFlag} -n #{@createOTUnetworkFlag} -q #{@createTaxaSummaries} "
      cmd << "-d #{@runLoopWithNormalizedDataFlag} -e #{@alignmentMethod} -g #{@makeTreeMethod} >#{@outputDir}/qiime.log 2>#{@outputDir}/qiime.error.log"
      $stdout.puts cmd
      system(cmd)
      sucess = true
       if(!$?.success?)
            sucess = false
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
       if(@outputArray.size==2)
         projectPlot()
         if(!$?.success?)
            sucess = false
            @exitCode = $?.exitstatus
            raise "project plot failed"
         end
       end
       if( sucess == true)
          sendSEmail()
       end
       
       
   end
   
    ##tar of output directory
   def compression
    
    ##Preparing directory structure for "project" area to display html
    system("mkdir -p #{@outputDir}/htmlPages/#{@studyName1}/QIIME/#{@jobName1}")
    system("cp -r #{@outputDir}/QIIME_result/plots #{@outputDir}/htmlPages/#{@studyName1}/QIIME/#{@jobName1}")
      
     Dir.chdir("#{@outputDir}/QIIME_result")
     #system("tar -zcf #{@sampleSetName1}.tar.gz * --exclude=*.log --exclude=*.sra --exclude=*.sff --exclude=*.local.metadata")
      system("tar czf raw.results.tar.gz * --exclude=filtered_aln --exclude=taxa --exclude=aln --exclude=plots")
     system("tar czf phylogenetic.result.tar.gz filtered_aln")
     system("tar czf taxanomy.result.tar.gz taxa")
     system("tar czf fasta.result.tar.gz aln")
     system("tar czf plots.result.tar.gz plots")
    
     Dir.chdir(@scratch)
     
   end
   
   ##Calling script to create html pages of plot in project area
   def projectPlot
      jsonLocation  = CGI.escape("#{@scratch}/jobFile.json")
      htmlLocation = CGI.escape("#{@outputDir}/htmlPages/#{@studyName1}")
      system("importMicrobiomeProjectFiles.rb -j #{jsonLocation} -i #{htmlLocation} >#{@outputDir}/project_plot.log 2>#{@outputDir}/project_plot.error.log")
      
   end
   
     ##uploading files on specified location
   def upload
     begin
          apicaller =ApiCaller.new(@hostOutput,"",@user,@pass)
          restPath = @pathOutput
          
          
          ##uplaoding otu table
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/otu.table/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/QIIME_result/otu_table.txt","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded otu_table.txt "
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
          
          ##uplaoding phylogenetic tarred files
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/phylogenetic.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/QIIME_result/phylogenetic.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded phylogenetic.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
          ##uplaoding taxanomy tarred files   
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/taxanomy.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/QIIME_result/taxanomy.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded taxanomy.result.tar.gz"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
           
           ##Uploading  fasta files  
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/fasta.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/QIIME_result/fasta.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded fasta.result.tar.gz "
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end
            
           ##Uploading plots files  
            path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/plots.result.tar.gz/data"
            apicaller.setRsrcPath(path)
            infile = File.open("#{@outputDir}/QIIME_result/plots.result.tar.gz","r")
            apicaller.put(infile)
             if apicaller.succeeded?
               $stdout.puts "successfully uploaded plots.result.tar.gz plots"
             else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
              raise "#{apicaller.apiStatusObj['msg']}"
             end  
            
          #uploading raw results file back
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/raw.results.tar.gz/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@outputDir}/QIIME_result/raw.results.tar.gz","r")
           apicaller.put(infile)
            if apicaller.succeeded?
               $stdout.puts "successfully uploaded raw.results.tar.gz "
            else
               $stderr.puts apicaller.parseRespBody()
               $stderr.puts "API response; statusCode: #{apicaller.apiStatusObj['statusCode']}, message: #{apicaller.apiStatusObj['msg']}"
               @exitCode = apicaller.apiStatusObj['statusCode']
             raise "#{apicaller.apiStatusObj['msg']}"
            end
            
             #uploading metadata file back
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/sample.metadata/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@outputDir}/metadata.txt","r")
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
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/settings.json/data"
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
            
            ##uploading mapping.txt file
           path = restPath +"/file/MicrobiomeWorkBench/#{@studyName1}/QIIME/#{@jobName1}/mapping.txt/data"
           apicaller.setRsrcPath(path)
           infile = File.open("#{@outputDir}/QIIME_result/mapping.txt","r")
           apicaller.put(infile)
            if apicaller.succeeded?
               $stdout.puts "successfully uploaded mapping.txt file "
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
   Study Name             : #{CGI.unescape(@studyName1)}
   Job Name               : #{CGI.unescape(@jobName1)}
   
Result File Location in the Genboree Workbench:
   Group : #{@grpOutput}
   DataBase : #{@dbOutput}
   Path to File:
      Files
      * MicrobiomeWorkBench
         * #{CGI.unescape(@studyName1)}
            *QIIME
               *#{CGI.unescape(@jobName1)}"
                          
                          
                          
        if(@outputArray.size ==2)
           @outputArray[1] = @outputArray.chomp('?')
           prj =  @outputArray[1].split(/\/prj\//)
           
           body <<"
Plots URL (click or paste in browser to access file):
    Prj: #{prj[1]}
    URL: 
http://#{@hostOutput}/java-bin/project.jsp?projectName=#{prj[1]}

           "
        end
        
               
                  
body <<"
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
   JobID                  : #{@jobID}
   Study Name             : #{CGI.unescape(@studyName1)}
   Job Name               : #{CGI.unescape(@jobName1)}
   
  
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
  
  
   def QiimeWrapper.usage(msg='')
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
      def QiimeWrapper.processArguements()
        # We want to add all the prop_keys as potential command line options
          optsArray = [ ['--jsonFile' ,'-j', GetoptLong::REQUIRED_ARGUMENT],
                        ['--help'      ,'-h',GetoptLong::NO_ARGUMENT]
                      ]
          progOpts = GetoptLong.new(*optsArray)
          QiimeWrapper.usage("USAGE ERROR: some required arguments are missing") unless(progOpts.getMissingOptions().empty?)
          optsHash = progOpts.to_hash
        
          Coverage if(optsHash.empty? or optsHash.key?('--help'));
          return optsHash
      end 

end

begin
optsHash = QiimeWrapper.processArguements()
performQCUsingFindPeaks = QiimeWrapper.new(optsHash)
performQCUsingFindPeaks.work()
rescue => err
      $stderr.puts "Details: #{err.message}"
      $stderr.puts err.backtrace.join("\n")
     performQCUsingFindPeaks.sendFailureEmail(err.message)
end
