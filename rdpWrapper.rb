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


class RDPWrapper


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

    @jobName = jsonObj["settings"]["jobName"]
    @studyName = jsonObj["settings"]["studyName"]
    @fileNameBuffer = []

    @cgiJobName = CGI.escape(@jobName)
    @filJobName = @cgiJobName.gsub(/%[0-9a-f]{2,2}/i, "_")
    @cgiStudyName = CGI.escape(@studyName)
    @filStudyName = @cgiStudyName.gsub(/%[0-9a-f]{2,2}/i, "_")


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
    @success = false
  end

def downloadData
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
      httpResp = apicaller.get(){|chunk|
        fullChunk = "#{@buff}#{chunk}"
        @buff = ''
        fullChunk.each_line{ |line|
          if(line[-1].ord == 10)
            saveFile.write line
          else
            @buff += line
          end
        }
      }
      saveFile.close
      Dir.chdir(@outputDir)
      system("tar -zxf #{@outputDir}/#{File.basename(@input[i])}.tar.gz")
      Dir.chdir(@scratch)
      #downloading metadata file
      pathR = "#{path}/sample.metadata/data?"
      apicaller.setRsrcPath(pathR)
      @buff = ''
      httpResp = apicaller.get(){|chunk|
        lines = chunk.split(/\n/)
        if(i==0)
          saveFileM.print "#{lines[0]}\n"
        end
        for ii in 1...lines.size
          saveFileM.print "#{lines[ii]}\n"
        end
      }
    end
    saveFileM.close
  end


  def work
    system("mkdir -p #{@scratch}")
    Dir.chdir(@scratch)
    @outputDir = "#{@scratch}/#{@filJobName}"
    system("mkdir -p #{@outputDir}")
    downloadData()
    ##Calling rdp pipeline
    rdpExecutable = "run_RDP_pipeline.rb"
    cmd = " #{rdpExecutable} #{@outputDir}/metadata.txt  #{@outputDir}>#{@outputDir}/rdp.log 2>#{@outputDir}/rdp.error.log"
    $stdout.puts cmd
    system(cmd)
    if(!$?.success?)
      @exitCode = $?.exitstatus
      raise " Error running #{rdpExecutable}"
    else
      @success = true
    end
    if(@success) then
      compressFiles()
      uploadData()
      sendSuccessEmail()
    end
  end


  ##tar of output directory
  def compressFiles
    Dir.chdir("#{@outputDir}/RDPsummary")
    #system("tar -zcf #{@sampleSetName1}.tar.gz * --exclude=*.log --exclude=*.sra --exclude=*.sff --exclude=*.local.metadata")
    system("tar czf class.result.tar.gz class")
    system("tar czf domain.result.tar.gz domain")
    system("tar czf family.result.tar.gz family")
    system("tar czf genus.result.tar.gz genus")
    system("tar czf order.result.tar.gz order")
    system("tar czf phyla.result.tar.gz phyla")
    system("tar czf species.result.tar.gz species")
    system("tar czf pdf.result.tar.gz 'find . -name `*.pdf`'")
    Dir.chdir(@scratch)
  end


  def uploadUsingAPI(studyName,toolName,jobName,fileName,filePath)
    restPath = @pathOutput
    path = restPath +"/file/MicrobiomeWorkBench/#{studyName}/#{toolName}/#{jobName}/#{fileName}/data"
    @apicaller.setRsrcPath(path)
    infile = File.open("#{filePath}","r")
    @apicaller.put(infile)
    if @apicaller.succeeded?
      $stdout.puts "successfully uploaded #{fileName} "
    else
      $stderr.puts @apicaller.parseRespBody()
      $stderr.puts "API response; statusCode: #{@apicaller.apiStatusObj['statusCode']}, message: #{@apicaller.apiStatusObj['msg']}"
      @exitCode = @apicaller.apiStatusObj['statusCode']
      raise "#{@apicaller.apiStatusObj['msg']}"
    end
  end

def sendFailureEmail(errMsg)
  body =
      "
      Hello #{@user_first.capitalize} #{@user_last.capitalize}

      Your #{@toolTitle} job was unsuccessful.

      Job Summary:
      JobID                  : #{@jobID}
      Study Name             : #{@studyName}
      Job Name               : #{@jobName}

      Error Message : #{errMsg}
      Exit Status   : #{@exitCode}
      Please Contact the Genboree team with above information.

      The Genboree Team"

      subject = "Genboree: Your #{@toolTitle} job was unsuccessful"
    if (!@email.nil?) then
      sendEmail(subject,body)
    end
end


def sendSuccessEmail
    body =
    "
    Hello #{@user_first.capitalize} #{@user_last.capitalize}

    Your #{@toolTitle} job has completed successfully.

    Job Summary:

    JobID                  : #{@jobID}
    Study Name             : #{@studyName}
    Job Name               : #{@jobName}

    Settings:
    rdpVersion: 2.2
    rdpBootstrapCutoff: 0.8

    Result File Location in the Genboree Workbench:
    Group : #{@grpOutput}
    DataBase : #{@dbOutput}
    Path to File:
    Files
    * MicrobiomeWorkBench
      * #{@studyName}
        *RDP
          *#{@jobName}
            

    The Genboree Team"
    subject = "Genboree: Your #{@toolTitle} job is complete "
    if (!@email.nil?) then sendEmail(subject,body) end
end



  def uploadData
      @apicaller = ApiCaller.new(@hostOutput,"",@user,@pass)
      restPath = @pathOutput
      @success = false
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"class.result.tar.gz","#{@outputDir}/RDPsummary/class.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"domain.result.tar.gz","#{@outputDir}/RDPsummary/domain.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"family.result.tar.gz","#{@outputDir}/RDPsummary/family.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"genus.result.tar.gz","#{@outputDir}/RDPsummary/genus.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"order.result.tar.gz","#{@outputDir}/RDPsummary/order.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"phyla.result.tar.gz","#{@outputDir}/RDPsummary/phyla.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"species.result.tar.gz","#{@outputDir}/RDPsummary/species.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"pdf.result.tar.gz","#{@outputDir}/RDPsummary/pdf.result.tar.gz")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"counts.xlsx","#{@outputDir}/RDPreport/counts.xlsx")
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"normalized.xlsx","#{@outputDir}/RDPreport/normalized.xlsx")

      # upload metadata file
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"sample.metadata","#{@outputDir}/metadata.txt")
      # upload json setting file
      uploadUsingAPI(@cgiStudyName, @toolTitle,@cgiJobName,"settings.json","#{@scratch}/jobFile.json")
      @success = true
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


  def RDPWrapper.usage(msg='')
    unless(msg.empty?)
      puts "\n#{msg}\n"
    end
    puts "

    PROGRAM DESCRIPTION:
    RDP wrapper for microbiome workbench
    COMMAND LINE ARGUMENTS:
    --file         | -j => Input json file
    --help         | -h => [Optional flag]. Print help info and exit.

    usage:

    ruby rdpWrapper.rb -f jsonFile
    ";
    exit;
  end #

  # Process Arguments form the command line input
  def RDPWrapper.processArguments()
    # We want to add all the prop_keys as potential command line options
    optsArray = [ ['--jsonFile' ,'-j', GetoptLong::REQUIRED_ARGUMENT],
      ['--help'      ,'-h',GetoptLong::NO_ARGUMENT]
    ]
    progOpts = GetoptLong.new(*optsArray)
    RDPWrapper.usage("USAGE ERROR: some required arguments are missing") unless(progOpts.getMissingOptions().empty?)
    optsHash = progOpts.to_hash

    RDPWrapper.usage if(optsHash.empty? or optsHash.key?('--help'));
    return optsHash
  end
end

begin
optsHash = RDPWrapper.processArguments()
rdpWrapper = RDPWrapper.new(optsHash)
rdpWrapper.work()
    rescue => err
      $stderr.puts "Details: #{err.message}"
      $stderr.puts err.backtrace.join("\n")
     rdpWrapper.sendFailureEmail(err.message)
end
