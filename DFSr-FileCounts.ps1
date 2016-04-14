$PercentDifferent = 0
#-----------------------------------------------------------------------
$To = 'Email@SomeWhere.com'
$From = 'Email@SomeWhere.com'
$Subject = "DFSr File Counts - $((get-date).ToString('MM/dd/yyyy'))"
$MailServer = 'mail.SomeWhere.com'
#-----------------------------------------------------------------------
#-----------------------------------------------------------------------
$Style = "<style>"
$Style = $Style + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
$Style = $Style + "TH{border-width: 1px;padding: 5px;border-style: solid;border-color: black;background-color:DarkGray;text-align:center}"
$Style = $Style + "TD{border-width: 1px;padding: 5px;border-style: solid;border-color: black;text-align:center}"
$Style = $Style + "</style>"
#-----------------------------------------------------------------------
$elapsed = [System.Diagnostics.Stopwatch]::StartNew()
$Global:EmailBody = @()
$Global:File_Path = Split-Path -Path $MyInvocation.MyCommand.Definition -Parent
If (-Not(Test-Path "$Global:File_Path\Health_Reports")){New-Item "$Global:File_Path\Health_Reports" -ItemType Directory}
$Global:Reports_Path = "$File_Path\Health_Reports"
If (-Not(Test-Path "$Global:File_Path\Logs")){New-Item "$Global:File_Path\Logs" -ItemType Directory}
$Global:LogFile = "$Global:File_Path\Logs\$(Get-Date -format yyyy-MM-dd)-DFS-FileCounts.csv"
Get-ChildItem -Path "$Global:File_Path\Logs" | Where {$_.PSisContainer -eq $false -and $_.LastWriteTime -lt (Get-date).AddDays(-183)} | Remove-Item -Force
$Global:ReportsGenerated = @()
Function GenerateReports{
    Remove-Item "$Reports_Path\*.*" 
    $Domains = 'Domain1','Domain2'
    $Global:JobCount = 0
    Foreach ($Domain in $Domains){
        dfsradmin rg list /Domain:$Domain /attr:rgname /CSV | Where {-not($_ -eq 'RgName' -or $_ -eq "" -or $_ -eq 'Domain System Volume')} | Foreach {
            $Global:JobCount++
            If ($_ -eq 'Domain System Volume'){
                $HTML_Path = """$Reports_Path\$($($Domain.split("."))[0])-SYSVOL"""
                $Global:ReportsGenerated += "$($($Domain.split("."))[0])-SYSVOL"
            }#End of If
            Else{
                $HTML_Path = """$Reports_Path\$_"""
                $Global:ReportsGenerated += "$_"
            }#End of Else
            $RepGroup = """$_"""
            while (@(Get-Job -State Running).Count -ge 50) {Start-Sleep -Seconds 2}
                Start-Job -Name "DFSrHealth-$RepGroup" -ScriptBlock {param($Domain,$RepGroup,$HTML_Path)
                &cmd /c "DfsrAdmin.exe Health New /Domain:$Domain /RgName:$RepGroup /RepName:$HTML_Path /FsCount:True"
            } -ArgumentList $Domain,$RepGroup,$HTML_Path
        }#Foreach {$RepGroupNames
    }#Foreach ($Domain in $Domains)
}#Function GenerateReports
GenerateReports
$Global:ReportsProcessed = @()
Function ParseReports {
    Get-Job | Wait-Job -Timeout 5400
    Get-ChildItem -path $Reports_Path *.html | Remove-Item -Force
    $Global:Processed = 0
    $error.Clear()
    $Global:OutPut = @()
    Get-ChildItem -Path $Reports_Path\*.* -Include "*.xml" | Foreach {
    $CurrentFileName = $_.BaseName
    [xml]$XML = Get-Content $_
    $Math = @()
    $ReplicationGroup = $($XML.dfsReplicationReport.header.replicationGroup.name)
    $XML.dfsReplicationReport.members.Server | Foreach {
        $Server = $_.Name
        $_.contentSets | Foreach {
            $_.Set | Foreach {
                $Data = New-Object psobject
                $Data | Add-Member -MemberType "noteproperty" -Name 'Replication Group' -Value $RepLicationGroup
                $Data | Add-Member -MemberType "noteproperty" -Name 'Replicated Folder' -Value $_.Name 
                $Data | Add-Member -MemberType "noteproperty" -Name 'Server' -Value $Server
                $Data | Add-Member -MemberType "noteproperty" -Name 'Actual Size' -Value ''
                $Data | Add-Member -MemberType "noteproperty" -Name 'File Count' -Value ''
                $Data | Add-Member -MemberType "noteproperty" -Name 'Folder Count' -Value ''
                
                $MathData = New-Object psobject
                $MathData | Add-Member -MemberType "noteproperty" -Name 'RepGroup' -Value $_.Name
                $_.folder | Where {$_.Type -eq 'root'} | Foreach {
                    $MathData | Add-Member -MemberType "noteproperty" -Name 'MathCount' -Value $_.FileCount
                    $Math += $MathData
                    $Data.'File Count' = $_.FileCount
                    $Data.'Folder Count' = $_.FolderCount
                    if ([int64]$_.Size -lt 1kb) {$Data.'Actual Size' = "$([math]::round($_.Size, 2))"}
                    elseif ([int64]$_.Size -lt 1mb) {$Data.'Actual Size' = "$([math]::round($_.Size / 1KB, 2)) kb"}
                    elseif ([int64]$_.Size -lt 1gb) {$Data.'Actual Size' = "$([math]::round($_.Size / 1MB, 2)) MB"}
                    elseif ([int64]$_.Size -lt 1tb) {$Data.'Actual Size' = "$([math]::round($_.Size / 1GB, 2)) GB"}
                    elseif ([int64]$_.Size -lt 1pb) {$Data.'Actual Size' = "$([math]::round($_.Size / 1TB, 2)) TB"}
                    $Global:OutPut += $Data
                }#$_.folder | Where {$_.Type -eq 'root'} | Foreach
            }# $_.Set | Foreach
        }#$_.contentSets | Foreach
    }#$XML.dfsReplicationReport.members.Server | Foreach
    Foreach($a in 0..((($Math.RepGroup | select -Unique).Count) - 1 )){
        $Numbers = @()
        $Search = $Math.RepGroup[$a]
        $Numbers += $Math | Where {$_.RepGroup -eq $Search}
        $Sum = $Numbers.MathCount | measure -Sum
        If($Sum.Sum -gt 0){
            $Percent = @()
            Foreach ($Number in $Numbers.MathCount){
                $Percent += [math]::Round($Number/$Sum.Sum*100)
            }
            If (($Percent | Measure -Maximum).Maximum -ne ($Percent | Measure -Minimum).Minimum){
                $Diff = ($Percent | Measure -Maximum ).Maximum - ($Percent | Measure -Minimum).Minimum
                If($Diff -gt $PercentDifferent){
                    $Global:EmailBody += $OutPut | Where{$_.'Replication Group' -eq $ReplicationGroup -and $_.'Replicated Folder' -eq $Search}
                    $Global:EmailBody = $Global:EmailBody | select -Unique 'Replication Group','Replicated Folder','Server','Actual Size','File Count','Folder Count'
                }
            }
        }
        $Numbers = $Null
        $Search = $Null
        $Sum = $Null
        $Percent = $Null
        $Diff = $Null
    }#Foreach($a in 1..($Math.RepGroup | select -Unique).Count)
    $Global:Processed++
    $Global:ReportsProcessed += $CurrentFileName
    
    }#Get-ChildItem -Path C:\TEMP\Health_Reports\*.* -Include "ChicagoIL.xml" | Foreach
}#Function ParseReports
ParseReports
$FailedToProcess = $Null
$FailedToProcess = Compare-Object -ReferenceObject $Global:ReportsGenerated -DifferenceObject $Global:ReportsProcessed -IncludeEqual | Where {$_.SideIndicator -ne '=='}
If ($FailedToProcess -ne $Null){
   $FailedAddedToEmail = @()
   $FailedAddedToEmail += $FailedToProcess.InputObject
   $FailedAddedToEmail = $FailedAddedToEmail | Select @{N='Failed';E={$_}},@{N='Groups';E={''}}
   $EmailTable2 = ((($FailedAddedToEmail | ConvertTo-Html -Fragment).Replace('<th>Groups</th>','')).Replace('<th></th>','')).Replace('<td></td>','') #| Out-String
}
Else{$EmailTable2 = ''}
Get-Job | Remove-Job -Force
$Global:OutPut | Export-Csv $LogFile -NoTypeInformation
If ($Global:EmailBody -notlike ''){
    Send-MailMessage -To $To -Subject $Subject -From $From -BodyAsHtml -Body ($Global:EmailBody | Sort 'Replication Group','Replicated Folder','Server' | ConvertTo-Html -head $Style -body "<H2>DFSr File Counts Greater than $PercentDifferent percent difference.</H2>Jobs Ran = $Global:JobCount, Jobs Processed = $Global:Processed <br><br>" -PostContent "<br><br>$EmailTable2<br><br>Detailed Log - $($LogFile.Replace('C:\',"\\$(get-content env:computername)\$($LogFile.substring(0,1))$\"))<br><br>Total Elapsed Time: $($elapsed.Elapsed.ToString())" | Out-String) -SmtpServer $MailServer
}
Else{Send-MailMessage -To $To -Subject $Subject -From $From -BodyAsHtml -Body (ConvertTo-Html -body "<H2>DFSr File Counts Greater than .5 percent difference.</H2>Jobs Ran = $Global:JobCount, Jobs Processed = $Global:Processed<br><br><font size=6 color=Green>Nothing to Report</font>" -PostContent "<br><br>$EmailTable2<br><br>Detailed Log - $($LogFile.Replace('C:\',"\\$(get-content env:computername)\$($LogFile.substring(0,1))$\"))<br><br>Total Elapsed Time: $($elapsed.Elapsed.ToString())" | Out-String) -SmtpServer $MailServer}