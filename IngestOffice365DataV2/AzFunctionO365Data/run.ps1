# Input bindings are passed in via param block.
param($Timer)

# Get the current universal time in the default string format
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' property is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"

# Connect with Managed Identity (for Azure Table storage)
if ($env:MSI_SECRET -and (Get-Module -ListAvailable Az.Accounts)) {
    Connect-AzAccount -Identity
}

#region Environment Variables
$Office365ContentTypes = $env:contentTypes
$Office365RecordTypes   = $env:recordTypes
$Office365CustomLog     = $env:customLogName          # Base name only (e.g. O365Management)
$AzureTenantId          = $env:tenantGuid
$AADAppClientId         = $env:clientID 
$AADAppClientSecret     = $env:clientSecret 
$AADAppClientDomain     = $env:domain
$AADAppPublisher        = $env:publisher

# === NEW: DCR + DCE settings (required for modern ingestion) ===
$DceEndpoint            = $env:dceEndpoint            # Logs ingestion URI
$DcrImmutableId         = $env:dcrImmutableId         # dcr-...
$StreamName             = "Custom-$Office365CustomLog" # e.g. Custom-O365Management

$storageAccountName     = $env:StorageAccountName
$storageAccountTableName = "o365managementapiexecutions"
#endregion

# Validate DCE endpoint
if (-Not [string]::IsNullOrEmpty($DceEndpoint)) {
    if ($DceEndpoint.Trim() -notmatch '^https:\/\/.*\.ingest\.monitor\.azure\.com$') {
        Write-Error -Message "O365Data: Invalid DCE Endpoint URI." -ErrorAction Stop
        Exit
    }
}

# ===================================================================
# NEW: Get Bearer token for Logs Ingestion API (using Managed Identity)
# ===================================================================
function Get-LogIngestionToken {
    [cmdletbinding()]
    param()
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://monitor.azure.com" -ErrorAction Stop
        return "Bearer $($token.Token)"
    }
    catch {
        Write-Error "Failed to acquire Managed Identity token for monitor.azure.com: $($_.Exception.Message)"
        throw
    }
}

# ===================================================================
# NEW: Send data using modern Logs Ingestion API (DCR + DCE)
# ===================================================================
function Send-ToLogAnalytics {
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)]
        [psobject]$o365Data,          # Can be single object or array
        [string]$streamName = $StreamName
    )

    if ($o365Data -isnot [array]) { $o365Data = @($o365Data) }

    $tempData     = @()
    $tempDataSize = 0
    $maxBatchSize = 25MB   # Conservative safe limit for modern API

    foreach ($record in $o365Data) {
        $tempData += $record
        $tempDataSize += ($record | ConvertTo-Json -Depth 5).Length

        if ($tempDataSize -gt $maxBatchSize) {
            $body = $tempData | ConvertTo-Json -Depth 5 -Compress

            $headers = @{
                "Authorization" = Get-LogIngestionToken
                "Content-Type"  = "application/json"
            }

            $uri = "$DceEndpoint/dataCollectionRules/$DcrImmutableId/streams/$streamName`?api-version=2023-01-01"

            try {
                $null = Invoke-RestMethod -Uri $uri -Method Post -Body $body -Headers $headers
                Write-Host "✅ Sent batch of $($tempData.Count) records to stream $streamName"
            }
            catch {
                Write-Error "❌ Failed to send batch: $($_.Exception.Message)"
                throw
            }

            $tempData     = @()
            $tempDataSize = 0
        }
    }

    # Send any remaining data
    if ($tempData.Count -gt 0) {
        $body = $tempData | ConvertTo-Json -Depth 5 -Compress

        $headers = @{
            "Authorization" = Get-LogIngestionToken
            "Content-Type"  = "application/json"
        }

        $uri = "$DceEndpoint/dataCollectionRules/$DcrImmutableId/streams/$streamName`?api-version=2023-01-01"

        try {
            $null = Invoke-RestMethod -Uri $uri -Method Post -Body $body -Headers $headers
            Write-Host "✅ Sent final batch of $($tempData.Count) records to stream $streamName"
        }
        catch {
            Write-Error "❌ Failed to send final batch: $($_.Exception.Message)"
            throw
        }
    }
}

# ===================================================================
# Keep existing helper functions (unchanged)
# ===================================================================
function Convert-ObjectToHashTable {
    [CmdletBinding()]
    param([parameter(Mandatory=$true,ValueFromPipeline=$true)][pscustomobject]$Object)
    $HashTable = @{}
    $ObjectMembers = Get-Member -InputObject $Object -MemberType *Property
    foreach ($Member in $ObjectMembers) {
        $HashTable.$($Member.Name) = $Object.$($Member.Name)
    }
    return $HashTable
}

function Get-AuthToken {
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)][string]$ClientID,
        [parameter(Mandatory = $true)][string]$ClientSecret,
        [Parameter(Mandatory = $true)][string]$tenantdomain,
        [Parameter(Mandatory = $true)][string]$TenantGUID
    )

    $body = @{grant_type="client_credentials";resource=$OfficeLoginUri;client_id=$ClientID;client_secret=$ClientSecret}
    $oauth = Invoke-RestMethod -Method Post -Uri "$AzureAADLoginUri/$tenantdomain/oauth2/token?api-version=1.0" -Body $body
    $headerParams = @{'Authorization'="$($oauth.token_type) $($oauth.access_token)"}
    return $headerParams 
}

function Get-O365Data{
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)][string]$startTime,
        [Parameter(Mandatory = $true)][string]$endTime,
        [Parameter(Mandatory = $true)][psobject]$headerParams,
        [Parameter(Mandatory = $true)][string]$tenantGuid
    )

    $contentTypes = $Office365ContentTypes.split(",")

    # API front end for GCC-High
    if ($OfficeLoginUri.split('.')[2] -eq "us") {
        $OfficeLoginUri = "https://manage.office365.us"
    }

    foreach($contentType in $contentTypes){
        $contentType = $contentType.Trim()
        $listAvailableContentUri = "$OfficeLoginUri/api/v1.0/$tenantGUID/activity/feed/subscriptions/content?contentType=$contentType&PublisherIdentifier=$AADAppPublisher&startTime=$startTime&endTime=$endTime"
        
        Write-Output $listAvailableContentUri

        do {
            $contentResult = Invoke-RestMethod -Method GET -Headers $headerParams -Uri $listAvailableContentUri
            Write-Output $contentResult.Count

            foreach($obj in $contentResult){
                $data = Invoke-RestMethod -Method GET -Headers $headerParams -Uri ($obj.contentUri)
                Write-Output $data.Count

                foreach($event in $data){
                    if($Office365RecordTypes -eq "0"){
                        if(($event.Source) -ne "Cloud App Security"){
                            $ht = $event | Convert-ObjectToHashTable
                            Send-ToLogAnalytics -o365Data $ht
                        }
                    }
                    else {
                        $types = ($Office365RecordTypes).split(",")
                        if(($event.RecordType) -in $types){
                            $ht = $event | Convert-ObjectToHashTable
                            Send-ToLogAnalytics -o365Data $ht
                        }
                    }
                }
            }

            # Pagination
            $nextPageResult = Invoke-WebRequest -Method GET -Headers $headerParams -Uri $listAvailableContentUri
            if ($null -ne $nextPageResult.Headers.NextPageUrl) {
                $listAvailableContentUri = $nextPageResult.Headers.NextPageUrl
            } else {
                $nextPage = $false
            }
        } until ($nextPage -eq $false)
    }

    # Update last run time
    $endTime = $currentUTCtime | Get-Date -Format yyyy-MM-ddTHH:mm:ss
    Add-AzTableRow -table $o365TimeStampTbl -PartitionKey "Office365" -RowKey "lastExecutionEndTime" -property @{"lastExecutionEndTimeValue"=$endTime} -UpdateExisting
}

# ===================================================================
# Storage Table logic (unchanged)
# ===================================================================
$storageAccountContext = New-AzStorageContext -StorageAccountName $storageAccountName -UseConnectedAccount
$StorageTable = Get-AzStorageTable -Name $storageAccountTableName -Context $storageAccountContext -ErrorAction Ignore

if($null -eq $StorageTable.Name){      
    $startTime = $currentUTCtime.AddSeconds(-300) | Get-Date -Format yyyy-MM-ddTHH:mm:ss
    New-AzStorageTable -Name $storageAccountTableName -Context $storageAccountContext
    $o365TimeStampTbl = (Get-AzStorageTable -Name $storageAccountTableName -Context $storageAccountContext.Context).cloudTable    
    Add-AzTableRow -table $o365TimeStampTbl -PartitionKey "Office365" -RowKey "lastExecutionEndTime" -property @{"lastExecutionEndTimeValue"=$startTime} -UpdateExisting
}
Else {
    $o365TimeStampTbl = (Get-AzStorageTable -Name $storageAccountTableName -Context $storageAccountContext.Context).cloudTable
}

$lastExecutionEndTime = Get-AzTableRow -table $o365TimeStampTbl -partitionKey "Office365" -RowKey "lastExecutionEndTime" -ErrorAction Ignore
$lastlogTime = $lastExecutionEndTime.lastExecutionEndTimeValue
$startTime = $lastlogTime | Get-Date -Format yyyy-MM-ddTHH:mm:ss
$endTime   = $currentUTCtime | Get-Date -Format yyyy-MM-ddTHH:mm:ss

# Get O365 auth token and start ingestion
$headerParams = Get-AuthToken $AADAppClientId $AADAppClientSecret $AADAppClientDomain $AzureTenantId
Get-O365Data $startTime $endTime $headerParams $AzureTenantId

Write-Host "PowerShell timer trigger function completed! TIME: $currentUTCtime"
