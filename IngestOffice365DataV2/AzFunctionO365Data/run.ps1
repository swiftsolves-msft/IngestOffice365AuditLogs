# Input bindings are passed in via param block.
param($Timer)

# Get the current universal time in the default string format
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' property is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"

# Connect with Managed Identity (for ARM API + DCR ingestion)
if (-not (Get-AzContext)) {
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
$OfficeLoginUri         = $env:OfficeLoginUri
$AzureAADLoginUri       = $env:AzureAADLoginUri

# === DCR + DCE settings (required for modern ingestion) ===
$DceEndpoint            = $env:dceEndpoint            # Logs ingestion URI
$DcrImmutableId         = $env:dcrImmutableId         # dcr-...
$StreamName             = "Custom-$Office365CustomLog" # e.g. Custom-O365Management

# === State tracking via app settings ===
$timeDiff               = [int]$env:timeDiff          # Initial lookback in seconds (e.g. -300)
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
            $body = $tempData | ConvertTo-Json -Depth 5 -Compress -AsArray

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
        $body = $tempData | ConvertTo-Json -Depth 5 -Compress -AsArray

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
# Update Function App's own app setting via ARM REST API
# ===================================================================
function Update-AppSetting {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][string]$Value
    )

    $subId = $env:WEBSITE_OWNER_NAME.Split('+')[0]
    $rg    = $env:WEBSITE_RESOURCE_GROUP
    $app   = $env:WEBSITE_SITE_NAME

    $armToken = (Get-AzAccessToken -ResourceUrl "https://management.azure.com" -ErrorAction Stop).Token

    $listUri = "https://management.azure.com/subscriptions/$subId/resourceGroups/$rg/providers/Microsoft.Web/sites/$app/config/appsettings/list?api-version=2022-03-01"
    $current = Invoke-RestMethod -Uri $listUri -Method Post -Headers @{ Authorization = "Bearer $armToken" } -ContentType "application/json"

    $props = @{}
    foreach ($p in $current.properties.PSObject.Properties) {
        $props[$p.Name] = $p.Value
    }
    $props[$Key] = $Value

    $putUri = "https://management.azure.com/subscriptions/$subId/resourceGroups/$rg/providers/Microsoft.Web/sites/$app/config/appsettings?api-version=2022-03-01"
    $body   = @{ properties = $props } | ConvertTo-Json -Depth 5 -Compress

    $null = Invoke-RestMethod -Uri $putUri -Method Put -Headers @{ Authorization = "Bearer $armToken" } -Body $body -ContentType "application/json"
    Write-Host "Updated app setting '$Key' = '$Value'"
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

# ===================================================================
# Parse a UTC datetime string to ISO 8601 round-trip format, or $null
# ===================================================================
function ConvertTo-IsoUtcString {
    param([string]$value)
    if ([string]::IsNullOrEmpty($value)) { return $null }
    try {
        return [datetime]::Parse($value, $null, [System.Globalization.DateTimeStyles]::AssumeUniversal).ToUniversalTime().ToString("o")
    }
    catch { return $null }
}

# ===================================================================
# Map a raw O365 event object to the full O365_CL stream schema.
# All 80 stream columns are pre-parsed here so the DCR transform is
# a passthrough (required for Auxiliary / data-lake tables in Sentinel).
# ===================================================================
function Convert-EventToRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Event
    )

    $tg = if ($Event.CreationTime) {
        ConvertTo-IsoUtcString $Event.CreationTime
    } else { $currentUTCtime.ToString("o") }

    return [ordered]@{
        # Required stream fields
        TimeGenerated                              = $tg
        RawData                                   = ($Event | ConvertTo-Json -Depth 10 -Compress)

        # Common audit fields
        Operation_s                               = if ($null -ne $Event.Operation) { [string]$Event.Operation } else { $null }
        UserId_s                                  = if ($null -ne $Event.UserId) { [string]$Event.UserId } else { $null }
        UserKey_g                                 = if ($null -ne $Event.UserKey) { [string]$Event.UserKey } else { $null }
        UserType_d                                = if ($null -ne $Event.UserType) { [double]$Event.UserType } else { $null }
        RecordType_d                              = if ($null -ne $Event.RecordType) { [double]$Event.RecordType } else { $null }
        Version_d                                 = if ($null -ne $Event.Version) { [double]$Event.Version } else { $null }
        Workload_s                                = if ($null -ne $Event.Workload) { [string]$Event.Workload } else { $null }
        ResultStatus_s                            = if ($null -ne $Event.ResultStatus) { [string]$Event.ResultStatus } else { $null }
        OrganizationId_g                          = if ($null -ne $Event.OrganizationId) { [string]$Event.OrganizationId } else { $null }
        Id_g                                      = if ($null -ne $Event.Id) { [string]$Event.Id } else { $null }
        ClientIP_s                                = if ($null -ne $Event.ClientIP) { [string]$Event.ClientIP } else { $null }
        ObjectId_s                                = if ($null -ne $Event.ObjectId) { [string]$Event.ObjectId } else { $null }
        CreationTime_t                            = ConvertTo-IsoUtcString $Event.CreationTime
        ExternalAccess_b                          = if ($null -ne $Event.ExternalAccess) { [bool]$Event.ExternalAccess } else { $null }
        OriginatingServer_s                       = if ($null -ne $Event.OriginatingServer) { [string]$Event.OriginatingServer } else { $null }
        OrganizationName_s                        = if ($null -ne $Event.OrganizationName) { [string]$Event.OrganizationName } else { $null }
        TokenObjectId_s                           = if ($null -ne $Event.TokenObjectId) { [string]$Event.TokenObjectId } else { $null }
        TokenTenantId_s                           = if ($null -ne $Event.TokenTenantId) { [string]$Event.TokenTenantId } else { $null }
        AuthType_s                                = if ($null -ne $Event.AuthType) { [string]$Event.AuthType } else { $null }
        TokenType_s                               = if ($null -ne $Event.TokenType) { [string]$Event.TokenType } else { $null }

        # AppAccessContext (nested object)
        AppAccessContext_IssuedAtTime_t           = ConvertTo-IsoUtcString $Event.AppAccessContext.IssuedAtTime
        AppAccessContext_UniqueTokenId_s          = if ($null -ne $Event.AppAccessContext.UniqueTokenId) { [string]$Event.AppAccessContext.UniqueTokenId } else { $null }
        AppAccessContext_AADSessionId_s           = if ($null -ne $Event.AppAccessContext.AADSessionId) { [string]$Event.AppAccessContext.AADSessionId } else { $null }
        AppAccessContext_APIId_s                  = if ($null -ne $Event.AppAccessContext.APIId) { [string]$Event.AppAccessContext.APIId } else { $null }
        AppAccessContext_ClientAppId_s            = if ($null -ne $Event.AppAccessContext.ClientAppId) { [string]$Event.AppAccessContext.ClientAppId } else { $null }

        # Teams / General Audit
        DeviceInformation_s                       = if ($null -ne $Event.DeviceInformation) { [string]$Event.DeviceInformation } else { $null }
        ChatName_s                                = if ($null -ne $Event.ChatName) { [string]$Event.ChatName } else { $null }
        ChannelGuid_s                             = if ($null -ne $Event.ChannelGuid) { [string]$Event.ChannelGuid } else { $null }
        TeamGuid_s                                = if ($null -ne $Event.TeamGuid) { [string]$Event.TeamGuid } else { $null }
        ChannelName_s                             = if ($null -ne $Event.ChannelName) { [string]$Event.ChannelName } else { $null }
        TeamName_s                                = if ($null -ne $Event.TeamName) { [string]$Event.TeamName } else { $null }
        Members_s                                 = if ($null -ne $Event.Members) { $Event.Members | ConvertTo-Json -Depth 5 -Compress } else { $null }
        UserTenantId_g                            = if ($null -ne $Event.UserTenantId) { [string]$Event.UserTenantId } else { $null }
        MessageVisibilityTime_t                   = ConvertTo-IsoUtcString $Event.MessageVisibilityTime
        ChatThreadId_s                            = if ($null -ne $Event.ChatThreadId) { [string]$Event.ChatThreadId } else { $null }
        MessageId_s                               = if ($null -ne $Event.MessageId) { [string]$Event.MessageId } else { $null }
        ItemName_s                                = if ($null -ne $Event.ItemName) { [string]$Event.ItemName } else { $null }
        ParticipantInfo_HasForeignTenantUsers_b   = if ($null -ne $Event.ParticipantInfo.HasForeignTenantUsers) { [bool]$Event.ParticipantInfo.HasForeignTenantUsers } else { $null }
        ParticipantInfo_HasGuestUsers_b           = if ($null -ne $Event.ParticipantInfo.HasGuestUsers) { [bool]$Event.ParticipantInfo.HasGuestUsers } else { $null }
        ParticipantInfo_HasOtherGuestUsers_b      = if ($null -ne $Event.ParticipantInfo.HasOtherGuestUsers) { [bool]$Event.ParticipantInfo.HasOtherGuestUsers } else { $null }
        ParticipantInfo_HasUnauthenticatedUsers_b = if ($null -ne $Event.ParticipantInfo.HasUnauthenticatedUsers) { [bool]$Event.ParticipantInfo.HasUnauthenticatedUsers } else { $null }
        ParticipantInfo_ParticipatingTenantIds_s  = if ($null -ne $Event.ParticipantInfo.ParticipatingTenantIds) { $Event.ParticipantInfo.ParticipatingTenantIds | ConvertTo-Json -Depth 5 -Compress } else { $null }
        ExtraProperties_s                         = if ($null -ne $Event.ExtraProperties) { $Event.ExtraProperties | ConvertTo-Json -Depth 5 -Compress } else { $null }
        CommunicationType_s                       = if ($null -ne $Event.CommunicationType) { [string]$Event.CommunicationType } else { $null }
        MessageVersion_s                          = if ($null -ne $Event.MessageVersion) { [string]$Event.MessageVersion } else { $null }

        # Exchange Admin (RecordType 1)
        ModifiedObjectResolvedName_s              = if ($null -ne $Event.ModifiedObjectResolvedName) { [string]$Event.ModifiedObjectResolvedName } else { $null }
        Parameters_s                              = if ($null -ne $Event.Parameters) { $Event.Parameters | ConvertTo-Json -Depth 5 -Compress } else { $null }
        ModifiedProperties_s                      = if ($null -ne $Event.ModifiedProperties) { $Event.ModifiedProperties | ConvertTo-Json -Depth 5 -Compress } else { $null }
        AppPoolName_s                             = if ($null -ne $Event.AppPoolName) { [string]$Event.AppPoolName } else { $null }
        CorrelationID_s                           = if ($null -ne $Event.CorrelationID) { [string]$Event.CorrelationID } else { $null }
        RequestId_s                               = if ($null -ne $Event.RequestId) { [string]$Event.RequestId } else { $null }

        # Exchange Mailbox base (RecordTypes 2 + 3)
        LogonType_d                               = if ($null -ne $Event.LogonType) { [double]$Event.LogonType } else { $null }
        InternalLogonType_d                       = if ($null -ne $Event.InternalLogonType) { [double]$Event.InternalLogonType } else { $null }
        MailboxGuid_s                             = if ($null -ne $Event.MailboxGuid) { [string]$Event.MailboxGuid } else { $null }
        MailboxOwnerUPN_s                         = if ($null -ne $Event.MailboxOwnerUPN) { [string]$Event.MailboxOwnerUPN } else { $null }
        MailboxOwnerSid_s                         = if ($null -ne $Event.MailboxOwnerSid) { [string]$Event.MailboxOwnerSid } else { $null }
        LogonUserSid_s                            = if ($null -ne $Event.LogonUserSid) { [string]$Event.LogonUserSid } else { $null }
        LogonUserDisplayName_s                    = if ($null -ne $Event.LogonUserDisplayName) { [string]$Event.LogonUserDisplayName } else { $null }
        ClientInfoString_s                        = if ($null -ne $Event.ClientInfoString) { [string]$Event.ClientInfoString } else { $null }
        ClientIPAddress_s                         = if ($null -ne $Event.ClientIPAddress) { [string]$Event.ClientIPAddress } else { $null }
        SessionId_s                               = if ($null -ne $Event.SessionId) { [string]$Event.SessionId } else { $null }
        AppId_s                                   = if ($null -ne $Event.AppId) { [string]$Event.AppId } else { $null }
        ClientAppId_s                             = if ($null -ne $Event.ClientAppId) { [string]$Event.ClientAppId } else { $null }
        HostAppId_s                               = if ($null -ne $Event.HostAppId) { [string]$Event.HostAppId } else { $null }

        # ExchangeMailboxAuditRecord (RecordType 2 — single item)
        Item_Subject_s                            = if ($null -ne $Event.Item.Subject) { [string]$Event.Item.Subject } else { $null }
        Item_InternetMessageId_s                  = if ($null -ne $Event.Item.InternetMessageId) { [string]$Event.Item.InternetMessageId } else { $null }
        Item_ImmutableId_s                        = if ($null -ne $Event.Item.ImmutableId) { [string]$Event.Item.ImmutableId } else { $null }
        Item_SizeInBytes_d                        = if ($null -ne $Event.Item.SizeInBytes) { [double]$Event.Item.SizeInBytes } else { $null }
        Item_IsRecord_b                           = if ($null -ne $Event.Item.IsRecord) { [bool]$Event.Item.IsRecord } else { $null }
        Item_Attachments_s                        = if ($null -ne $Event.Item.Attachments) { $Event.Item.Attachments | ConvertTo-Json -Depth 5 -Compress } else { $null }
        Item_ParentFolder_Path_s                  = if ($null -ne $Event.Item.ParentFolder.Path) { [string]$Event.Item.ParentFolder.Path } else { $null }
        SaveToSentItems_b                         = if ($null -ne $Event.SaveToSentItems) { [bool]$Event.SaveToSentItems } else { $null }
        SendAsUserSmtp_s                          = if ($null -ne $Event.SendAsUserSmtp) { [string]$Event.SendAsUserSmtp } else { $null }
        SendOnBehalfOfUserSmtp_s                  = if ($null -ne $Event.SendOnBehalfOfUserSmtp) { [string]$Event.SendOnBehalfOfUserSmtp } else { $null }

        # ExchangeMailboxAuditGroupRecord (RecordType 3 — group items)
        CrossMailboxOperation_b                   = if ($null -ne $Event.CrossMailboxOperation) { [bool]$Event.CrossMailboxOperation } else { $null }
        DestFolder_Path_s                         = if ($null -ne $Event.DestFolder.Path) { [string]$Event.DestFolder.Path } else { $null }
        Folder_Path_s                             = if ($null -ne $Event.Folder.Path) { [string]$Event.Folder.Path } else { $null }
        AffectedItems_s                           = if ($null -ne $Event.AffectedItems) { $Event.AffectedItems | ConvertTo-Json -Depth 5 -Compress } else { $null }
    }
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

                $blobData = @()
                foreach($event in $data){
                    $matchesFilter = $false
                    if($Office365RecordTypes -eq "0"){
                        if(($event.Source) -ne "Cloud App Security"){
                            $matchesFilter = $true
                        }
                    }
                    else {
                        $types = ($Office365RecordTypes).split(",")
                        if(($event.RecordType) -in $types){
                            $matchesFilter = $true
                        }
                    }

                    if ($matchesFilter) {
                        $blobData += Convert-EventToRecord -Event $event
                    }
                }
                if ($blobData.Count -gt 0) {
                    Send-ToLogAnalytics -o365Data $blobData
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

    # Update last run time via app setting
    $endTime = $currentUTCtime | Get-Date -Format yyyy-MM-ddTHH:mm:ss    
    Update-AppSetting -Key "LastExecutionEndTime" -Value $endTime
}

# ===================================================================
# State tracking via app setting
# ===================================================================
$lastlogTime = $env:LastExecutionEndTime

if ([string]::IsNullOrEmpty($lastlogTime)) {
    $startTime = $currentUTCtime.AddSeconds($timeDiff) | Get-Date -Format yyyy-MM-ddTHH:mm:ss
    Write-Host "No previous execution found. Looking back $timeDiff seconds."
} else {
    $startTime = Get-Date -Date $lastlogTime -Format yyyy-MM-ddTHH:mm:ss
    Write-Host "Resuming from last execution end time: $startTime"
}
$endTime = $currentUTCtime | Get-Date -Format yyyy-MM-ddTHH:mm:ss

# Get O365 auth token and start ingestion
$headerParams = Get-AuthToken $AADAppClientId $AADAppClientSecret $AADAppClientDomain $AzureTenantId
Get-O365Data $startTime $endTime $headerParams $AzureTenantId

Write-Host "PowerShell timer trigger function completed! TIME: $currentUTCtime"