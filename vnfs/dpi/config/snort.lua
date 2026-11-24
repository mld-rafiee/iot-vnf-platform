---------------------------------------------------------------------------
-- Snort 3 Configuration for IoT VNF DPI
-- /etc/snort/snort.lua
--
-- Based on production Snort3 configuration guidelines.
-- Adapted for IoT sensor payload inspection in an NFV testbed.
--
-- References:
--   - Snort 3 official documentation: https://snort.org/documents
--   - NIST SP 800-213: IoT Device Cybersecurity Guidance
--   - ETSI EN 303 645: Cyber Security for Consumer IoT
---------------------------------------------------------------------------

---------------------------------------------------------------------------
-- SECTION 1: NETWORK VARIABLES
---------------------------------------------------------------------------

-- Home network: the IoT sensor network we are protecting
-- In Kubernetes pod networking, this covers the cluster pod CIDR
HOME_NET     = 'any'
EXTERNAL_NET = 'any'

-- Server definitions
DNS_SERVERS   = '$HOME_NET'
HTTP_SERVERS  = '$HOME_NET'
SQL_SERVERS   = '$HOME_NET'
SMTP_SERVERS  = '$HOME_NET'
SSH_SERVERS   = '$HOME_NET'
FTP_SERVERS   = '$HOME_NET'
SIP_SERVERS   = '$HOME_NET'
TELNET_SERVERS = '$HOME_NET'

-- Rule and list paths
RULE_PATH       = '/etc/snort/rules'
WHITE_LIST_PATH = '/etc/snort/lists'
BLACK_LIST_PATH = '/etc/snort/lists'

---------------------------------------------------------------------------
-- SECTION 2: DATA ACQUISITION (DAQ)
-- pcap mode — reads from FIFO pipe written by the VNF process
---------------------------------------------------------------------------

daq =
{
    -- pcap: reads from file/FIFO — used by DPI VNF to inject payloads
    -- In production on a real network interface, change to 'afpacket'
    module   = 'pcap',
    input    = '/tmp/snort_input.pcap',
    snaplen  = 65535,
}

---------------------------------------------------------------------------
-- SECTION 3: STREAM PROCESSING
-- Stateful tracking of TCP/UDP sessions
---------------------------------------------------------------------------

stream_tcp =
{
    policy          = 'linux',
    session_timeout = 60,
    max_sessions    = 65536,
    max_queued_bytes = 1048576,
    overlap_policy  = 'last',
}

stream_udp =
{
    session_timeout = 30,
    max_sessions    = 32768,
}

stream_icmp =
{
    session_timeout = 30,
    max_sessions    = 8192,
}

stream_ip =
{
    max_frags       = 16384,
    policy          = 'linux',
    session_timeout = 30,
}

---------------------------------------------------------------------------
-- SECTION 4: NORMALIZERS
-- Protocol normalization before rule matching
---------------------------------------------------------------------------

normalizer =
{
    ip4 =
    {
        df  = true,
        rf  = true,
        tos = true,
        trim = true,
    },
    tcp =
    {
        ips  = true,
        rsv  = true,
        pad  = true,
        opts = true,
    },
}

---------------------------------------------------------------------------
-- SECTION 5: INSPECTORS
-- Deep packet inspection modules
---------------------------------------------------------------------------

-- HTTP inspector — IoT gateways may use HTTP REST APIs
http_inspect =
{
    request_depth               = 65535,
    response_depth              = 65535,
    max_headers                 = 100,
    max_header_length           = 4096,
    max_uri_length              = 4096,
    normalize_javascript        = true,
    extended_response_inspection = true,
}

-- SSL/TLS inspector
ssl =
{
    trust_servers          = true,
    max_client_hello_length = 65535,
}

-- DNS inspector
dns =
{
    enable_rdata_overflow = true,
}

-- SSH inspector
ssh =
{
    max_encrypted_packets = 25,
    max_client_bytes      = 19600,
}

---------------------------------------------------------------------------
-- SECTION 6: DETECTION ENGINE
---------------------------------------------------------------------------

detection =
{
    -- Use hyperscan if available (requires libhyperscan)
    -- Falls back to ac_full automatically if not installed
    search_engine     = 'hyperscan',
    split_any_any     = true,
    enable_builtin_rules = true,
}

---------------------------------------------------------------------------
-- SECTION 7: IPS RULES
-- Includes both community rules and IoT-specific custom rules
---------------------------------------------------------------------------

ips =
{
    enable_builtin_rules = true,

    -- Rule files
    rules = [[
        include $RULE_PATH/local.rules
        include $RULE_PATH/iot-anomaly.rules
    ]],

    -- Network and port variables used in rules
    variables =
    {
        nets =
        {
            HOME_NET      = HOME_NET,
            EXTERNAL_NET  = EXTERNAL_NET,
            HTTP_SERVERS  = HTTP_SERVERS,
            SQL_SERVERS   = SQL_SERVERS,
            DNS_SERVERS   = DNS_SERVERS,
            SSH_SERVERS   = SSH_SERVERS,
            SMTP_SERVERS  = SMTP_SERVERS,
            FTP_SERVERS   = FTP_SERVERS,
            SIP_SERVERS   = SIP_SERVERS,
            TELNET_SERVERS = TELNET_SERVERS,
        },
        ports =
        {
            -- IoT VNF listens on UDP 9001
            HTTP_PORTS      = '80 443 8080 8443',
            SHELLCODE_PORTS = '!80',
            SSH_PORTS       = '22',
            FTP_PORTS       = '21',
            SMTP_PORTS      = '25 465 587',
            DNS_PORTS       = '53',
            FILE_DATA_PORTS = '80 443 8080 8443 110 143',
            ORACLE_PORTS    = '1024:',
            GTP_PORTS       = '2123 2152 3386',
        },
    },
}

---------------------------------------------------------------------------
-- SECTION 8: EVENT FILTERING
-- Suppress noisy low-value events to reduce alert fatigue
---------------------------------------------------------------------------

-- Suppress specific rules that generate excessive false positives
-- in an IoT environment
suppress =
{
    -- Example: suppress ICMP unreachable floods (common in IoT networks)
    -- { gid = 1, sid = 408 },
}

-- Rate-limit repetitive alerts from same source
event_filter =
{
    -- Limit each rule to 1 alert per source per minute
    -- { gid = 1, sid = 9001001, type = 'limit',
    --   track = 'by_src', count = 1, seconds = 60 },
}

---------------------------------------------------------------------------
-- SECTION 9: OUTPUT AND LOGGING
---------------------------------------------------------------------------

-- Fast one-line alert format — tailed by the VNF Python process
alert_fast =
{
    file   = true,
    packet = false,
    limit  = 100,   -- MB
}

-- JSON alerts for SIEM/Prometheus integration
alert_json =
{
    file   = true,
    limit  = 100,
    fields = 'timestamp msg src_addr src_port dst_addr dst_port proto action gid sid rev',
}

-- CSV for easy parsing
alert_csv =
{
    file   = true,
    fields = 'timestamp pkt_num proto pkt_gen pkt_len dir src_addr src_port dst_addr dst_port rule msg',
}

---------------------------------------------------------------------------
-- SECTION 10: PERFORMANCE AND PROCESS SETTINGS
---------------------------------------------------------------------------

memory =
{
    -- Memory cap in MB — tune based on worker-3 available RAM
    cap = 512,
}

process =
{
    -- Run as daemon alongside the Python VNF process
    daemon   = true,
    set_uid  = 'snort',
    set_gid  = 'snort',
    pid_file = '/var/run/snort.pid',
}

---------------------------------------------------------------------------
-- END OF CONFIGURATION
---------------------------------------------------------------------------
