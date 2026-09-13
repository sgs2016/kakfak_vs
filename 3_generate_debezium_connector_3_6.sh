#!/bin/bash
###############################################
# Set Envirnment
###############################################
set -euo pipefail
ask() {
    local prompt="$1"
    local default="${2:-}"

    if [[ -n "$default" ]]; then
        read -rp "$prompt [$default]: " value
        echo "${value:-$default}"
    else
        read -rp "$prompt: " value
        echo "$value"
    fi
}
ok() {
    printf "[ OK ] %s\n" "$1"
}
warn() {
    printf "[WARN] %s\n" "$1"
}
fail() {
    printf "[FAIL] %s\n" "$1"
}


#################################
#Json escape function
#################################
json_escape() {
    printf '%s' "$1" \
        | sed \
            -e 's/\\/\\\\/g' \
            -e 's/"/\\"/g' \
            -e ':a' \
            -e 'N' \
            -e '$!ba' \
            -e 's/\r/\\r/g' \
            -e 's/\n/\\n/g'
}


#repodb database
DB_USER="C##DEBEZIUM"
DB_PASSWORD="${DB_PASSWORD:-D3bez1m_2025}"

REPO_USER="C##DEBEZIUM"
REPO_PASSWORD="D3bez1m_2025"

REPO_CONNECT_STRING="//dbvib8007a.internal.draexlmaier.com:2483/mes_central_debezium.dbs.internal.draexlmaier.com"


# Template Version
TEMPLATE_VERSION=""


########################################
# run_sql - call any sql on source db
########################################

run_sql() {
sqlplus -s "/@${CONNECT_ALIAS} as sysdba" <<EOF
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET LONG 1000000
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;
WHENEVER OSERROR EXIT FAILURE;

$1

EXIT
EOF

}


####Help Intro
clear

############################################################
# DEBEZIUM CONNECTOR GENERATOR - INTRO
############################################################

echo
echo "============================================================"
echo "        DEBEZIUM CONNECTOR GENERATOR"
echo "============================================================"
echo
echo "This wizard will prepare and generate the Debezium"
echo "SOURCE and SINK connectors for an Oracle MES database."
echo
echo "The process will:"
echo
echo "  1. Connect to the source CDB - Enter only the  CDB name like c100"
echo "  2. Detect database role and database server nodes"
echo "  3. Detect PDB and MES schema"
echo "  4. Detect PDB service"
echo "  5. Build connector metadata"
echo "  6. Validate source database"
echo "  7. Validate target MES_CENTRAL database"
echo "  8. Generate replication metadata"
echo "  9. Load Kafka configuration"
echo " 10. Generate SOURCE connector"
echo " 11. Generate SINK connector"
echo " 12. Generate CLOB SINK connector"
echo " 13. Save configuration in repository"
echo
echo "------------------------------------------------------------"
echo
echo "IMPORTANT:"
echo "  As prerequiste is mandatory as Sorce db to be prepared "
echo "  Scripts to be run before are :"
echo "  1_add_local_kafka_TNS_entries.sh   - To cann connect on remote database "
echo "  2_prepare_source_db_kafka.sh       - Create objects and set source db for replication"
echo
echo "  This wizard does NOT deploy connectors."
echo
echo "  It will only:"
echo "    - discover"
echo "    - validate"
echo "    - generate"
echo "    - save configuration on repository database"
echo "    - save json files for src and snk conenctors"
echo
echo "  Connector deployment is performed separately."
echo
echo "------------------------------------------------------------"
echo

read -rp "Start connector generation? (Y/N): " START_GENERATION

if [[ ! "${START_GENERATION}" =~ ^[Yy]$ ]]; then

    echo
    echo "[INFO] Connector generation cancelled."
    echo

    exit 0

fi

echo
echo "[INFO] Starting Debezium connector generation..."
echo
sleep 1
clear

echo
echo "╔══════════════════════════════════════════════════════════╗"
echo "║    DEBEZIUM CONNECTORS Source /Sink SETUP                ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo

############################################################
# ENVIRONMENT
############################################################

echo
echo "Environment"
echo "------------------------------------------------------------"
echo "Allowed values: PROD, DEV, QUAL"
echo

while true; do
    read -rp "Environment [PROD]: " ENVIRONMENT_INPUT

    ENVIRONMENT_INPUT="${ENVIRONMENT_INPUT:-PROD}"

    case "${ENVIRONMENT_INPUT^^}" in
        PROD)
            ENVIRONMENT="PROD"
            break
            ;;
        DEV)
            ENVIRONMENT="DEV"
            break
            ;;
        QUAL)
            ENVIRONMENT="QUAL"
            break
            ;;
        *)
            echo
            echo "[ERROR] Invalid environment."
            echo "        Allowed values: PROD, DEV, QUAL"
            echo
            ;;
    esac
done

echo
echo "[ OK ] Environment : ${ENVIRONMENT}"


echo
echo "Step 1 - Database Information"
echo "------------------------------------------------------------"

DB_NAME=$(ask "Database Name: " "C286")
CDB_NAME=$(echo "${DB_NAME}" | tr '[:lower:]' '[:upper:]')


##############################################################################
# DATABASE CONNECTION / ROLE DISCOVERY
##############################################################################

echo
echo "DATABASE CONNECTION CHECK"
echo "--------------------------------------"
echo "Connecting to                  : ${DB_NAME}_RZ1"
echo

DB_ROLE=$(
sqlplus -s "/@${DB_NAME}_RZ1 as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;

SELECT DATABASE_ROLE
FROM V\$DATABASE;

EXIT
EOF
)

SQL_RC=$?

DB_ROLE=$(echo "$DB_ROLE" | xargs)

if [[ $SQL_RC -ne 0 || -z "$DB_ROLE" ]]; then

    echo "[FAIL] Database connection failed"
    echo
    echo "Connection : ${DB_NAME}_RZ1"
    echo
    exit 1

fi

ok   "       Database connection successful"
ok   "       Database role      : $DB_ROLE"



##############################################################################
# DETERMINE DATABASE INSTANCE SUFFIX
##############################################################################

case "$DB_ROLE" in
    PRIMARY)
        DB_SUFFIX="RZ1"
        ;;
    PHYSICAL\ STANDBY)
        DB_SUFFIX="RZ2"
        ;;
    LOGICAL\ STANDBY)
        DB_SUFFIX="RZ2"
        ;;
    *)
        echo
        echo "[FAIL] Unsupported database role: $DB_ROLE"
        exit 1
        ;;
esac

CONNECT_ALIAS="${DB_NAME}_${DB_SUFFIX}"
ok   "       Connection alias    : $CONNECT_ALIAS"
echo

##############################################################################
# PDB DISCOVERY
##############################################################################

echo "Searching for PDB matching PXXX"
echo "----------------------------------------------"

PDB_LIST=$(
sqlplus -s "/@${CONNECT_ALIAS} as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;

SELECT lower(name)
FROM V\$PDBS
WHERE REGEXP_LIKE(name, '^p[0-9]+$', 'i')
ORDER BY name;

EXIT
EOF
)

SQL_RC=$?
PDB_LIST=$(echo "$PDB_LIST" | sed '/^[[:space:]]*$/d' | xargs)

if [[ $SQL_RC -ne 0 ]]; then
    echo
    fail "Unable to query PDBs from ${CONNECT_ALIAS}"
    exit 1
fi

if [[ -z "$PDB_LIST" ]]; then
    echo
    fail "No PDB matching PXXX found in CDB [$DB_NAME]"
    exit 1
fi

PDB_COUNT=$(echo "$PDB_LIST" | wc -w | xargs)

if [[ "$PDB_COUNT" -ne 1 ]]; then

    echo
    fail "Multiple PDBs matching PXXX found:"
    echo "$PDB_LIST" | tr ' ' '\n'
    echo
    exit 1

fi

PDB_NAME=$(echo "$PDB_LIST" | tr '[:upper:]' '[:lower:]')
ok "PDB Detected           :  $PDB_NAME"



##############################################################################
# DISCOVER PDB SERVICE
##############################################################################

echo
echo "PDB service name  discovery"
echo "--------------------------------------"

GOT_SERVICE_NAME=$(
sqlplus -s "/@${CONNECT_ALIAS} as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT FAILURE;

ALTER SESSION SET CONTAINER=CDB\$ROOT;

SELECT LOWER(REGEXP_SUBSTR(name, '^[^.]+'))
FROM v\$services
WHERE pdb = UPPER('$PDB_NAME')
  AND LOWER(name) LIKE 'pcm_%'
  OR LOWER(name) LIKE 'hr_%'
ORDER BY name
FETCH FIRST 1 ROW ONLY;

EXIT
EOF
)

GOT_SERVICE_NAME=$(echo "$GOT_SERVICE_NAME" | xargs)

if [[ -z "$GOT_SERVICE_NAME" ]]; then
    echo
    fail "No pcm_* service found for PDB [$PDB_NAME]"
    exit 1
fi

SERVICE_NAME="$GOT_SERVICE_NAME"

ok "PDB service name detected: $SERVICE_NAME"


##############################################################################
# Discover source schema from PDB
##############################################################################
echo
echo " SOURCE SCHEMA DISCOVERY"
echo "--------------------------------------"

GOT_SCHEMA_NAME=$(
sqlplus -s "/@${CONNECT_ALIAS} as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT FAILURE;

ALTER SESSION SET CONTAINER=$PDB_NAME;

SELECT username
FROM dba_users
WHERE username LIKE 'MES_PROD%'
  AND oracle_maintained = 'N'
  AND username NOT LIKE 'MES_PROD%READ'
ORDER BY username;

EXIT
EOF
)

GOT_SCHEMA_NAME=$(echo "$GOT_SCHEMA_NAME" | xargs)

if [[ -z "$GOT_SCHEMA_NAME" ]]; then
    echo
    fail "No MES_PROD schema found in PDB [$PDB_NAME]"
    exit 1
fi

SCHEMA=$(echo "$GOT_SCHEMA_NAME" | tr '[:lower:]' '[:upper:]')
ok "Source schema detected: $SCHEMA"


##############################################################################
# HOSTNAME
##############################################################################
echo
echo " Detect Database Hosts "
echo "--------------------------------------"

DB_HOST1=$(
sqlplus -s "/@${DB_NAME}_RZ1 as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;

SELECT  lower(REGEXP_SUBSTR(host_name, '^[^.]+'))
FROM V\$INSTANCE;

EXIT
EOF
)

SQL_RC=$?
DB_HOST1=$(echo "$DB_HOST1" | xargs)


DB_HOST2=$(
sqlplus -s "/@${DB_NAME}_RZ2 as sysdba" <<EOF
SET TERMOUT OFF
SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;

SELECT  lower(REGEXP_SUBSTR(host_name, '^[^.]+'))
FROM V\$INSTANCE;

EXIT
EOF
)

SQL_RC=$?
DB_HOST2=$(echo "$DB_HOST2" | xargs)



ok "Hostname detected for RZ1 is $DB_HOST1"
ok "Hostname detected for RZ2 is $DB_HOST2"

#########################################
# MES Schema Version
#########################################

echo
echo "MES VERSION"
echo "--------------------------------------"

SCHEMA_UPPER="${SCHEMA^^}"

if [[ "${SCHEMA_UPPER}" =~ S[0-9]{2}$ ]]; then

    MES_VERSION="2"

elif [[ "${SCHEMA_UPPER}" =~ 0[0-9]{2}$ ]]; then

    MES_VERSION="1"

else
    echo
    echo "[FAIL] Unable to determine MES version from schema:"
    echo "       ${SCHEMA}"
    echo
    echo "Expected schema format:"
    echo "  MES_PROD_SXX  -> MES 2"
    echo "  MES_PROD_0XX  -> MES 1"
    echo
    exit 1
fi

ok " MES Version is :MES ${MES_VERSION}"
echo

###########################################
# Site -Location - Autodetected
##########################################
echo "Site Location   "
echo "--------------------------------------"
SITE="${DB_HOST1:2:3}"
SITE_LOWER=$(echo "$SITE" | tr '[:upper:]' '[:lower:]')
SITE="${SITE_LOWER}"
ok "SITE/LOCATION: ${SITE}"

##########################################
# DB PORT
##########################################

DB_PORT="2483"

MES_ID="M${SCHEMA: -3}"
HEARTBEAT_ID="${SCHEMA: -2}"
CONNECTOR_ID="${SCHEMA: -2}"

############################################################
# SNAPSHOT MODE
############################################################

echo
echo "Snapshot Mode"
echo "------------------------------------------------------------"
echo "Available values:"
echo "[ no_data ]    [ configuration_based ] [ initial ]"
echo

while true; do
    read -rp "Snapshot Mode [no_data]: " SNAPSHOT_MODE_INPUT

    SNAPSHOT_MODE_INPUT="${SNAPSHOT_MODE_INPUT:-no_data}"

    case "${SNAPSHOT_MODE_INPUT,,}" in
        no_data)
            CFG_SNAPSHOT_MODE="no_data"
            break
            ;;

        configuration_based)
            CFG_SNAPSHOT_MODE="configuration_based"
            break
            ;;

        initial)
            CFG_SNAPSHOT_MODE="initial"
            break
            ;;

        *)
            echo
            echo "[ERROR] Invalid Snapshot Mode."
            echo "        Allowed values: no_data, configuration_based, initial "
            echo
            ;;
    esac
done

ok "Snapshot Mode : ${CFG_SNAPSHOT_MODE}"


#############################################################
# LOGMINER BUFFER TYPE
############################################################

DEFAULT_BUFFER_TYPE="memory"

echo
echo "LogMiner Buffer Type"
echo "------------------------------------------------------------"
echo "Available values:"
echo "[ infinispan_embedded ]    [ memory ]"
echo

while true; do
    read -rp "LogMiner Buffer Type [memory]: " BUFFER_TYPE_INPUT

    BUFFER_TYPE_INPUT="${BUFFER_TYPE_INPUT:-memory}"

    case "${BUFFER_TYPE_INPUT,,}" in
        infinispan_embedded)
            CFG_LOG_MINING_BUFFER_TYPE="infinispan_embedded"
            break
            ;;

        memory)
            CFG_LOG_MINING_BUFFER_TYPE="memory"
            break
            ;;

        *)
            echo
            echo "[ERROR] Invalid LogMiner Buffer Type."
            echo "        Allowed values: infinispan_embedded, memory"
            echo
            ;;
    esac
done

ok "LogMiner Buffer Type : ${CFG_LOG_MINING_BUFFER_TYPE}"






####################################################
# AUTOCOMPLETE
####################################################

SERVICE_NAME_FULL="${SERVICE_NAME}.dbs.internal.draexlmaier.com"
DB_HOST1_FULL="${DB_HOST1}.internal.draexlmaier.com"
DB_HOST2_FULL="${DB_HOST2}.internal.draexlmaier.com"


####################################################
# DERIVED VALUES
####################################################

SITE_UPPER=$(echo "$SITE" | tr '[:lower:]' '[:upper:]')
SITE_LOWER=$(echo "$SITE" | tr '[:upper:]' '[:lower:]')
MES_LOWER=$(echo "$MES_ID" | tr '[:upper:]' '[:lower:]')

TOPIC_PREFIX="SFM.MES${MES_VERSION}.${SITE_UPPER}.${MES_ID}"
HEARTBEAT_TOPIC="${TOPIC_PREFIX}.${MES_ID}_heartbeat"
CONNECTOR_NAME="src.sfm.${SITE_LOWER}.${MES_LOWER}.${SERVICE_NAME}"
DATABASE_URL="jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=${DB_HOST1_FULL})(PORT=${DB_PORT}))(ADDRESS=(PROTOCOL=TCP)(HOST=${DB_HOST2_FULL})(PORT=${DB_PORT})))(CONNECT_DATA=(SERVICE_NAME=${SERVICE_NAME_FULL}))(FAILOVER=ON)(LOAD_BALANCE=OFF))"

####################################################
# Schema Registry
####################################################

SCHEMA_HISTORY_TOPIC="${TOPIC_PREFIX}.${MES_ID}_schema-changes"

####################################################
# HEARTBEAT ACTION QUERY
####################################################

HEARTBEAT_ACTION_QUERY="UPDATE C##DEBEZIUM.DEBEZIUM_HEARTBEAT SET CNT = CNT + 1,SOURCE_TS = TO_TIMESTAMP(TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6'), 'YYYY-MM-DD HH24:MI:SS.FF6'), SOURCE_TS_UTC = TO_TIMESTAMP(TO_CHAR(SYSTIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'YYYY-MM-DD HH24:MI:SS.FF6'), LOCATION = '${SITE_UPPER}', CLIENT_NAME = '${MES_ID}', CONNECTOR_NAME = '${CONNECTOR_NAME}' WHERE ID = ${HEARTBEAT_ID}"


##############################################################################
# CONNECTOR OUTPUT DIRECTORY
##############################################################################


CONNECTOR_FOLDER="${CONNECTOR_NAME#src.}"
CONNECTOR_FOLDER="${CONNECTOR_FOLDER//./_}"

CONNECTOR_BASE_DIR="./connectors"

case "${ENVIRONMENT}" in

    PROD)
        CONNECTOR_ENV_DIR="${CONNECTOR_BASE_DIR}/prod"
        ;;

    QUAL)
        CONNECTOR_ENV_DIR="${CONNECTOR_BASE_DIR}/qual"
        ;;

    DEV)
        CONNECTOR_ENV_DIR="${CONNECTOR_BASE_DIR}/dev"
        ;;

    *)
        echo
        echo "[FAIL] Unsupported environment: ${ENVIRONMENT}"
        echo "       Allowed values: PROD, QUAL, DEV"
        echo
        exit 1
        ;;

esac


CONNECTOR_DIR="${CONNECTOR_ENV_DIR}/${CONNECTOR_FOLDER}"

mkdir -p "${CONNECTOR_DIR}"


echo
echo "Connector output directory:"
echo "  Environment : ${ENVIRONMENT}"
echo "  Directory   : ${CONNECTOR_DIR}"
echo



####################################################
# REVIEW
####################################################

echo
echo "Step 4 - Review"
echo "------------------------------------------------------"
echo "Environment     : $ENVIRONMENT"
echo "DB Name         : $DB_NAME"
echo "PDB Name        : $PDB_NAME"
echo "Service Name    : $SERVICE_NAME_FULL"
echo "Primary Host    : $DB_HOST1_FULL"
echo "Secondary Host  : $DB_HOST2_FULL"
echo "-----------------------------------------------------"
echo "Schema          : $SCHEMA"
echo "MES_Version     : $MES_VERSION"
echo "MES ID          : $MES_ID"
echo "Site            : $SITE_UPPER"
echo "-----------------------------------------------------"
echo "Connector Name  : $CONNECTOR_NAME"
echo "Connector ID    : $CONNECTOR_ID"
echo "Topic Prefix    : $TOPIC_PREFIX"
echo "Heartbeat Topic : $HEARTBEAT_TOPIC"
echo "-----------------------------------------------------"
echo "JDBC Connect String is: $DATABASE_URL "
echo "Schema Registry Topic : $SCHEMA_HISTORY_TOPIC"
echo "Snapshot Mode         : $CFG_SNAPSHOT_MODE"
echo "LogMiner Buffer Type  : $CFG_LOG_MINING_BUFFER_TYPE"
echo "Connector Folder      : $CONNECTOR_DIR "
echo


####################################################
# Test Source Database Connection
####################################################

echo
echo "Testing source database connection..."

SOURCE_CONNECTION_RESULT=$(run_sql "
SELECT 'CONNECTED'
FROM dual;
" 2>&1)

SOURCE_SQL_RC=$?

SOURCE_CONNECTION_RESULT=$(
    echo "${SOURCE_CONNECTION_RESULT}" |
    sed '/^[[:space:]]*$/d' |
    xargs
)

####################################################
# Check SQLPlus connection
####################################################

if [[ ${SOURCE_SQL_RC} -ne 0 ]]; then

    echo "[FAIL] Source database connection failed"
    echo "Connection : ${CONNECT_ALIAS}"
    echo "Oracle error:"
    echo "${SOURCE_CONNECTION_RESULT}"

    exit 1

fi

####################################################
# Validate SQL result
####################################################

if [[ "${SOURCE_CONNECTION_RESULT}" != "CONNECTED" ]]; then

    echo "[FAIL] Source database connection test failed"
    echo "Connection : ${CONNECT_ALIAS}"
    echo "Connection result:"
    echo "${SOURCE_CONNECTION_RESULT}"

    exit 1

fi


####################################################
# Connection successful
####################################################

ok "Source database connection successful"
ok "Connection : ${CONNECT_ALIAS}"


####################################################
# Continue
####################################################
#echo
#read -rp "Continue with collecting data (Y/N)? " CONFIRM

#if [[ ! "${CONFIRM}" =~ ^[Yy]$ ]]; then
#    echo
#    echo "[INFO] Process cancelled by user."
#    exit 0
#fi



####################################################
# Target Database
####################################################
echo "-----------------------------------------------------------"
echo "TARGET (MES_CENTRAL) DATABASE - ENVIRONEMENT "
echo " Environment : ${ENVIRONMENT}"

case "${ENVIRONMENT}" in

############################################################
# PROD
############################################################
    PROD)

        echo "[INFO] Selecting PROD target database..."

        TARGET_DB_USER="MES_REPORTING"
        TARGET_DB_PASSWORD="ME5_reporting"
        if [ "$MES_VERSION" = "1" ]
        then
            TARGET_SERVICE_NAME="MES_REPORTING"
        else
            TARGET_SERVICE_NAME="MES2_REPORTING"
        fi

        TARGET_JDBC_URL="jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib83a.internal.draexlmaier.com)(PORT=2483))(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib83b.internal.draexlmaier.com)(PORT=2483)))(CONNECT_DATA=(SERVICE_NAME=${TARGET_SERVICE_NAME}.dbs.internal.draexlmaier.com))(FAILOVER=ON)(LOAD_BALANCE=OFF))"
        TARGET_CONNECTION_URL="(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib83a.internal.draexlmaier.com)(PORT=2483))(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib83b.internal.draexlmaier.com)(PORT=2483)))(CONNECT_DATA=(SERVICE_NAME=${TARGET_SERVICE_NAME}.dbs.internal.draexlmaier.com))(FAILOVER=ON)(LOAD_BALANCE=OFF))"
        ;;

############################################################
# QUAL / DEV
############################################################

    QUAL|DEV)

        echo "[INFO] Selecting ${ENVIRONMENT} Q/D target database..."

        TARGET_DB_USER="MES_REPORTING"
        TARGET_DB_PASSWORD='M3s$Anal1ze'
        TARGET_SERVICE_NAME="mes_central_debezium"
        TARGET_JDBC_URL="jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib8007a.internal.draexlmaier.com)(PORT=2483))(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib8007b.internal.draexlmaier.com)(PORT=2483)))(CONNECT_DATA=(SERVICE_NAME=${TARGET_SERVICE_NAME}.dbs.internal.draexlmaier.com))(FAILOVER=ON)(LOAD_BALANCE=OFF))"
        TARGET_CONNECTION_URL="(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib8007a.internal.draexlmaier.com)(PORT=2483))(ADDRESS=(PROTOCOL=TCP)(HOST=dbvib8007b.internal.draexlmaier.com)(PORT=2483)))(CONNECT_DATA=(SERVICE_NAME=mes_central_debezium.dbs.internal.draexlmaier.com))(FAILOVER=ON)(LOAD_BALANCE=OFF))"
        ;;
esac


####################################################
# Export  Common target connection variable
####################################################

TARGET_SQLPLUS_CONNECT_STRING="${TARGET_CONNECTION_URL}"

####################################################
# Target SQLPlus Function
####################################################

run_target_sql() {

    sqlplus -L -s \
        "${TARGET_DB_USER}/${TARGET_DB_PASSWORD}@${TARGET_CONNECTION_URL}" <<EOF

SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET TERMOUT OFF
SET ECHO OFF
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE;
WHENEVER OSERROR EXIT FAILURE;

$1

EXIT SUCCESS

EOF

}

####################################################
# Test Target Database Connection
####################################################

echo
echo " Testing Target Database Connection"
echo "------------------------------------------------------------"

echo "Environment    : ${ENVIRONMENT}"
echo "Target Service : ${TARGET_SERVICE_NAME}"
echo "Target User    : ${TARGET_DB_USER}"
echo

if TARGET_CONNECTION_RESULT=$(run_target_sql "
SELECT 'CONNECTED' FROM dual;
" 2>&1); then
    TARGET_SQL_RC=0
else
    TARGET_SQL_RC=$?
fi


####################################################
# Check SQLPlus connection
####################################################

if [[ $TARGET_SQL_RC -ne 0 ]]; then

    echo
    echo "[FAIL] Target database connection failed"
    echo
    echo "Environment    : ${ENVIRONMENT}"
    echo "Target Service : ${TARGET_SERVICE_NAME}"
    echo "Target User    : ${TARGET_DB_USER}"
    echo
    echo "Oracle error:"
    echo "${TARGET_CONNECTION_RESULT}"
    echo
    sleep 2
    exit 1

fi


####################################################
# Validate SQL result
####################################################

TARGET_CONNECTION_RESULT=$(echo "${TARGET_CONNECTION_RESULT}" | xargs)

if [[ "${TARGET_CONNECTION_RESULT}" != "CONNECTED" ]]; then

    echo
    echo "[FAIL] Target database connection test failed"
    echo
    echo "Environment    : ${ENVIRONMENT}"
    echo "Target Service : ${TARGET_SERVICE_NAME}"
    echo "Target User    : ${TARGET_DB_USER}"
    echo
    echo "Connection result:"
    echo "${TARGET_CONNECTION_RESULT}"
    echo
    sleep 2
    exit 1

fi


####################################################
# Connection successful
####################################################

echo
echo "[ OK ] Target ${ENVIRONMENT} database connection successful"
echo "      Target Service : ${TARGET_SERVICE_NAME}"
echo "      Target User    : ${TARGET_DB_USER}"
echo
sleep 2

####################################################
# Display Target Database Information
####################################################

echo
echo "Target Environment        : ${ENVIRONMENT}"
echo "Target User               : ${TARGET_DB_USER}"
echo "Target Service            : ${TARGET_SERVICE_NAME}"
echo "Target JDBC URL           : ${TARGET_JDBC_URL}"
echo "Target SQLPlus Connection : ${TARGET_CONNECTION_URL}"
echo "Target Connection Result  : ${TARGET_CONNECTION_RESULT}"
echo


####################################################
# Continue Confirmation
####################################################
#echo
#read -rp "Continue - next steps ( table_list, msg key columns ...   (Y/N)? " CONFIRM
#[[ ! "$CONFIRM" =~ ^[Yy]$ ]] && exit 0

#sleep 2



#======================================================
# Generating table.include.list
#=====================================================

echo
echo "Generating table.include.list ..."

TABLE_INCLUDE_LIST=$(run_sql "
ALTER SESSION SET CONTAINER=${PDB_NAME};
SELECT LISTAGG(a.owner || '.' || a.table_name, ',')
       WITHIN GROUP (ORDER BY a.table_name)
FROM dba_tables a
JOIN DRSADMIN.REPL_TABLES b
  ON UPPER(a.table_name) = UPPER(b.RT_TABLE_NAME)
WHERE a.owner = UPPER('${SCHEMA}');
" | tr -d '\r\n')

TABLE_INCLUDE_LIST="C##DEBEZIUM.DEBEZIUM_HEARTBEAT,${TABLE_INCLUDE_LIST}"

echo
echo "[OK] table.include.list generated"
#echo
#echo $TiABLE_INCLUDE_LIST
#echo

TABLE_COUNT=$(echo "$TABLE_INCLUDE_LIST" | tr ',' '\n' | wc -l)

ok " $TABLE_COUNT tables found"
echo
sleep 2

#======================================================
# Generating column.propagate.source.type
#=====================================================


echo
echo "Generating column.propagate.source.type ..."

COLUMN_PROPAGATE=$(run_sql "
ALTER SESSION SET CONTAINER=${PDB_NAME};
SET TERMOUT OFF
SET SERVEROUTPUT OFF

SELECT LISTAGG(
       a.owner || '.' || a.table_name || '.' || c.column_name,
       ',')
       WITHIN GROUP (ORDER BY a.table_name, c.column_name)
FROM dba_tables a
JOIN DRSADMIN.REPL_TABLES b
  ON UPPER(a.table_name) = UPPER(b.RT_TABLE_NAME)
JOIN all_tab_columns c
  ON c.owner = a.owner
 AND c.table_name = a.table_name
WHERE a.owner = UPPER('${SCHEMA}')
  AND c.data_type IN ('CLOB','NCLOB');
" | tr -d '\r\n')

ok " column.propagate.source.type generated"
sleep 2

#======================================================
# Generating reselector columns
#=====================================================
echo
echo "Generating reselector columns ..."

RESELECT_COLUMNS=$(run_sql "
ALTER SESSION SET CONTAINER=${PDB_NAME};
SET TERMOUT OFF
SET SERVEROUTPUT OFF

SELECT LISTAGG(
       a.owner || '.' || a.table_name || ':' || c.column_name,
       ',')
       WITHIN GROUP (ORDER BY a.table_name, c.column_name)
FROM dba_tables a
JOIN DRSADMIN.REPL_TABLES b
  ON UPPER(a.table_name) = UPPER(b.RT_TABLE_NAME)
JOIN all_tab_columns c
  ON c.owner = a.owner
 AND c.table_name = a.table_name
WHERE a.owner = UPPER('${SCHEMA}')
  AND c.data_type IN ('CLOB','NCLOB');
" | tr -d '\r\n')

ok " reselector columns generated"
sleep 2

#======================================================
# Generating message.key.columns
#======================================================
echo
echo "Generating message.key.columns ..."

MESSAGE_KEY_COLUMNS=$(run_sql "
ALTER SESSION SET CONTAINER=${PDB_NAME};

SET TERMOUT OFF
--SET SERVEROUTPUT OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF
SET VERIFY OFF
SET HEADING OFF
SET PAGESIZE 0
SET LONG 1000000
SET LONGCHUNKSIZE 1000000
SET LINESIZE 32767
SET TRIMSPOOL ON

DECLARE
    v_result   CLOB := 'C##DEBEZIUM.DEBEZIUM_HEARTBEAT:ID';

BEGIN

    FOR rec IN
    (
        WITH
        pk_base AS
        (
            SELECT
                ac.owner,
                ac.table_name,
                LISTAGG(acc.column_name, ',')
                    WITHIN GROUP (ORDER BY acc.position) AS pk_cols
            FROM all_constraints ac
            JOIN all_cons_columns acc
              ON ac.owner = acc.owner
             AND ac.constraint_name = acc.constraint_name
            JOIN DRSADMIN.REPL_TABLES rt
              ON UPPER(acc.table_name) = UPPER(rt.rt_table_name)

            WHERE ac.constraint_type = 'P'
              AND ac.owner = UPPER('${SCHEMA}')

--Excluded columns
              AND acc.column_name NOT IN
              (
                  'ADEF_ARCL_INDEX',
                  'MSJO_PROG_CL_INDEX'
              )

            GROUP BY
                ac.owner,
                ac.table_name
        ),

        cl_index_cols AS
        (
            SELECT
                atc.owner,
                atc.table_name,
                atc.column_name
            FROM all_tab_columns atc
            JOIN DRSADMIN.REPL_TABLES rt
              ON UPPER(atc.table_name) = UPPER(rt.rt_table_name)

            WHERE atc.owner = UPPER('${SCHEMA}')
              AND atc.column_name LIKE '%CL_INDEX%'

-- Excluded columns
              AND atc.column_name NOT IN
              (
                  'ADEF_ARCL_INDEX',
                  'MSJO_PROG_CL_INDEX'
              )
        ),

        final_keys AS
        (
            SELECT
                pk.owner,
                pk.table_name,

                CASE
                    WHEN COUNT(cl.column_name) = 0
                    THEN pk.pk_cols

                    ELSE
                        LISTAGG(cl.column_name, ',')
                            WITHIN GROUP (ORDER BY cl.column_name)
                        || ',' ||
                        pk.pk_cols
                END AS combined_keys

            FROM pk_base pk

            LEFT JOIN cl_index_cols cl
              ON pk.owner = cl.owner
             AND pk.table_name = cl.table_name

             -- Don't duplicate PK columns
             AND INSTR(
                    ',' || pk.pk_cols || ',',
                    ',' || cl.column_name || ','
                 ) = 0

            GROUP BY
                pk.owner,
                pk.table_name,
                pk.pk_cols
        )

        SELECT
            owner || '.' || table_name || ':' || combined_keys AS msg_key

        FROM final_keys

        ORDER BY
            owner,
            table_name
    )

    LOOP

        v_result := v_result || ';' || rec.msg_key;

    END LOOP;

    DBMS_OUTPUT.PUT_LINE(v_result);

END;
/
EXIT
")




MESSAGE_KEY_COLUMNS=$(echo "$MESSAGE_KEY_COLUMNS" \
    | sed '/^$/d' \
    | tr -d '\r' \
    | tr -d '\n')

KEY_COUNT=$(echo "$MESSAGE_KEY_COLUMNS" | tr ';' '\n' | wc -l)

echo "Length: ${#MESSAGE_KEY_COLUMNS}"
echo "[ OK ] message.key.columns generated"
echo "[ OK ] ${KEY_COUNT} table key definitions found"
sleep 2

printf '%s\n' "$MESSAGE_KEY_COLUMNS" > /tmp/msg_keys.txt


#============================================
#Output
#============================================

############################################################
# Generated Values Summary
############################################################

echo
read -rp "Display generated values? (Y/N): " SHOW_VALUES
if [[ "${SHOW_VALUES}" =~ ^[Yy]$ ]]; then

    echo
    echo "------------------------------------------------------------"
    echo " TABLE_INCLUDE_LIST"
    echo "------------------------------------------------------------"
    echo "${TABLE_INCLUDE_LIST}"
    echo

    echo "------------------------------------------------------------"
    echo " COLUMN_PROPAGATE"
    echo "------------------------------------------------------------"
    echo "${COLUMN_PROPAGATE}"
    echo

    echo "------------------------------------------------------------"
    echo " RESELECT_COLUMNS"
    echo "------------------------------------------------------------"
    echo "${RESELECT_COLUMNS}"
    echo

    echo "------------------------------------------------------------"
    echo " MESSAGE_KEY_COLUMNS"
    echo "------------------------------------------------------------"
    echo "${MESSAGE_KEY_COLUMNS}"
    echo

    read -rp "Continue? (Y/N): " CONFIRM

    if [[ ! "${CONFIRM}" =~ ^[Yy]$ ]]; then
        echo
        echo "[INFO] Process cancelled by user."
        echo
        exit 0
    fi

else

    echo "[INFO] Generated fields are available in JSON conenctors anyway :)."
    echo

fi

sleep 1


############################################################
# Template parameters for DEBEZIUM SOURCE CONNECTOR GENERATOR
# Version : 1.0
############################################################



############################################################
# Load external environment-specific configurations
############################################################

#echo
#echo "============================================================"
#echo " LOAD ENVIRONMENT CONFIGURATION"
#echo "============================================================"
#echo
#echo "Environment : ${ENVIRONMENT}"
#echo


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


############################################################
# TEMPLATE VERSION SELECTION
############################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_CONFIG_DIR="${SCRIPT_DIR}/templates/${ENVIRONMENT,,}"

if [[ ! -d "${ENV_CONFIG_DIR}" ]]; then
    echo
    fail "Environment template directory not found: ${ENV_CONFIG_DIR}"
    exit 1
fi

# Discover available template versions from source .conf files in templates/<env>/
AVAILABLE_VERSIONS=()
for conf_file in "${ENV_CONFIG_DIR}"/kafka_connector_source_defaults_${ENVIRONMENT,,}*.conf; do
    if [[ -f "$conf_file" ]]; then
        filename=$(basename "$conf_file")
        if [[ "$filename" =~ _([0-9]+(_[0-9]+)?)\.conf$ ]]; then
            AVAILABLE_VERSIONS+=("${BASH_REMATCH[1]}")
        elif [[ "$filename" == "kafka_connector_source_defaults_${ENVIRONMENT,,}.conf" ]]; then
            AVAILABLE_VERSIONS+=("base")
        fi
    fi
done

# Fallback default versions if none detected
if [[ ${#AVAILABLE_VERSIONS[@]} -eq 0 ]]; then
    AVAILABLE_VERSIONS=("36" "base")
fi

# Remove duplicates & sort
AVAILABLE_VERSIONS=($(echo "${AVAILABLE_VERSIONS[@]}" | tr ' ' '\n' | sort -u))

echo
echo "============================================================"
echo " TEMPLATE VERSION SELECTION"
echo "============================================================"
echo "Environment        : ${ENVIRONMENT}"
echo "Template Directory : ${ENV_CONFIG_DIR}"

echo
echo "Available Template Versions:"
for i in "${!AVAILABLE_VERSIONS[@]}"; do
    ver="${AVAILABLE_VERSIONS[$i]}"
    printf "  %d) %s\n" "$((i+1))" "${ver}"
done

DEFAULT_CHOICE="${#AVAILABLE_VERSIONS[@]}"

echo
while true; do
    read -rp "Select Template Version [${AVAILABLE_VERSIONS[$((DEFAULT_CHOICE-1))]}]: " VERSION_INPUT

    if [[ -z "${VERSION_INPUT}" ]]; then
        TEMPLATE_VERSION="${AVAILABLE_VERSIONS[$((DEFAULT_CHOICE-1))]}"
        break
    elif [[ "${VERSION_INPUT}" =~ ^[0-9]+$ ]] && (( VERSION_INPUT >= 1 && VERSION_INPUT <= ${#AVAILABLE_VERSIONS[@]} )); then
        TEMPLATE_VERSION="${AVAILABLE_VERSIONS[$((VERSION_INPUT-1))]}"
        break
    elif [[ " ${AVAILABLE_VERSIONS[*]} " =~ " ${VERSION_INPUT} " ]]; then
        TEMPLATE_VERSION="${VERSION_INPUT}"
        break
    else
        echo
        echo "[ERROR] Invalid selection. Enter a number (1-${#AVAILABLE_VERSIONS[@]}) or version name."
        echo
    fi
done

ok "Selected Template Version : ${TEMPLATE_VERSION}"


############################################################
# Load Configuration Files
############################################################

if [[ "${TEMPLATE_VERSION}" == "base" ]]; then
    KAFKA_SOURCE_CONFIG_FILE="${ENV_CONFIG_DIR}/kafka_connector_source_defaults_${ENVIRONMENT,,}.conf"
    KAFKA_SINK_CONFIG_FILE="${ENV_CONFIG_DIR}/kafka_connector_sink_defaults_${ENVIRONMENT,,}.conf"
else
    KAFKA_SOURCE_CONFIG_FILE="${ENV_CONFIG_DIR}/kafka_connector_source_defaults_${ENVIRONMENT,,}_${TEMPLATE_VERSION}.conf"

    if [[ -f "${ENV_CONFIG_DIR}/kafka_connector_sink_defaults_${ENVIRONMENT,,}_${TEMPLATE_VERSION}.conf" ]]; then
        KAFKA_SINK_CONFIG_FILE="${ENV_CONFIG_DIR}/kafka_connector_sink_defaults_${ENVIRONMENT,,}_${TEMPLATE_VERSION}.conf"
    else
        KAFKA_SINK_CONFIG_FILE="${ENV_CONFIG_DIR}/kafka_connector_sink_defaults_${ENVIRONMENT,,}.conf"
    fi
fi

############################################################
# Validate configuration files
############################################################

for CONFIG_FILE in \
    "${KAFKA_SOURCE_CONFIG_FILE}" \
    "${KAFKA_SINK_CONFIG_FILE}"
do

    if [[ ! -f "${CONFIG_FILE}" ]]; then

        echo
        echo "[FAIL] Configuration file not found:"
        echo "       ${CONFIG_FILE}"
        echo

        exit 1

    fi

done


############################################################
# Load configurations
############################################################
echo "---------------------------------------------------"
echo "[INFO] Loading Kafka configuration template files :"

# shellcheck disable=SC1090
source "${KAFKA_SOURCE_CONFIG_FILE}"

echo "[ OK ] Kafka SOURCE configuration loaded:  ${KAFKA_SOURCE_CONFIG_FILE}"

# shellcheck disable=SC1090
source "${KAFKA_SINK_CONFIG_FILE}"
echo "[ OK ] Kafka SINK configuration loaded:    ${KAFKA_SINK_CONFIG_FILE}"

echo
echo " All Kafka template conf  loaded successfully"
echo "---------------------------------------------------"
echo




############################################################
# SRC  CONNECTOR DEFAULTS - Template Parameters
############################################################
CFG_CONNECTOR_CLASS="io.debezium.connector.oracle.OracleConnector"
CFG_BEFORE_STATE_INCLUDE="true"
CFG_DATABASE_CONNECTION_ADAPTER="logminer"
PDB_NAME_UPPER=$(echo "$PDB_NAME" | tr '[:lower:]' '[:upper:]')
CFG_LOB_ENABLED="true"
CFG_LOG_MINING_ARCHIVE_LOG_ONLY_MODE="false"
CFG_LOG_MINING_BATCH_SIZE_DEFAULT="20000"
CFG_LOG_MINING_BATCH_SIZE_MIN="10000"
CFG_LOG_MINING_BATCH_SIZE_MAX="40000"
CFG_LOG_MINING_QUERY_FILTER_MODE="in"
CFG_LOG_MINING_SESSION_MAX_MS="600000"
CFG_LOG_MINING_STRATEGY="online_catalog"
CFG_LOG_MINING_TRANSACTION_RETENTION_MS="0"
CFG_LOG_MINING_USERNAME_EXCLUDE_LIST="DRSADMIN,MES_MAINTAIN"
CFG_POST_PROCESSORS="reselector"
CFG_DATABASE_ORACLE_JDBC_DEFAULT_ROW_PREFETCH="10000"
CFG_PRIMARY_KEY_MODE="record_key"
CFG_PROVIDE_TRANSACTION_METADATA="true"
CFG_SCHEMA_CHANGE_EVENT_EXCLUDE_LIST="TRUNCATE"
CFG_SIGNAL_ENABLED="true"
CFG_SIGNAL_POLL_INTERVAL_MS="6000"
CFG_SKIP_EVENTS_WITHOUT_ROW_DATA="true"
CFG_SKIP_MESSAGES_WITHOUT_CHANGE="true"
CFG_SKIP_UNPARSEABLE_UNDO="true"
CFG_SKIPPED_OPERATIONS="t"
CFG_SNAPSHOT_FETCH_SIZE="10000"
CFG_SNAPSHOT_LOCKING_MODE="none"
CFG_SNAPSHOT_MAX_THREADS="1"
CFG_SNAPSHOT_MODE="${CFG_SNAPSHOT_MODE}"
CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_DATA="false"
CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_ON_DATA_ERROR="false"
CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_ON_SCHEMA_ERROR="false"
CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_SCHEMA="true"
CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_START_STREAM="true"
CFG_TASKS_MAX="1"
CFG_TIME_PRECISION_MODE="connect"
CFG_TOMBSTONES_ON_DELETE="false"
CFG_HEARTBEAT_INTERVAL_MS="60000"

#Version > 3.4
CFG_LOG_MINING_WINDOW_MAX_MS="18000000"
CFG_LOG_MINING_LOG_COUNT="2"

############################################################
# TOPIC DEFAULTS
############################################################

CFG_TOPIC_CREATION_ENABLE="true"
CFG_TOPIC_CREATION_DEFAULT_CLEANUP_POLICY="delete"
CFG_TOPIC_CREATION_DEFAULT_MIN_INSYNC_REPLICAS="2"
CFG_TOPIC_CREATION_DEFAULT_PARTITIONS="1"
CFG_TOPIC_CREATION_DEFAULT_REPLICATION_FACTOR="3"
CFG_TOPIC_CREATION_DEFAULT_RETENTION_MS="432000000"

############################################################
# PRODUCER DEFAULTS
############################################################
CFG_PRODUCER_OVERRIDE_BATCH_SIZE="1024000"
CFG_PRODUCER_OVERRIDE_COMPRESSION_TYPE="zstd"
CFG_PRODUCER_OVERRIDE_LINGER_MS="60000"
CFG_PRODUCER_OVERRIDE_MAX_REQUEST_SIZE="65538900"

############################################################
# CONVERTERS
############################################################

CFG_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter"
CFG_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter"
CFG_KEY_CONVERTER_SCHEMAS_ENABLE="true"
CFG_VALUE_CONVERTER_SCHEMAS_ENABLE="true"
CFG_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL="true"
CFG_SCHEMA_HISTORY_INTERNAL_STORE_ONLY_CAPTURED_TABLES_DDL="true"
CFG_VALUE_CONVERTER_CONNECT_META_DATA="false"
CFG_VALUE_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT="true"




# END Template Parameters


JSON_FILE="${CONNECTOR_DIR}/${CONNECTOR_NAME}.json"

##Transform escape for print \
CFG_KAFKA_SASL_JAAS_CONFIG_JSON=$(json_escape "$CFG_KAFKA_SASL_JAAS_CONFIG")

INFINISPAN_BLOCK=""
if [[ "${CFG_LOG_MINING_BUFFER_TYPE}" == "infinispan_embedded" ]]; then
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL:-}")
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS:-}")
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS:-}")
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACKS_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACKS:-}")
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES:-}")
    CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS_JSON=$(json_escape "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS:-}")

    INFINISPAN_BLOCK=$(cat <<EOF
    "log.mining.buffer.infinispan.cache.events": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_EVENTS_JSON}",
    "log.mining.buffer.infinispan.cache.global": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_GLOBAL_JSON}",
    "log.mining.buffer.infinispan.cache.processed_transactions": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_PROCESSED_TRANSACTIONS_JSON}",
    "log.mining.buffer.infinispan.cache.rollbacks": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_ROLLBACKS_JSON}",
    "log.mining.buffer.infinispan.cache.schema_changes": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_SCHEMA_CHANGES_JSON}",
    "log.mining.buffer.infinispan.cache.transactions": "${CFG_LOG_MINING_BUFFER_INFINISPAN_CACHE_TRANSACTIONS_JSON}",
EOF
)
fi

#To be added for version => 3.5
#"log.mining.log.count.min"=${CFG_LOG_MINING_LOG_COUNT_MIN}

generate_json() {

cat > "${JSON_FILE}" <<EOF
{
"name": "${CONNECTOR_NAME}",
"config":
{
    "before.state.include": "${CFG_BEFORE_STATE_INCLUDE}",
    "column.propagate.source.type": "${COLUMN_PROPAGATE}",
    "connector.class": "${CFG_CONNECTOR_CLASS}",
    "database.connection.adapter": "${CFG_DATABASE_CONNECTION_ADAPTER}",
    "database.dbname": "${CDB_NAME}",
    "database.oracle.jdbc.defaultRowPrefetch": "${CFG_DATABASE_ORACLE_JDBC_DEFAULT_ROW_PREFETCH}",
    "database.password": "${DB_PASSWORD}",
    "database.pdb.name": "${PDB_NAME_UPPER}",
    "database.url": "${DATABASE_URL}",
    "database.user": "${DB_USER}",
    "heartbeat.action.query": "${HEARTBEAT_ACTION_QUERY}",
    "heartbeat.interval.ms": "${CFG_HEARTBEAT_INTERVAL_MS}",
    "heartbeat.topic.name": "${HEARTBEAT_TOPIC}",
    "key.converter": "${CFG_KEY_CONVERTER}",
    "key.converter.basic.auth.credentials.source": "${CFG_KEY_CONVERTER_BASIC_AUTH_SOURCE}",
    "key.converter.basic.auth.user.info": "${CFG_KEY_CONVERTER_BASIC_AUTH_USER_INFO}",
    "key.converter.schema.registry.ssl.truststore.location": "${CFG_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "key.converter.schema.registry.ssl.truststore.password": "${CFG_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "key.converter.schema.registry.url": "${CFG_KEY_CONVERTER_SCHEMA_REGISTRY_URL}",
    "key.converter.schemas.enable": "${CFG_KEY_CONVERTER_SCHEMAS_ENABLE}",
    "lob.enabled": "${CFG_LOB_ENABLED}",
    "log.mining.archive.log.only.mode": "${CFG_LOG_MINING_ARCHIVE_LOG_ONLY_MODE}",
${INFINISPAN_BLOCK}
    "log.mining.buffer.type": "${CFG_LOG_MINING_BUFFER_TYPE}",
    "log.mining.log.count.min": "${CFG_LOG_MINING_LOG_COUNT}",
    "log.mining.session.max.ms": "${CFG_LOG_MINING_SESSION_MAX_MS}",
    "log.mining.strategy": "${CFG_LOG_MINING_STRATEGY}",
    "log.mining.transaction.retention.ms": "${CFG_LOG_MINING_TRANSACTION_RETENTION_MS}",
    "log.mining.username.exclude.list": "${CFG_LOG_MINING_USERNAME_EXCLUDE_LIST}",
    "log.mining.window.max.ms": "${CFG_LOG_MINING_WINDOW_MAX_MS}",
    "message.key.columns": "${MESSAGE_KEY_COLUMNS}",
    "post.processors": "${CFG_POST_PROCESSORS}",
    "post.processors.reselector.reselect.columns.include.list": "${RESELECT_COLUMNS}",
    "post.processors.reselector.type": "io.debezium.processors.reselect.ReselectColumnsPostProcessor",
    "primary.key.mode": "${CFG_PRIMARY_KEY_MODE}",
    "producer.override.batch.size": "${CFG_PRODUCER_OVERRIDE_BATCH_SIZE}",
    "producer.override.compression.type": "${CFG_PRODUCER_OVERRIDE_COMPRESSION_TYPE}",
    "producer.override.linger.ms": "${CFG_PRODUCER_OVERRIDE_LINGER_MS}",
    "producer.override.max.request.size": "${CFG_PRODUCER_OVERRIDE_MAX_REQUEST_SIZE}",
    "producer.override.sasl.jaas.config": "${CFG_PRODUCER_SASL_JAAS_CONFIG}",
    "producer.override.sasl.mechanism": "${CFG_PRODUCER_SASL_MECHANISM}",
    "producer.override.security.protocol": "${CFG_PRODUCER_SECURITY_PROTOCOL}",
    "producer.override.ssl.key.password": "${CFG_PRODUCER_SSL_KEY_PASSWORD}",
    "producer.override.ssl.keystore.location": "${CFG_PRODUCER_SSL_KEYSTORE_LOCATION}",
    "producer.override.ssl.keystore.password": "${CFG_PRODUCER_SSL_KEYSTORE_PASSWORD}",
    "producer.override.ssl.keystore.type": "${CFG_PRODUCER_SSL_KEYSTORE_TYPE}",
    "producer.override.ssl.truststore.location": "${CFG_PRODUCER_SSL_TRUSTSTORE_LOCATION}",
    "producer.override.ssl.truststore.password": "${CFG_PRODUCER_SSL_TRUSTSTORE_PASSWORD}",
    "producer.override.ssl.truststore.type": "${CFG_PRODUCER_SSL_TRUSTSTORE_TYPE}",
    "provide.transaction.metadata": "${CFG_PROVIDE_TRANSACTION_METADATA}",
    "schema.change.event.exclude.list": "${CFG_SCHEMA_CHANGE_EVENT_EXCLUDE_LIST}",
    "schema.history.internal.consumer.sasl.jaas.config": "${CFG_SCHEMA_HISTORY_CONSUMER_SASL_JAAS_CONFIG}",
    "schema.history.internal.consumer.sasl.mechanism": "${CFG_SCHEMA_HISTORY_CONSUMER_SASL_MECHANISM}",
    "schema.history.internal.consumer.security.protocol": "${CFG_SCHEMA_HISTORY_CONSUMER_SECURITY_PROTOCOL}",
    "schema.history.internal.consumer.ssl.key.password": "${CFG_SCHEMA_HISTORY_CONSUMER_SSL_KEY_PASSWORD}",
    "schema.history.internal.consumer.ssl.keystore.location": "${CFG_SCHEMA_HISTORY_CONSUMER_SSL_KEYSTORE_LOCATION}",
    "schema.history.internal.consumer.ssl.keystore.password": "${CFG_SCHEMA_HISTORY_CONSUMER_SSL_KEYSTORE_PASSWORD}",
    "schema.history.internal.consumer.ssl.truststore.location": "${CFG_SCHEMA_HISTORY_CONSUMER_SSL_TRUSTSTORE_LOCATION}",
    "schema.history.internal.consumer.ssl.truststore.password": "${CFG_SCHEMA_HISTORY_CONSUMER_SSL_TRUSTSTORE_PASSWORD}",
    "schema.history.internal.kafka.bootstrap.servers": "${CFG_KAFKA_BOOTSTRAP_SERVERS}",
    "schema.history.internal.kafka.topic": "${SCHEMA_HISTORY_TOPIC}",
    "schema.history.internal.producer.sasl.jaas.config": "${CFG_SCHEMA_HISTORY_PRODUCER_SASL_JAAS_CONFIG}",
    "schema.history.internal.producer.sasl.mechanism": "${CFG_SCHEMA_HISTORY_PRODUCER_SASL_MECHANISM}",
    "schema.history.internal.producer.security.protocol": "${CFG_SCHEMA_HISTORY_PRODUCER_SECURITY_PROTOCOL}",
    "schema.history.internal.producer.ssl.key.password": "${CFG_SCHEMA_HISTORY_PRODUCER_SSL_KEY_PASSWORD}",
    "schema.history.internal.producer.ssl.keystore.location": "${CFG_SCHEMA_HISTORY_PRODUCER_SSL_KEYSTORE_LOCATION}",
    "schema.history.internal.producer.ssl.keystore.password": "${CFG_SCHEMA_HISTORY_PRODUCER_SSL_KEYSTORE_PASSWORD}",
    "schema.history.internal.producer.ssl.truststore.location": "${CFG_SCHEMA_HISTORY_PRODUCER_SSL_TRUSTSTORE_LOCATION}",
    "schema.history.internal.producer.ssl.truststore.password": "${CFG_SCHEMA_HISTORY_PRODUCER_SSL_TRUSTSTORE_PASSWORD}",
    "schema.history.internal.skip.unparseable.ddl": "${CFG_SCHEMA_HISTORY_INTERNAL_SKIP_UNPARSEABLE_DDL}",
    "schema.history.internal.store.only.captured.tables.ddl": "${CFG_SCHEMA_HISTORY_INTERNAL_STORE_ONLY_CAPTURED_TABLES_DDL}",
    "signal.data.collection": "${PDB_NAME_UPPER}.C##DEBEZIUM.DEBEZIUM_SCN_TRACKER",
    "signal.enabled": "${CFG_SIGNAL_ENABLED}",
    "signal.poll.interval.ms": "${CFG_SIGNAL_POLL_INTERVAL_MS}",
    "skip.events.without.row.data": "${CFG_SKIP_EVENTS_WITHOUT_ROW_DATA}",
    "skip.messages.without.change": "${CFG_SKIP_MESSAGES_WITHOUT_CHANGE}",
    "skip.unparseable.undo": "${CFG_SKIP_UNPARSEABLE_UNDO}",
    "skipped.operations": "${CFG_SKIPPED_OPERATIONS}",
    "snapshot.fetch.size": "${CFG_SNAPSHOT_FETCH_SIZE}",
    "snapshot.locking.mode": "${CFG_SNAPSHOT_LOCKING_MODE}",
    "snapshot.max.threads": "${CFG_SNAPSHOT_MAX_THREADS}",
    "snapshot.mode": "${CFG_SNAPSHOT_MODE}",
    "snapshot.mode.configuration.based.snapshot.data": "${CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_DATA}",
    "snapshot.mode.configuration.based.snapshot.on.data.error": "${CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_ON_DATA_ERROR}",
    "snapshot.mode.configuration.based.snapshot.on.schema.error": "${CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_ON_SCHEMA_ERROR}",
    "snapshot.mode.configuration.based.snapshot.schema": "${CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_SCHEMA}",
    "snapshot.mode.configuration.based.start.stream": "${CFG_SNAPSHOT_MODE_CONFIGURATION_BASED_START_STREAM}",
    "table.include.list": "${TABLE_INCLUDE_LIST}",
    "tasks.max": "${CFG_TASKS_MAX}",
    "time.precision.mode": "${CFG_TIME_PRECISION_MODE}",
    "tombstones.on.delete": "${CFG_TOMBSTONES_ON_DELETE}",
    "topic.creation.default.cleanup.policy": "${CFG_TOPIC_CREATION_DEFAULT_CLEANUP_POLICY}",
    "topic.creation.default.min.insync.replicas": "${CFG_TOPIC_CREATION_DEFAULT_MIN_INSYNC_REPLICAS}",
    "topic.creation.default.partitions": "${CFG_TOPIC_CREATION_DEFAULT_PARTITIONS}",
    "topic.creation.default.replication.factor": "${CFG_TOPIC_CREATION_DEFAULT_REPLICATION_FACTOR}",
    "topic.creation.default.retention.ms": "${CFG_TOPIC_CREATION_DEFAULT_RETENTION_MS}",
    "topic.creation.enable": "${CFG_TOPIC_CREATION_ENABLE}",
    "topic.prefix": "${TOPIC_PREFIX}",
    "value.converter": "${CFG_VALUE_CONVERTER}",
    "value.converter.basic.auth.credentials.source": "${CFG_VALUE_CONVERTER_BASIC_AUTH_SOURCE}",
    "value.converter.basic.auth.user.info": "${CFG_VALUE_CONVERTER_BASIC_AUTH_USER_INFO}",
    "value.converter.connect.meta.data": "${CFG_VALUE_CONVERTER_CONNECT_META_DATA}",
    "value.converter.enhanced.avro.schema.support": "${CFG_VALUE_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT}",
    "value.converter.schema.registry.ssl.truststore.location": "${CFG_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "value.converter.schema.registry.ssl.truststore.password": "${CFG_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "value.converter.schema.registry.url": "${CFG_VALUE_CONVERTER_SCHEMA_REGISTRY_URL}",
    "value.converter.schemas.enable": "${CFG_VALUE_CONVERTER_SCHEMAS_ENABLE}"
}
}
EOF

}
##Generate Source Connector Json
generate_json



#############################################################
#Generate Sink conenctors
#############################################################


####################################################
# Generate PK_FIELDS
####################################################

PK_FIELDS=$(echo "$MESSAGE_KEY_COLUMNS" |  sed 's/C##DEBEZIUM\.DEBEZIUM_HEARTBEAT:ID;//')

#echo "Length: ${#PK_FIELDS}"
#echo "[OK] pk.fields generated"

####################################################
# Common Sink Defaults
####################################################
SINK_CONNECTION_USERNAME="${TARGET_DB_USER}"
SINK_CONNECTION_PASSWORD="${TARGET_DB_PASSWORD}"
SINK_CONNECTION_URL="${TARGET_JDBC_URL}"
SINK_COLLECTION_NAME_FORMAT="MES_REPORTING.\${source.table}"
SINK_CONNECTOR_CLASS="io.debezium.connector.jdbc.JdbcSinkConnector"
SINK_AUTO_CREATE="false"
SINK_STD_INSERT_MODE="upsert"
SINK_STD_DELETE_ENABLED="true"
SINK_BATCH_SIZE="10000"
SINK_CONNECTION_AUTOCOMMIT="false"
SINK_CONSUMER_FETCH_MAX_BYTES="104857600"
SINK_CONSUMER_MAX_POLL_RECORDS="50000"
SINK_ERRORS_TOLERANCE="none"
SINK_HIBERNATE_DRIVER_CLASS="oracle.jdbc.OracleDriver"
SINK_HIBERNATE_FETCH_SIZE="10000"
SINK_INSERT_BATCH_SIZE="50000"
SINK_JDBC_BATCH_SIZE="10000"
SINK_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter"
SINK_KEY_CONVERTER_SCHEMAS_ENABLE="true"
SINK_MAX_IN_FLIGHT_REQUESTS="6"
SINK_PRIMARY_KEY_MODE="record_key"
SINK_RETRY_BACKOFF_MS="500"
SINK_SCHEMA_EVOLUTION="none"
SINK_TASKS_MAX="1"
SINK_TRANSFORMS="dropDupHeaders"
SINK_DROP_HEADERS="__debezium.context.connectorName,__debezium.context.taskId,__debezium.context.connectorLogicalName"
SINK_DROP_HEADERS_TYPE="org.apache.kafka.connect.transforms.DropHeaders"
SINK_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter"
SINK_VALUE_CONVERTER_SCHEMAS_ENABLE="true"
SINK_TOPICS_REGEX="${TOPIC_PREFIX}\\\\.${SCHEMA}\\\\.(?!DOCUMENTATIONEVENTS\$).*"
SINK_PK_FIELDS="${PK_FIELDS}"


####################################################
# Sink Connector Name
####################################################

SINK_CONNECTOR_NAME="snk.sfm.${SITE_LOWER}.${MES_LOWER}.${SERVICE_NAME}"
SINK_JSON_FILE="${CONNECTOR_DIR}/${SINK_CONNECTOR_NAME}.json"



####################################################
# CLOB Sink Connector
####################################################

SINK_CLOB_CONNECTOR_NAME="${SINK_CONNECTOR_NAME}.lob"
SINK_CLOB_COLLECTION_NAME_FORMAT="MES_REPORTING.\${topic}"
SINK_CLOB_MAX_IN_FLIGHT_REQUESTS="1"
SINK_CLOB_TOPICS_REGEX="${TOPIC_PREFIX}.${SCHEMA}.DOCUMENTATIONEVENTS"
SINK_CLOB_TRANSFORMS="renameTopic"
SINK_CLOB_RENAME_TOPIC_REGEX="${TOPIC_PREFIX}.${SCHEMA}.DOCUMENTATIONEVENTS"
SINK_CLOB_RENAME_TOPIC_REPLACEMENT="DOCUMENTATIONEVENTS_STAGE"
SINK_CLOB_RENAME_TOPIC_TYPE="org.apache.kafka.connect.transforms.RegexRouter"
SINK_CLOB_INSERT_MODE="insert"
SINK_CLOB_DELETE_ENABLED=""




####################################################
# Sink Connector Name  -  File
####################################################

SINK_CONNECTOR_NAME="snk.sfm.${SITE_LOWER}.${MES_LOWER}.${SERVICE_NAME}"
SINK_JSON_FILE="${CONNECTOR_DIR}/${SINK_CONNECTOR_NAME}.json"


####################################################
# CLOB Sink Connector Name - File
####################################################

SINK_CLOB_CONNECTOR_NAME="${SINK_CONNECTOR_NAME}.lob"
SINK_CLOB_JSON_FILE="${CONNECTOR_DIR}/${SINK_CLOB_CONNECTOR_NAME}.json"




############################################################
# Generate Main JDBC Sink JSON
############################################################



generate_sink_json() {

cat > "${SINK_JSON_FILE}" <<EOF
{
    "auto.create": "${SINK_AUTO_CREATE}",
    "batch.size": "${SINK_BATCH_SIZE}",
    "collection.name.format": "${SINK_COLLECTION_NAME_FORMAT}",
    "connection.autocommit": "${SINK_CONNECTION_AUTOCOMMIT}",
    "connection.password": "${SINK_CONNECTION_PASSWORD}",
    "connection.url": "${SINK_CONNECTION_URL}",
    "connection.username": "${SINK_CONNECTION_USERNAME}",
    "connector.class": "${SINK_CONNECTOR_CLASS}",
    "consumer.override.fetch.max.bytes": "${SINK_CONSUMER_FETCH_MAX_BYTES}",
    "consumer.override.max.poll.records": "${SINK_CONSUMER_MAX_POLL_RECORDS}",
    "consumer.override.sasl.jaas.config": "${CFG_SINK_CONSUMER_SASL_JAAS_CONFIG}",
    "consumer.override.sasl.mechanism": "${CFG_SINK_CONSUMER_SASL_MECHANISM}",
    "consumer.override.security.protocol": "${CFG_SINK_CONSUMER_SECURITY_PROTOCOL}",
    "consumer.override.ssl.key.password": "${CFG_SINK_CONSUMER_SSL_KEY_PASSWORD}",
    "consumer.override.ssl.keystore.location": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_LOCATION}",
    "consumer.override.ssl.keystore.password": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_PASSWORD}",
    "consumer.override.ssl.keystore.type": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_TYPE}",
    "consumer.override.ssl.truststore.location": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_LOCATION}",
    "consumer.override.ssl.truststore.password": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_PASSWORD}",
    "consumer.override.ssl.truststore.type": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_TYPE}",
    "delete.enabled": "${SINK_STD_DELETE_ENABLED}",
    "errors.tolerance": "${SINK_ERRORS_TOLERANCE}",
    "hibernate.connection.driver_class": "${SINK_HIBERNATE_DRIVER_CLASS}",
    "hibernate.jdbc.fetch_size": "${SINK_HIBERNATE_FETCH_SIZE}",
    "insert.batch.size": "${SINK_INSERT_BATCH_SIZE}",
    "insert.mode": "${SINK_STD_INSERT_MODE}",
    "jdbc.batch.size": "${SINK_JDBC_BATCH_SIZE}",
    "key.converter": "${SINK_KEY_CONVERTER}",
    "key.converter.basic.auth.credentials.source": "${CFG_SINK_KEY_CONVERTER_BASIC_AUTH_SOURCE}",
    "key.converter.basic.auth.user.info": "${CFG_SINK_KEY_CONVERTER_BASIC_AUTH_USER_INFO}",
    "key.converter.enhanced.avro.schema.support": "${CFG_SINK_KEY_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT}",
    "key.converter.schema.registry.ssl.truststore.location": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "key.converter.schema.registry.ssl.truststore.password": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "key.converter.schema.registry.ssl.truststore.type": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE}",
    "key.converter.schema.registry.url": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_URL}",
    "key.converter.schemas.enable": "${SINK_KEY_CONVERTER_SCHEMAS_ENABLE}",
    "max.in.flight.requests": "${SINK_MAX_IN_FLIGHT_REQUESTS}",
    "pk.fields": "${SINK_PK_FIELDS}",
    "primary.key.mode": "${SINK_PRIMARY_KEY_MODE}",
    "retry.backoff.ms": "${SINK_RETRY_BACKOFF_MS}",
    "schema.evolution": "${SINK_SCHEMA_EVOLUTION}",
    "tasks.max": "${SINK_TASKS_MAX}",
    "topics.regex": "${SINK_TOPICS_REGEX}",
    "transforms": "${SINK_TRANSFORMS}",
    "transforms.dropDupHeaders.headers": "${SINK_DROP_HEADERS}",
    "transforms.dropDupHeaders.type": "${SINK_DROP_HEADERS_TYPE}",
    "value.converter": "${SINK_VALUE_CONVERTER}",
    "value.converter.basic.auth.credentials.source": "${CFG_SINK_VALUE_CONVERTER_BASIC_AUTH_SOURCE}",
    "value.converter.basic.auth.user.info": "${CFG_SINK_VALUE_CONVERTER_BASIC_AUTH_USER_INFO}",
    "value.converter.enhanced.avro.schema.support": "${CFG_SINK_VALUE_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT}",
    "value.converter.schema.registry.ssl.truststore.location": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "value.converter.schema.registry.ssl.truststore.password": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "value.converter.schema.registry.ssl.truststore.type": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE}",
    "value.converter.schema.registry.url": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_URL}",
    "value.converter.schemas.enable": "${SINK_VALUE_CONVERTER_SCHEMAS_ENABLE}"
}
EOF

}

# Generating Sink Connectors
generate_sink_json



############################################################
# Generate CLOB Sink JSON
############################################################

generate_sink_clob_json() {

cat > "${SINK_CLOB_JSON_FILE}" <<EOF

{
    "auto.create": "${SINK_AUTO_CREATE}",
    "batch.size": "${SINK_BATCH_SIZE}",
    "collection.name.format": "${SINK_CLOB_COLLECTION_NAME_FORMAT}",
    "connection.autocommit": "${SINK_CONNECTION_AUTOCOMMIT}",
    "connection.password": "${SINK_CONNECTION_PASSWORD}",
    "connection.url": "${SINK_CONNECTION_URL}",
    "connection.username": "${SINK_CONNECTION_USERNAME}",
    "connector.class": "${SINK_CONNECTOR_CLASS}",
    "consumer.override.fetch.max.bytes": "${SINK_CONSUMER_FETCH_MAX_BYTES}",
    "consumer.override.max.poll.records": "${SINK_CONSUMER_MAX_POLL_RECORDS}",
    "consumer.override.sasl.jaas.config": "${CFG_SINK_CONSUMER_SASL_JAAS_CONFIG}",
    "consumer.override.sasl.mechanism": "${CFG_SINK_CONSUMER_SASL_MECHANISM}",
    "consumer.override.security.protocol": "${CFG_SINK_CONSUMER_SECURITY_PROTOCOL}",
    "consumer.override.ssl.key.password": "${CFG_SINK_CONSUMER_SSL_KEY_PASSWORD}",
    "consumer.override.ssl.keystore.location": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_LOCATION}",
    "consumer.override.ssl.keystore.password": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_PASSWORD}",
    "consumer.override.ssl.keystore.type": "${CFG_SINK_CONSUMER_SSL_KEYSTORE_TYPE}",
    "consumer.override.ssl.truststore.location": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_LOCATION}",
    "consumer.override.ssl.truststore.password": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_PASSWORD}",
    "consumer.override.ssl.truststore.type": "${CFG_SINK_CONSUMER_SSL_TRUSTSTORE_TYPE}",
    "errors.tolerance": "${SINK_ERRORS_TOLERANCE}",
    "hibernate.connection.driver_class": "${SINK_HIBERNATE_DRIVER_CLASS}",
    "hibernate.jdbc.fetch_size": "${SINK_HIBERNATE_FETCH_SIZE}",
    "insert.batch.size": "${SINK_INSERT_BATCH_SIZE}",
    "insert.mode": "${SINK_CLOB_INSERT_MODE}",
    "jdbc.batch.size": "${SINK_JDBC_BATCH_SIZE}",
    "key.converter": "${SINK_KEY_CONVERTER}",
    "key.converter.basic.auth.credentials.source": "${CFG_SINK_KEY_CONVERTER_BASIC_AUTH_SOURCE}",
    "key.converter.basic.auth.user.info": "${CFG_SINK_KEY_CONVERTER_BASIC_AUTH_USER_INFO}",
    "key.converter.enhanced.avro.schema.support": "${CFG_SINK_KEY_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT}",
    "key.converter.schema.registry.ssl.truststore.location": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "key.converter.schema.registry.ssl.truststore.password": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "key.converter.schema.registry.ssl.truststore.type": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE}",
    "key.converter.schema.registry.url": "${CFG_SINK_KEY_CONVERTER_SCHEMA_REGISTRY_URL}",
    "key.converter.schemas.enable": "${SINK_KEY_CONVERTER_SCHEMAS_ENABLE}",
    "max.in.flight.requests": "${SINK_CLOB_MAX_IN_FLIGHT_REQUESTS}",
    "primary.key.mode": "${SINK_PRIMARY_KEY_MODE}",
    "schema.evolution": "${SINK_SCHEMA_EVOLUTION}",
    "tasks.max": "${SINK_TASKS_MAX}",
    "topics.regex": "${SINK_CLOB_TOPICS_REGEX}",
    "transforms": "${SINK_CLOB_TRANSFORMS}",
    "transforms.renameTopic.regex": "${SINK_CLOB_RENAME_TOPIC_REGEX}",
    "transforms.renameTopic.replacement": "${SINK_CLOB_RENAME_TOPIC_REPLACEMENT}",
    "transforms.renameTopic.type": "${SINK_CLOB_RENAME_TOPIC_TYPE}",
    "value.converter": "${SINK_VALUE_CONVERTER}",
    "value.converter.basic.auth.credentials.source": "${CFG_SINK_VALUE_CONVERTER_BASIC_AUTH_SOURCE}",
    "value.converter.basic.auth.user.info": "${CFG_SINK_VALUE_CONVERTER_BASIC_AUTH_USER_INFO}",
    "value.converter.enhanced.avro.schema.support": "${CFG_SINK_VALUE_CONVERTER_ENHANCED_AVRO_SCHEMA_SUPPORT}",
    "value.converter.schema.registry.ssl.truststore.location": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION}",
    "value.converter.schema.registry.ssl.truststore.password": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD}",
    "value.converter.schema.registry.ssl.truststore.type": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE}",
    "value.converter.schema.registry.url": "${CFG_SINK_VALUE_CONVERTER_SCHEMA_REGISTRY_URL}",
    "value.converter.schemas.enable": "${SINK_VALUE_CONVERTER_SCHEMAS_ENABLE}"
}
EOF

}

# Generating Sink CLOB Json
generate_sink_clob_json



####################################################
# Repository DB
####################################################

run_repo_sql() {

sqlplus -s "${REPO_USER}/${REPO_PASSWORD}@${REPO_CONNECT_STRING}" <<EOF
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET LONG 1000000
SET LINESIZE 32767

$1

EXIT
EOF

}

####################################################
# Verify Target Repository Connection
####################################################
echo
echo
echo "Testing repository  database connection..."

if REPO_CONNECTION_RESULT=$(run_repo_sql "
SELECT 'CONNECTED'
FROM dual;
"); then
    REPO_SQL_RC=0
else
    REPO_SQL_RC=$?
fi

if [[ ${REPO_SQL_RC} -ne 0 ]]; then

    echo
    echo "[FAIL] Repository database connection failed."
    echo
    echo "${REPO_CONNECTION_RESULT}"
    echo

    exit 1

fi


REPO_CONNECTION_RESULT=$(
    echo "${REPO_CONNECTION_RESULT}" |
    sed '/^[[:space:]]*$/d' |
    xargs
)


if [[ "${REPO_CONNECTION_RESULT}" != "CONNECTED" ]]; then

    echo
    echo "[FAIL] Repository connection validation failed."
    echo
    echo "${REPO_CONNECTION_RESULT}"
    echo

    exit 1

fi

ok " Repository connection successful"
echo

####################################################
# Check Connector Exists
####################################################

echo
echo "Checking repository if exists  Connector with ID ${CONNECTOR_ID}..."

if CONNECTOR_EXISTS=$(run_repo_sql "
SELECT COUNT(*)
FROM C##DEBEZIUM.CDC_CONNECTOR
WHERE CONNECTOR_ID = ${CONNECTOR_ID};
"); then
    CONNECTOR_CHECK_RC=0
else
    CONNECTOR_CHECK_RC=$?
fi

if [[ ${CONNECTOR_CHECK_RC} -ne 0 ]]; then
    echo
    echo "[FAIL] Unable to check CDC_CONNECTOR."
    echo
    echo "${CONNECTOR_EXISTS}"
    echo
    exit 1
fi

CONNECTOR_EXISTS=$(echo "${CONNECTOR_EXISTS}" | xargs)

if [[ "${CONNECTOR_EXISTS}" -gt 0 ]]; then
    echo "[INFO] Connector ID ${CONNECTOR_ID} already exists."
    echo

    run_repo_sql "
    SELECT
           CONNECTOR_ID
        || ' | '
        || NVL(CONNECTOR_TYPE,'NULL')
        || ' | SRC='
        || NVL(SRC_CONNECTOR_NAME,'NULL')
        || ' | SNK='
        || NVL(SNK_CONNECTOR_NAME,'NULL')
        || ' | LOB='
        || NVL(SNK_LOB_CONNECTOR_NAME,'NULL')
        || ' | STATUS='
        || NVL(STATUS,'NULL')
    FROM C##DEBEZIUM.CDC_CONNECTOR
    WHERE CONNECTOR_ID = ${CONNECTOR_ID};
    "

#read -rp "Update connector ${CONNECTOR_ID} (Y/N)? " ANSWER
#    if [[ ! "${ANSWER}" =~ ^[Yy]$ ]]; then
#        echo
#        echo "[INFO] Update cancelled."
#        echo
#        exit 0
#    fi

#    echo
#    echo "[INFO] Updating connector ${CONNECTOR_ID}..."

else
    echo
    echo "[FAIL] Connector ID ${CONNECTOR_ID} does not exist in CDC_CONNECTOR."
    echo
    echo "!!! This step  should have been created during source database preparation."
    echo " Run first step to prepare database [ 2_prepare_source_db_kafka.sh ]"
    exit 1

fi


####################################################
# MERGE CONNECTOR METADATA
####################################################

merge_connector_repository() {
    run_repo_sql "
MERGE INTO C##DEBEZIUM.CDC_CONNECTOR t
USING
(
    SELECT
        ${CONNECTOR_ID}              AS CONNECTOR_ID,
        '${CONNECTOR_NAME}'          AS SRC_CONNECTOR_NAME,
        '${SINK_CONNECTOR_NAME}'     AS SNK_CONNECTOR_NAME,
        '${SINK_CLOB_CONNECTOR_NAME}' AS SNK_LOB_CONNECTOR_NAME
    FROM dual
) s
ON
(
    t.CONNECTOR_ID = s.CONNECTOR_ID
)
WHEN MATCHED THEN
    UPDATE SET
        t.CONNECTOR_TYPE         = 'KAFKA_SRC_SNK',
        t.SRC_CONNECTOR_NAME     = s.SRC_CONNECTOR_NAME,
        t.SNK_CONNECTOR_NAME     = s.SNK_CONNECTOR_NAME,
        t.SNK_LOB_CONNECTOR_NAME = s.SNK_LOB_CONNECTOR_NAME,
        t.SCHEMA_NAME             = '${SCHEMA}',
        t.MES_VERSION             = ${MES_VERSION},
        t.SITE                    = '${SITE_UPPER}',
        t.MES_ID                  = '${MES_ID}',
        t.DB_NAME                 = '${DB_NAME}',
        t.PDB_NAME                = '${PDB_NAME}',
        t.SERVICE_NAME            = '${SERVICE_NAME}',
        t.HEARTBEAT_ID            = ${HEARTBEAT_ID},
        t.DB_USER                 = '${DB_USER}',
        t.UPDATED_AT              = SYSDATE,
        t.ENVIRONMENT             = '${ENVIRONMENT}',
                t.STATUS                                  = 'GENERATED'
WHEN NOT MATCHED THEN
    INSERT
    (
        CONNECTOR_ID,
        CONNECTOR_TYPE,
        SRC_CONNECTOR_NAME,
        SNK_CONNECTOR_NAME,
        SNK_LOB_CONNECTOR_NAME,
        SCHEMA_NAME,
        SITE,
        MES_VERSION,
        MES_ID,
        DB_NAME,
        PDB_NAME,
        SERVICE_NAME,
        HEARTBEAT_ID,
        DB_USER,
        STATUS,
        CREATED_AT,
        UPDATED_AT,
        ENVIRONMENT
    )
    VALUES
    (
        s.CONNECTOR_ID,
        'KAFKA_SRC_SNK',
        s.SRC_CONNECTOR_NAME,
        s.SNK_CONNECTOR_NAME,
        s.SNK_LOB_CONNECTOR_NAME,
        '${SCHEMA}',
        '${SITE_UPPER}',
        ${MES_VERSION},
        '${MES_ID}',
        '${DB_NAME}',
        '${PDB_NAME}',
        '${SERVICE_NAME}',
        ${HEARTBEAT_ID},
        '${DB_USER}',
        'GENERATED',
        SYSDATE,
        SYSDATE,
        '${ENVIRONMENT}'
    );
COMMIT;
"
}

echo
echo "Saving connector metadata..."
if ! merge_connector_repository; then
     echo
     echo "[FAIL] Unable to merge connector metadata."
     echo
     exit 1
fi
ok "Connector metadata merged"
echo



####################################################
# Store Generated JSON
####################################################

update_json_repository() {

    run_repo_sql "
    UPDATE C##DEBEZIUM.CDC_CONNECTOR
       SET GENERATED_JSON = EMPTY_CLOB(),
           STATUS = 'GENERATED',
           UPDATED_AT = SYSDATE
     WHERE CONNECTOR_ID = ${CONNECTOR_ID};

    COMMIT;
    "


    JSON_ESCAPED=$(sed "s/'/''/g" "${JSON_FILE}")


    echo "$JSON_ESCAPED" |
    fold -w 3000 |
    while IFS= read -r CHUNK
    do
        run_repo_sql "
        UPDATE C##DEBEZIUM.CDC_CONNECTOR
           SET GENERATED_JSON =
               GENERATED_JSON || TO_CLOB(q'~${CHUNK}~')
         WHERE CONNECTOR_ID = ${CONNECTOR_ID};
        COMMIT;
        "

    done
}





####################################################
# Display Result
####################################################

echo
echo "Repository Entry"

run_repo_sql "
SELECT
       CONNECTOR_ID
    || ' | '
    || CONNECTOR_TYPE
    || ' | '
    || SRC_CONNECTOR_NAME
    || ' | '
    || STATUS
FROM C##DEBEZIUM.CDC_CONNECTOR
WHERE CONNECTOR_ID = ${CONNECTOR_ID};"


run_repo_sql "
SELECT DBMS_LOB.GETLENGTH(GENERATED_JSON)
FROM C##DEBEZIUM.CDC_CONNECTOR
WHERE CONNECTOR_ID = ${CONNECTOR_ID};"

update_sink_json_repository() {

    run_repo_sql "
    UPDATE C##DEBEZIUM.CDC_CONNECTOR
       SET GENERATED_SINK_JSON = EMPTY_CLOB(),
           UPDATED_AT = SYSDATE
     WHERE CONNECTOR_ID = ${CONNECTOR_ID};

    COMMIT;
    "


    JSON_ESCAPED=$(sed "s/'/''/g" "${SINK_JSON_FILE}")


    echo "$JSON_ESCAPED" |
    fold -w 3000 |
    while IFS= read -r CHUNK
    do

        run_repo_sql "
        UPDATE C##DEBEZIUM.CDC_CONNECTOR
           SET GENERATED_SINK_JSON =
               GENERATED_SINK_JSON || TO_CLOB(q'~${CHUNK}~')
         WHERE CONNECTOR_ID = ${CONNECTOR_ID};

        COMMIT;
        "

    done
}


update_sink_clob_json_repository() {

    run_repo_sql "
    UPDATE C##DEBEZIUM.CDC_CONNECTOR
       SET GENERATED_SINK_CLOB_JSON = EMPTY_CLOB(),
           UPDATED_AT = SYSDATE
     WHERE CONNECTOR_ID = ${CONNECTOR_ID};

    COMMIT;
    "


    JSON_ESCAPED=$(sed "s/'/''/g" "${SINK_CLOB_JSON_FILE}")


    echo "$JSON_ESCAPED" |
    fold -w 3000 |
    while IFS= read -r CHUNK
    do

        run_repo_sql "
        UPDATE C##DEBEZIUM.CDC_CONNECTOR
           SET GENERATED_SINK_CLOB_JSON =
               GENERATED_SINK_CLOB_JSON || TO_CLOB(q'~${CHUNK}~')
         WHERE CONNECTOR_ID = ${CONNECTOR_ID};

        COMMIT;
        "

    done
}


echo
echo "Saving source JSON..."

update_json_repository

ok "Source JSON saved"


echo
echo "Saving Sink JSON..."

update_sink_json_repository

echo "[OK] Sink JSON saved"

echo
echo "Saving Sink CLOB JSON..."

update_sink_clob_json_repository

echo "[OK] Sink CLOB JSON saved"


run_repo_sql "
SELECT
    DBMS_LOB.GETLENGTH(GENERATED_JSON)
 || ' | '
 || DBMS_LOB.GETLENGTH(GENERATED_SINK_JSON)
 || ' | '
 || DBMS_LOB.GETLENGTH(GENERATED_SINK_CLOB_JSON)
FROM C##DEBEZIUM.CDC_CONNECTOR
 WHERE CONNECTOR_ID = ${CONNECTOR_ID};
"


echo
echo "[OK] Repository database update completed"
echo "-------------------------------------------------------"
echo



echo "Generated files "
echo "---------------------------------------------------"
echo "Environment              : ${ENVIRONMENT}"
echo
echo "Source Connector Name    : ${CONNECTOR_NAME}"
echo "Source Connector         : ${JSON_FILE}"
echo
echo "Sink Connector Name      : ${SINK_CONNECTOR_NAME}"
echo "Sink JSON File           : ${SINK_JSON_FILE}"
echo
echo "Sink CLOB Connector Name : ${SINK_CLOB_CONNECTOR_NAME}"
echo "Sink CLOB JSON File      : ${SINK_CLOB_JSON_FILE}"
echo "---------------------------------------------------"


