-- ============================================================================
-- BACKFILL ALL TABLES - WISE DEV ENVIRONMENT
-- ============================================================================
-- This script loads historical data from S3 into all Snowflake landing tables
-- Path pattern: s3://cda-raw-dev/clients/wise/{source}/{table}/run_date=YYYY-MM-DD/*.jsonl.gz
--
-- Usage: Run this entire script or individual sections as needed
-- ============================================================================

USE DATABASE WISE_DEV_RAW;
USE SCHEMA RAW;
USE WAREHOUSE WISE_DEV_LOADING_WH;

-- ============================================================================
-- SCALE UP WAREHOUSE FOR BULK LOADING
-- ============================================================================
ALTER WAREHOUSE WISE_DEV_LOADING_WH SET WAREHOUSE_SIZE = 'LARGE';

-- ============================================================================
-- HARVEST (3 TABLES)
-- ============================================================================

-- Harvest: Clients
COPY INTO HARVEST_CLIENTS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/harvest/clients/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Harvest: Projects
COPY INTO HARVEST_PROJECTS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/harvest/projects/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Harvest: Time Entries
COPY INTO HARVEST_TIME_ENTRIES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/harvest/time_entries/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- INTACCT (3 TABLES)
-- ============================================================================

-- Intacct: Customers
COPY INTO INTACCT_CUSTOMERS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/intacct/customers/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Intacct: GL Entries
COPY INTO INTACCT_GL_ENTRIES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/intacct/gl_entries/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Intacct: Revenue Recognition
COPY INTO INTACCT_REVENUE_RECOGNITION (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/intacct/revenue_recognition/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- JIRA (1 TABLE)
-- ============================================================================

-- Jira: Issues
COPY INTO JIRA_ISSUES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/jira/issues/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- MIXPANEL (1 TABLE)
-- ============================================================================

-- Mixpanel: Events
COPY INTO MIXPANEL_EVENTS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/mixpanel/events/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- SALESFORCE (3 TABLES)
-- ============================================================================

-- Salesforce: Accounts
COPY INTO SF_ACCOUNTS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/salesforce/accounts/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Salesforce: Opportunities
COPY INTO SF_OPPORTUNITIES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/salesforce/opportunities/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Salesforce: Users
COPY INTO SF_USERS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/salesforce/users/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- STRIPE (8 TABLES)
-- ============================================================================

-- Stripe: Balance Transactions
COPY INTO STRIPE_BALANCE_TRANSACTIONS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/balance_transactions/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Charges
COPY INTO STRIPE_CHARGES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/charges/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Customers
COPY INTO STRIPE_CUSTOMERS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/customers/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Disputes
COPY INTO STRIPE_DISPUTES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/disputes/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Invoice Line Items
COPY INTO STRIPE_INVOICE_LINE_ITEMS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/invoice_line_items/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Invoices
COPY INTO STRIPE_INVOICES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/invoices/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Refunds
COPY INTO STRIPE_REFUNDS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/refunds/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Stripe: Subscriptions
COPY INTO STRIPE_SUBSCRIPTIONS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/stripe/subscriptions/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- ZENDESK (4 TABLES)
-- ============================================================================

-- Zendesk: Organizations
COPY INTO ZENDESK_ORGANIZATIONS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/zendesk/organizations/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Zendesk: Ticket Events
COPY INTO ZENDESK_TICKET_EVENTS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/zendesk/ticket_events/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Zendesk: Tickets
COPY INTO ZENDESK_TICKETS (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/zendesk/tickets/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- Zendesk: Time Entries
COPY INTO ZENDESK_TIME_ENTRIES (METADATA_FILENAME, METADATA_ROW_NUMBER, RECORD_CONTENT)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @WISE_DEV_RAW_STAGE/clients/wise/zendesk/time_entries/
)
PATTERN = '.*.jsonl.gz'
ON_ERROR = SKIP_FILE;

-- ============================================================================
-- SCALE DOWN WAREHOUSE
-- ============================================================================
ALTER WAREHOUSE WISE_DEV_LOADING_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Check row counts for all tables

SELECT 'HARVEST_CLIENTS' AS table_name, COUNT(*) AS row_count FROM HARVEST_CLIENTS
UNION ALL
SELECT 'HARVEST_PROJECTS', COUNT(*) FROM HARVEST_PROJECTS
UNION ALL
SELECT 'HARVEST_TIME_ENTRIES', COUNT(*) FROM HARVEST_TIME_ENTRIES
UNION ALL
SELECT 'INTACCT_CUSTOMERS', COUNT(*) FROM INTACCT_CUSTOMERS
UNION ALL
SELECT 'INTACCT_GL_ENTRIES', COUNT(*) FROM INTACCT_GL_ENTRIES
UNION ALL
SELECT 'INTACCT_REVENUE_RECOGNITION', COUNT(*) FROM INTACCT_REVENUE_RECOGNITION
UNION ALL
SELECT 'JIRA_ISSUES', COUNT(*) FROM JIRA_ISSUES
UNION ALL
SELECT 'MIXPANEL_EVENTS', COUNT(*) FROM MIXPANEL_EVENTS
UNION ALL
SELECT 'SF_ACCOUNTS', COUNT(*) FROM SF_ACCOUNTS
UNION ALL
SELECT 'SF_OPPORTUNITIES', COUNT(*) FROM SF_OPPORTUNITIES
UNION ALL
SELECT 'SF_USERS', COUNT(*) FROM SF_USERS
UNION ALL
SELECT 'STRIPE_BALANCE_TRANSACTIONS', COUNT(*) FROM STRIPE_BALANCE_TRANSACTIONS
UNION ALL
SELECT 'STRIPE_CHARGES', COUNT(*) FROM STRIPE_CHARGES
UNION ALL
SELECT 'STRIPE_CUSTOMERS', COUNT(*) FROM STRIPE_CUSTOMERS
UNION ALL
SELECT 'STRIPE_DISPUTES', COUNT(*) FROM STRIPE_DISPUTES
UNION ALL
SELECT 'STRIPE_INVOICE_LINE_ITEMS', COUNT(*) FROM STRIPE_INVOICE_LINE_ITEMS
UNION ALL
SELECT 'STRIPE_INVOICES', COUNT(*) FROM STRIPE_INVOICES
UNION ALL
SELECT 'STRIPE_REFUNDS', COUNT(*) FROM STRIPE_REFUNDS
UNION ALL
SELECT 'STRIPE_SUBSCRIPTIONS', COUNT(*) FROM STRIPE_SUBSCRIPTIONS
UNION ALL
SELECT 'ZENDESK_ORGANIZATIONS', COUNT(*) FROM ZENDESK_ORGANIZATIONS
UNION ALL
SELECT 'ZENDESK_TICKET_EVENTS', COUNT(*) FROM ZENDESK_TICKET_EVENTS
UNION ALL
SELECT 'ZENDESK_TICKETS', COUNT(*) FROM ZENDESK_TICKETS
UNION ALL
SELECT 'ZENDESK_TIME_ENTRIES', COUNT(*) FROM ZENDESK_TIME_ENTRIES
ORDER BY table_name;

-- View load history for all tables
SELECT 
  TABLE_NAME,
  FILE_NAME,
  ROW_COUNT,
  ROW_PARSED,
  STATUS,
  LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => '%',
  START_TIME => DATEADD(hours, -2, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
