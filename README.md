# Case Study: Apex Capital - Finance Analytics & AI Platform

Business Context

Apex Capital provides:
- Credit cards
- Personal loans
- Merchant payments

Finance leaders want a single view of profitability across:
- Customers
- Products
- Regions

The data flow is:

SQL Data Warehouse → Python Processing → Tableau Dashboards → AI Chatbot

SQL is used to extract and aggregate raw financial data.

Python is used to clean, re-pivot, and apply business logic.

Tableau is used for visualization and reporting.

An AI chatbot sits on top to answer finance questions.
________________________________________
Available Data (in SQL Warehouse)

### customers
- customer_id
- customer_name
- segment
- region

### transactions
- txn_id
- customer_id
- product
- amount
- txn_type   (purchase, refund, interest, fee)
- txn_date

### chargebacks
- xn_id
- chargeback_amount
- chargeback_date
________________________________________
### SECTION 1 – SQL (Data Extraction Layer)
SQL is used only to produce clean, analytics-ready datasets.

#### Q1

Define Net Revenue for Apex Capital.

    Net Revenue = Money In – Money Out

    Money In – From Transactions table purchase, fee and interest 

    Money Out – From Transactions table refund and from Chargebacks table chargeback_amount

    Net Revenue = (purchase + fee + interest) – (refund + chargeback)

Which fields from the SQL tables will be used?

    From ‘transactions’:
        txn_id,
        amount,
        txn_type

    From ‘chargebacks:
        txn_id,
        chargeback_amount

#### Q2

Write a SQL query that produces this dataset for the last 6 months:

| month | customer_id | product | gross_revenue | refunds | chargebacks | net_revenue |

(This output will be sent to Python.)

```
/*
Net revenue (last 6 calendar months)
Time period: From start of current month minus 6 months
*/
-- GRAIN: One row per month per customer per product
SELECT
    DATE_TRUNC('month', t.txn_date) AS month,
    t.customer_id,
    t.product,

    SUM(IFF(t.txn_type IN ('purchase', 'interest', 'fee'), t.amount, 0)) AS gross_revenue,
    SUM(IFF(t.txn_type = 'refund', t.amount, 0)) AS refunds,
    SUM(COALESCE(cb.chargebacks, 0)) AS chargebacks,

    SUM(IFF(t.txn_type IN ('purchase', 'interest', 'fee'), t.amount, 0))
    - SUM(IFF(t.txn_type = 'refund', t.amount, 0))
    - SUM(COALESCE(cb.chargebacks, 0)) AS net_revenue

FROM APEX_CAPITAL.FINANCE.TRANSACTIONS_DEDUP t
LEFT JOIN APEX_CAPITAL.FINANCE.CHARGEBACKS_BY_TXN cb ON t.txn_id = cb.txn_id
WHERE 1=1
    AND t.txn_date >= DATEADD(month, -6, DATE_TRUNC('month', CURRENT_DATE())) -- last 6 months + current month
    -- AND customer_id = 'C001'
GROUP BY 1,2,3
ORDER BY 1,2,3;
```

#### Q3

What checks would you add in SQL to make sure:

- No duplicate transactions exist
```
Check #1: Duplicate transactions
-- Expected: 0 rows
SELECT
    txn_id,
    COUNT(*) AS cnt
FROM APEX_CAPITAL.FINANCE.TRANSACTIONS
GROUP BY txn_id
HAVING COUNT(*) > 1;
```
```
Check #2: Join risk (transactions x chargebacks)
-- If this returns rows, joining chargebacks to transactions table could duplicate amounts
-- Expected: 0 rows
SELECT
    t.txn_id,
    COUNT(*) AS cnt
FROM APEX_CAPITAL.FINANCE.TRANSACTIONS t
LEFT JOIN APEX_CAPITAL.FINANCE.CHARGEBACKS c ON t.txn_id = c.txn_id
GROUP BY t.txn_id
HAVING COUNT(*) > 1;
```
```
/*============================================================
To make sure no duplicate transactions exist
============================================================*/
WITH cte_transactions AS (
SELECT *
FROM APEX_CAPITAL.FINANCE.TRANSACTIONS
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_date DESC) = 1
)
```

- Chargebacks are applied only once
```
Check #3: Multiple chargeback per txn_id
-- Expected: 0 rows
SELECT
    txn_id,
    COUNT(*) AS cnt,
    SUM(chargeback_amount) AS total_chargeback
FROM APEX_CAPITAL.FINANCE.CHARGEBACKS
GROUP BY txn_id
HAVING COUNT(*) > 1;
```
```
/*============================================================
If we assume that there would be multiple chargebacks per transaction
then we aggregate all chargebacks per transaction and left join on transactions table
============================================================*/

WITH cte_chargebacks AS (
SELECT
    txn_id,
    SUM(chargeback_amount) AS chargebacks
FROM APEX_CAPITAL.FINANCE.CHARGEBACKS
GROUP BY txn_id;
)
```
