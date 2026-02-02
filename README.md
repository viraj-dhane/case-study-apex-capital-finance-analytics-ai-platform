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
- txn_id
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
/*============================================================
Generate monthly net revenue by customer and product for the last 6 months.
Assumptions:
If CHARGEBACKS has multiple rows per txn_id, this join can duplicate
transaction rows and inflate totals unless chargebacks are pre-aggregated.
============================================================*/

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

FROM APEX_CAPITAL.FINANCE.TRANSACTIONS t
LEFT JOIN APEX_CAPITAL.FINANCE.CHARGEBACKS cb ON t.txn_id = cb.txn_id
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
/*============================================================
Check #1: Duplicate transactions
Verify txn_id is unique in TRANSACTIONS (no duplicate transactions)
Expected: 0 rows
============================================================*/
SELECT
    txn_id,
    COUNT(*) AS cnt
FROM APEX_CAPITAL.FINANCE.TRANSACTIONS
GROUP BY txn_id
HAVING COUNT(*) > 1;
```
```
/*============================================================
Check #2: Join risk (transactions x chargebacks)
Detect row multiplication risk when joining CHARGEBACKS to TRANSACTIONS.
If a txn_id maps to multiple chargeback rows, sums can be inflated unless chargebacks are pre-aggregated.
If this returns rows, joining chargebacks to transactions table could duplicate amounts
Assumption:
One chargeback record per txn_id (or chargebacks are pre-aggregated per txn_id before join).
Expected: 0 rows
============================================================*/
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
Purpose: Create a de-duplicated TRANSACTIONS dataset by keeping one row per txn_id.
Assumption: If duplicates exist, the most recent txn_date record is the correct one.
============================================================*/
WITH cte_transactions AS (
SELECT *
FROM APEX_CAPITAL.FINANCE.TRANSACTIONS
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY txn_date DESC) = 1
)
SELECT *
FROM cte_transactions
```

- Chargebacks are applied only once
```
/*============================================================
Check #3: Multiple chargeback per txn_id
Verify that each transaction has at most one chargeback record.
Assumption:
Each txn_id should appear at most once in CHARGEBACKS.
Expected: 0 rows
============================================================*/
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
Validate that total chargebacks applied to a transaction do not exceed the total monetized transaction amount.
Assumptions:
Multiple chargebacks per transaction are allowed.
Expected: 0 rows
============================================================*/
--GRAIN: One row per transaction
WITH cte_chargebacks AS (
SELECT
    txn_id,
    SUM(chargeback_amount) AS chargebacks
FROM APEX_CAPITAL.FINANCE.CHARGEBACKS
GROUP BY txn_id
),
cte_transactions AS (
SELECT
    txn_id,
    SUM(IFF(t.txn_type IN ('purchase', 'interest', 'fee'), t.amount, 0)) AS total_amount
FROM FROM APEX_CAPITAL.FINANCE.TRANSACTIONS
GROUP BY txn_id
)
SELECT
  t.txn_id,
  t.total_amount,
  cb.chargebacks,
  (cb.chargebacks - t.purchase_amount) AS over_by
FROM cte_transactions t
LEFT JOIN cte_chargebacks cb
  ON t.txn_id = cb.txn_id
WHERE cb.chargebacks > t.total_amount;
```
________________________________________
### SECTION 2 – Python (Business Logic & Re-Pivoting)
The SQL output is now loaded into Python as a dataframe.

#### Q4

The SQL output is in this format:

| month | customer_id | product | net_revenue |

But Tableau needs this:

| month | customer_id | total_revenue | loan_revenue | card_revenue | merchant_revenue |

How would you pivot and transform this using Python?
```
"""
Purpose:
Transform long format revenue data (month, customer_id, product, net_revenue)
into wide format columns required by Tableau:
"""
# Read SQL output exported to CSV
df = pd.read_csv(r"C:\Users\Viraj\Desktop\#Job Hunt\Applications\RingCentral\data sets\sql-output-for-python.csv")

#print(df)
#print(df.info())

# Standardize column names and data types
df.columns = df.columns.str.lower()
df['month'] = pd.to_datetime(df['month'])
df['customer_id'] = df['customer_id'].astype('str')
df['product'] = df['product'].astype('str')

# Pivot: convert product rows into product revenue columns
pivot_df = (
    df.pivot_table(
        index=["month", "customer_id"],
        columns="product",
        values="net_revenue",
        aggfunc="sum",     # if multiple transacrions for same product in same month then add
        fill_value=0    # missing product revenue becomes 0
    )
    .reset_index()
)

# Rename pivoted product columns to required Tableau names
pivot_df = pivot_df.rename(columns={
    "loan": "loan_revenue",
    "card": "card_revenue",
    "merchant": "merchant_revenue"
})

# Compute total revenue across products
pivot_df["total_revenue"] = (pivot_df["loan_revenue"] + pivot_df["card_revenue"] + pivot_df["merchant_revenue"])

# Final Tableau schema and sort by customer_id and month
pivot_df = pivot_df[["month", "customer_id", "total_revenue", "loan_revenue", "card_revenue", "merchant_revenue"]].sort_values(["customer_id", "month"])

# Remove pivot table column index name
pivot_df.columns.name = None

#print(pivot_df)
```

#### Q5

How would you:

•	Handle missing months
```
"""
Purpose:
- Ensure each customer has a continuous monthly time series (fill missing months)
- Why: Tableau trend lines and rolling calculations break when there is no activity
  (customer-months) for any month.
- How: Create a complete (month × customer) grid and fill missing revenue
  values with 0 to represent no activity.
"""
df = pivot_df.copy()

# Standardize types and ordering
df["month"] = pd.to_datetime(df["month"])
df["customer_id"] = df["customer_id"].astype(str)
df = df.sort_values(["customer_id", "month"]).reset_index(drop=True)

# print(df)

# Step 1: Get full month range across dataset (Month Start frequency)
all_months = pd.date_range(
    start=df["month"].min(),
    end=df["month"].max(),
    freq="MS"
)
# print(all_months)

# Step 2: Create all month × customer combinations
customers = df["customer_id"].unique()

full_index = pd.MultiIndex.from_product(
    [all_months, customers],           # (month, customer)
    names=["month", "customer_id"]
)

# Step 3: Reindex to insert missing rows
df = (
    df.set_index(["month", "customer_id"])
      .reindex(full_index)
      .reset_index()
)
#print(df)

# Step 4: Fill missing revenue values with 0
revenue_columns = ["total_revenue", "loan_revenue", "card_revenue", "merchant_revenue"]
for col in revenue_columns:
    df[col] = df[col].fillna(0)

df = df.sort_values(["customer_id", "month"]).reset_index(drop=True)
```

•	Smooth out outliers
```
"""
Purpose:
- Smooth outliers using past 3 month rolling average
- Why: Month to month revenue can be noisy due to one-time spikes or drops.
- How: Apply a 3-month rolling average on total_revenue per customer to
  smooth short-term volatility.
"""
# 3 month rolling average on total revenue per customer
df["revenue_smoothed"] = (
    df.groupby("customer_id")["total_revenue"]
      .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
).round(2)
```

•	Flag customers with declining revenue
```
"""
Purpose:
- Flag customers with sustained declining revenue based on smoothed trends.
- Assumption: Declining  revenue is (based on smoothed revenue) 3 consecutive previous months of negative change
- Why: Identify customers with sustained revenue decline
- How: Compute month over month change on smoothed revenue and flag cases
  where the last 3 consecutive changes are negative.
"""

# Step 1: month over month change on smoothed revenue
df["mom_change"] = (df.groupby("customer_id")["revenue_smoothed"].diff())

# Step 2: Rolling window over last 3 changes
# If 3 (previous) consecutive values are less that 0 then decline_flag = 1 else 0
df["decline_flag"] = (
    df.groupby("customer_id")["mom_change"]
      .transform(lambda x: x.rolling(3).apply(lambda v: int((v < 0).all()), raw=True))
      .fillna(0)
      .astype(int)
)
```

#### Q6

Using Python, how would you create: (for Tableau to consume?)

•	A revenue trend table
```
"""
Purpose:
- Create a revenue trend table for Tableau consumption, including:
  revenue_smoothed and decline_flag.
"""
trend_df = df[
    [
        "month",
        "customer_id",
        "total_revenue",
        "loan_revenue",
        "card_revenue",
        "merchant_revenue",
        "revenue_smoothed",
        "decline_flag"
    ]
].copy()

trend_df = trend_df.sort_values(["customer_id", "month"])

# Month label for Tableau display / filtering
trend_df["month_label"] = trend_df["month"].dt.strftime("%Y-%m")

# print(trend_df)
```

•	A simple next-month forecast
```
"""
Purpose:
- Create a next month forecast per customer using:
  average of the last 3 months of total_revenue.
"""
# Step 1: Identify the latest month present in the dataset
latest_month = trend_df["month"].max()

# Step 2: Define the forecast month as the next month (month start)
forecast_month = latest_month + pd.offsets.MonthBegin(1)

# Step 3: Compute forecast per customer - avg. of last 3 months total_revenue
forecast_df = (
    trend_df.sort_values(["customer_id", "month"])
      .groupby("customer_id")
      .apply(lambda x: x["total_revenue"].tail(3).mean())
      .reset_index(name="forecast_total_revenue")
)

# Step 4: Add metadata columns for Tableau
forecast_df["forecast_month"] = forecast_month
forecast_df["forecast_method"] = "avg_last_3_months"

# Step 5: Reorder + sort output columns
forecast_df = forecast_df[
    ["forecast_month", "customer_id", "forecast_total_revenue", "forecast_method"]
].sort_values("customer_id")

print(forecast_df)
```
