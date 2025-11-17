# README – Springer Capital Data Engineer Take-Home Assessment

##  Overview

This project is a solution to the **Springer Capital – Data Engineer Intern Take-Home Test**.

The objective is to:

1. **Profile** referral-related datasets  
2. **Clean, process, and join** all CSV tables  
3. **Implement business logic** to validate referral rewards  
4. **Detect potential fraud** based on the provided rules  
5. **Generate a final output report (CSV)**  
6. **Containerize the solution using Docker**  
7. **Document the pipeline & provide a data dictionary**

This README explains:

- Project structure  
- How to set up the environment  
- How to run the pipeline (local & Docker)  
- Output files  
- Data dictionary location  

---

#  Project Structure

```
.
├── data/            # Input CSV files (lead_log, user_logs, user_referrals, referral rewards,user_referral_logs,user_referral_statuses,paid_transactions etc.)              
├── output/results
│   └── results after running docker commands       # Final output report (46 rows expected)
├── your_script.py                 # Main processing script
├── Dockerfile                     # Docker container definition
├── Result of Data Profiling       # Word file storing result of Data profiling
├── referral_report.csv            # The output file 
├── Springer_Capital_Data_Dictionary.xlsx           # Business-friendly data dictionary
└── README.md                      # This file
```

---

## Expected Outputs

| Output File | Description |
|------------|-------------|
| `output/referral_report.csv` | Final report containing validity status for each referral (46 rows expected) |
---

#  2. Running with Docker (Recommended)

## Step 1 — Build Docker Image

Run inside the project directory:

```bash
docker build -t referral-app .
```

## Step 2 — Run the Container

```bash
docker run --rm \
  -v "$PWD/output":/app/output \
  -v "$PWD/data":/app/data \
  referral-app
```

**Volumes ensure that:**

- Input CSV files are read **from your host**
- Output report is saved **to your host**, not inside the container

---

#  4. Work flow of pipeline

##  1. Data Loading

Loads all seven referral-related CSV files:

- `lead_log`
- `user_referrals`
- `user_referral_logs`
- `user_logs`
- `user_referral_statuses`
- `referral_rewards`
- `paid_transactions`

---

##  2. Data Profiling

For each input table:

- Null count per column  
- Distinct value count per column  


##  3. Data Cleaning

- Handle missing values  
- Convert timestamps from UTC → local timezone  
- Apply correct data types  
- String cleanup (InitCap except homeclub names)  

---

## ✔ 4. Data Processing

Includes:

- Joining all tables with proper keys  
- Removing duplicates  
- Converting referral source category:

```sql
CASE  
  WHEN referral_source = 'User Sign Up' THEN 'Online'
  WHEN referral_source = 'Draft Transaction' THEN 'Offline'
  WHEN referral_source = 'Lead' THEN lead_logs.source_category
END
```

- Ensuring no nulls remain in the final dataset  

---

##  5. Business Logic Validation (Fraud Rules)

A new boolean column is created:

```
is_business_logic_valid
```

### Valid Reward Conditions

1. reward_value > 0  
2. referral_status = “Berhasil”  
3. referral has transaction_id  
4. transaction_status = paid  
5. transaction_type = new  
6. transaction_at > referral_at  
7. transaction_at is in same month  
8. referrer membership not expired  
9. referrer not deleted  
10. reward granted  

### Valid (Alternative)

Referral status = "Menunggu" or "Tidak Berhasil" and reward_value is null  

### Invalid Reward Conditions (Examples)

- reward_value > 0 but referral_status ≠ "Berhasil"  
- reward_value > 0 but no transaction exists  
- transaction happened before referral  
- missing reward although referral is successful  
- referral contains a transaction but no reward assigned  

---

##  6. Output Generation

Writes:

```
output/results/referral_report.csv
```


---

# 7 Data Dictionary

A user-friendly **data_dictionary.xlsx** is included at the project root.

It contains:

- Column name  
- Data type  
- Meaning (business definition), For non-technical team  
- Sample value  
- Constraints  
---
