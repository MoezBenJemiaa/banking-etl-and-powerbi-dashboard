#!/usr/bin/env python3
"""
csv_to_relational_xlsx.py

Reads a wide CSV (default: Comprehensive_Banking_Database.csv), normalizes it into relational tables
(Customers, Accounts, Transactions, Loans, Cards, Feedback, Branches, Anomalies) and writes them as
separate sheets in an .xlsx file (default: Comprehensive_Banking_Database.xlsx).

Usage:
    # Use default filenames in current directory:
    python csv_to_relational_xlsx.py

    # Or provide input and output explicitly:
    python csv_to_relational_xlsx.py Comprehensive_Banking_Database.csv output.xlsx

Dependencies:
    pip install pandas openpyxl

What it does:
- Matches columns case-insensitively (trims whitespace).
- Coerces common date and numeric columns.
- Generates a deterministic AccountID (CustomerID + AccountType + DateOfAccountOpening) when needed.
- Drops duplicates for master tables and creates one sheet per relational table.
- Produces a DataDictionary sheet describing primary keys / relationships.

If your input file is exactly 'Comprehensive_Banking_Database.csv' in the current directory,
you can simply run the script without arguments and it will produce 'Comprehensive_Banking_Database.xlsx'.
"""
import argparse
import hashlib
import sys
from pathlib import Path
from typing import List, Dict

import pandas as pd

# Expected column names (used only for matching; matching is case-insensitive)
EXPECTED_COLS = [
    "Customer ID", "First Name", "Last Name", "Age", "Gender", "Address", "City", "Contact Number", "Email",
    "Account Type", "Account Balance", "Date Of Account Opening", "Last Transaction Date",
    "TransactionID", "Transaction Date", "Transaction Type", "Transaction Amount", "Account Balance After Transaction",
    "Branch ID",
    "Loan ID", "Loan Amount", "Loan Type", "Interest Rate", "Loan Term", "Approval/Rejection Date", "Loan Status",
    "CardID", "Card Type", "Credit Limit", "Credit Card Balance", "Minimum Payment Due", "Payment Due Date",
    "Last Credit Card Payment Date", "Rewards Points",
    "Feedback ID", "Feedback Date", "Feedback Type", "Resolution Status", "Resolution Date",
    "Anomaly"
]


def find_col(df_cols: List[str], target: str):
    target_key = target.strip().lower()
    for c in df_cols:
        if c is None:
            continue
        if c.strip().lower() == target_key:
            return c
    return None


def make_account_id(customer_id: str, account_type: str, open_date: str) -> str:
    base = f"{customer_id or ''}|{account_type or ''}|{open_date or ''}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"ACC_{h}"


def build_relational_sheets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    cols = list(df.columns)

    def c(name):
        return find_col(cols, name)

    # Coerce common date columns
    date_cols = [
        "Date Of Account Opening", "Last Transaction Date", "Transaction Date",
        "Approval/Rejection Date", "Payment Due Date", "Last Credit Card Payment Date", "Feedback Date"
    ]
    for dc in date_cols:
        actual = c(dc)
        if actual:
            df[actual] = pd.to_datetime(df[actual], errors="coerce", infer_datetime_format=True)

    # Coerce numeric columns
    numeric_cols = [
        "Account Balance", "Transaction Amount", "Account Balance After Transaction",
        "Loan Amount", "Interest Rate", "Credit Limit", "Credit Card Balance", "Minimum Payment Due", "Rewards Points"
    ]
    for nc in numeric_cols:
        actual = c(nc)
        if actual:
            s = df[actual].astype(str).str.replace(r"[^\d\.\-eE]", "", regex=True)
            df[actual] = pd.to_numeric(s, errors="coerce")

    cust_col = c("Customer ID")
    acct_type_col = c("Account Type")
    open_date_col = c("Date Of Account Opening")

    if cust_col is None:
        raise ValueError("Input CSV does not contain a 'Customer ID' column (case-insensitive match).")

    df["__account_open_date_str"] = df[open_date_col].astype(str) if open_date_col else pd.Series([""] * len(df))
    df["__customer_id_str"] = df[cust_col].astype(str)

    df["AccountID"] = df.apply(
        lambda r: make_account_id(r["__customer_id_str"], (r[acct_type_col] if acct_type_col else ""), r["__account_open_date_str"]),
        axis=1
    )

    # Customers table
    customers_cols = [cust_col, c("First Name"), c("Last Name"), c("Age"), c("Gender"), c("Address"), c("City"),
                      c("Contact Number"), c("Email")]
    customers_cols = [col for col in customers_cols if col is not None]
    customers = df[customers_cols].drop_duplicates(subset=[cust_col]).reset_index(drop=True)
    customers = customers.rename(columns={cust_col: "CustomerID"})

    # Accounts table
    accounts_cols = ["AccountID", cust_col, acct_type_col, c("Account Balance"), open_date_col, c("Last Transaction Date"), c("Branch ID")]
    accounts_cols = [col for col in accounts_cols if col is not None]
    accounts = df[accounts_cols].drop_duplicates(subset=["AccountID"]).reset_index(drop=True)
    rename_map = {}
    if cust_col:
        rename_map[cust_col] = "CustomerID"
    if acct_type_col:
        rename_map[acct_type_col] = "AccountType"
    if open_date_col:
        rename_map[open_date_col] = "DateOfAccountOpening"
    if c("Last Transaction Date"):
        rename_map[c("Last Transaction Date")] = "LastTransactionDate"
    if c("Account Balance"):
        rename_map[c("Account Balance")] = "AccountBalance"
    if c("Branch ID"):
        rename_map[c("Branch ID")] = "BranchID"
    accounts = accounts.rename(columns=rename_map)

    # Transactions table
    txid_col = c("TransactionID")
    transactions_cols = []
    if txid_col:
        transactions_cols.append(txid_col)
    transactions_cols += ["AccountID", c("Transaction Date"), c("Transaction Type"), c("Transaction Amount"), c("Account Balance After Transaction"), c("Branch ID")]
    transactions_cols = [col for col in transactions_cols if col is not None]
    transactions = df[transactions_cols].reset_index(drop=True)
    tx_rename = {}
    if txid_col:
        tx_rename[txid_col] = "TransactionID"
    if c("Transaction Date"):
        tx_rename[c("Transaction Date")] = "TransactionDate"
    if c("Transaction Type"):
        tx_rename[c("Transaction Type")] = "TransactionType"
    if c("Transaction Amount"):
        tx_rename[c("Transaction Amount")] = "TransactionAmount"
    if c("Account Balance After Transaction"):
        tx_rename[c("Account Balance After Transaction")] = "AccountBalanceAfterTransaction"
    if c("Branch ID"):
        tx_rename[c("Branch ID")] = "BranchID"
    transactions = transactions.rename(columns=tx_rename)
    if "TransactionID" in transactions.columns:
        transactions = transactions.drop_duplicates(subset=["TransactionID"])

    # Loans table
    loanid_col = c("Loan ID")
    loans_cols = []
    if loanid_col:
        loans_cols.append(loanid_col)
    loans_cols += [cust_col, c("Loan Amount"), c("Loan Type"), c("Interest Rate"), c("Loan Term"), c("Approval/Rejection Date"), c("Loan Status"), c("Branch ID")]
    loans_cols = [col for col in loans_cols if col is not None]
    loans = df[loans_cols].drop_duplicates(subset=[loanid_col] if loanid_col else None).reset_index(drop=True)
    loan_rename = {}
    if loanid_col:
        loan_rename[loanid_col] = "LoanID"
    if cust_col:
        loan_rename[cust_col] = "CustomerID"
    if c("Loan Amount"):
        loan_rename[c("Loan Amount")] = "LoanAmount"
    if c("Loan Type"):
        loan_rename[c("Loan Type")] = "LoanType"
    if c("Interest Rate"):
        loan_rename[c("Interest Rate")] = "InterestRate"
    if c("Loan Term"):
        loan_rename[c("Loan Term")] = "LoanTerm"
    if c("Approval/Rejection Date"):
        loan_rename[c("Approval/Rejection Date")] = "ApprovalRejectionDate"
    if c("Loan Status"):
        loan_rename[c("Loan Status")] = "LoanStatus"
    if c("Branch ID"):
        loan_rename[c("Branch ID")] = "BranchID"
    loans = loans.rename(columns=loan_rename)

    # Cards table
    cardid_col = c("CardID")
    cards_cols = []
    if cardid_col:
        cards_cols.append(cardid_col)
    cards_cols += [cust_col, c("Card Type"), c("Credit Limit"), c("Credit Card Balance"), c("Minimum Payment Due"),
                   c("Payment Due Date"), c("Last Credit Card Payment Date"), c("Rewards Points")]
    cards_cols = [col for col in cards_cols if col is not None]
    cards = df[cards_cols].drop_duplicates(subset=[cardid_col] if cardid_col else None).reset_index(drop=True)
    card_rename = {}
    if cardid_col:
        card_rename[cardid_col] = "CardID"
    if cust_col:
        card_rename[cust_col] = "CustomerID"
    if c("Card Type"):
        card_rename[c("Card Type")] = "CardType"
    if c("Credit Limit"):
        card_rename[c("Credit Limit")] = "CreditLimit"
    if c("Credit Card Balance"):
        card_rename[c("Credit Card Balance")] = "CreditCardBalance"
    if c("Minimum Payment Due"):
        card_rename[c("Minimum Payment Due")] = "MinimumPaymentDue"
    if c("Payment Due Date"):
        card_rename[c("Payment Due Date")] = "PaymentDueDate"
    if c("Last Credit Card Payment Date"):
        card_rename[c("Last Credit Card Payment Date")] = "LastCreditCardPaymentDate"
    if c("Rewards Points"):
        card_rename[c("Rewards Points")] = "RewardsPoints"
    cards = cards.rename(columns=card_rename)

    # Feedback table
    feedbackid_col = c("Feedback ID")
    feedback_cols = []
    if feedbackid_col:
        feedback_cols.append(feedbackid_col)
    feedback_cols += [cust_col, c("Feedback Date"), c("Feedback Type"), c("Resolution Status"), c("Resolution Date")]
    feedback_cols = [col for col in feedback_cols if col is not None]
    feedback = df[feedback_cols].drop_duplicates(subset=[feedbackid_col] if feedbackid_col else None).reset_index(drop=True)
    fb_rename = {}
    if feedbackid_col:
        fb_rename[feedbackid_col] = "FeedbackID"
    if cust_col:
        fb_rename[cust_col] = "CustomerID"
    if c("Feedback Date"):
        fb_rename[c("Feedback Date")] = "FeedbackDate"
    if c("Feedback Type"):
        fb_rename[c("Feedback Type")] = "FeedbackType"
    if c("Resolution Status"):
        fb_rename[c("Resolution Status")] = "ResolutionStatus"
    if c("Resolution Date"):
        fb_rename[c("Resolution Date")] = "ResolutionDate"
    feedback = feedback.rename(columns=fb_rename)

    # Branches table
    branch_col = c("Branch ID")
    if branch_col:
        branches = df[[branch_col]].drop_duplicates().reset_index(drop=True)
        branches = branches.rename(columns={branch_col: "BranchID"})
    else:
        branches = pd.DataFrame(columns=["BranchID"])

    # Anomalies table
    anomaly_col = c("Anomaly")
    anomalies = pd.DataFrame()
    if anomaly_col is not None and anomaly_col in df.columns:
        anomalies = df[[cust_col, anomaly_col]].dropna(subset=[anomaly_col]).drop_duplicates().reset_index(drop=True)
        anomalies = anomalies.rename(columns={cust_col: "CustomerID", anomaly_col: "Anomaly"})
    else:
        anomalies = pd.DataFrame(columns=["CustomerID", "Anomaly"])

    # Data dictionary
    dd_rows = [
        {"Table": "Customers", "PrimaryKey": "CustomerID", "Notes": "Customer master data"},
        {"Table": "Accounts", "PrimaryKey": "AccountID", "ForeignKeys": "CustomerID", "Notes": "One or more accounts per customer"},
        {"Table": "Transactions", "PrimaryKey": "TransactionID (may be null)", "ForeignKeys": "AccountID, BranchID", "Notes": "Transaction records"},
        {"Table": "Loans", "PrimaryKey": "LoanID", "ForeignKeys": "CustomerID, BranchID", "Notes": "Loan records if present"},
        {"Table": "Cards", "PrimaryKey": "CardID", "ForeignKeys": "CustomerID", "Notes": "Credit card records"},
        {"Table": "Feedback", "PrimaryKey": "FeedbackID", "ForeignKeys": "CustomerID", "Notes": "Customer feedback"},
        {"Table": "Branches", "PrimaryKey": "BranchID", "Notes": "Branch identifiers"},
        {"Table": "Anomalies", "PrimaryKey": "", "ForeignKeys": "CustomerID", "Notes": "Rows flagged as anomalies (if any)"}
    ]
    data_dictionary = pd.DataFrame(dd_rows)

    df.drop(columns=["__account_open_date_str", "__customer_id_str"], inplace=True, errors="ignore")

    sheets = {
        "Customers": customers,
        "Accounts": accounts,
        "Transactions": transactions,
        "Loans": loans,
        "Cards": cards,
        "Feedback": feedback,
        "Branches": branches,
        "Anomalies": anomalies,
        "DataDictionary": data_dictionary
    }

    return sheets


def write_sheets_to_excel(sheets: Dict[str, pd.DataFrame], output_path: str):
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            sheet_name = name if len(name) <= 31 else name[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def main():
    parser = argparse.ArgumentParser(description="Normalize a wide CSV into relational sheets in an XLSX file.")
    parser.add_argument("input_csv", nargs="?", help="Path to input CSV file (default: Comprehensive_Banking_Database.csv)")
    parser.add_argument("output_xlsx", nargs="?", help="Path to output XLSX file (default: Comprehensive_Banking_Database.xlsx)")
    args = parser.parse_args()

    default_input = "Comprehensive_Banking_Database.csv"
    default_output = "Comprehensive_Banking_Database.xlsx"

    input_csv = args.input_csv or default_input
    output_xlsx = args.output_xlsx or default_output

    input_path = Path(input_csv)
    if not input_path.exists():
        print(f"Input file '{input_csv}' not found. Please place '{default_input}' in the current directory or provide a path.", file=sys.stderr)
        sys.exit(1)

    try:
        # Read CSV preserving raw strings for IDs; allow commonly used NA tokens
        df = pd.read_csv(str(input_path), dtype=str, keep_default_na=False, na_values=["", "NA", "N/A"])
    except Exception as e:
        print(f"Failed to read CSV '{input_csv}': {e}", file=sys.stderr)
        sys.exit(2)

    # Normalize column names (trim)
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]

    try:
        sheets = build_relational_sheets(df)
    except Exception as e:
        print(f"Failed while building relational sheets: {e}", file=sys.stderr)
        sys.exit(3)

    try:
        write_sheets_to_excel(sheets, output_xlsx)
    except Exception as e:
        print(f"Failed while writing Excel '{output_xlsx}': {e}", file=sys.stderr)
        sys.exit(4)

    print(f"Wrote Excel file: {output_xlsx}")
    for name, df_sheet in sheets.items():
        print(f" - {name}: {len(df_sheet)} rows")
    print("Done.")


if __name__ == "__main__":
    main()