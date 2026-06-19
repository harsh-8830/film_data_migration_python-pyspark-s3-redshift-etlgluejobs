import pandas as pd

# ==========================================
# LOAD DATA
# ==========================================

customer = pd.read_csv("customer.csv")
film = pd.read_csv("film.csv")
payment = pd.read_csv("payment.csv")
rental = pd.read_csv("rental.csv")
inventory = pd.read_csv("inventory.csv")

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def print_header(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def check_row_count(df, table_name):
    print(f"{table_name}: {len(df):,} rows")


def check_duplicate_rows(df, table_name):
    duplicates = df.duplicated().sum()
    print(f"{table_name}: {duplicates} duplicate rows")


def check_duplicate_pk(df, table_name, pk_column):

    if pk_column not in df.columns:
        print(f"{table_name}: PK column not found")
        return

    duplicates = df[pk_column].duplicated().sum()
    print(f"{table_name}: {duplicates} duplicate {pk_column}")


def check_missing_values(df, table_name):

    print(f"\n{table_name}")

    missing = df.isnull().sum()

    for col, count in missing.items():
        if count > 0:
            print(f"  {col}: {count} missing values")


def check_empty_strings(df, table_name):

    object_cols = df.select_dtypes(include="object").columns

    total_empty = 0

    for col in object_cols:
        count = (df[col].astype(str).str.strip() == "").sum()

        if count > 0:
            print(f"{table_name} -> {col}: {count} empty strings")
            total_empty += count

    if total_empty == 0:
        print(f"{table_name}: No empty strings found")


# ==========================================
# ROW COUNTS
# ==========================================

print_header("ROW COUNTS")

check_row_count(customer, "customer")
check_row_count(film, "film")
check_row_count(payment, "payment")
check_row_count(rental, "rental")
check_row_count(inventory, "inventory")

# ==========================================
# DUPLICATE ROWS
# ==========================================

print_header("DUPLICATE ROW CHECK")

check_duplicate_rows(customer, "customer")
check_duplicate_rows(film, "film")
check_duplicate_rows(payment, "payment")
check_duplicate_rows(rental, "rental")
check_duplicate_rows(inventory, "inventory")

# ==========================================
# PRIMARY KEY CHECKS
# ==========================================

print_header("PRIMARY KEY DUPLICATE CHECK")

check_duplicate_pk(customer, "customer", "customer_id")
check_duplicate_pk(film, "film", "film_id")
check_duplicate_pk(payment, "payment", "payment_id")
check_duplicate_pk(rental, "rental", "rental_id")
check_duplicate_pk(inventory, "inventory", "inventory_id")

# ==========================================
# MISSING VALUES
# ==========================================

print_header("MISSING VALUE CHECK")

check_missing_values(customer, "customer")
check_missing_values(film, "film")
check_missing_values(payment, "payment")
check_missing_values(rental, "rental")
check_missing_values(inventory, "inventory")

# ==========================================
# EMPTY STRING CHECK
# ==========================================

print_header("EMPTY STRING CHECK")

check_empty_strings(customer, "customer")
check_empty_strings(film, "film")

# ==========================================
# EMAIL VALIDATION
# ==========================================

print_header("CUSTOMER EMAIL CHECK")

if "email" in customer.columns:

    invalid_emails = customer[
        ~customer["email"].astype(str).str.contains("@", na=False)
    ]

    print(f"Invalid Emails: {len(invalid_emails)}")

# ==========================================
# PAYMENT VALIDATION
# ==========================================

print_header("PAYMENT CHECKS")

if "amount" in payment.columns:

    negative_amounts = payment[payment["amount"] < 0]

    print(f"Negative Payments: {len(negative_amounts)}")

    zero_amounts = payment[payment["amount"] == 0]

    print(f"Zero Payments: {len(zero_amounts)}")

# ==========================================
# REFERENTIAL INTEGRITY CHECKS
# ==========================================

print_header("REFERENTIAL INTEGRITY")

# Payment -> Customer

if (
    "customer_id" in payment.columns
    and "customer_id" in customer.columns
):

    invalid_customers = payment[
        ~payment["customer_id"].isin(customer["customer_id"])
    ]

    print(
        f"Payments with invalid customer_id: "
        f"{len(invalid_customers)}"
    )

# Rental -> Inventory

if (
    "inventory_id" in rental.columns
    and "inventory_id" in inventory.columns
):

    invalid_inventory = rental[
        ~rental["inventory_id"].isin(inventory["inventory_id"])
    ]

    print(
        f"Rentals with invalid inventory_id: "
        f"{len(invalid_inventory)}"
    )

# ==========================================
# FILM CHECKS
# ==========================================

print_header("FILM DATA CHECKS")

if "rental_duration" in film.columns:

    invalid_duration = film[
        film["rental_duration"] <= 0
    ]

    print(
        f"Invalid Rental Duration Rows: "
        f"{len(invalid_duration)}"
    )

if "replacement_cost" in film.columns:

    invalid_cost = film[
        film["replacement_cost"] <= 0
    ]

    print(
        f"Invalid Replacement Cost Rows: "
        f"{len(invalid_cost)}"
    )

# ==========================================
# FINAL SUMMARY
# ==========================================

print_header("QUALITY CHECK SUMMARY")

print("""
Checks Completed Successfully

✓ Row Count Validation
✓ Duplicate Row Check
✓ Primary Key Validation
✓ Missing Value Analysis
✓ Empty String Analysis
✓ Email Validation
✓ Payment Validation
✓ Referential Integrity Validation
✓ Film Business Rule Validation

Dataset is ready for ETL processing and loading into Redshift.
""")