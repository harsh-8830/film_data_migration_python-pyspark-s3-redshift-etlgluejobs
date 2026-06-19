import pandas as pd
import matplotlib.pyplot as plt

# ==========================================
# LOAD DATA
# ==========================================

customer = pd.read_csv("customer.csv")
film = pd.read_csv("film.csv")
payment = pd.read_csv("payment.csv")
rental = pd.read_csv("rental.csv")

# ==========================================
# DATASET OVERVIEW
# ==========================================

print("=" * 60)
print("DATASET OVERVIEW")
print("=" * 60)

datasets = {
    "Customer": customer,
    "Film": film,
    "Payment": payment,
    "Rental": rental
}

for name, df in datasets.items():
    print(f"\n{name}")
    print("-" * 30)
    print(f"Shape: {df.shape}")

# ==========================================
# COLUMN INFORMATION
# ==========================================

print("\n" + "=" * 60)
print("COLUMN INFORMATION")
print("=" * 60)

for name, df in datasets.items():
    print(f"\n{name} Columns:")
    print(df.columns.tolist())

# ==========================================
# DATA TYPES
# ==========================================

print("\n" + "=" * 60)
print("DATA TYPES")
print("=" * 60)

for name, df in datasets.items():
    print(f"\n{name}")
    print(df.dtypes)

# ==========================================
# MISSING VALUES
# ==========================================

print("\n" + "=" * 60)
print("MISSING VALUES")
print("=" * 60)

for name, df in datasets.items():
    print(f"\n{name}")
    print(df.isnull().sum())

# ==========================================
# PAYMENT ANALYSIS
# ==========================================

print("\n" + "=" * 60)
print("PAYMENT ANALYSIS")
print("=" * 60)

print("\nPayment Statistics")
print(payment["amount"].describe())

print(f"\nTotal Revenue: ${payment['amount'].sum():,.2f}")
print(f"Average Payment: ${payment['amount'].mean():.2f}")

# Payment Distribution Chart
plt.figure(figsize=(8, 5))
payment["amount"].hist(bins=20)
plt.title("Payment Amount Distribution")
plt.xlabel("Amount")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

# ==========================================
# CUSTOMER ANALYSIS
# ==========================================

print("\n" + "=" * 60)
print("CUSTOMER ANALYSIS")
print("=" * 60)

if "active" in customer.columns:

    active_counts = customer["active"].value_counts()

    print("\nActive Customer Counts")
    print(active_counts)

    plt.figure(figsize=(6, 4))
    active_counts.plot(kind="bar")
    plt.title("Active vs Inactive Customers")
    plt.xlabel("Status")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()

# ==========================================
# TOP CUSTOMERS
# ==========================================

print("\n" + "=" * 60)
print("TOP CUSTOMERS BY REVENUE")
print("=" * 60)

top_customers = (
    payment.groupby("customer_id")["amount"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print(top_customers)

plt.figure(figsize=(10, 5))
top_customers.plot(kind="bar")
plt.title("Top 10 Customers by Revenue")
plt.xlabel("Customer ID")
plt.ylabel("Revenue")
plt.tight_layout()
plt.show()

# ==========================================
# FILM ANALYSIS
# ==========================================

print("\n" + "=" * 60)
print("FILM ANALYSIS")
print("=" * 60)

if "rating" in film.columns:

    rating_counts = film["rating"].value_counts()

    print("\nFilm Rating Distribution")
    print(rating_counts)

    plt.figure(figsize=(8, 5))
    rating_counts.plot(kind="bar")
    plt.title("Film Ratings")
    plt.xlabel("Rating")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()

# Rental Duration

if "rental_duration" in film.columns:

    print("\nRental Duration Statistics")
    print(film["rental_duration"].describe())

    plt.figure(figsize=(8, 5))
    film["rental_duration"].hist(bins=10)
    plt.title("Rental Duration Distribution")
    plt.xlabel("Days")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()

# Most Expensive Films

if "replacement_cost" in film.columns:

    print("\nTop 10 Most Expensive Films")

    expensive_films = (
        film[["title", "replacement_cost"]]
        .sort_values("replacement_cost", ascending=False)
        .head(10)
    )

    print(expensive_films)

# ==========================================
# RENTAL ANALYSIS
# ==========================================

print("\n" + "=" * 60)
print("RENTAL ANALYSIS")
print("=" * 60)

print(f"\nTotal Rentals: {len(rental):,}")

# ==========================================
# FINAL SUMMARY
# ==========================================

print("\n" + "=" * 60)
print("EDA SUMMARY")
print("=" * 60)

print(f"""
Customers : {len(customer):,}
Films     : {len(film):,}
Payments  : {len(payment):,}
Rentals   : {len(rental):,}

Total Revenue : ${payment['amount'].sum():,.2f}

Basic Insights:
- Checked dataset size and structure
- Verified missing values
- Analyzed payment distribution
- Identified top customers by revenue
- Analyzed film ratings
- Examined rental duration distribution
- Identified highest replacement cost films
""")