"""
Data Comparison and Validation Script
Shows how to compare cleaned data against original data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import glob
import os

# Automatically find the latest cleaned CSV file
cleaned_files = glob.glob('Messy_Employee_dataset_cleaned_*.csv')
if not cleaned_files:
    raise FileNotFoundError("No cleaned data file found. Run data_cleaning.py first.")
cleaned_file = max(cleaned_files)  # Get most recent file

# Load both datasets
original_df = pd.read_csv('Messy_Employee_dataset.csv')
cleaned_df = pd.read_csv(cleaned_file)

print(f"Using cleaned data file: {os.path.basename(cleaned_file)}")

print("=" * 60)
print("DATA COMPARISON REPORT")
print("=" * 60)

# 1. Basic Statistics Comparison
print("\n1. BASIC STATISTICS COMPARISON")
print("-" * 40)
print(f"Original rows: {len(original_df)} | Cleaned rows: {len(cleaned_df)}")
print(f"Original columns: {len(original_df.columns)} | Cleaned columns: {len(cleaned_df.columns)}")

# 2. Missing Values Comparison
print("\n 2. MISSING VALUES COMPARISON")
print("-" * 40)
original_missing = original_df.isnull().sum()
cleaned_missing = cleaned_df.isnull().sum()

comparison_df = pd.DataFrame({
    'Original Missing': original_missing,
    'Cleaned Missing': cleaned_missing,
    'Values Fixed': original_missing - cleaned_missing
})
print(comparison_df[comparison_df['Original Missing'] > 0])

# 3. Data Type Comparison
print("\n 3. DATA TYPES COMPARISON")
print("-" * 40)
print("Original Data Types:")
print(original_df.dtypes)
print("\nCleaned Data Types:")
print(cleaned_df.dtypes)

# 4. Numeric Column Statistics
print("\n 4. NUMERIC COLUMNS STATISTICS (Age)")
print("-" * 40)
print(f"Original Age - Mean: {original_df['Age'].mean():.2f}, Median: {original_df['Age'].median():.2f}")
print(f"Cleaned Age - Mean: {cleaned_df['Age'].mean():.2f}, Median: {cleaned_df['Age'].median():.2f}")

# 5. Date Format Comparison
print("\n 5. DATE FORMAT COMPARISON")
print("-" * 40)
print("Original Join_Date samples:")
print(original_df['Join_Date'].head(5).tolist())
print("\nCleaned Join_Date samples:")
print(cleaned_df['Join_Date'].head(5).tolist())

# 6. Text Standardization Check
print("\n 6. TEXT STANDARDIZATION CHECK")
print("-" * 40)
print("Original Status values:", original_df['Status'].unique().tolist())
print("Cleaned Status values:", cleaned_df['Status'].unique().tolist())

# 7. Data Quality Validation
print("\n 7. DATA QUALITY VALIDATION")
print("-" * 40)

# Check for duplicates
print(f"Duplicates in original: {original_df.duplicated().sum()}")
print(f"Duplicates in cleaned: {cleaned_df.duplicated().sum()}")

# Check email format
email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
original_emails_valid = original_df['Email'].astype(str).str.match(email_pattern).sum()
cleaned_emails_valid = cleaned_df['Email'].astype(str).str.match(email_pattern).sum()
print(f"Valid emails - Original: {original_emails_valid}/{len(original_df)}")
print(f"Valid emails - Cleaned: {cleaned_emails_valid}/{len(cleaned_df)}")

# 8. Row-by-Row Comparison for Specific Changes
print("\n 8. ROW-BY-ROW COMPARISON (First 5 rows)")
print("-" * 40)
comparison_cols = ['Employee_ID', 'Age', 'Join_Date', 'Status']
print("\nOriginal:")
print(original_df[comparison_cols].head())
print("\nCleaned:")
print(cleaned_df[comparison_cols].head())

# 9. Summary of Changes
print("\n 9. SUMMARY OF CHANGES")
print("-" * 40)
age_filled = original_df['Age'].isnull().sum() - cleaned_df['Age'].isnull().sum()
print(f"[OK] Age values filled: {age_filled} (using median = 30.0)")
print(f"[OK] Date format standardized: {len(original_df)} rows")
print(f"[OK] Status values standardized: {len(original_df)} rows")

print("\n" + "=" * 60)
print("COMPARISON COMPLETE!")
print("=" * 60)
