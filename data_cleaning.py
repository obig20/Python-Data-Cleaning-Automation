import pandas as pd
import sys
import os
from datetime import datetime

class DataCleaner:
    
    def __init__(self):
        self.cleaning_log = []
        self.report = {}
        
    def log_action(self, action, details=""):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] {action}"
        if details:
            entry += f": {details}"
        self.cleaning_log.append(entry)
        print(f"  {action}")
    
    def load_data(self, filepath):
        try:
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                print(f"Error: Only CSV files are supported")
                sys.exit(1)
            
            self.log_action(f"Loaded data - {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            print(f"Error loading file: {e}")
            sys.exit(1)
    
    def clean_missing_values(self, df):
        df_clean = df.copy()
        missing_before = df.isnull().sum().sum()
        
        for col in df.columns:
            if df[col].isnull().any():
                missing_count = df[col].isnull().sum()
                
                if pd.api.types.is_numeric_dtype(df[col]):
                    fill_value = df[col].median()
                    df_clean[col] = df[col].fillna(fill_value)
                    self.log_action(f"Filled {missing_count} missing values in {col}")
                elif pd.api.types.is_string_dtype(df[col]):
                    fill_value = df[col].mode()[0] if not df[col].mode().empty else 'Unknown'
                    df_clean[col] = df[col].fillna(fill_value)
                    self.log_action(f"Filled {missing_count} missing values in {col}")
        
        missing_after = df_clean.isnull().sum().sum()
        self.report['missing_values_fixed'] = missing_before - missing_after
        
        return df_clean
    
    def fix_data_types(self, df):
        df_fixed = df.copy()
        
        for col in df.columns:
            # Fix date columns
            if 'date' in col.lower():
                try:
                    df_fixed[col] = pd.to_datetime(df[col], errors='coerce')
                    self.log_action(f"Converted {col} to date format")
                except:
                    pass
            
            # Fix numeric columns stored as text
            elif pd.api.types.is_string_dtype(df[col]):
                if df[col].str.contains(r'^-?\d+\.?\d*$', na=False).any():
                    try:
                        df_fixed[col] = pd.to_numeric(df[col], errors='coerce')
                        self.log_action(f"Converted {col} to numeric format")
                    except:
                        pass
        
        return df_fixed
    
    def remove_duplicates(self, df):
        duplicates = df.duplicated().sum()
        
        if duplicates > 0:
            df_clean = df.drop_duplicates()
            self.log_action(f"Removed {duplicates} duplicate rows")
            self.report['duplicates_removed'] = duplicates
        else:
            df_clean = df
        
        return df_clean
    
    def standardize_text(self, df):
        df_clean = df.copy()
        text_columns = df.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            df_clean[col] = df[col].astype(str).str.strip()
            if 'name' in col.lower():
                df_clean[col] = df_clean[col].str.title()
        
        if len(text_columns) > 0:
            self.log_action(f"Standardized {len(text_columns)} text columns")
        
        return df_clean
    
    def validate_data(self, df):
        issues = []
        
        # Check for common data issues
        for col in df.columns:
            col_lower = col.lower()
            if 'age' in col_lower:
                negative_ages = (df[col] < 0).sum()
                if negative_ages > 0:
                    issues.append(f"{negative_ages} negative values in {col}")
            
            if 'salary' in col_lower:
                zero_salaries = (df[col] <= 0).sum()
                if zero_salaries > 0:
                    issues.append(f"{zero_salaries} zero/negative values in {col}")
        
        if issues:
            self.log_action(f"Found {len(issues)} data issues")
        else:
            self.log_action("Data validation passed")
        
        self.report['validation_issues'] = issues
        
        return df
    
    def save_results(self, df, input_filename):
        base_name = os.path.splitext(input_filename)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save cleaned data
        cleaned_file = f"{base_name}_cleaned_{timestamp}.csv"
        df.to_csv(cleaned_file, index=False, encoding='utf-8')
        
        # Save cleaning log (use UTF-8 with replace for any unencodable characters)
        log_file = f"{base_name}_log_{timestamp}.txt"
        with open(log_file, 'w', encoding='utf-8', errors='replace') as f:
            f.write("DATA CLEANING REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Original file: {input_filename}\n")
            f.write(f"Cleaned on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("Cleaning Steps:\n")
            f.write("-" * 30 + "\n")
            for log_entry in self.cleaning_log:
                f.write(log_entry + "\n")

            f.write(f"\nFinal Dataset:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Rows: {len(df)}\n")
            f.write(f"Columns: {len(df.columns)}\n")
            f.write(f"Column names: {', '.join(df.columns)}\n")
        
        print(f"\nResults saved:")
        print(f"  Cleaned data: {cleaned_file}")
        print(f"  Log file: {log_file}")
        
        return cleaned_file
    
    def clean_file(self, input_file):
        print(f"\nStarting data cleaning for: {os.path.basename(input_file)}")
        print("-" * 50)
        
        df = self.load_data(input_file)
        df = self.fix_data_types(df)
        df = self.clean_missing_values(df)
        df = self.standardize_text(df)
        df = self.remove_duplicates(df)
        df = self.validate_data(df)
        
        cleaned_file = self.save_results(df, input_file)
        
        print(f"\nSummary:")
        print(f"  Rows processed: {len(df)}")
        print(f"  Columns processed: {len(df.columns)}")
        print(f"  Missing values fixed: {self.report.get('missing_values_fixed', 0)}")
        print(f"  Duplicates removed: {self.report.get('duplicates_removed', 0)}")
        
        if self.report.get('validation_issues'):
            print(f"  Validation issues: {len(self.report['validation_issues'])} found")
        
        print(f"\nCleaning completed successfully.")
        
        return cleaned_file


def main():
    if len(sys.argv) < 2:
        print("Usage: python data_cleaning.py <file_path>")
        print("Example: python data_cleaning.py Messy_Employee_dataset.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    
    cleaner = DataCleaner()
    cleaner.clean_file(input_file)


if __name__ == "__main__":
    main()