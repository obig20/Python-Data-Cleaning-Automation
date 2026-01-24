import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime

class DataCleaner:
    
    def __init__(self):
        self.cleaning_log = []
        self.report = {}
        
    def log_action(self, action, details):
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.cleaning_log.append(f"[{timestamp}] {action}: {details}")
        print(f"✓ {action}")
    
    def load_data(self, filepath):
    
        try:
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
                df = pd.read_excel(filepath)
            else:
                raise ValueError("Unsupported file format. Use .csv or .xlsx")
            
            self.log_action("Data loaded", f"{len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            sys.exit(1)
    
    def clean_missing_values(self, df):
        """Smart missing value handling"""
        df_clean = df.copy()
        
        # Track missing values
        missing_before = df.isnull().sum().sum()
        
        # Auto-detect and fill based on column type
        for col in df.columns:
            if df[col].isnull().any():
                missing_count = df[col].isnull().sum()
                
                if pd.api.types.is_numeric_dtype(df[col]):
                    # Fill numeric with median
                    fill_value = df[col].median()
                    df_clean[col] = df[col].fillna(fill_value)
                    self.log_action(f"Filled missing {col}", 
                                f"{missing_count} values with median: {fill_value}")
                elif pd.api.types.is_string_dtype(df[col]):
                    # Fill text with mode or 'Unknown'
                    if df[col].mode().empty:
                        fill_value = 'Unknown'
                    else:
                        fill_value = df[col].mode()[0]
                    df_clean[col] = df[col].fillna(fill_value)
                    self.log_action(f"Filled missing {col}", 
                                f"{missing_count} values with '{fill_value}'")
                elif 'date' in col.lower() or 'time' in col.lower():
                    # For dates, keep as missing or fill with placeholder
                    df_clean[col] = df[col]
                    self.log_action(f"Kept missing dates in {col}", 
                                f"{missing_count} date values")
        
        missing_after = df_clean.isnull().sum().sum()
        self.report['missing_values_fixed'] = missing_before - missing_after
        
        return df_clean
    
    def fix_data_types(self, df):
        
        df_fixed = df.copy()
        
        for col in df.columns:
            original_type = str(df[col].dtype)
            
            # Detect date columns
            if 'date' in col.lower() or 'join' in col.lower() or 'time' in col.lower():
                try:
                    df_fixed[col] = pd.to_datetime(df[col], errors='coerce')
                    new_type = 'datetime64[ns]'
                    if original_type != new_type:
                        self.log_action(f"Converted {col}", f"{original_type} → {new_type}")
                except:
                    pass
            
            # Detect numeric columns stored as text
            elif pd.api.types.is_string_dtype(df[col]):
                # Check if column contains numbers
                sample = df[col].dropna().head(100)
                if sample.str.contains(r'^-?\d+\.?\d*$', na=False).any():
                    try:
                        df_fixed[col] = pd.to_numeric(df[col], errors='coerce')
                        new_type = str(df_fixed[col].dtype)
                        if original_type != new_type:
                            self.log_action(f"Converted {col}", f"{original_type} → {new_type}")
                    except:
                        pass
        
        return df_fixed
    
    def remove_duplicates(self, df):
        """Find and handle duplicate rows"""
        duplicates = df.duplicated().sum()
        
        if duplicates > 0:
            df_clean = df.drop_duplicates()
            self.log_action("Removed duplicates", f"{duplicates} duplicate rows removed")
            self.report['duplicates_removed'] = duplicates
        else:
            df_clean = df
            self.log_action("Checked duplicates", "No duplicates found")
        
        return df_clean
    
    def standardize_text(self, df):
        """Standardize text formatting"""
        df_clean = df.copy()
        
        text_columns = df.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            # Trim whitespace
            df_clean[col] = df[col].astype(str).str.strip()
            
            # Standardize case for common fields
            if 'name' in col.lower() or 'department' in col.lower():
                df_clean[col] = df_clean[col].str.title()
            
            # Remove extra spaces
            df_clean[col] = df_clean[col].str.replace(r'\s+', ' ', regex=True)
        
        self.log_action("Standardized text", f"{len(text_columns)} text columns cleaned")
        
        return df_clean
    
    def validate_data(self, df):
        """Run data validation checks"""
        validation_issues = []
        
        # Check for negative ages
        if 'age' in [col.lower() for col in df.columns]:
            age_col = [col for col in df.columns if col.lower() == 'age'][0]
            negative_ages = (df[age_col] < 0).sum()
            if negative_ages > 0:
                validation_issues.append(f"{negative_ages} negative age values")
        
        # Check for unrealistic salaries
        if 'salary' in [col.lower() for col in df.columns]:
            salary_col = [col for col in df.columns if col.lower() == 'salary'][0]
            if salary_col in df.columns:
                zero_salaries = (df[salary_col] == 0).sum()
                if zero_salaries > 0:
                    validation_issues.append(f"{zero_salaries} zero salary values")
        
        # Check for future dates
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                future_dates = (df[col] > pd.Timestamp.now()).sum()
                if future_dates > 0:
                    validation_issues.append(f"{future_dates} future dates in {col}")
        
        if validation_issues:
            self.log_action("Validation issues found", "; ".join(validation_issues))
        else:
            self.log_action("Data validation", "All checks passed")
        
        self.report['validation_issues'] = validation_issues
        
        return df
    
    def generate_summary(self, df):
        """Generate data summary"""
        summary = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': list(df.columns),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'memory_usage': df.memory_usage(deep=True).sum() / 1024 / 1024,  # MB
        }
        
        # Add column summaries
        column_summaries = {}
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                column_summaries[col] = {
                    'type': 'numeric',
                    'missing': df[col].isnull().sum(),
                    'unique': df[col].nunique(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                }
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                column_summaries[col] = {
                    'type': 'datetime',
                    'missing': df[col].isnull().sum(),
                    'unique': df[col].nunique(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                }
            else:
                column_summaries[col] = {
                    'type': 'text',
                    'missing': df[col].isnull().sum(),
                    'unique': df[col].nunique(),
                    'top_values': df[col].value_counts().head(3).to_dict()
                }
        
        summary['column_details'] = column_summaries
        self.report['summary'] = summary
        
        return summary
    
    def save_results(self, df, input_filename):
        """Save cleaned data and reports"""
        # Generate output filename
        base_name = os.path.splitext(input_filename)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save cleaned data
        cleaned_file = f"{base_name}_CLEANED_{timestamp}.csv"
        df.to_csv(cleaned_file, index=False)
        
        # Save cleaning log
        log_file = f"{base_name}_CLEANING_LOG_{timestamp}.txt"
        with open(log_file, 'w') as f:
            f.write("DATA CLEANING REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Input file: {input_filename}\n")
            f.write(f"Cleaning date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Rows processed: {len(df)}\n")
            f.write(f"Columns processed: {len(df.columns)}\n\n")
            
            f.write("CLEANING ACTIONS:\n")
            f.write("-" * 50 + "\n")
            for log_entry in self.cleaning_log:
                f.write(log_entry + "\n")
            
            f.write("\nDATA SUMMARY:\n")
            f.write("-" * 50 + "\n")
            summary = self.generate_summary(df)
            f.write(f"Total rows: {summary['total_rows']}\n")
            f.write(f"Total columns: {summary['total_columns']}\n")
            f.write(f"Memory usage: {summary['memory_usage']:.2f} MB\n")
        
        # Save summary as CSV
        summary_file = f"{base_name}_SUMMARY_{timestamp}.csv"
        summary_df = pd.DataFrame([{
            'Metric': 'Total Rows',
            'Value': summary['total_rows']
        }, {
            'Metric': 'Total Columns',
            'Value': summary['total_columns']
        }, {
            'Metric': 'Missing Values Fixed',
            'Value': self.report.get('missing_values_fixed', 0)
        }, {
            'Metric': 'Duplicates Removed',
            'Value': self.report.get('duplicates_removed', 0)
        }])
        summary_df.to_csv(summary_file, index=False)
        
    
        print(f"   Cleaned data: {cleaned_file}")
        print(f"   Cleaning log: {log_file}")
        print(f"   Data summary: {summary_file}")
        
        return cleaned_file, log_file, summary_file
    
    def clean_file(self, input_file):
        """Main cleaning pipeline"""
        print("=" * 60)
        print("PROFESSIONAL DATA CLEANING SERVICE")
        print("=" * 60)
        print(f"Processing: {input_file}")
        print("-" * 60)
        
        # Load data
        df = self.load_data(input_file)
        

        
        df = self.fix_data_types(df)
        df = self.clean_missing_values(df)
        df = self.standardize_text(df)
        df = self.remove_duplicates(df)
        df = self.validate_data(df)

        cleaned_file, log_file, summary_file = self.save_results(df, input_file)
  
        print(f" Summary:")
        print(f"   • Input: {input_file}")
        print(f"   • Output: {cleaned_file}")
        print(f"   • Rows processed: {len(df)}")
        print(f"   • Issues fixed: {self.report.get('missing_values_fixed', 0) + self.report.get('duplicates_removed', 0)}")
        
        return cleaned_file

def main():
    """Main function - run with python clean_my_data.py yourfile.csv"""
    if len(sys.argv) < 2:

        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)
    
    # Run cleaner
    cleaner = DataCleaner()
    cleaner.clean_file(input_file)

if __name__ == "__main__":
    main()