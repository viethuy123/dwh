import great_expectations as gx
from great_expectations.core import ExpectationSuite
import great_expectations.expectations as gxe
import pandas as pd
import numpy as np
from datetime import datetime
import os
import time

def validate_dataframe(
    df: pd.DataFrame, 
    suite_name: str = "default_suite",
    config_path: str = "/opt/airflow/great_expectations/great_expectations.yml"
  ) -> dict:

    retries = 3
    for attempt in range(retries):
        if os.path.exists(config_path) and os.path.getsize(config_path) > 0:
            try:
                context = gx.get_context(context_root_dir="/opt/airflow/great_expectations") # type: ignore
                break
            except Exception as e:
                print(f"Attempt {attempt+1}: Failed to load GE context: {e}")
        else:
            print(f"Attempt {attempt+1}: Config file not ready or empty.")
        time.sleep(3)  # wait a bit before retrying
    else:
        raise RuntimeError("GE config file is not ready after retries.")
    
    # Auto-detect ID and numeric columns
    id_columns = [col for col in df.columns if 'id' in col.lower()]
    numeric_cols = df.select_dtypes(include=np.number).columns
    
    # Create suite with your exact style
    expectation_suite = context.suites.add_or_update(
        ExpectationSuite(
            name=suite_name,
            expectations=[
                # 1. Non-null (all columns)
                *[gxe.ExpectColumnValuesToNotBeNull(column=col) 
                  for col in df.columns],
                
                # 2. Unique (ID columns)
                *[gxe.ExpectColumnValuesToBeUnique(column=col) 
                  for col in id_columns],
                
                # 3. Value ranges (numeric columns)
                *[gxe.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=float(df[col].quantile(0.05)),
                    max_value=float(df[col].quantile(0.95)),
                    mostly=0.95
                  ) for col in numeric_cols],
                
                # 4. Data types (all columns)
                *[gxe.ExpectColumnValuesToBeOfType(
                    column=col,
                    type_=str(df[col].dtype)
                  ) for col in df.columns],
                
                # 5. Outliers (numeric columns)
                *[gxe.ExpectColumnValuesToNotBeInSet(
                    column=col,
                    value_set=df[col][
                        (np.abs(df[col] - df[col].mean()) / df[col].std()) > 3
                    ].tolist(),
                    mostly=0.99
                  ) for col in numeric_cols]
            ]
        )
    )
    
    # Run validation
    validator = context.data_sources.add_pandas(name=f"pd_{datetime.now()}").read_dataframe(df)
    results = validator.validate(expect=expectation_suite)
    
    return results.to_json_dict()