import pandas as pd 
import numpy as np
import os 
from datetime import datetime
from modules.libs import filtering

if __name__ == "__main__":
    DATA_NOW = datetime.now().strftime("%d-%m-%Y")
    PATH_LOAN = "raw_files/loan_applications.csv"
    PATH_FEATURES = "raw_files/credit_features_subset.csv"

    #---------IMPORTING FILES---------#
    loan = pd.read_csv(PATH_LOAN)
    features = pd.read_csv(PATH_FEATURES)

    #-----FILTERING------------------#
    filtered_loan = filtering(loan)

    final_file = filtered_loan.merge(features, 
                                        how ='left', 
                                            on ="UID" )    

    #----EXPORTING FILE------#
    final_file.to_excel(f"treated_file/final_file_{DATA_NOW}.xlsx")
    print("Treated sucess")