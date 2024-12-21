def filtering(loan):
    import pandas as pd 
    filtered_loan = loan[loan.LoanPurpose.str.lower() == "car"]
    filtered_loan = filtered_loan[["UID", "LoanPurpose"]]
    return filtered_loan
