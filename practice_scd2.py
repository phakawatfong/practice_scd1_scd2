import pandas as pd

# trg
data = {
    'employee_no': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'employee_name': ['John Doe', 'Jane Smith', 'Michael Johnson', 'Emily Davis', 'David Brown', 'Susan Wilson', 'Robert Lee', 'Linda White', 'William Hall', 'Sarah Clark'],
    'job': ['Engineer', 'Manager', 'Analyst', 'Designer', 'Technician', 'Administrator', 'Developer', 'Manager', 'Engineer', 'Analyst'],
    'hiredate': ['2022-01-15', '2021-03-10', '2021-06-25', '2022-04-05', '2022-02-18', '2021-11-30', '2022-03-12', '2021-07-19', '2021-09-08', '2022-05-20'],
    'salary': [80000, 95000, 60000, 70000, 55000, 75000, 85000, 90000, 82000, 63000],
    'department_no': [1, 2, 1, 3, 2, 1, 3, 2, 1, 4]
}

df = pd.DataFrame(data)

# print(df)


emp_scd=df[['employee_no','employee_name','salary','department_no']]
#for first time load versioning is 1 to identify records as latest
emp_scd['flag']=1

# print(emp_scd)


# src
data_src = {
    'employee_no': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    'employee_name': ['John Doe', 'Jane Smith', 'Michael Johnson', 'Emily Davis', 'David Brown', 'Susan Wilson', 'Robert Lee', 'Linda White', 'William Hall', 'Sarah Clark', 'George Turner', 'Amy Miller', 'Brian Moore', 'Karen Anderson', 'Daniel Lewis'],
    'job': ['Engineer', 'Manager', 'Analyst', 'Designer', 'Technician', 'Administrator', 'Developer', 'Manager', 'Engineer', 'Analyst', 'Engineer', 'Manager', 'Analyst', 'Designer', 'Technician'],
    'hiredate': ['2022-01-15', '2021-03-10', '2021-06-25', '2022-04-05', '2022-02-18', '2021-11-30', '2022-03-12', '2021-07-19', '2021-09-08', '2022-05-20', '2023-01-05', '2022-06-15', '2021-08-29', '2022-02-10', '2022-11-22'],
    'salary': [80000, 95000, 60000, 70000, 60000, 75000, 86000, 95000, 82000, 65000, 70000, 95000, 62000, 61000, 67000],
    'department_no': [1, 2, 1, 3, 2, 1, 3, 2, 1, 4, 1, 2, 3, 3, 2]
}

df_src = pd.DataFrame(data_src)

# print(df_src)

#rename columns is source and target for identification
emp_src=df_src[['employee_no','employee_name','salary','department_no']]

emp_src=emp_src[['employee_no','employee_name','salary','department_no']]
emp_src.rename(columns={'employee_no':'employee_no_src','salary':'salary_src'},inplace=True)
emp_scd.rename(columns={'employee_no':'employee_no_tgt','salary':'salary_tgt'},inplace=True)

#left join both the source and target dataframes to identify new records
join_df=pd.merge(emp_src,emp_scd,left_on='employee_no_src',right_on='employee_no_tgt',how='left')



join_df['INS_FLAG']=join_df[['employee_no_src','employee_no_tgt']].apply(lambda x:'I' if pd.isnull(x[1]) else 'N', axis=1)
# join_df['INS_FLAG']=join_df[['employee_no_src','employee_no_tgt']]
print("this is join_df")
print(join_df)

ins_rec=join_df[join_df['INS_FLAG']=='I']

print(ins_rec)


ins_rec=ins_rec[['employee_no_src','employee_name_x','salary_src','department_no_x']]
ins_rec.rename(columns={'employee_no_src':'employee_no','employee_name_x':'employee_name','salary_src':'salary','department_no_x':'department_no'},inplace=True)
ins_df=ins_rec[['employee_no','employee_name','salary','department_no']]
# #flag=1 for versioning of new records
ins_df['flag']=1


print("INSERT !!!!!!!!!!!!")
print(ins_df.shape)

# df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
# update_df=pd.concat([emp_scd, pd.DataFrame([ins_df])],ignore_index=True)

emp_scd.rename(columns={'employee_no_tgt':'employee_no','salary_tgt':'salary'},inplace=True)
print("ORGINAL !!!!!!!!!!!!")
print(emp_scd.shape)


# update_df=pd.concat([emp_scd, pd.DataFrame([ins_df])],ignore_index=True)
update_df = pd.concat([emp_scd, ins_df], sort=False)
print("UPDATE !!!!!!!!!!!!")
print(update_df)

join_df['INS_UPD_FLAG']=join_df[['employee_no_src','employee_no_tgt','salary_src','salary_tgt']].apply(lambda x:'UI' if x[0]==x[1] and x[2]!=x[3] else 'N', axis=1)

print(join_df)

ins_upd_rec=join_df[join_df['INS_UPD_FLAG']=='UI']
ins_upd_rec=ins_upd_rec[['employee_no_src','employee_name_x','salary_src','department_no_x']]
ins_upd_rec['flag']=1
ins_upd_rec.rename(columns={'employee_no_src':'employee_no','employee_name_x':'employee_name','salary_src':'salary','department_no_x':'department_no'},inplace=True)

print("insert upate")
print(ins_upd_rec)


# Create DataFrames


# Set the 'employee_no' column as the index for both DataFrames
update_df.set_index('employee_no', inplace=True)
ins_upd_rec.set_index('employee_no', inplace=True)


print(update_df)
print("SKDJHASKJDNBASKJDBNASKJDNKJASD")
print(ins_upd_rec)

# Update the 'target_df' with the new records and set the flag to 0 for the old records
update_df.loc[ins_upd_rec.index, 'flag'] = 0

print('sieruognjflkjvndoiklpjoklfj')
print(update_df)

print(';swedoihfjreoijeoijoweijdoi')
print(ins_upd_rec.index)


update_df = pd.concat([update_df, ins_upd_rec], sort=False)

print('sieruognjflkjvndoiklpjoklfj')
print(update_df)
