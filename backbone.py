import pandas as pd
from pathlib import Path
import numpy as np

def downcastdf(df):
    # Try to reduce DB size
    for column in df:
        if df[column].dtype == "float64":
            df[column]=pd.to_numeric(df[column], downcast="float")
        if df[column].dtype == "int64":
            df[column]=pd.to_numeric(df[column], downcast="integer")
    return df

# FLATTENING DATABASE TO SINGLE CSV

def generate_merged_csv(mainpath= "/media/hdd/Datasets/culinarydb"):
    # Get the data and merge it into a combined frame

    main_path = Path(mainpath)
    rd = pd.read_csv(main_path/"01_Recipe_Details.csv")
    ing = pd.read_csv(main_path/"02_Ingredients.csv")
    cing = pd.read_csv(main_path/"03_Compound_Ingredients.csv")
    alis = pd.read_csv(main_path/"04_Recipe-Ingredients_Aliases.csv")

    cols = ['Aliased Ingredient Name','Entity ID']

    df2 =pd.pivot_table(alis, columns=cols,index=['Recipe ID'], aggfunc=lambda x:','.join(list(set(x))))
    df2 = df2.where(~df2.notna(), 1)
    df2 = df2.fillna(0)
    df2.columns = [col[-2] for col in df2.columns.values]
                
    df2 = df2.join(rd.set_index("Recipe ID"), on="Recipe ID", how="right")
    df2 = df2.drop(["Source", "Cuisine"], axis = 1)
    df2 = downcastdf(df2)
    df2.to_csv(main_path/"flattened_recipies.csv")

def get_names(df, query, exclude):
    # Get names of the recipies you can make
    if len(exclude)>0: 
        query1 = query.copy()
        query.extend(exclude)
        query.append("Title")
        temp = df[query].copy()
        corr = np.array([temp[x]==1.0 for x in query1]).T
        corr2 = np.array([temp[x]==1.0 for x in exclude]).T
        temp["yes"] = [int(x.all()) for x in corr]
        temp["no"] = [int(x.all()) for x in corr2]
        temp = temp.fillna(0.0)
        temp = temp[(temp["yes"] == 1.0) & (temp["no"] == 0.0)]
        return temp["Title"].values
    else:
        query.append("Title")
        temp = df[query]
        corr = np.array([temp[x]==1.0 for x in query[:-1]]).T
        temp["yes"] = [int(x.all()) for x in corr]
        return temp[temp["yes"]==1]["Title"].values

def preprocess(lis):
    # lowercase, remove extra space
    if lis == None: return []
    elif len(lis) == 1: 
        if len(lis[0]) ==0: return []
    else:
        return [str(x).lower().strip() for x in lis.split(",")]

def read_data(df, include, exclude):
    df = df.fillna(0)
    listed = get_names(df, preprocess(include), preprocess(exclude))
    print(f"{len(listed)} Recipies found \n{listed}")

def find_ingredients(df, name):
    df_t = df[df["Title"] == name]
    df_t = df_t.loc[:, (df_t != 0).any(axis=0)]
    print(df_t)

    