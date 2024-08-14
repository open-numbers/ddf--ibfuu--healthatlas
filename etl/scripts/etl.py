"""script to create dataset from the excel file in etl/source"""


from typing import Iterable
import os
import json
import pandas as pd


source_file_path = '../source/boendebarometer.ddf.xlsx'
output_dir = '../../'
# source_file_path = os.path.join(os.path.abspath(__file__), '../source/boendebarometer.ddf.xlsx')

def read_source(sheet_name: str, **kwargs):
    return pd.read_excel(source_file_path, sheet_name=sheet_name, **kwargs)

index_df = read_source('index', dtype=str).set_index('sheet')

# prepare a datatype dictionary for correctly infer the datatypes in all sheets
dictionary_df = read_source('dictionary', dtype=str).set_index('concept')
string_columns = dictionary_df[dictionary_df['concept_type'] != 'measure']
dtypes = dict([(x, 'str') for x in string_columns.index.values])

# I will just do very minimum validations in this script.
# step 1: load all concepts, save to ddf--concepts.csv
concept_files = index_df[index_df['type'] == 'concepts'].index.values
concept_dfs = [read_source(x, dtype=str) for x in concept_files]
concept_df = pd.concat(concept_dfs, ignore_index=True)

def check_columns(columns: Iterable):
    set_diff = set(columns) - set(concept_df.index)
    if len(set_diff) != 0:
        raise ValueError(f'please make sure following concepts are in concepts sheets: {list(set_diff)}')


# 1.0 we must have concept and concept_type columns
if 'concept' not in concept_df.columns or 'concept_type' not in concept_df.columns:
    print(f'columns: {concept_df.columns}')
    raise ValueError('concept and concept_type column must exist in concepts sheets.')

concept_df = concept_df.set_index('concept')

# 1.1 ensure all columns are concepts
concept_columns = concept_df.columns
concept_columns_to_check = concept_columns.drop('concept_type')  # concept_type is reserved
check_columns(concept_columns_to_check)

# 1.2 ensure if there is eneity_set, we should have a domain column
if not concept_df[concept_df.concept_type == 'entity_set'].empty:
    for c, row in concept_df[concept_df.concept_type == 'entity_set'].iterrows():
        if pd.isnull(row['domain']):
            raise ValueError(f'domain is empty for entity_set: {c}')

# 1.3 convert drill_up values to json string
def drill_up_to_json(val: str):
    if pd.isnull(val):
        return val
    lst = val.split(',')
    return json.dumps(lst)

concept_df['drill_up'] = concept_df['drill_up'].map(drill_up_to_json)

concept_df.to_csv(os.path.join(output_dir, 'ddf--concepts.csv'))


# step 2: load all entities
entity_files = index_df[index_df['type'] == 'entities']
entity_files

for sheet in entity_files.index:
    entity_df = read_source(sheet, dtype=str)
    # I will assume that the sheet name is the primary key column.
    # so check if it's a entity_set or entity_domain
    if concept_df.loc[sheet].concept_type == 'entity_domain':
        out_file_name = f'ddf--entities--{sheet}.csv'
    elif concept_df.loc[sheet].concept_type == 'entity_set':
        domain = concept_df.loc[sheet, 'domain']
        out_file_name = f'ddf--entities--{domain}--{sheet}.csv'
    else:
        raise ValueError(f'{sheet} is not an entity domain or entity set, but it is used as primary key in entity sheets.')
    # add 'is--' column for entity_sets
    if concept_df.loc[sheet, 'concept_type'] == 'entity_set' and f'is--{sheet}' not in entity_df.columns:
        entity_df[f'is--{sheet}'] = 'TRUE'
    # then check every columns are concepts
    columns_to_check = filter(lambda x: not x.startswith('is--'), entity_df.columns)
    check_columns(columns_to_check)
    # save to file
    entity_df.to_csv(os.path.join(output_dir, out_file_name), index=False)


# step 3: load all datapoints
datapoint_files = index_df[index_df['type'] == 'datapoints']
datapoint_files

for sheet in datapoint_files.index:
    datapoint_df = read_source(sheet, dtype=dtypes)
    # auto detect which are primary keys
    pkeys = []
    for col in datapoint_df.columns:
        if concept_df.loc[col, 'concept_type'] in ['entity_domain', 'entity_set', 'time']:
            pkeys.append(col)
    # set index
    datapoint_df = datapoint_df.set_index(pkeys)
    # serve each indicators
    pkeys_str = '--'.join(pkeys)
    for col in datapoint_df:
        out_file_name = f'ddf--datapoints--{col}--by--{pkeys_str}.csv'
        datapoint_df[col].dropna().to_csv(os.path.join(output_dir, out_file_name))

print('ddf dataset created successfully.')
