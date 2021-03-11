"""
This module is used to classify tags from different sources to get unique categories.
For that reason google language API is used. Scraped tag is passed to API (the tag can be reapeted several
times in the input, because API has a minimum length threshold) to get the category name.
"""

from connect_to_db import connect_to_mpdprocessing_new_engine
from collections import defaultdict, Counter
from google.cloud import language
from utils import add_data
import pandas as pd
import numpy as np
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Eventmoon-cb642c460e65.json'


def classify(text: str) -> str:
    """Classify the input text into categories"""
    language_client = language.LanguageServiceClient()

    document = language.types.Document(
        content=text,
        type=language.enums.Document.Type.PLAIN_TEXT)
    response = language_client.classify_text(document)
    categories = response.categories
    for category in categories:
        return category.name
    return None


def flatten_list_column(df: pd.DataFrame, lst_col: str) -> pd.DataFrame:
    """Flattens the column in df, which contains list objects"""
    return pd.DataFrame({
        col: np.repeat(df[col].values, df[lst_col].str.len())
        for col in df.columns.drop(lst_col)}
    ).assign(**{lst_col: np.concatenate(df[lst_col].values)})[df.columns]


def get_cat(subcat: str, cat2subcat: dict) -> str:
    """Gets the category name for the given subcategory"""
    for cat, subcats in cat2subcat.items():
        if subcat in subcats:
            return cat


def keep_frequent_tags(tags_counted: list) -> list:
    """Filter out tags that have low frequency (less then 8 events had such tag)."""
    frequent_tags = []
    for tag, number in tags_counted:
        if number > 8:
            frequent_tags.append(tag)
    return frequent_tags


def create_tags2event_table(event_df: pd.DataFrame, tag_table: pd.DataFrame, tags: list) -> pd.DataFrame:
    """Creates a table with columns 'event_id' and 'tag_id' to get the connection from event to tag"""
    tags2event_table = event_df[['id', 'tags']].dropna(subset=['tags'])
    tags2event_table['tags'] = tags2event_table['tags'].apply(lambda x: x.split(','))
    tags2event_table = flatten_list_column(tags2event_table, 'tags')
    tags2event_table['tags'] = tags2event_table['tags'].str.strip().str.lower()
    tags2event_table = tags2event_table[tags2event_table['tags'].isin(tags)]

    tag_table_index = tag_table.set_index('tag')
    tags2event_table['tags'] = tags2event_table['tags'].apply(lambda x: tag_table_index.loc[x, 'id'])
    tags2event_table = tags2event_table.rename(columns={'id': 'event_id', 'tags': 'tag_id'})
    return tags2event_table


def classify_tags(tags: list) -> dict:
    """Classifies tag names to get unique categories (Google API is used)"""
    # Classify tags
    subcat2tag = {}
    for tag_to_check in tags:
        res = classify(' '.join([tag_to_check for _ in range(30)]))
        subcat2tag[tag_to_check] = res.strip('/').split('/') if res else res

    # Filter subcat2tag
    subcat2tag = {tag_real: tag_new for tag_real, tag_new in subcat2tag.items() if tag_new is not None}

    return subcat2tag


def create_tag_table(tags_to_check: list) -> pd.DataFrame:
    """Creates a tag table with columns 'id' and 'tag' to latter push to DB"""
    tag_table_data = []
    count_data = 1
    for tag in tags_to_check:
        tag_table_data.append((count_data, tag))
        count_data += 1
    return pd.DataFrame(tag_table_data, columns=['id', 'tag'])


def create_cat2subcat_mapping(subcat2tag: dict) -> dict:
    """Creates mapping from category to subcategory"""
    cat2subcat = defaultdict(set)
    for tag_real, tag_new in subcat2tag.items():
        for n_tag, tag in enumerate(tag_new):
            if n_tag == 0:
                cat = tag
            cat2subcat[cat].add(tag)
    return cat2subcat


def create_category_table(cat2subcat: dict) -> pd.DataFrame:
    """Creates a category table with columns 'id' and 'category' to latter push to DB"""
    return pd.DataFrame({'id': [i + 1 for i in range(len(cat2subcat))], 'category': list(cat2subcat.keys())})


def create_subcategory_table(cat2subcat: dict) -> pd.DataFrame:
    """Creates a subcategory table with columns 'id' and 'subcategory' to latter push to DB"""
    all_subcategory = set()
    for subcategory in cat2subcat.values():
        all_subcategory |= subcategory

    return pd.DataFrame({'id': [i + 1 for i in range(len(all_subcategory))], 'subcategory': list(all_subcategory)})


def create_cat2subcat_table(subcategory_table: pd.DataFrame, category_table: pd.DataFrame,
                            cat2subcat: dict) -> pd.DataFrame:
    """Creates a category_subcategory table with columns 'category_id' and 'subcategory_id' to latter push to DB"""
    cat2subcat_table = subcategory_table.rename(columns={'id': 'subcategory_id'})

    cat2subcat_table['category_id'] = cat2subcat_table['subcategory'].apply(
        lambda x: category_table[category_table['category'] == get_cat(x, cat2subcat)]['id'].iloc[0])

    return cat2subcat_table.drop(columns=['subcategory'])


def create_subcat2tag_table(subcategory_table: pd.DataFrame, tag_table: pd.DataFrame, subcat2tag: dict) -> pd.DataFrame:
    """Creates a subcategory_tag table with columns 'tag_id' and 'subcategory_id' to latter push to DB"""
    subcat2tag_table = tag_table.rename(columns={'id': 'tag_id'})
    subcat2tag_table['subcats'] = subcat2tag_table['tag'].apply(lambda x: subcat2tag[x])

    subcat2tag_table = flatten_list_column(subcat2tag_table, 'subcats')

    subcat_table_index = subcategory_table.set_index('subcategory')
    subcat2tag_table['subcats'] = subcat2tag_table['subcats'].apply(lambda x: subcat_table_index.loc[x, 'id'])
    del subcat_table_index
    subcat2tag_table = subcat2tag_table.drop(columns=['tag'])

    return subcat2tag_table.rename(columns={'id': 'tag_id', 'subcats': 'subcategory_id'})


def process_tags(event_df: pd.DataFrame, event2time: dict) -> None:
    """
    Main function to process tags.
    It takes tags, fillters them, classifies them, gets category and subcategories for each tag and pushes related
    tables to DB.
    """
    connection = connect_to_mpdprocessing_new_engine()

    tags = event_df['tags'].dropna().apply(lambda x: x.split(',')).tolist()
    tags = [tag.strip().lower() for tags_event in tags for tag in tags_event if not tag.strip('< ').isdigit()]
    tags_counted = Counter(tags).most_common()
    tags_to_check = keep_frequent_tags(tags_counted)
    print(f'Unique tags to process: {len(tags_to_check)}')

    subcat2tag = classify_tags(tags_to_check)
    final_tags = list(subcat2tag.keys())
    tag_table = create_tag_table(final_tags)
    tags2event_table = create_tags2event_table(event_df, tag_table, final_tags)

    cat2subcat = create_cat2subcat_mapping(subcat2tag)
    category_table = create_category_table(cat2subcat)
    subcategory_table = create_subcategory_table(cat2subcat)

    cat2subcat_table = create_cat2subcat_table(subcategory_table, category_table, cat2subcat)
    subcat2tag_table = create_subcat2tag_table(subcategory_table, tag_table, subcat2tag)

    tag_table = tag_table.rename(columns={'tag': 'name'})
    subcategory_table = subcategory_table.rename(columns={'subcategory': 'name'})
    category_table = category_table.rename(columns={'category': 'name'})

    # Add tag, subcategory, category and get mappings
    tag_id_mapping = add_data(df=tag_table, sql_table_name='tag', connection=connection)
    subcategory_id_mapping = add_data(df=subcategory_table, sql_table_name='subcategory', connection=connection)
    category_id_mapping = add_data(df=category_table, sql_table_name='category', connection=connection)

    # Map ids to ids in database
    tags2event_table['tag_id'] = tags2event_table['tag_id'].apply(lambda x: tag_id_mapping[x])
    tags2event_table['event_id'] = tags2event_table['event_id'].apply(lambda x: event2time[x])

    subcat2tag_table['subcategory_id'] = subcat2tag_table['subcategory_id'].apply(lambda x: subcategory_id_mapping[x])
    subcat2tag_table['tag_id'] = subcat2tag_table['tag_id'].apply(lambda x: tag_id_mapping[x])

    cat2subcat_table['category_id'] = cat2subcat_table['category_id'].apply(lambda x: category_id_mapping[x])
    cat2subcat_table['subcategory_id'] = cat2subcat_table['subcategory_id'].apply(lambda x: subcategory_id_mapping[x])

    # Add mapping tables to have connection among tables
    add_data(df=tags2event_table, sql_table_name='tag__event', connection=connection, return_mapping=False)
    add_data(df=subcat2tag_table, sql_table_name='subcategory__tag', connection=connection, return_mapping=False)
    add_data(df=cat2subcat_table, sql_table_name='category__subcategory', connection=connection, return_mapping=False)
    print('Finished')
