from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##
# @def fetch_and_extract
# @brief Fetches webpage content for a given URL, extracts article details, and returns a list of dictionaries.
# @param url URL of the webpage to fetch
# @param source Name of the source
# @param selector Selector to extract article links
# @return List of dictionaries containing article details
##


def fetch_and_extract(url, source, selector):
    """
    Fetches webpage content for a given URL, extracts article details, and returns a list of dictionaries.
    """
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        links = [a['href'] for a in soup.select(selector) if 'href' in a.attrs]
        links = [link if link.startswith('http') else url + link for link in links]

        data = []

        for link in links:
            response = requests.get(link, timeout=10)
            article_soup = BeautifulSoup(response.text, 'html.parser')
            title_element = article_soup.find('title')
            title = title_element.text.strip() if title_element else None
            paragraphs = article_soup.find_all('p')
            description = ' '.join([p.text.strip() for p in paragraphs if p.text.strip()]) if paragraphs else None

            if title and description:
                title = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', title)).strip()
                description = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', description)).strip()
                data.append({
                    'title': title,
                    'description': description,
                    'source': source,
                    'url': link
                })

        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        return []

##
# @def load
# @brief Loads the transformed data into a CSV file.
# @param data List of dictionaries containing article details
##

def load(data):
    """
    Loads the transformed data into a CSV file.
    """
    try:
        # Try to load existing CSV file
        df = pd.read_csv('/mnt/d/MLOps/MLOPS_A2/Data.csv')
    except FileNotFoundError:
        # If file doesn't exist, create an empty DataFrame
        df = pd.DataFrame()

    # Append new data to existing DataFrame or create new DataFrame if empty
    new_df = pd.DataFrame(data)
    df = pd.concat([df, new_df], ignore_index=True)

    # Write DataFrame to CSV file
    df.to_csv('/mnt/d/MLOps/MLOPS_A2/Data.csv', index=False)

dag = DAG('MLOPS_Data_Scrapped', start_date=datetime(2024, 5, 7), schedule="@daily")

##
# @def fetch_and_extract
# @brief Fetches webpage content for a given URL, extracts article details, and returns a list of dictionaries.
# @param url URL of the webpage to fetch
# @param source Name of the source
# @param selector Selector to extract article links
# @return List of dictionaries containing article details
##


with dag:
    # Initialize Git and DVC in the directory
    init_git = BashOperator(
        task_id='InitiateGit',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && git init "
    )
    init_dvc = BashOperator(
        task_id='InitiateDVC',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && dvc init"
    )

    # Add remote repositories for Git and DVC
    init_add_git_repo = BashOperator(
        task_id='init_repogit',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && git remote add origin git@github.com:Ammar123890/Mlops_-Assignment2.git "
    )
    # Add remote repositories for Git and DVC
    init_add_dvc_drive = BashOperator(
        task_id='init_repodvc',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && dvc remote add -d myremote gdrive://1OtPEaU__vXt2Swm6puLNsOuxgjq6mqU3"
    )

    # Ensure the remote repo setup depends on the initial git and DVC setup
    init_git >> init_dvc >> init_add_git_repo >>  init_add_dvc_drive

    # Define sources and tasks for each source
    sources = {'BBC': {'url': 'https://www.bbc.com', 'selector': 'a[data-testid="internal-link"]'},
               'Dawn': {'url': 'https://www.dawn.com', 'selector': 'article.story a.story__link'}}
    last_task = init_add_dvc_drive
    for source, info in sources.items():
        extract_and_transform_task = PythonOperator(
            task_id=f'extract_and_transform_{source.lower()}',
            python_callable=fetch_and_extract,
            op_kwargs={'url': info['url'], 'source': source, 'selector': info['selector']}
        )

        load_task = PythonOperator(
            task_id=f'load_{source.lower()}',
            python_callable=load,
            op_args=[extract_and_transform_task.output],
        )

        last_task >> extract_and_transform_task >> load_task
        last_task = load_task
    # DVC and Git final tasks
    dvc_task = BashOperator(
        task_id='dvc_Push_Add',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && dvc add Data.csv && dvc push"
    )

    git_push = BashOperator(
        task_id='git_PUsh_Add',
        bash_command="cd /mnt/d/MLOps/MLOPS_A2 && git add Data.csv.dvc && git commit -m 'Updated articles' && git push origin master"
    )
    # Ensure these final tasks follow the last load task
    last_task >> dvc_task >> git_push