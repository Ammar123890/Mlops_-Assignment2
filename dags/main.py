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

def fetch_and_transform(url, source, selector):
    """
    Fetches webpage content from the specified URL, extracts article details,
    and returns a list of dictionaries containing the article details.
    """
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.select(selector) if 'href' in a.attrs]
        links = [link if link.startswith('http') else url + link for link in links]

        articles = []
        for link in links:
            article_response = requests.get(link, timeout=10)
            article_soup = BeautifulSoup(article_response.text, 'html.parser')
            title = article_soup.find('title').text.strip() if article_soup.find('title') else None
            paragraphs = article_soup.find_all('p')
            description = ' '.join(p.text.strip() for p in paragraphs if p.text.strip())

            if title and description:
                articles.append({
                    'title': re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', title)),
                    'description': re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', description)),
                    'source': source,
                    'url': link
                })
        return articles
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch {url}: {str(e)}")
        return []

def load_articles(data):
    """
    Loads the extracted data into a CSV file.
    """
    try:
        df = pd.read_csv('./MLOPS_A2/Data.csv')
    except FileNotFoundError:
        df = pd.DataFrame()

    new_df = pd.DataFrame(data)
    df = pd.concat([df, new_df], ignore_index=True)
    df.to_csv('./MLOPS_A2/Data.csv', index=False)

dag = DAG(
    'MLOPS_Data_Scraping',
    start_date=datetime(2024, 5, 7),
    schedule_interval="@daily"
)

with dag:
    init_git = BashOperator(
        task_id='init_git',
        bash_command="cd ./MLOPS_A2 && git init"
    )
    init_dvc = BashOperator(
        task_id='init_dvc',
        bash_command="cd ./MLOPS_A2 && dvc init"
    )

    add_git_repo = BashOperator(
        task_id='add_git_repo',
        bash_command="cd ./MLOPS_A2 && git remote add origin git@github.com:Ammar123890/MLOps_Assignment2.git"
    )

    add_dvc_remote = BashOperator(
        task_id='add_dvc_remote',
        bash_command="cd ./MLOPS_A2 && dvc remote add -d myremote gdrive://1OtPEaU__vXt2Swm6puLNsOuxgjq6mqU3"
    )

    init_git >> init_dvc >> add_git_repo >> add_dvc_remote

    for source, details in {'BBC': {'url': 'https://www.bbc.com', 'selector': 'a[data-testid="internal-link"]'}, 'Dawn': {'url': 'https://www.dawn.com', 'selector': 'article.story a.story__link'}}.items():
        fetch_transform_task = PythonOperator(
            task_id=f'fetch_transform_{source.lower()}',
            python_callable=fetch_and_transform,
            op_kwargs={'url': details['url'], 'source': source, 'selector': details['selector']}
        )

        load_task = PythonOperator(
            task_id=f'load_{source.lower()}',
            python_callable=load_articles,
            op_args=[fetch_transform_task.output],
        )

        add_dvc_remote >> fetch_transform_task >> load_task

    dvc_add_push = BashOperator(
        task_id='dvc_add_push',
        bash_command="cd ./MLOPS_A2 && dvc add Data.csv && dvc push"
    )

    git_commit_push = BashOperator(
        task_id='git_commit_push',
        bash_command="cd ./MLOPS_A2 && git add Data.csv.dvc && git commit -m 'Updated articles' && git push origin master"
    )

    load_task >> dvc_add_push >> git_commit_push
