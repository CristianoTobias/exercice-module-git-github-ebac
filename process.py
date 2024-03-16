import requests
import time
import csv
import random
from bs4 import BeautifulSoup
from multiprocessing import Pool

# global headers to be used for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

MAX_PROCESSES = 10

def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    title = None
    date = None
    rating = None
    plot_text = None

    movie_data = movie_soup.find('div', attrs={'sc-491663c0-3 bdjVSf'})
    if movie_data:
        title_element = movie_data.find('h1', attrs={'data-testid': 'hero__pageTitle'}).find('span')
        title = title_element.get_text().strip() if title_element else None

        date_element = movie_data.find('a', attrs={'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color'})
        date = date_element.get_text().strip() if date_element else None

    rating_element = movie_soup.find('span', attrs={'sc-bde20123-1 cMEQkK'})
    rating = rating_element.get_text() if rating_element else None

    plot_element = movie_soup.find('span', attrs={'class': 'sc-466bb6c-2 chnFO'})
    plot_text = plot_element.get_text().strip() if plot_element else None

    with open('movies_process.csv', mode='a') as file:
        movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if all([title, date, rating, plot_text]):
            print(title, date, rating, plot_text)
            movie_writer.writerow([title, date, rating, plot_text])

def extract_movies(movie_links):
    with Pool(processes=MAX_PROCESSES) as pool:
        pool.map(extract_movie_details, movie_links)

def main():
    start_time = time.time()

    # IMDB Most Popular Movies - 100 movies
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    extract_movies(movie_links)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()
