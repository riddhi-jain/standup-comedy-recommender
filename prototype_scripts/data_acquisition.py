"""
Contains functions for scraping stand-up comedy transcripts from https://scrapsfromtheloft.com/stand-up-comedy-scripts/.
If this file is run as a script, will scrape comedy transcripts, create 2 related dataframes (transcripts and metadata),
and persist them to .pkl files in a local folder `data`.

Stephen Kaplan, 2020-08-10
"""
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import string
import json

from app.instance.creds import OMDB_API_KEY


def get_all_comedy_transcript_elements():
    """
    Returns all elements on the page that contain the titles of and href's to each stand-up comedy transcript.

    :return: HTML elements for all stand-up comedy transcripts on the page.
    :rtype: list
    """
    response = requests.get('https://scrapsfromtheloft.com/stand-up-comedy-scripts/')
    soup = BeautifulSoup(response.content, 'html.parser')
    parsing_results = soup.find_all('a', class_='title')

    return list(parsing_results)


def get_image_url(comedian, title, year):
    """

    :param comedian:
    :param title:
    :param year:
    :return:
    """
    title_formatted = title.replace(' ', '+').replace("’", '%27').replace('.', '%2E')
    comedian_formatted = comedian.replace(' ', '+').replace("’", '%27').replace('.', '%2E')

    # manual title adjustments/mappings for consistency with others (incorrect name or title/comedian swapped)
    title = 'Comedy Central Presents' if title == 'Comedy Central Special' else title
    if comedian in ['The Standups', 'Comedy Central Presents']:
        title_placeholder = comedian
        comedian = title
        title = title_placeholder

    # search by both title and comedian in case one works and the other doesn't
    response_title = requests.get(url=f'https://omdbapi.com/?t={title_formatted}&y={year}&apikey={OMDB_API_KEY}')
    response_comedian = requests.get(url=f'https://omdbapi.com/?t={comedian_formatted}&y={year}&apikey={OMDB_API_KEY}')

    if 'Poster' in json.loads(response_title.content):
        image_url = json.loads(response_title.content)['Poster']
    elif 'Poster' in json.loads(response_comedian.content):
        image_url = json.loads(response_comedian.content)['Poster']
    else:
        image_url = f'static/images/{comedian} - {title}.jpg'

    return image_url


def parse_comedy_metadata(raw_title):
    """
    Attempts to parse comedian name, comedy special title, and year from raw text. Returns None if unable to parse
    using that regular expression, indicating that it is a title for a non-standard comedy special. Also returns None
    if the transcript is not in English.

    :param str raw_title: Raw title containing multiple pieces of information from list of transcripts on page.
    :return: Metadata (Comedian name, comedy special title, and year performed).
    :rtype: dict
    """
    # define standard regex matching format for comedy titles
    regex_matches = re.match(r'(.+):\s(.+)\s\((\d+)\)', raw_title)

    # list of title tags that indicate that the transcript is not in english or from a non-standard comedy special
    removal_flags = ['Testo italiano completo', 'Trascrizione italiana', 'Traduzione italiana',     # Italian
                     'Transcripción completa',                                                      # Spanish
                     'Monologue', 'MONOLOGUE',                                                      # SNL / Monologues
                     'Always Be Late', 'Dumb Americans', 'The Philadelphia Incident', 'The Berkeley Concert']   # Misc

    if (regex_matches is None) or any(word in raw_title for word in removal_flags):
        return None
    else:
        comedian = string.capwords(regex_matches.group(1))
        title = string.capwords(regex_matches.group(2))
        year = int(regex_matches.group(3))
        return {
            'Comedian': comedian,
            'Title': title,
            'Year': year,
            'ImageURL': get_image_url(comedian, title, year)
        }


def parse_comedy_transcript(transcript_url):
    """
    Parses an individual comedy transcript.

    :param str transcript_url: URL to page containing transcript
    :return: Full raw transcript.
    :rtype: str
    """
    # navigate to page containing transcript and parse entire thing
    response = requests.get(transcript_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    transcript = soup.find('div', class_='post-content').text

    return transcript


def parse_comedy_metadata_and_transcripts(comedy_transcript_elements):
    """
    Main parsing function.

    :param list comedy_transcript_elements: HTML elements for all stand-up comedy transcripts on the page.
    :return: List of dictionaries containing parsed metadata and list of parased transcript strings.
    :rtype: tuple
    """
    parsed_metadata = []
    transcripts = []
    # loops through each HTML element containing comedy transcript metadata text and link to transcript
    for idx, element in enumerate(comedy_transcript_elements):
        m = parse_comedy_metadata(element.text)
        # if unable to parse using the method above, skips that entry. this usually indicates that it is the title of
        # non-standard comedy routine (such as a short sketch on David Letterman)
        if m is None:
            continue

        parsed_metadata.append(m)

        # parse transcript
        transcripts.append(parse_comedy_transcript(element['href']))

    return parsed_metadata, transcripts


def scrape_comedy_transcripts():
    """
    Main function for scraping stand-up comedy transcripts and metadata from
    https://scrapsfromtheloft.com/stand-up-comedy-scripts/.

    Saves metadata and transcript dataframes to .pkl files in the `data` folder.
    """
    print('Scraping comedy transcript data...')

    # scrape data
    comedy_transcript_elements = get_all_comedy_transcript_elements()
    parsed_metadata, parsed_transcripts = parse_comedy_metadata_and_transcripts(comedy_transcript_elements)

    # create dataframes
    df_metadata = pd.DataFrame(parsed_metadata)
    df_transcripts = pd.DataFrame({'Text': parsed_transcripts})

    # pickle dataframes
    df_metadata.to_pickle('data/standup_comedy_metadata.pkl')
    df_transcripts.to_pickle('data/raw_standup_comedy_transcripts.pkl')

    print(f'{len(parsed_metadata)} comedy transcript data successfully acquired and saved.')


if __name__ == "__main__":
    scrape_comedy_transcripts()
