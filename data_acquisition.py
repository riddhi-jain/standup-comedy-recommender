"""
Contains functions for scraping stand-up comedy transcripts from https://scrapsfromtheloft.com/stand-up-comedy-scripts/.
This file can be run as a script.

Stephen Kaplan, 2020-08-10
"""
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd


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

    # list of title tags that indicate that the transcript is not in english
    foreign_language_tags = ['Testo italiano completo', 'Trascrizione italiana', 'Traduzione italiana',   # italian
                             'Transcripción completa']                                                    # spanish

    if (regex_matches is None) or any(word in raw_title for word in foreign_language_tags):
        return None
    else:
        return {
            'Comedian': regex_matches.group(1),
            'Title': regex_matches.group(2),
            'Year': int(regex_matches.group(3))
        }


def format_metadata(raw_metadata):
    """
    Formats all text data in raw standup comedy metadata dataframe.

    :param pandas.DataFrame raw_metadata: DataFrame containing columns: Comedian name, title, and year of performance.
    :return: Processed DataFrame with same format as input.
    """
    # convert all text data to have only first letter capitalized
    metadata = raw_metadata.copy()
    metadata['Comedian'] = metadata['Comedian'].str.title()
    metadata['Title'] = metadata['Title'].str.title()

    return metadata


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
    df_raw_metadata = pd.DataFrame(parsed_metadata)
    df_metadata = format_metadata(df_raw_metadata)
    df_transcripts = pd.DataFrame({'Text': parsed_transcripts})

    # pickle dataframes
    df_metadata.to_pickle('data/standup_comedy_metadata.pkl')
    df_transcripts.to_pickle('data/raw_standup_comedy_transcripts.pkl')

    print(f'{len(parsed_metadata)} comedy transcript data successfully acquired and saved.')


if __name__ == "__main__":
    scrape_comedy_transcripts()
