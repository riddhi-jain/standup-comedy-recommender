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

from app.creds import OMDB_API_KEY

MANUAL_IMAGE_REPLACEMENTS = {
    "Patton Oswalt - I Love Everything": "https://m.media-amazon.com/images/M/MV5BNTAxNDk5OTMtNDZiYy00NDc5LWJmYTgt"
                                         "OWM4NTg3MDIyZTg4XkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_.jpg",
    "Russell Peters - Deported": "https://m.media-amazon.com/images/M/MV5BNDY1ZjJkZjUtNGY3Ni00NjQwLTk3ZmUtOWE5ZWNk"
                                 "YTNhYWE5XkEyXkFqcGdeQXVyMTExMTc4Mjk1._V1_.jpg",
    "Jo Koy - Lights Out": "https://images-na.ssl-images-amazon.com/images/I/71e6MG0piFL._RI_.jpg",
    "Lee Mack - Live": "https://images-na.ssl-images-amazon.com/images/I/81ak-ugJICL._SL1500_.jpg",
    "Daniel Sloss - X": "https://images-na.ssl-images-amazon.com/images/I/61HRMusU9sL._RI_.jpg",
    "Pete Davidson - SMD": "https://images-na.ssl-images-amazon.com/images/I/91pKSaY4URL._RI_.jpg",
    "Sara Pascoe - LadsLadsLads": "https://a.ltrbxd.com/resized/film-poster/5/2/3/0/0/5/523005-sara-pascoe-live-"
                                  "ladsladslads-0-230-0-345-crop.jpg",
    "Emily Heller - Ice Thickeners": "https://streaming-engine-assets.rftslb.com/posters/image/image/"
                                     "1011500/list_page_main.jpg",
    "Wanda Sykes - Not Normal": "https://m.media-amazon.com/images/M/MV5BOGZlNTQ0MjQtMDBmMi00OGQ2LTlmODUtMTI1ZGI5ZT"
                                "AzY2ZkXkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_.jpg",
    "Kevin Hart - Irresponsible": "https://occ-0-1009-1001.1.nflxso.net/dnm/api/v6/XsrytRUxks8BtTRf9HNlZkW2tvY/AAA"
                                  "ABTWNtRGyj4ZvPpmE65-1It9FyHDM10rOx61M9P99MVKR66M7yMol6XTmliZHCyX02dhkmuRlbfK_Dc"
                                  "d9m-ytIn0EX-hMjBmyAA.jpg",
    "Ellen Degeneres - Relatable": "https://occ-0-1001-999.1.nflxso.net/art/58afa/e8a9f3db123a9f2318cf76283d2d66c"
                                   "6d5858afa.jpg",
    "Vir Das - Losing It": "https://m.media-amazon.com/images/M/MV5BZWFhNmFjMzQtZDE4OS00MjY2LWJlMTktN2UzYjk5Zjk5Nz"
                           "U2XkEyXkFqcGdeQXVyNjI0MDg2NzE@._V1_.jpg",
    "Mo Amer - The Vagabond": "https://img.reelgood.com/content/movie/8829a6d0-6fcc-4825-862e-16e8a29"
                              "ce116/poster-780.jpg",
    "Joe Rogan - Strange Times": "https://m.media-amazon.com/images/M/MV5BNzc2Mzg5YmMtMzM1NC00NDgwLTljYWQtZDdkNzB"
                                 "mNjZkNmJhXkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_.jpg",
    "Daniel Sloss - Dark": "https://pbs.twimg.com/media/DoPUGEnWkAA3IlM.jpg",
    "Daniel Sloss - Jigsaw": "https://pbs.twimg.com/media/DoPUGEnWkAA3IlM.jpg",
    "Daniel Tosh - Comedy Central Presents": "https://image.tmdb.org/t/p/w500/223LsiI29uOQpdN32QdeMTfmB5R.jpg",
    "Dave Chappelle - HBO Comedy Half-Hour": "https://i.pinimg.com/originals/fa/0c/1e/fa0c1e2ae6e175c6fd"
                                             "6469453b5476d2.jpg",
    "Brad Williams - Daddy Issues": "https://images-na.ssl-images-amazon.com/images/I/81eQG-LuvML._SL1500_.jpg",
    "Maria Bamford - Old Baby": "https://comedydynamics.com/wp-content/uploads/2019/02/Maria-Bamford-Old-V.jpg",
    "Russell Peters - Outsourced": "https://images-na.ssl-images-amazon.com/images/I/81pa6vYuLZL._SL1500_.jpg",
    "Nikki Glaser - Perfect": "https://img.reelgood.com/content/movie/8bfbe764-cd5c-4332-a15c-439446"
                              "148071/poster-780.jpg",
    "Ricky Gervais - Humanity": "https://m.media-amazon.com/images/M/MV5BZTgxZGRhY2EtNzg0OC00ODA2LWFhM2ItZmRmYW"
                                "Y5NGI4MGRhXkEyXkFqcGdeQXVyMTMxODk2OTU@._V1_.jpg",
    "Brian Regan - Standing Up": "https://m.media-amazon.com/images/M/MV5BMTYyNDE3NTc2Nl5BMl5BanBnXkFtZT"
                                 "cwODM4MzA1MQ@@._V1_.jpg",
    "Chris Tucker - Live": "https://m.media-amazon.com/images/M/MV5BMTU1MzU2Njg2MF5BMl5BanBnXkFtZTgwNTM"
                           "0MzgxNjE@._V1_.jpg",
    "Ellen Degeneres - The Beginning": "https://images-na.ssl-images-amazon.com/images/I/91blTMH3l6L._RI_.jpg",
    "Maz Jobrani - Immigrant": "https://m.media-amazon.com/images/M/MV5BMmI0MTBlNzktZDJlNC00OGE1LTgwNGUtODllOG"
                               "E4OTQyNTY1XkEyXkFqcGdeQXVyMTk2ODU0OTM@._V1_.jpg",
    "Whitney Cummings - I Love You": "https://images-na.ssl-images-amazon.com/images/I/61rcd3l1XuL.jpg",
    "Patton Oswalt - Annihilation": "https://m.media-amazon.com/images/M/MV5BZGE4NDVjZGQtNzc0Yi00YTE2LWFiN2EtYTNi"
                                    "Y2ZiNjliYWQ1XkEyXkFqcGdeQXVyMTk3NDAwMzI@._V1_.jpg",
    "Chris Rock - Bigger & Blacker": "https://m.media-amazon.com/images/M/MV5BODQzOWExYjktZDQ2OC00YTA5LWI3YzctMDFjN"
                                     "jc3ZTM5NTA0XkEyXkFqcGdeQXVyMTk3NDAwMzI@._V1_.jpg",
    "Erik Griffin - The Ugly Truth": "https://m.media-amazon.com/images/M/MV5BZmU0NjNhY2ItMzg3ZC00ZTM4LTliMjgtZm"
                                     "YzYWM4YTU2YmVkXkEyXkFqcGdeQXVyNTM3MDMyMDQ@._V1_.jpg",
    "Norm Macdonald - One Night Stand": "https://images-na.ssl-images-amazon.com/images/S/pv-target-images/4131b"
                                        "f9a3627ab6cec0dd57d335447ddc9a5f60b2ec0a39a83b4e0f60e09470e._V_SX300_.jpg",
    "Dylan Moran - Off the Hook": "https://images-na.ssl-images-amazon.com/images/I/A1l-Gzb8dkL._RI_.jpg",
    "Richard Pryor - Live & Smokin’": "https://occ-0-116-114.1.nflxso.net/art/5be58/0268fffdd24c4843e88d761b96fb"
                                      "588f6435be58.jpg",
    "Joe Rogan - Triggered": "https://m.media-amazon.com/images/M/MV5BMGM5NjlmZTgtMTgyZS00MGQ2LTgyNDQtM2EzZTU2N"
                             "WUzMzJhXkEyXkFqcGdeQXVyNjU2MTA3OTY@._V1_SX666_CR0,0,666,999_AL_.jpg",
    "Trevor Noah - Lost in Translation": "https://images.justwatch.com/poster/11654253/s592",
    "Bo Burnham - What": "https://pm1.narvii.com/6824/e64b4478b14bc98e1bdac09664b5d5bb01c5a51bv2_00.jpg",
    "John Mulaney - The Comeback Kid": "https://m.media-amazon.com/images/M/MV5BMDQ3NjU0NmQtYjgyZS00MzIzLWJjND"
                                       "EtMWY5YjczYjc0MTMyXkEyXkFqcGdeQXVyMjI0MjUyNTc@._V1_.jpg",
    "Bill Hicks - Revelations": "https://m.media-amazon.com/images/M/MV5BMjM1OTAwMDE3N15BMl5BanBnXkFtZTgwNjkz"
                                "MzYwNzE@._V1_.jpg",
    "George Carlin - Again!": "https://images-na.ssl-images-amazon.com/images/I/71c-BlpoKiL._RI_.jpg",
    "Louis C.K. - Hilarious": "https://images-na.ssl-images-amazon.com/images/I/51cTzhJbsuL._AC_SY445_.jpg",
    "Louis C.K. - Shameless": "https://images-na.ssl-images-amazon.com/images/I/61s1KHMPyOL.jpg",
    "Louis C.K. - Oh My God": "https://images-na.ssl-images-amazon.com/images/I/81yIHMSddAL._RI_.jpg",
    "Patrice O’Neal": "https://images-na.ssl-images-amazon.com/images/I/61mGKbweejL._SL1260_.jpg",
    "Jim Jefferies - Bare": "https://m.media-amazon.com/images/M/MV5BMmRlNDE0ZDctZTMwYi00MWQ2LTk2MzctNjdjODM0YW"
                            "Y1MjNmXkEyXkFqcGdeQXVyMjI0MjUyNTc@._V1_.jpg"
}


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
    Returns a public URL to the "movie poster" for a given comedy special. Due to the inconsistency in availability
    of these images, this function tries a few methods: Searches the OMDB API by title, searches the OMDB API by
    comedian, checks the static/images directory, and checks a lookup table containing image URLs.

    :param str comedian: Name of the comedian.
    :param title: Title of the stand-up comedy special
    :param year: Release year for the standup comedy special.
    :return: Public URL to a relevant movie poster/graphic for the comedy special.
    :rtype: str
    """
    title_formatted = title.replace(' ', '+').replace("’", '%27').replace('.', '%2E')
    comedian_formatted = comedian.replace(' ', '+').replace("’", '%27').replace('.', '%2E')

    # search by both title and comedian in case one works and the other doesn't
    response_title = requests.get(url=f'https://omdbapi.com/?t={title_formatted}&y={year}&apikey={OMDB_API_KEY}')
    response_comedian = requests.get(url=f'https://omdbapi.com/?t={comedian_formatted}&y={year}&apikey={OMDB_API_KEY}')

    # insert image URL manually if in lookup table above
    try:
        image_url = MANUAL_IMAGE_REPLACEMENTS[f'{comedian} - {title}']
    except KeyError:
        if 'Poster' in json.loads(response_title.content):                  # try searching OMDB API with title
            image_url = json.loads(response_title.content)['Poster']
        elif 'Poster' in json.loads(response_comedian.content):             # try searching OMDB API with comedian
            image_url = json.loads(response_comedian.content)['Poster']
        else:                                                               # use static image file
            image_url = f'static/images/{comedian} - {title}.jpg'

    return image_url


def replace_if_title_does_not_begin_with(text, incorrect_fragment, replacement):
    """
    A wrapper for the str.replace() function that only replaces the text fragment if the input string does NOT
    begin with the fragment.

    :param str text: Text to modify.
    :param incorrect_fragment: Text fragment to replace.
    :param replacement: Replacement text fragment.
    :return: Modified text.
    :rtype: str
    """
    if text[:len(incorrect_fragment)] != incorrect_fragment:
        text = text.replace(incorrect_fragment, replacement)

    return text


def custom_format(comedian, title):
    """

    :param comedian:
    :param title:
    :return:
    """
    # swap comedian and title for special cases
    if comedian in ['The Standups', 'Comedy Central Presents']:
        title_placeholder = comedian
        comedian = title
        title = title_placeholder

    formatted = []
    for text in [comedian, title]:
        text = replace_if_title_does_not_begin_with(text, 'In ', 'in ')
        text = replace_if_title_does_not_begin_with(text, 'At ', 'at ')
        text = replace_if_title_does_not_begin_with(text, 'On ', 'on ')
        text = replace_if_title_does_not_begin_with(text, 'Of ', 'of ')
        text = replace_if_title_does_not_begin_with(text, 'To ', 'to ')
        text = replace_if_title_does_not_begin_with(text, 'The ', 'the ')
        text = replace_if_title_does_not_begin_with(text, 'From ', 'from ')
        text = replace_if_title_does_not_begin_with(text, 'For ', 'for ')
        text = replace_if_title_does_not_begin_with(text, 'And ', 'and ')
        text = replace_if_title_does_not_begin_with(text, 'Is ', 'is ')
        text = replace_if_title_does_not_begin_with(text, ' A ', ' a ')

        custom_mappings = {
            'Hbo Comedy Half-hour': 'HBO Comedy Half-Hour',
            'T.j. Miller': 'T.J. Miller',
            'Smd': 'SMD',
            'Ladsladslads': 'LadsLadsLads',
            'Protected: Katherine Ryan': 'Katherine Ryan',
            'Live (at the Time)': 'Live (At the Time)',
            'Gabriel “fluffy” Iglesias': 'Gabriel “Fluffy” Iglesias',
            'John Leguizamo’s Road to Broadway': 'John Leguizamo',
            'D.l. Hughley': 'D.L. Hughley',
            'Live Iv – Science': 'Live IV – Science',
            'Comedy Central Special': 'Comedy Central Presents',
            'Stand-up Comedian': 'Stand-Up Comedian',
            'Ricky Gervais Live 2': 'Ricky Gervais',
            'Politics': 'Live 2: Politics',
            'Russell Howard Live': 'Russell Howard',
            'Christina Pazsitzky': 'Christina P',
            'Live in Madison Square Garden': 'Live at Madison Square Garden',
            'A Piece of My Mind – Godbless America': 'A Piece of My Mind – God Bless America',
            'Kill the Messenger – London, New York, Johannesburg': 'Kill the Messenger',
            'Live from D.c.': 'Live from D.C.',
            'If I Could Reach Out Through Your Tv and Strangle You I Would': 'If I Could Reach Out Through Your TV and '
                                                                             'Strangle You, I Would',
            'Live and Smokin’': 'Live & Smokin’',
            '…here and Now': 'Here and Now',
            'Again!*': 'Again!',
            'Louis C.k.': 'Louis C.K.',
            'Smart and Classy': 'Smart & Classy',
            'Oh Come On': 'Oh, Come On',
            'Comin’ in Hot': 'Comin’ In Hot',
            'This is Me Now': "This Is Me Now",
            'Jesus is Magic': 'Jesus Is Magic',
            "Frankie Boyle Live 2": "Frankie Boyle",
            "Patrice O’neal": "Patrice O’Neal"
        }

        # try to return title mapping. if doesn't exist, return text as is
        try:
            formatted.append(custom_mappings[text])
        except KeyError:
            formatted.append(text)

    return formatted[0], formatted[1]


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
    removal_flags = [
        # Foreign Language
        'Testo italiano completo', 'Trascrizione italiana', 'Traduzione italiana', 'Transcripción completa',
        # SNL / Monologues
        'Monologue', 'MONOLOGUE',
        # Not available as a film
        'Always Be Late', 'Dumb Americans', 'THE PHILADELPHIA INCIDENT', 'THE BERKELEY CONCERT',
        'LIVE AT THE O2 LONDON', 'LIVE AT LAFF STOP']

    if (regex_matches is None) or any(word in raw_title for word in removal_flags):
        return None
    else:
        comedian = string.capwords(regex_matches.group(1))
        title = string.capwords(regex_matches.group(2))
        comedian, title = custom_format(comedian, title)                       # to handle special cases and errors

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
