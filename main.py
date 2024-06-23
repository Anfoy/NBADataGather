from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
import pandas as pd
import time
from datetime import datetime, timedelta
import os
import requests

# Path to the CSV file
CSV_FILE_PATH = 'FifteenYearsNBAMatchups.csv'

# Function to load the last game date from the CSV file
def load_last_game_date():
    if os.path.exists(CSV_FILE_PATH):
        existing_data = pd.read_csv(CSV_FILE_PATH)
        existing_data['GAME_DATE'] = pd.to_datetime(existing_data['GAME_DATE'])
        if not existing_data.empty:
            return existing_data['GAME_DATE'].max()
    return None

class Game:
    def __init__(self, game_id, game_date, home_team, away_team, game_type, overtime, home_score, away_score, home_starters, away_starters, home_bench, away_bench, home_starters_stats, away_starters_stats, home_bench_stats, away_bench_stats):
        self.game_id = game_id
        self.game_date = game_date
        self.home_team = home_team
        self.away_team = away_team
        self.game_type = game_type
        self.overtime = overtime
        self.home_score = home_score
        self.away_score = away_score
        self.home_starters = home_starters
        self.away_starters = away_starters
        self.home_bench = home_bench
        self.away_bench = away_bench
        self.home_starters_stats = home_starters_stats
        self.away_starters_stats = away_starters_stats
        self.home_bench_stats = home_bench_stats
        self.away_bench_stats = away_bench_stats

    def to_dict(self):
        return {
            'GAME_ID': self.game_id,
            'GAME_DATE': self.game_date,
            'HOME_TEAM': self.home_team,
            'AWAY_TEAM': self.away_team,
            'GAME_TYPE': self.game_type,
            'OVERTIME': self.overtime,
            'HOME_TEAM_SCORE': self.home_score,
            'AWAY_TEAM_SCORE': self.away_score,
            'HOME_STARTERS': ', '.join(self.home_starters),
            'AWAY_STARTERS': ', '.join(self.away_starters),
            'HOME_BENCH': ', '.join(self.home_bench),
            'AWAY_BENCH': ', '.join(self.away_bench),
            'HOME_STARTERS_STATS': str(self.home_starters_stats),
            'AWAY_STARTERS_STATS': str(self.away_starters_stats),
            'HOME_BENCH_STATS': str(self.home_bench_stats),
            'AWAY_BENCH_STATS': str(self.away_bench_stats)
        }

    def to_display_dict(self):
        game_dict = self.to_dict()
        del game_dict['GAME_ID']  # Remove GAME_ID for display purposes
        return game_dict

# Function to fetch data for a given date
def fetch_data_for_date(game_date):
    url = f"https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed?gamedate={game_date}&platform=web"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Ocp-Apim-Subscription-Key": "747fa6900c6c4e89a58b81b72f36eb96",
        "Referer": "https://www.nba.com/",
        "Sec-Ch-Ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Origin": "https://www.nba.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for date {game_date}: {e}")
        return None

# Function to extract game info
def extract_game_info(data, game_date):
    game_info = {}
    for module in data.get("modules", []):
        for card in module.get("cards", []):
            card_data = card.get("cardData", {})
            game_id = card_data.get("gameId", "")
            season_type = card_data.get("seasonType", "Regular Season")

            # Check gameDetailsHeader to determine if it is an In-Season Tournament or Play-In game
            game_details_header = card_data.get("gameDetailsHeader", {})
            non_spoiler_header = game_details_header.get("nonSpoilerHeader", "")
            if "In-Season Tournament" in non_spoiler_header:
                game_type = "In-Season Tournament"
            elif "Play-In" in non_spoiler_header:
                game_type = "Play-In"
            else:
                game_type = season_type

            game_info[game_id] = {
                "seasonType": game_type,
                "gameDate": game_date
            }
    return game_info


# Function to get all games for a specific season with validation
def get_all_games_for_season(season, retries=5, timeout=30):
    all_games = pd.DataFrame()
    for attempt in range(retries):
        try:
            # Fetch games for the given season
            gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, timeout=timeout)
            games = gamefinder.get_data_frames()[0]

            # Check if the DataFrame is empty
            if games.empty:
                print(f"No games found for the season {season}.")
                return games

            # Filter out games that are not part of the regular season or playoffs
            games = games[games.GAME_ID.str.startswith(('002', '004', '005', '006'))]  # Regular season and playoffs
            return games
        except Exception as e:
            print(f"Error fetching games for the season {season}: {e}")
            time.sleep(5 * (2 ** attempt))  # Exponential backoff
    return pd.DataFrame()

# Get all NBA teams
nba_teams = teams.get_teams()

# Load the last game date
last_game_date = load_last_game_date()
if last_game_date:
    print(f"Last game date: {last_game_date}")
    last_game_year = last_game_date.year
else:
    print("No previous game data found.")
    last_game_date = datetime.strptime('2009-01-01', '%Y-%m-%d')
    last_game_year = 2009

# Determine the last completed season
current_year = datetime.now().year
current_month = datetime.now().month

if current_month < 10:  # If before October, last completed season is the previous year
    last_completed_season_year = current_year - 1
else:  # If after September, last completed season is the current year
    last_completed_season_year = current_year

# Define the season range up to the last completed season
season_start_year = last_game_year
seasons = [f'{year}-{str(year+1)[-2:]}' for year in range(season_start_year, last_completed_season_year + 1)]

# Retrieve all games for the specified seasons for all teams
all_games = pd.DataFrame()
for season in seasons:
    team_games = get_all_games_for_season(season)
    if team_games.empty:
        print(f"Skipping season {season} as no data is available.")
    else:
        all_games = pd.concat([all_games, team_games])

# Check if 'GAME_DATE' exists before proceeding
if 'GAME_DATE' not in all_games.columns:
    print("No 'GAME_DATE' column found. Exiting.")
    exit()

# Filter games by the last game date
all_games['GAME_DATE'] = pd.to_datetime(all_games['GAME_DATE'])
recent_games = all_games[all_games['GAME_DATE'] > last_game_date].copy()

recent_games.drop_duplicates(subset=['GAME_ID'], inplace=True)

if recent_games.empty:
    print("No new games found since the last run.")
    exit()  # Exit the program if no new games are found
else:
    print(f"Total new games found: {len(recent_games)}")

# Determine home and away teams
recent_games['HOME_TEAM'] = recent_games['MATCHUP'].apply(lambda x: x.split(' vs. ')[0] if 'vs.' in x else x.split(' @ ')[1])
recent_games['AWAY_TEAM'] = recent_games['MATCHUP'].apply(lambda x: x.split(' @ ')[0] if '@' in x else x.split(' vs. ')[1])

# Extract home and away scores
def get_scores(row):
    if 'vs.' in row['MATCHUP']:  # Home team
        home_score = int(row['PTS'])
        away_score = int(row['PTS'] - row['PLUS_MINUS'])
    else:  # Away team
        away_score = int(row['PTS'])
        home_score = int(row['PTS'] - row['PLUS_MINUS'])
    return home_score, away_score

# Add score columns
recent_games['HOME_TEAM_SCORE'], recent_games['AWAY_TEAM_SCORE'] = zip(*recent_games.apply(get_scores, axis=1))

# Initialize columns for starters and bench players stats
columns_to_add = ['OVERTIME', 'HOME_STARTERS', 'AWAY_STARTERS', 'HOME_BENCH', 'AWAY_BENCH',
                  'HOME_STARTERS_STATS', 'AWAY_STARTERS_STATS', 'HOME_BENCH_STATS', 'AWAY_BENCH_STATS']
for col in columns_to_add:
    recent_games[col] = ""

# Function to get player data with retries and batching
def get_player_data(game_ids, retries=5, timeout=30):
    all_player_stats = {}
    failed_game_ids = []
    total_games = len(game_ids)
    for i, game_id in enumerate(game_ids, start=1):
        for attempt in range(retries):
            try:
                boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=timeout)
                player_stats = boxscore.get_data_frames()[0]
                all_player_stats[game_id] = player_stats
                break  # Exit retry loop on success
            except Exception as e:
                time.sleep(5 * (2 ** attempt))  # Exponential backoff
        else:
            print(f"Failed to retrieve player data for game {game_id} after {retries} attempts")
            failed_game_ids.append(game_id)  # Log failure
            all_player_stats[game_id] = None  # Log failure
    return all_player_stats, failed_game_ids

# Get player data for all games
game_ids = recent_games['GAME_ID'].unique()
player_data, failed_game_ids = get_player_data(game_ids)

# Retry fetching data for failed game IDs
if failed_game_ids:
    print(f"Retrying failed game IDs: {failed_game_ids}")
    retry_player_data, retry_failed_game_ids = get_player_data(failed_game_ids)
    player_data.update(retry_player_data)
    failed_game_ids = retry_failed_game_ids

if failed_game_ids:
    print(f"Final failed game IDs: {failed_game_ids}")

# Function to get game types from JSON API
def get_game_types(dates):
    game_types = {}
    for game_date in dates:
        game_date_str = game_date.strftime("%m/%d/%Y")
        data = fetch_data_for_date(game_date_str)
        if data:
            game_info = extract_game_info(data, game_date_str)
            game_types.update(game_info)
    return game_types

# Get game types for all unique game dates
unique_dates = recent_games['GAME_DATE'].unique()
game_types = get_game_types(unique_dates)

# Process player data
games = []
for index, row in recent_games.iterrows():
    game_id = row['GAME_ID']
    player_stats = player_data.get(game_id)
    if player_stats is not None:
        # Filter starters (those who started the game)
        starters = player_stats[player_stats['START_POSITION'] != ''].copy()
        bench = player_stats[player_stats['START_POSITION'] == ''].copy()

        # Get home and away starters
        home_starters = starters[starters['TEAM_ABBREVIATION'] == row['HOME_TEAM']]['PLAYER_NAME'].tolist()
        away_starters = starters[starters['TEAM_ABBREVIATION'] == row['AWAY_TEAM']]['PLAYER_NAME'].tolist()

        # Get home and away bench players
        home_bench = bench[bench['TEAM_ABBREVIATION'] == row['HOME_TEAM']]['PLAYER_NAME'].tolist()
        away_bench = bench[bench['TEAM_ABBREVIATION'] == row['AWAY_TEAM']]['PLAYER_NAME'].tolist()

        # Get stats for home and away starters
        home_starters_stats = starters[starters['TEAM_ABBREVIATION'] == row['HOME_TEAM']][['PLAYER_NAME', 'PTS', 'REB', 'AST', 'MIN']].to_dict('records')
        away_starters_stats = starters[starters['TEAM_ABBREVIATION'] == row['AWAY_TEAM']][['PLAYER_NAME', 'PTS', 'REB', 'AST', 'MIN']].to_dict('records')

        # Get stats for home and away bench players
        home_bench_stats = bench[bench['TEAM_ABBREVIATION'] == row['HOME_TEAM']][['PLAYER_NAME', 'PTS', 'REB', 'AST', 'MIN']].to_dict('records')
        away_bench_stats = bench[bench['TEAM_ABBREVIATION'] == row['AWAY_TEAM']][['PLAYER_NAME', 'PTS', 'REB', 'AST', 'MIN']].to_dict('records')

        # Determine game type
        game_type_info = game_types.get(game_id, {})
        game_type = game_type_info.get("seasonType", "Regular Season")

        overtime = "Yes" if row['MIN'] > 260 else "No"

        # Create a Game object
        game = Game(
            game_id=game_id,
            game_date=row['GAME_DATE'],
            home_team=row['HOME_TEAM'],
            away_team=row['AWAY_TEAM'],
            game_type=game_type,
            overtime=overtime,
            home_score=row['HOME_TEAM_SCORE'],
            away_score=row['AWAY_TEAM_SCORE'],
            home_starters=home_starters,
            away_starters=away_starters,
            home_bench=home_bench,
            away_bench=away_bench,
            home_starters_stats=home_starters_stats,
            away_starters_stats=away_starters_stats,
            home_bench_stats=home_bench_stats,
            away_bench_stats=away_bench_stats
        )
        games.append(game)
    else:
        print(f"No player data available for game {game_id}")

# Sort games by game date
sorted_games = sorted(games, key=lambda x: x.game_date)

# Convert sorted games to a DataFrame
final_matchups = pd.DataFrame([game.to_display_dict() for game in sorted_games])

# Append the new games to the existing CSV file or create a new file if it doesn't exist
if os.path.exists(CSV_FILE_PATH):
    existing_data = pd.read_csv(CSV_FILE_PATH)
    existing_data['GAME_DATE'] = pd.to_datetime(existing_data['GAME_DATE'])
    combined_data = pd.concat([existing_data, final_matchups])
    combined_data.drop_duplicates(inplace=True)
    combined_data.to_csv(CSV_FILE_PATH, index=False)
else:
    final_matchups.to_csv(CSV_FILE_PATH, index=False)

print("Process Completed")
