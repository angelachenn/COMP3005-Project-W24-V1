import json
from db import DB

class Loader:
  # The competitions we care about
  COMPETITIONS = [
      (11, 90), # ("La Liga", "2020/2021")
      (11, 42), # ("La Liga", "2019/2020")
      (11, 4), # ("La Liga", "2018/2019")
      (2, 44) #("Premier League", "2003/2004")
    ]
  
  # The IDs of La Liga and Premier League
  COMPETITION_IDS = [11, 2]
    
  def __init__(self, cur, conn, data_path):
    self.db = DB(cur, conn)
    self.data_path = data_path
  
  def load(self):
    self.db.setup_tables()
    print("Successfully created tables.")
    self.__load_competitions()
    print("Successfully loaded competitions.")
    self.__load_matches()
    print("Successfully loaded matches.")
    self.__load_lineups()
    print("Successfully loaded lineups.")
    self.__load_events()
    print("Successfully loaded events.")

  # Private functions
  def __load_competitions(self):
    competitions = json.loads(open(f"{self.data_path}/competitions.json").read())
    for competition in competitions:
      if (competition["competition_id"], competition["season_id"]) in Loader.COMPETITIONS:
        self.db.insert_competition(competition)

  def __load_matches(self):
    for competition in Loader.COMPETITIONS:
      comp_id, season_id = competition
      matches = json.loads(open(f"{self.data_path}/matches/{comp_id}/{season_id}.json").read())
      
      for match in matches:
        try:
          home_team_manager = match["home_team"]["managers"][0]
          away_team_manager = match["away_team"]["managers"][0]
          self.db.insert_manager(home_team_manager)
          self.db.insert_manager(away_team_manager)
        except KeyError:
          continue

        home_team = {k.removeprefix("home_"): v for k, v in match["home_team"].items()}
        away_team = {k.removeprefix("away_"): v for k, v in match["away_team"].items()}
        
        home_team = self.__parse_team(home_team, home_team_manager["id"])
        self.db.insert_team(home_team)
        away_team = self.__parse_team(away_team, away_team_manager["id"])
        self.db.insert_team(away_team)

        match.update(match["home_team"])
        match.update(match["away_team"])

        match["competition_id"], match["season_id"] = competition
        self.db.insert_match(match)

  def __load_lineups(self):
    matches = self.db.get_matches()
    
    for match in matches:
      lineups = json.loads(open(f"{self.data_path}/lineups/{match[0]}.json").read())
      
      for lineup in lineups:
        lineup["match_id"] = match[0]
        self.db.insert_lineup(lineup)

  def __load_events(self):
    matches = self.db.get_matches()
    
    for match in matches:
      events = json.loads(open(f"{self.data_path}/events/{match[0]}.json").read())
      
      for event in events:
        if "player" in event:
          self.db.insert_player(event["player"])
          
        event["match_id"] = match[0]
        event = self.__parse_event(event)
        self.db.insert_event(event)
        
        if "tactics" in event:
          tactic = event["tactics"]
          tactic["event_id"] = event["id"]
          self.db.insert_tactic(tactic)

        match (event["type"]["name"]):
          case "Pass":
            pass_type = event["pass"]
            pass_type["event_id"] = event["id"]
            
            if "recipient" in pass_type:
              self.db.insert_player(pass_type["recipient"])
            
            pass_type = self.__parse_pass(pass_type)
            self.db.insert_pass(pass_type)
            
          case "Shot":
            shot_type = event["shot"]
            shot_type["event_id"] = event["id"]
            shot_type = self.__parse_shot(shot_type)
            self.db.insert_shot(shot_type)
            
          case "Interception":
            interception_type = event["interception"]
            interception_type["event_id"] = event["id"]
            interception_type["outcome_id"], interception_type["outcome_name"] = tuple(interception_type["outcome"].values())
            self.db.insert_interception(interception_type)
            
          case "Dribble":
            dribble_type = event["dribble"]
            dribble_type["event_id"] = event["id"]
            dribble_type["outcome_id"], dribble_type["outcome_name"] = tuple(dribble_type["outcome"].values())
            self.db.insert_dribble(dribble_type)
            
          case _:
            pass
  
  def __parse_team(self, team, manager_id):
    team.pop("managers")
    team["manager_id"] = manager_id
    return team
  
  def __parse_event(self, event):
    event["player_id"] = event.get("player", {}).get("id")

    type = event.get("type")
    event["type_id"] = type["id"]
    self.db.insert_types(type)

    play_pattern = event.get("play_pattern")
    event["play_pattern_id"] = play_pattern["id"]
    self.db.insert_play_patterns(play_pattern)

    if "position" in event:
      position = event.get("position")
      event["position_id"] = position["id"]
      self.db.insert_positions(position)

    event["possession_team_id"] = event.get("possession_team", {}).get("id")
    event["team_id"] = event.get("team", {}).get("id")
    event["location_x"], event["location_y"] = event.get("location", [None, None])[:2]
    event_type = event.get("type", {}).get("name")
    type = "goalkeeper" if event_type == "Goalkeeper" else event_type.replace(" ", "_").lower()
    
    if event["type"]["name"] not in ["Shot", "Pass", "Dribble", "Interception"]:
      event["type_metadata"] = str(event.get(type) or "")
    
    return event
  
  def __parse_pass(self, pass_type):
    pass_type["recipient_id"] = pass_type.get("recipient", {}).get("id")
    pass_type["end_location_x"], pass_type["end_location_y"] = pass_type.get("end_location", [None, None])[:2]
    pass_type["pass_cross"] = pass_type.get("cross")
    pass_type["type_id"], pass_type["type_name"] = tuple(
      pass_type.get("type", {}).get(key) 
      for key in ("id", "name")
    )
    pass_type["outcome_id"], pass_type["outcome_name"] = tuple(
      pass_type.get("outcome", {}).get(key) 
      for key in ("id", "name")
    )
    
    return pass_type

  def __parse_shot(self, shot_type):
    shot_type["xg"] = shot_type["statsbomb_xg"]
    shot_type["end_location_x"], shot_type["end_location_y"] = shot_type.get("end_location", [None, None])[:2] 
    shot_type["type_id"], shot_type["type_name"] = tuple(
      shot_type.get("type", {}).get(key) 
      for key in ("id", "name")
    )
    shot_type["outcome_id"], shot_type["outcome_name"] = tuple(
      shot_type.get("outcome", {}).get(key) 
      for key in ("id", "name")
    )
    
    return shot_type

