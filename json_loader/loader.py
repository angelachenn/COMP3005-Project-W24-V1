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
  
  # The IDs of La Liga and Premier Leage
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
        self.db.insert_team({k.removeprefix("home_"): v for k, v in match["home_team"].items()})
        self.db.insert_team({k.removeprefix("away_"): v for k, v in match["away_team"].items()})
        
        match.update(match["home_team"])
        match.update(match["away_team"])
        match["competition_id"], match["season_id"] = competition

        self.db.insert_match(match)

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

  def __parse_event(self, event):
    event["player_id"] = event.get("player", {}).get("id")
    event["type_id"], event["type_name"] = tuple(
      event.get("type", {}).get(key) 
      for key in ("id", "name")
    )
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

