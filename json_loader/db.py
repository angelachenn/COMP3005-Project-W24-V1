from columns import COLS

class DB:
  def __init__(self, cur, conn):
    self.cur = cur 
    self.conn = conn
    
  def setup_tables(self):
    self.__drop_tables()
    self.__create_tables()
    self.__optimize_tables()
    
  def insert_competition(self, val): self.__insert(val, "competitions")
  def insert_team(self, val): self.__insert(val, "teams", True, "team_id")
  def insert_player(self, val): self.__insert(val, "players", True, "id")
  def insert_match(self, val): self.__insert(val, "matches")
  def insert_event(self, val): self.__insert(val, "events")
  def insert_pass(self, val): self.__insert(val, "passes")
  def insert_shot(self, val): self.__insert(val, "shots")
  def insert_interception(self, val): self.__insert(val, "interceptions")
  def insert_dribble(self, val): self.__insert(val, "dribbles")
  
  def get_matches(self):
    self.cur.execute('SELECT match_id FROM matches;')
    return self.cur.fetchall()
  
  # Private methods
  def __insert(self, val, type, conflict=False, conflict_id=None):
    query = f"""INSERT INTO {type} ({', '.join(COLS[type])}) 
              VALUES ({', '.join(['%s'] * len(COLS[type]))}) 
              {f"ON CONFLICT ({conflict_id}) DO NOTHING" if conflict else ""};
            """
    self.cur.execute(query, self.__format_entry(val, COLS[type]))
    self.conn.commit()
    
  def __format_entry(self, data, cols):
    return tuple(
      str(data[col]) if col in data and isinstance(data[col], (list, dict)) else data.get(col)
      for col in cols
    )

  def __drop_tables(self):
    self.cur.execute(f"DROP TABLE IF EXISTS dribbles;")
    self.cur.execute(f"DROP TABLE IF EXISTS interceptions;")
    self.cur.execute(f"DROP TABLE IF EXISTS shots;")
    self.cur.execute(f"DROP TABLE IF EXISTS passes;")
    self.cur.execute(f"DROP TABLE IF EXISTS events;")
    self.cur.execute(f"DROP TABLE IF EXISTS matches;")
    self.cur.execute(f"DROP TABLE IF EXISTS competitions;")
    self.cur.execute(f"DROP TABLE IF EXISTS teams;")
    self.cur.execute(f"DROP TABLE IF EXISTS players;")
    self.conn.commit() 

  def __create_tables(self):
    self.cur.execute("""CREATE TABLE IF NOT EXISTS competitions (
      competition_id SMALLINT NOT NULL,
      season_id SMALLINT NOT NULL,
      country_name TEXT NOT NULL,
      competition_name TEXT NOT NULL,
      competition_gender TEXT NOT NULL,
      season_name TEXT NOT NULL,
      competition_international BOOL NOT NULL,
      competition_youth BOOL NOT NULL,
      PRIMARY KEY (competition_id, season_id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS teams (
      team_id SMALLINT NOT NULL PRIMARY KEY,
      team_name TEXT NOT NULL,
      team_gender TEXT NOT NULL,
      team_group TEXT,
      country TEXT NOT NULL,
      managers TEXT);"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS matches (
      match_id INT NOT NULL PRIMARY KEY,
      match_date DATE NOT NULL,
      kick_off TIME NOT NULL,
      competition_id SMALLINT NOT NULL,
      season_id SMALLINT NOT NULL,
      home_score SMALLINT NOT NULL,
      away_score SMALLINT NOT NULL,
      match_status TEXT NOT NULL,
      home_team_id SMALLINT NOT NULL,
      away_team_id SMALLINT NOT NULL,
      match_week SMALLINT NOT NULL,
      competition_stage TEXT NOT NULL,
      stadium TEXT,
      referee TEXT,
      FOREIGN KEY (competition_id, season_id) REFERENCES competitions(competition_id, season_id),
      FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
      FOREIGN KEY (away_team_id) REFERENCES teams(team_id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS players (
      id INT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS events (
      id TEXT NOT NULL PRIMARY KEY,
      index SMALLINT NOT NULL,
      period SMALLINT NOT NULL,
      timestamp TIME NOT NULL,
      minute SMALLINT NOT NULL,
      second SMALLINT NOT NULL,
      type_id SMALLINT NOT NULL,
      type_name TEXT NOT NULL,
      possession SMALLINT NOT NULL,
      play_pattern TEXT NOT NULL,
      location_x FLOAT,
      location_y FLOAT,
      position TEXT,
      duration FLOAT,
      under_pressure BOOL,
      off_camera BOOL,
      out BOOL,
      tactics TEXT,
      type_metadata TEXT,
      possession_team_id SMALLINT NOT NULL,
      team_id SMALLINT NOT NULL,
      player_id INT,
      match_id INT NOT NULL,
      FOREIGN KEY (possession_team_id) REFERENCES teams(team_id),
      FOREIGN KEY (team_id) REFERENCES teams(team_id),
      FOREIGN KEY (player_id) REFERENCES players(id),
      FOREIGN KEY (match_id) REFERENCES matches(match_id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS passes (
      event_id TEXT NOT NULL PRIMARY KEY,
      recipient_id INT,
      length FLOAT NOT NULL,
      angle FLOAT NOT NULL,
      height TEXT NOT NULL,
      end_location_x FLOAT NOT NULL,
      end_location_y FLOAT NOT NULL,
      type_id SMALLINT,
      type_name TEXT,
      body_part TEXT,
      outcome_id SMALLINT,
      outcome_name TEXT,
      aerial_won BOOL,
      switch BOOL,
      technique TEXT,
      through_ball BOOL,
      deflected BOOL,
      pass_cross BOOL,
      outswinging BOOL,
      assisted_shot_id TEXT,
      shot_assist BOOL,
      no_touch BOOL,
      cut_back BOOL,
      inswinging BOOL,
      straight BOOL,
      goal_assist BOOL,
      miscommunication BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id),
      FOREIGN KEY (recipient_id) REFERENCES players(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS interceptions (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT NOT NULL,
      outcome_name TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS dribbles (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT NOT NULL,
      outcome_name TEXT NOT NULL,
      overrun BOOL,
      nutmeg BOOL,
      no_touch BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS shots (
      event_id TEXT NOT NULL PRIMARY KEY,
      xg FLOAT NOT NULL,
      end_location_x FLOAT NOT NULL,
      end_location_y FLOAT NOT NULL,
      key_pass_id TEXT,
      technique TEXT NOT NULL,
      body_part TEXT NOT NULL,
      type_id SMALLINT NOT NULL,
      type_name TEXT NOT NULL,
      outcome_id SMALLINT NOT NULL,
      outcome_name TEXT NOT NULL,
      freeze_frame TEXT,
      first_time BOOL,
      open_goal BOOL,
      aerial_won BOOL,
      deflected BOOL,
      saved_off_target BOOL,
      saved_to_post BOOL,
      redirect BOOL,
      follows_dribble BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.conn.commit()
    
  def __optimize_tables(self):
    # Create indices on foreign keys
    self.cur.execute("""
      CREATE INDEX idx_competition ON matches(competition_id, season_id);
      CREATE INDEX idx_home_team_id ON matches(home_team_id);
      CREATE INDEX idx_away_team_id ON matches(away_team_id);

      CREATE INDEX idx_possession_team_id ON events(possession_team_id);
      CREATE INDEX idx_event_team_id ON events(team_id);
      CREATE INDEX idx_player_id ON events(player_id);
      CREATE INDEX idx_match_id ON events(match_id);

      CREATE INDEX idx_recipient_id ON passes(recipient_id);
      CREATE INDEX idx_event_id_passes ON passes(event_id);

      CREATE INDEX idx_event_id_interceptions ON interceptions(event_id);

      CREATE INDEX idx_event_id_dribbles ON dribbles(event_id);

      CREATE INDEX idx_event_id_shots ON shots(event_id);
      """)
    
    self.conn.commit()
