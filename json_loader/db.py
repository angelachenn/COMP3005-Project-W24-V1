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
  def insert_manager(self, val): self.__insert(val, "managers", True, "id")
  def insert_team(self, val): self.__insert(val, "teams", True, "team_id")
  def insert_player(self, val): self.__insert(val, "players", True, "id")
  def insert_match(self, val): self.__insert(val, "matches")
  def insert_lineup(self, val): self.__insert(val, "lineups")
  def insert_event(self, val): self.__insert(val, "events")
  def insert_types(self, val): self.__insert(val, "types", True, "id")
  def insert_positions(self, val): self.__insert(val, "positions", True, "id")
  def insert_play_patterns(self, val): self.__insert(val, "play_patterns", True, "id")
  def insert_tactic(self, val): self.__insert(val, "tactics")
  def insert_pass(self, val): self.__insert(val, "passes")
  def insert_shot(self, val): self.__insert(val, "shots")
  def insert_interception(self, val): self.__insert(val, "interceptions")
  def insert_dribble(self, val): self.__insert(val, "dribbles")
  def insert_half_start(self, val): self.__insert(val, "half_starts")
  def insert_carry(self, val): self.__insert(val, "carries")
  def insert_ball_recovery(self, val): self.__insert(val, "ball_recoveries")
  def insert_block(self, val): self.__insert(val, "blocks")
  def insert_miscontrol(self, val): self.__insert(val, "miscontrols")
  def insert_foul_comitted(self, val): self.__insert(val, "fouls_committed")
  def insert_foul_won(self, val): self.__insert(val, "fouls_won")
  def insert_duel(self, val): self.__insert(val, "duels")
  def insert_clearance(self, val): self.__insert(val, "clearances")
  def insert_injury_stoppage(self, val): self.__insert(val, "injury_stoppages")
  def insert_bad_behavior(self, val): self.__insert(val, "bad_behaviors")
  def insert_substitution(self, val): self.__insert(val, "substitutions")
  def insert_ball_receipt(self, val): self.__insert(val, "ball_receipts")
  def insert_fifty_fifty(self, val): self.__insert(val, "fifty_fifties")
  def insert_goalkeeper(self, val): self.__insert(val, "goalkeepers")
  
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
    self.cur.execute(f"DROP TABLE IF EXISTS substitutions;")
    self.cur.execute(f"DROP TABLE IF EXISTS bad_behaviors;")
    self.cur.execute(f"DROP TABLE IF EXISTS injury_stoppages;")
    self.cur.execute(f"DROP TABLE IF EXISTS clearances;")
    self.cur.execute(f"DROP TABLE IF EXISTS duels;")
    self.cur.execute(f"DROP TABLE IF EXISTS fouls_won;")
    self.cur.execute(f"DROP TABLE IF EXISTS fouls_committed;")
    self.cur.execute(f"DROP TABLE IF EXISTS foul_wons;")
    self.cur.execute(f"DROP TABLE IF EXISTS foul_committeds;")
    self.cur.execute(f"DROP TABLE IF EXISTS miscontrols;")
    self.cur.execute(f"DROP TABLE IF EXISTS blocks;")
    self.cur.execute(f"DROP TABLE IF EXISTS ball_recoveries;")
    self.cur.execute(f"DROP TABLE IF EXISTS carries;")
    self.cur.execute(f"DROP TABLE IF EXISTS half_starts;")
    self.cur.execute(f"DROP TABLE IF EXISTS dribbles;")
    self.cur.execute(f"DROP TABLE IF EXISTS interceptions;")
    self.cur.execute(f"DROP TABLE IF EXISTS shots;")
    self.cur.execute(f"DROP TABLE IF EXISTS passes;")
    self.cur.execute(f"DROP TABLE IF EXISTS tactics;")
    self.cur.execute(f"DROP TABLE IF EXISTS lineups;")
    self.cur.execute(f"DROP TABLE IF EXISTS events;")
    self.cur.execute(f"DROP TABLE IF EXISTS types;")
    self.cur.execute(f"DROP TABLE IF EXISTS play_patterns;")
    self.cur.execute(f"DROP TABLE IF EXISTS positions;")
    self.cur.execute(f"DROP TABLE IF EXISTS matches;")
    self.cur.execute(f"DROP TABLE IF EXISTS competitions;")
    self.cur.execute(f"DROP TABLE IF EXISTS teams;")
    self.cur.execute(f"DROP TABLE IF EXISTS managers;")
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
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS managers (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL,
      nickname TEXT,
      dob DATE,
      country TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS teams (
      team_id SMALLINT NOT NULL PRIMARY KEY,
      team_name TEXT NOT NULL,
      team_gender TEXT NOT NULL,
      team_group TEXT,
      country TEXT NOT NULL,
      manager_id SMALLINT,
      FOREIGN KEY (manager_id) REFERENCES managers(id));"""
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

    self.cur.execute("""CREATE TABLE IF NOT EXISTS lineups (
      match_id INT NOT NULL,
      team_id SMALLINT NOT NULL,
      lineup TEXT NOT NULL,
      PRIMARY KEY (match_id, team_id),
      FOREIGN KEY (match_id) REFERENCES matches(match_id),
      FOREIGN KEY (team_id) REFERENCES teams(team_id));"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS players (
      id INT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS types (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS positions (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS play_patterns (
      id SMALLINT NOT NULL PRIMARY KEY,
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
      possession SMALLINT NOT NULL,
      play_pattern_id SMALLINT NOT NULL,
      location_x FLOAT,
      location_y FLOAT,
      position_id SMALLINT,
      duration FLOAT,
      under_pressure BOOL,
      off_camera BOOL,
      counterpress BOOL,
      out BOOL,
      tactics TEXT,
      possession_team_id SMALLINT NOT NULL,
      team_id SMALLINT NOT NULL,
      player_id INT,
      match_id INT NOT NULL,
      FOREIGN KEY (type_id) REFERENCES types(id),
      FOREIGN KEY (play_pattern_id) REFERENCES play_patterns(id),
      FOREIGN KEY (position_id) REFERENCES positions(id),
      FOREIGN KEY (possession_team_id) REFERENCES teams(team_id),
      FOREIGN KEY (team_id) REFERENCES teams(team_id),
      FOREIGN KEY (player_id) REFERENCES players(id),
      FOREIGN KEY (match_id) REFERENCES matches(match_id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS tactics (
      event_id TEXT NOT NULL PRIMARY KEY,
      formation INT NOT NULL,
      lineup TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
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
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS half_starts (
      event_id TEXT NOT NULL PRIMARY KEY,
      late_video_start BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS carries (
      event_id TEXT NOT NULL PRIMARY KEY,
      end_location TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS ball_recoveries (
      event_id TEXT NOT NULL PRIMARY KEY,
      offensive BOOL,
      recovery_failure BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS blocks (
      event_id TEXT NOT NULL PRIMARY KEY,
      deflection BOOL,
      save_block BOOL,
      offensive BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS miscontrols (
      event_id TEXT NOT NULL PRIMARY KEY,
      aerial_won BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS fouls_committed (
      event_id TEXT NOT NULL PRIMARY KEY,
      card TEXT NOT NULL,
      type TEXT NOT NULL,
      penalty BOOL,
      advantage BOOL,
      offensive BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS fouls_won (
      event_id TEXT NOT NULL PRIMARY KEY,
      penalty BOOL,
      advantage BOOL,
      defensive BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS duels (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome TEXT NOT NULL,
      type TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS clearances (
      event_id TEXT NOT NULL PRIMARY KEY,
      body_part TEXT NOT NULL,
      aerial_won BOOL,
      left_foot BOOL,
      right_foot BOOL,
      head BOOL,
      other BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS injury_stoppages (
      event_id TEXT NOT NULL PRIMARY KEY,
      in_chain BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS bad_behaviors (
      event_id TEXT NOT NULL PRIMARY KEY,
      card TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS substitutions (
      event_id TEXT NOT NULL PRIMARY KEY,
      replacement_id INT NOT NULL,
      outcome TEXT NOT NULL,
      FOREIGN KEY (replacement_id) REFERENCES players(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS ball_receipts (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS fifty_fifties (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS goalkeepers (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome TEXT NOT NULL,
      shot_saved_off_target BOOL,
      position TEXT,
      body_part TEXT,
      shot_saved_to_post BOOL,
      technique TEXT,
      lost_out BOOL,
      lost_in_play BOOL,
      success_in_play BOOL,
      type TEXT,
      end_location TEXT,
      punched_out BOOL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.conn.commit()
    
  def __optimize_tables(self):
    # Create indices on foreign keys
    self.cur.execute("""
      CREATE INDEX idx_manager_id ON teams(manager_id);
      
      CREATE INDEX idx_match_id_lineups ON lineups(match_id);
      CREATE INDEX idx_team_id_lineups ON lineups(team_id);

      CREATE INDEX idx_event_id_tactics ON tactics(event_id);

      CREATE INDEX idx_competition ON matches(competition_id, season_id);
      CREATE INDEX idx_home_team_id ON matches(home_team_id);
      CREATE INDEX idx_away_team_id ON matches(away_team_id);

      CREATE INDEX idx_type_id ON events(type_id);
      CREATE INDEX idx_play_pattern_id ON events(play_pattern_id);
      CREATE INDEX idx_position_id ON events(position_id);
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
