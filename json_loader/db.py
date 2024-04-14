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
  def insert_type(self, val): self.__insert(val, "types", True, "id")
  def insert_position(self, val): self.__insert(val, "positions", True, "id")
  def insert_play_pattern(self, val): self.__insert(val, "play_patterns", True, "id")
  def insert_outcome(self, val): self.__insert(val, "outcomes", True, "id")
  def insert_technique(self, val): self.__insert(val, "techniques", True, "id")
  def insert_body_part(self, val): self.__insert(val, "body_parts", True, "id")
  def insert_card(self, val): self.__insert(val, "cards", True, "id")
  def insert_height(self, val): self.__insert(val, "heights", True, "id")
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
  def insert_foul_committed(self, val): self.__insert(val, "fouls_committed")
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
    non_null_cols = [col for col in COLS[type] if col in val and val[col] != None]
    query = f"""INSERT INTO {type} ({', '.join(non_null_cols)}) 
              VALUES ({', '.join(['%s'] * len(non_null_cols))}) 
              {f"ON CONFLICT ({conflict_id}) DO NOTHING" if conflict else ""};
            """
    self.cur.execute(query, self.__format_entry(val, non_null_cols))
    self.conn.commit()
    
  def __format_entry(self, data, cols):
    return tuple(
      str(data[col]) if col in data and isinstance(data[col], (list, dict)) else data.get(col)
      for col in cols
    )

  def __drop_tables(self):
    self.cur.execute(f"DROP TABLE IF EXISTS ball_receipts;")
    self.cur.execute(f"DROP TABLE IF EXISTS goalkeepers;")
    self.cur.execute(f"DROP TABLE IF EXISTS fifty_fifties;")
    self.cur.execute(f"DROP TABLE IF EXISTS substitutions;")
    self.cur.execute(f"DROP TABLE IF EXISTS bad_behaviors;")
    self.cur.execute(f"DROP TABLE IF EXISTS injury_stoppages;")
    self.cur.execute(f"DROP TABLE IF EXISTS clearances;")
    self.cur.execute(f"DROP TABLE IF EXISTS duels;")
    self.cur.execute(f"DROP TABLE IF EXISTS fouls_won;")
    self.cur.execute(f"DROP TABLE IF EXISTS fouls_committed;")
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
    self.cur.execute(f"DROP TABLE IF EXISTS outcomes;")
    self.cur.execute(f"DROP TABLE IF EXISTS techniques;")
    self.cur.execute(f"DROP TABLE IF EXISTS body_parts;")
    self.cur.execute(f"DROP TABLE IF EXISTS cards;")
    self.cur.execute(f"DROP TABLE IF EXISTS heights;")
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
      dob DATE NOT NULL,
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

    self.cur.execute("""CREATE TABLE IF NOT EXISTS heights (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS cards (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS body_parts (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS techniques (
      id SMALLINT NOT NULL PRIMARY KEY,
      name TEXT NOT NULL);"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS outcomes (
      id SMALLINT NOT NULL PRIMARY KEY,
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
      location_x FLOAT NOT NULL DEFAULT 0.0,
      location_y FLOAT NOT NULL DEFAULT 0.0,
      position_id SMALLINT,
      duration FLOAT NOT NULL DEFAULT 0.0,
      under_pressure BOOL NOT NULL DEFAULT FALSE,
      off_camera BOOL NOT NULL DEFAULT FALSE,
      counterpress BOOL NOT NULL DEFAULT FALSE,
      out BOOL NOT NULL DEFAULT FALSE,
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
      height_id SMALLINT NOT NULL,
      end_location_x FLOAT NOT NULL,
      end_location_y FLOAT NOT NULL,
      type_id SMALLINT,
      body_part_id SMALLINT,
      outcome_id SMALLINT,
      aerial_won BOOL NOT NULL DEFAULT FALSE,
      switch BOOL NOT NULL DEFAULT FALSE,
      technique_id SMALLINT,
      through_ball BOOL NOT NULL DEFAULT FALSE,
      deflected BOOL NOT NULL DEFAULT FALSE,
      pass_cross BOOL NOT NULL DEFAULT FALSE,
      outswinging BOOL NOT NULL DEFAULT FALSE,
      assisted_shot_id TEXT,
      shot_assist BOOL NOT NULL DEFAULT FALSE,
      no_touch BOOL NOT NULL DEFAULT FALSE,
      cut_back BOOL NOT NULL DEFAULT FALSE,
      inswinging BOOL NOT NULL DEFAULT FALSE,
      straight BOOL NOT NULL DEFAULT FALSE,
      goal_assist BOOL NOT NULL DEFAULT FALSE,
      miscommunication BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id),
      FOREIGN KEY (height_id) REFERENCES heights(id),
      FOREIGN KEY (type_id) REFERENCES types(id),
      FOREIGN KEY (body_part_id) REFERENCES body_parts(id),
      FOREIGN KEY (technique_id) REFERENCES techniques(id),
      FOREIGN KEY (recipient_id) REFERENCES players(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS interceptions (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id),
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS dribbles (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT NOT NULL,
      overrun BOOL NOT NULL DEFAULT FALSE,
      nutmeg BOOL NOT NULL DEFAULT FALSE,
      no_touch BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id),
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS shots (
      event_id TEXT NOT NULL PRIMARY KEY,
      xg FLOAT NOT NULL,
      end_location_x FLOAT NOT NULL,
      end_location_y FLOAT NOT NULL,
      end_location_z FLOAT NOT NULL DEFAULT 0.0,
      key_pass_id TEXT,
      technique_id SMALLINT NOT NULL,
      body_part_id SMALLINT NOT NULL,
      type_id SMALLINT NOT NULL,
      outcome_id SMALLINT NOT NULL,
      freeze_frame TEXT,
      first_time BOOL NOT NULL DEFAULT FALSE,
      open_goal BOOL NOT NULL DEFAULT FALSE,
      aerial_won BOOL NOT NULL DEFAULT FALSE,
      deflected BOOL NOT NULL DEFAULT FALSE,
      saved_off_target BOOL NOT NULL DEFAULT FALSE,
      saved_to_post BOOL NOT NULL DEFAULT FALSE,
      redirect BOOL NOT NULL DEFAULT FALSE,
      follows_dribble BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id),
      FOREIGN KEY (technique_id) REFERENCES techniques(id),
      FOREIGN KEY (body_part_id) REFERENCES body_parts(id),
      FOREIGN KEY (type_id) REFERENCES types(id),
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS half_starts (
      event_id TEXT NOT NULL PRIMARY KEY,
      late_video_start BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS carries (
      event_id TEXT NOT NULL PRIMARY KEY,
      end_location TEXT NOT NULL,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS ball_recoveries (
      event_id TEXT NOT NULL PRIMARY KEY,
      offensive BOOL NOT NULL DEFAULT FALSE,
      recovery_failure BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS blocks (
      event_id TEXT NOT NULL PRIMARY KEY,
      deflection BOOL NOT NULL DEFAULT FALSE,
      save_block BOOL NOT NULL DEFAULT FALSE,
      offensive BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS miscontrols (
      event_id TEXT NOT NULL PRIMARY KEY,
      aerial_won BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS fouls_committed (
      event_id TEXT NOT NULL PRIMARY KEY,
      card_id SMALLINT,
      type_id SMALLINT,
      penalty BOOL NOT NULL DEFAULT FALSE,
      advantage BOOL NOT NULL DEFAULT FALSE,
      offensive BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (card_id) REFERENCES cards(id),
      FOREIGN KEY (type_id) REFERENCES types(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )

    self.cur.execute("""CREATE TABLE IF NOT EXISTS fouls_won (
      event_id TEXT NOT NULL PRIMARY KEY,
      penalty BOOL NOT NULL DEFAULT FALSE,
      advantage BOOL NOT NULL DEFAULT FALSE,
      defensive BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS duels (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT,
      type_id SMALLINT NOT NULL,
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id),
      FOREIGN KEY (type_id) REFERENCES types(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS clearances (
      event_id TEXT NOT NULL PRIMARY KEY,
      body_part_id SMALLINT NOT NULL,
      aerial_won BOOL NOT NULL DEFAULT FALSE,
      left_foot BOOL NOT NULL DEFAULT FALSE,
      right_foot BOOL NOT NULL DEFAULT FALSE,
      head BOOL NOT NULL DEFAULT FALSE,
      other BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (body_part_id) REFERENCES body_parts(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS injury_stoppages (
      event_id TEXT NOT NULL PRIMARY KEY,
      in_chain BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS bad_behaviors (
      event_id TEXT NOT NULL PRIMARY KEY,
      card_id SMALLINT NOT NULL,
      FOREIGN KEY (card_id) REFERENCES cards(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS substitutions (
      event_id TEXT NOT NULL PRIMARY KEY,
      replacement_id INT NOT NULL,
      outcome_id SMALLINT NOT NULL,
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id),
      FOREIGN KEY (replacement_id) REFERENCES players(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS ball_receipts (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT,
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS fifty_fifties (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT NOT NULL,
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.cur.execute("""CREATE TABLE IF NOT EXISTS goalkeepers (
      event_id TEXT NOT NULL PRIMARY KEY,
      outcome_id SMALLINT,
      shot_saved_off_target BOOL NOT NULL DEFAULT FALSE,
      position_id SMALLINT,
      body_part_id SMALLINT,
      shot_saved_to_post BOOL NOT NULL DEFAULT FALSE,
      technique_id SMALLINT,
      lost_out BOOL NOT NULL DEFAULT FALSE,
      lost_in_play BOOL NOT NULL DEFAULT FALSE,
      success_in_play BOOL NOT NULL DEFAULT FALSE,
      type TEXT NOT NULL,
      end_location TEXT,
      punched_out BOOL NOT NULL DEFAULT FALSE,
      FOREIGN KEY (outcome_id) REFERENCES outcomes(id),
      FOREIGN KEY (position_id) REFERENCES positions(id),
      FOREIGN KEY (body_part_id) REFERENCES body_parts(id),
      FOREIGN KEY (technique_id) REFERENCES techniques(id),
      FOREIGN KEY (event_id) REFERENCES events(id));"""
    )
    
    self.conn.commit()
    
  def __optimize_tables(self):
    # Create indices on foreign keys
    self.cur.execute("""
      CREATE INDEX fk_teams_managers ON teams(manager_id);
      
      CREATE INDEX fk_lineups_matches ON lineups(match_id);
      CREATE INDEX fk_lineups_teams ON lineups(team_id);

      CREATE INDEX fk_tactics_events ON tactics(event_id);

      CREATE INDEX fk_matches_competitions ON matches(competition_id, season_id);
      CREATE INDEX fk_matches_home_teams ON matches(home_team_id);
      CREATE INDEX fk_matches_away_teams ON matches(away_team_id);

      CREATE INDEX fk_events_types ON events(type_id);
      CREATE INDEX fk_events_play_patterns ON events(play_pattern_id);
      CREATE INDEX fk_events_positions ON events(position_id);
      CREATE INDEX fk_events_possession_teams ON events(possession_team_id);
      CREATE INDEX fk_events_teams ON events(team_id);
      CREATE INDEX fk_events_players ON events(player_id);
      CREATE INDEX fk_events_matches ON events(match_id);

      CREATE INDEX fk_passes_recipients ON passes(recipient_id);
      CREATE INDEX fk_passes_events ON passes(event_id);
      CREATE INDEX fk_passes_heights ON passes(height_id);
      CREATE INDEX fk_passes_types ON passes(type_id);
      CREATE INDEX fk_passes_body_parts ON passes(body_part_id);
      CREATE INDEX fk_passes_techniques ON passes(technique_id);

      CREATE INDEX fk_interceptions_events ON interceptions(event_id);
      CREATE INDEX fk_interceptions_outcomes ON interceptions(outcome_id);
      
      CREATE INDEX fk_dribbles_events ON dribbles(event_id);
      CREATE INDEX fk_dribbles_outcomes ON dribbles(outcome_id);
      
      CREATE INDEX fk_shots_events ON shots(event_id);
      CREATE INDEX fk_shots_techniques ON shots(technique_id);
      CREATE INDEX fk_shots_body_parts ON shots(body_part_id);
      CREATE INDEX fk_shots_types ON shots(type_id);
      CREATE INDEX fk_shots_outcomes ON shots(outcome_id);
      
      CREATE INDEX fk_fouls_committed_events ON fouls_committed(event_id);
      CREATE INDEX fk_fouls_committed_cards ON fouls_committed(card_id);
      CREATE INDEX fk_fouls_committed_types ON fouls_committed(type_id);
      
      CREATE INDEX fk_fouls_won_events ON fouls_won(event_id);
      
      CREATE INDEX fk_duels_events ON duels(event_id);
      CREATE INDEX fk_duels_outcomes ON duels(outcome_id);
      CREATE INDEX fk_duels_types ON duels(type_id);
      
      CREATE INDEX fk_clearances_events ON clearances(event_id);
      CREATE INDEX fk_clearances_body_parts ON clearances(body_part_id);
      
      CREATE INDEX fk_bad_behaviors_events ON bad_behaviors(event_id);
      CREATE INDEX fk_bad_behaviors_cards ON bad_behaviors(card_id);
      
      CREATE INDEX fk_substitutions_events ON substitutions(event_id);
      CREATE INDEX fk_substitutions_outcomes ON substitutions(outcome_id);
      CREATE INDEX fk_substitutions_replacements ON substitutions(replacement_id);
      
      CREATE INDEX fk_ball_receipts_events ON ball_receipts(event_id);
      CREATE INDEX fk_ball_receipts_outcomes ON ball_receipts(outcome_id);
      
      CREATE INDEX fk_fifty_fifties_events ON fifty_fifties(event_id);
      CREATE INDEX fk_fifty_fifties_outcomes ON fifty_fifties(outcome_id);
      
      CREATE INDEX fk_goalkeepers_events ON goalkeepers(event_id);
      CREATE INDEX fk_goalkeepers_outcomes ON goalkeepers(outcome_id);
      CREATE INDEX fk_goalkeepers_body_parts ON goalkeepers(body_part_id);
      CREATE INDEX fk_goalkeepers_techniques ON goalkeepers(technique_id);
      CREATE INDEX fk_goalkeepers_positions ON goalkeepers(position_id);
      
      CREATE INDEX fk_half_starts_events ON half_starts(event_id);
      CREATE INDEX fk_carries_events ON carries(event_id);
      CREATE INDEX fk_ball_recoveries_events ON ball_recoveries(event_id);
      CREATE INDEX fk_blocks_events ON blocks(event_id);
      CREATE INDEX fk_miscontrols_events ON miscontrols(event_id);
      CREATE INDEX fk_injury_stoppages_events ON injury_stoppages(event_id);
      """)
    
    # Create other indices
    self.cur.execute("""
      -- General
      CREATE INDEX idx_competitions_comeptition_name ON competitions(competition_name);
      CREATE INDEX idx_competitions_season_name ON competitions(season_name);
      
      CREATE INDEX idx_players_name ON players(name);
      
      -- Q1
      CREATE INDEX idx_shots_xg ON shots(xg);
      
      -- Q3
      CREATE INDEX idx_shots_first_time_true_only ON shots(first_time) WHERE first_time = TRUE;
      
      -- Q4
      CREATE INDEX idx_teams_team_name ON teams(team_name);
      
      -- Q7 & Q8
      CREATE INDEX idx_passes_through_ball_true_only ON passes(through_ball) WHERE through_ball = TRUE;
      
      """)
    
    self.conn.commit()
